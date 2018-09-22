/**
 * virtual_table.h
 */

#pragma once

#include "buffer/lru_replacer.h"
#include "catalog/schema.h"
#include "concurrency/transaction_manager.h"
#include "index/b_plus_tree_index.h"
#include "logging/log_manager.h"
#include "sqlite/sqlite3ext.h"
#include "table/table_heap.h"
#include "table/tuple.h"
#include "type/value.h"

namespace cmudb {
/* Helpers */
Schema *ParseCreateStatement(const std::string &sql);

IndexMetadata *ParseIndexStatement(std::string &sql,
                                   const std::string &table_name,
                                   Schema *schema);

Tuple ConstructTuple(Schema *schema, sqlite3_value **argv);

Index *ConstructIndex(IndexMetadata *metadata,
                      BufferPoolManager *buffer_pool_manager,
                      page_id_t root_id = INVALID_PAGE_ID);
Transaction *GetTransaction();

/* API declaration */
int VtabCreate(sqlite3 *db, void *pAux, int argc, const char *const *argv,
               sqlite3_vtab **ppVtab, char **pzErr);

int VtabConnect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                sqlite3_vtab **ppVtab, char **pzErr);

int VtabBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo);

int VtabDisconnect(sqlite3_vtab *pVtab);

int VtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor);

int VtabClose(sqlite3_vtab_cursor *cur);

int VtabFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum, const char *idxStr,
               int argc, sqlite3_value **argv);

int VtabNext(sqlite3_vtab_cursor *cur);

int VtabEof(sqlite3_vtab_cursor *cur);

int VtabColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i);

int VtabRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid);

int VtabCommit(sqlite3_vtab *pVTab);

int VtabBegin(sqlite3_vtab *pVTab);

// storage engine
class StorageEngine {
public:
  StorageEngine(std::string db_file_name) {
    ENABLE_LOGGING = false;

    // storage related
    disk_manager_ = new DiskManager(db_file_name);

    // log related
    log_manager_ = new LogManager(disk_manager_);

    buffer_pool_manager_ =
        new BufferPoolManager(BUFFER_POOL_SIZE, disk_manager_, log_manager_);

    // txn related
    lock_manager_ = new LockManager(true); // S2PL
    transaction_manager_ = new TransactionManager(lock_manager_, log_manager_);
  }

  ~StorageEngine() {
    if (ENABLE_LOGGING)
      log_manager_->StopFlushThread();
    delete disk_manager_;
    delete buffer_pool_manager_;
    delete log_manager_;
    delete lock_manager_;
    delete transaction_manager_;
  }

  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  LockManager *lock_manager_;
  TransactionManager *transaction_manager_;
  LogManager *log_manager_;
};

StorageEngine *storage_engine_;
// global transaction, sqlite does not support concurrent transaction
Transaction *global_transaction_ = nullptr;

class VirtualTable {
  friend class Cursor;

public:
  VirtualTable(Schema *schema, BufferPoolManager *buffer_pool_manager,
               LockManager *lock_manager, LogManager *log_manager, Index *index,
               page_id_t first_page_id = INVALID_PAGE_ID)
      : schema_(schema), index_(index) {
    if (first_page_id != INVALID_PAGE_ID) {
      // reopen an exist table
      table_heap_ = new TableHeap(buffer_pool_manager, lock_manager,
                                  log_manager, first_page_id);
    } else {
      // create table for the first time
      Transaction *txn = storage_engine_->transaction_manager_->Begin();
      table_heap_ =
          new TableHeap(buffer_pool_manager, lock_manager, log_manager, txn);
      storage_engine_->transaction_manager_->Commit(txn);
    }
  }

  ~VirtualTable() {
    delete schema_;
    delete table_heap_;
    delete index_;
  }

  // insert into table heap
  inline bool InsertTuple(const Tuple &tuple, RID &rid) {
    return table_heap_->InsertTuple(tuple, rid, GetTransaction());
  }

  // insert into index
  inline void InsertEntry(const Tuple &tuple, const RID &rid) {
    if (index_ == nullptr)
      return;
    // construct indexed key tuple
    std::vector<Value> key_values;

    for (auto &i : index_->GetKeyAttrs())
      key_values.push_back(tuple.GetValue(schema_, i));
    Tuple key(key_values, index_->GetKeySchema());
    index_->InsertEntry(key, rid, GetTransaction());
  }

  // delete from table heap
  // TODO: call makrdelete method from heaptable
  inline bool DeleteTuple(const RID &rid) {
    return table_heap_->MarkDelete(rid, GetTransaction());
  }

  // delete from index
  inline void DeleteEntry(const RID &rid) {
    if (index_ == nullptr)
      return;
    Tuple deleted_tuple(rid);
    table_heap_->GetTuple(rid, deleted_tuple, GetTransaction());
    // construct indexed key tuple
    std::vector<Value> key_values;

    for (auto &i : index_->GetKeyAttrs())
      key_values.push_back(deleted_tuple.GetValue(schema_, i));
    Tuple key(key_values, index_->GetKeySchema());
    index_->DeleteEntry(key, GetTransaction());
  }

  // update table heap tuple
  inline bool UpdateTuple(const Tuple &tuple, const RID &rid) {
    // if failed try to delete and insert
    return table_heap_->UpdateTuple(tuple, rid, GetTransaction());
  }

  inline TableIterator begin() { return table_heap_->begin(GetTransaction()); }

  inline TableIterator end() { return table_heap_->end(); }

  inline Schema *GetSchema() { return schema_; }

  inline Index *GetIndex() { return index_; }

  inline TableHeap *GetTableHeap() { return table_heap_; }

  inline page_id_t GetFirstPageId() { return table_heap_->GetFirstPageId(); }

private:
  sqlite3_vtab base_;
  // virtual table schema
  Schema *schema_;
  // to read/write actual data in table
  TableHeap *table_heap_;
  // to insert/delete index entry
  Index *index_ = nullptr;
};

class Cursor {
public:
  Cursor(VirtualTable *virtual_table)
      : table_iterator_(virtual_table->begin()), virtual_table_(virtual_table) {
  }

  inline void SetScanFlag(bool is_index_scan) {
    is_index_scan_ = is_index_scan;
  }

  inline bool IsIndexScan() { return is_index_scan_; }

  inline VirtualTable *GetVirtualTable() { return virtual_table_; }

  inline Schema *GetKeySchema() {
    return virtual_table_->index_->GetKeySchema();
  }
  // return rid at which cursor is currently pointed
  inline int64_t GetCurrentRid() {
    if (is_index_scan_)
      return results[offset_].Get();
    else
      return (*table_iterator_).GetRid().Get();
  }

  // return tuple at which cursor is currently pointed
  inline Value GetCurrentValue(Schema *schema, int column) {
    if (is_index_scan_) {
      RID rid = results[offset_];
      Tuple tuple(rid);
      virtual_table_->table_heap_->GetTuple(rid, tuple, GetTransaction());
      return tuple.GetValue(schema, column);
    } else {
      return table_iterator_->GetValue(schema, column);
    }
  }

  // move cursor up to next
  Cursor &operator++() {
    if (is_index_scan_)
      ++offset_;
    else
      ++table_iterator_;
    return *this;
  }
  // is end of cursor(no more tuple)
  inline bool isEof() {
    if (is_index_scan_)
      return offset_ == static_cast<int>(results.size());
    else
      return table_iterator_ == virtual_table_->end();
  }

  // wrapper around poit scan methods
  inline void ScanKey(const Tuple &key) {
    virtual_table_->index_->ScanKey(key, results);
  }

private:
  sqlite3_vtab_cursor base_; /* Base class - must be first */
  // for index scan
  std::vector<RID> results;
  int offset_ = 0;
  // for sequential scan
  TableIterator table_iterator_;
  // flag to indicate which scan method is currently used
  bool is_index_scan_ = false;
  VirtualTable *virtual_table_;
}; // namespace cmudb

} // namespace cmudb