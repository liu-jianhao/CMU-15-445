/**
 * table_heap.h
 *
 * doubly-linked list of heap pages
 */

#pragma once

#include "buffer/buffer_pool_manager.h"
#include "logging/log_manager.h"
#include "page/table_page.h"
#include "table/table_iterator.h"
#include "table/tuple.h"

namespace cmudb {

class TableHeap {
  friend class TableIterator;

public:
  ~TableHeap() {}

  // open a table heap
  TableHeap(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
            LogManager *log_manager, page_id_t first_page_id);

  // create table heap
  TableHeap(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
            LogManager *log_manager, Transaction *txn);

  // for insert, if tuple is too large (>~page_size), return false
  bool InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn);

  bool MarkDelete(const RID &rid, Transaction *txn); // for delete

  // if the new tuple is too large to fit in the old page, return false (will
  // delete and insert)
  bool UpdateTuple(const Tuple &tuple, const RID &rid, Transaction *txn);

  // commit/abort time
  void ApplyDelete(const RID &rid,
                   Transaction *txn); // when commit delete or rollback insert
  void RollbackDelete(const RID &rid, Transaction *txn); // when rollback delete

  bool GetTuple(const RID &rid, Tuple &tuple, Transaction *txn);

  bool DeleteTableHeap();

  TableIterator begin(Transaction *txn);

  TableIterator end();

  inline page_id_t GetFirstPageId() const { return first_page_id_; }

private:
  /**
   * Members
   */
  BufferPoolManager *buffer_pool_manager_;
  LockManager *lock_manager_;
  LogManager *log_manager_;
  page_id_t first_page_id_;
};

} // namespace cmudb
