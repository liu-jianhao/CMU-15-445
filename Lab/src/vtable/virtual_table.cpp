/**
 * virtual_table.cpp
 */
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/string_utility.h"
#include "page/header_page.h"
#include "vtable/virtual_table.h"

namespace cmudb {

SQLITE_EXTENSION_INIT1

/* API implementation */
int VtabCreate(sqlite3 *db, void *pAux, int argc, const char *const *argv,
               sqlite3_vtab **ppVtab, char **pzErr) {
  BufferPoolManager *buffer_pool_manager =
      storage_engine_->buffer_pool_manager_;
  LockManager *lock_manager = storage_engine_->lock_manager_;
  LogManager *log_manager = storage_engine_->log_manager_;

  // fetch header page from buffer pool
  HeaderPage *header_page =
      static_cast<HeaderPage *>(buffer_pool_manager->FetchPage(HEADER_PAGE_ID));

  // the first three parameter:(1) module name (2) database name (3)table name
  assert(argc >= 4);
  // parse arg[3](string that defines table schema)
  std::string schema_string(argv[3]);
  schema_string = schema_string.substr(1, (schema_string.size() - 2));
  Schema *schema = ParseCreateStatement(schema_string);

  // parse arg[4](string that defines table index)
  Index *index = nullptr;
  if (argc > 4) {
    std::string index_string(argv[4]);
    index_string = index_string.substr(1, (index_string.size() - 2));
    // create index object, allocate memory space
    IndexMetadata *index_metadata =
        ParseIndexStatement(index_string, std::string(argv[2]), schema);
    index = ConstructIndex(index_metadata, buffer_pool_manager);
  }
  // create table object, allocate memory space
  VirtualTable *table = new VirtualTable(schema, buffer_pool_manager,
                                         lock_manager, log_manager, index);

  // insert table root page info into header page
  header_page->InsertRecord(std::string(argv[2]), table->GetFirstPageId());
  buffer_pool_manager->UnpinPage(HEADER_PAGE_ID, true);

  // register virtual table within sqlite system
  schema_string = "CREATE TABLE X(" + schema_string + ");";
  assert(sqlite3_declare_vtab(db, schema_string.c_str()) == SQLITE_OK);

  *ppVtab = reinterpret_cast<sqlite3_vtab *>(table);
  return SQLITE_OK;
}

int VtabConnect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                sqlite3_vtab **ppVtab, char **pzErr) {
  assert(argc >= 4);
  std::string schema_string(argv[3]);
  // remove the very first and last character
  schema_string = schema_string.substr(1, (schema_string.size() - 2));
  // new virtual table object, allocate memory space
  Schema *schema = ParseCreateStatement(schema_string);

  BufferPoolManager *buffer_pool_manager =
      storage_engine_->buffer_pool_manager_;
  LockManager *lock_manager = storage_engine_->lock_manager_;
  LogManager *log_manager = storage_engine_->log_manager_;

  // Retrieve table root page info from header page
  HeaderPage *header_page =
      static_cast<HeaderPage *>(buffer_pool_manager->FetchPage(HEADER_PAGE_ID));
  page_id_t table_root_id;
  header_page->GetRootId(std::string(argv[2]), table_root_id);
  // parse arg[4](string that defines table index)
  Index *index = nullptr;
  if (argc > 4) {
    std::string index_string(argv[4]);
    index_string = index_string.substr(1, (index_string.size() - 2));
    // create index object, allocate memory space
    IndexMetadata *index_metadata =
        ParseIndexStatement(index_string, std::string(argv[2]), schema);
    // Retrieve index root page info from header page
    page_id_t index_root_id;
    header_page->GetRootId(index_metadata->GetName(), index_root_id);
    index = ConstructIndex(index_metadata, buffer_pool_manager, index_root_id);
  }
  VirtualTable *table =
      new VirtualTable(schema, buffer_pool_manager, lock_manager, log_manager,
                       index, table_root_id);

  // register virtual table within sqlite system
  schema_string = "CREATE TABLE X(" + schema_string + ");";
  assert(sqlite3_declare_vtab(db, schema_string.c_str()) == SQLITE_OK);

  *ppVtab = reinterpret_cast<sqlite3_vtab *>(table);
  buffer_pool_manager->UnpinPage(HEADER_PAGE_ID, false);
  return SQLITE_OK;
}

/*
 * we only support
 * (1) equlity check. e.g select * from foo where a = 1
 * (2) indexed column == predicated column
 */
int VtabBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo) {
  // LOG_DEBUG("VtabBestIndex");
  VirtualTable *table = reinterpret_cast<VirtualTable *>(tab);
  if (table->GetIndex() == nullptr)
    return SQLITE_OK;
  const std::vector<int> key_attrs = table->GetIndex()->GetKeyAttrs();
  // make sure indexed column == predicate column
  // e.g select * from foo where a = 1 and b =2; indexed column must be {a,b}
  if (pIdxInfo->nConstraint != (int)(key_attrs.size()))
    return SQLITE_OK;

  int counter = 0;
  bool is_index_scan = true;
  for (int i = 0; i < pIdxInfo->nConstraint; i++) {
    if (pIdxInfo->aConstraint[i].usable == 0)
      continue;
    int item = pIdxInfo->aConstraint[i].iColumn;
    // if predicate column is part of indexed column
    if (std::find(key_attrs.begin(), key_attrs.end(), item) !=
        key_attrs.end()) {
      // equlity check
      if (pIdxInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) {
        is_index_scan = false;
        break;
      }
      pIdxInfo->aConstraintUsage[i].argvIndex = (i + 1);
      counter++;
    }
  }

  if (counter == (int)key_attrs.size() && is_index_scan) {
    pIdxInfo->idxNum = 1;
  }
  return SQLITE_OK;
}

int VtabDisconnect(sqlite3_vtab *pVtab) {
  VirtualTable *virtual_table = reinterpret_cast<VirtualTable *>(pVtab);
  delete virtual_table;
  // delete all the global managers
  delete storage_engine_;
  return SQLITE_OK;
}

int VtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  // LOG_DEBUG("VtabOpen");
  // if read operation, begin transaction here
  if (global_transaction_ == nullptr) {
    VtabBegin(pVtab);
  }
  VirtualTable *virtual_table = reinterpret_cast<VirtualTable *>(pVtab);
  Cursor *cursor = new Cursor(virtual_table);
  *ppCursor = reinterpret_cast<sqlite3_vtab_cursor *>(cursor);

  return SQLITE_OK;
}

int VtabClose(sqlite3_vtab_cursor *cur) {
  // LOG_DEBUG("VtabClose");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  // if read operation, commit transaction here
  VtabCommit(nullptr);
  delete cursor;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the cursor object back
** to the first row of output. This method is always called at least
** once prior to any call to VtabColumn() or VtabRowid() or
** VtabEof().
*/
int VtabFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum, const char *idxStr,
               int argc, sqlite3_value **argv) {
  // LOG_DEBUG("VtabFilter");
  Cursor *cursor = reinterpret_cast<Cursor *>(pVtabCursor);
  Schema *key_schema;
  // if indexed scan
  if (idxNum == 1) {
    cursor->SetScanFlag(true);
    // Construct the tuple for point query
    key_schema = cursor->GetKeySchema();
    Tuple scan_tuple = ConstructTuple(key_schema, argv);
    cursor->ScanKey(scan_tuple);
  }
  return SQLITE_OK;
}

int VtabNext(sqlite3_vtab_cursor *cur) {
  // LOG_DEBUG("VtabNext");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  ++(*cursor);
  return SQLITE_OK;
}

int VtabEof(sqlite3_vtab_cursor *cur) {
  // LOG_DEBUG("VtabEof");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  return cursor->isEof();
}

int VtabColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i) {
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  Schema *schema = cursor->GetVirtualTable()->GetSchema();
  // get column type and value
  TypeId type = schema->GetType(i);
  Value v = cursor->GetCurrentValue(schema, i);

  switch (type) {
  case TypeId::TINYINT:
  case TypeId::BOOLEAN:
    sqlite3_result_int(ctx, (int)v.GetAs<int8_t>());
    break;
  case TypeId::SMALLINT:
    sqlite3_result_int(ctx, (int)v.GetAs<int16_t>());
    break;
  case TypeId::INTEGER:
    sqlite3_result_int(ctx, (int)v.GetAs<int32_t>());
    break;
  case TypeId::BIGINT:
    sqlite3_result_int64(ctx, (sqlite3_int64)v.GetAs<int64_t>());
    break;
  case TypeId::DECIMAL:
    sqlite3_result_double(ctx, v.GetAs<double>());
    break;
  case TypeId::VARCHAR:
    sqlite3_result_text(ctx, v.GetData(), -1, SQLITE_TRANSIENT);
    break;
  default:
    return SQLITE_ERROR;
  } // End of switch
  return SQLITE_OK;
}

int VtabRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
  // LOG_DEBUG("VtabRowid");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  // return rid of the tuple that cursor currently points at
  *pRowid = cursor->GetCurrentRid();
  return SQLITE_OK;
}

int VtabUpdate(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv,
               sqlite_int64 *pRowid) {
  // LOG_DEBUG("VtabUpdate");
  VirtualTable *table = reinterpret_cast<VirtualTable *>(pVTab);
  // The single row with rowid equal to argv[0] is deleted
  if (argc == 1) {
    const RID rid(sqlite3_value_int64(argv[0]));
    // delete entry from index
    table->DeleteEntry(rid);
    // delete tuple from table heap
    table->DeleteTuple(rid);
  }
  // A new row is inserted with a rowid argv[1] and column values in argv[2] and
  // following. If argv[1] is an SQL NULL, the a new unique rowid is generated
  // automatically.
  else if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    Schema *schema = table->GetSchema();
    Tuple tuple = ConstructTuple(schema, (argv + 2));
    // insert into table heap
    RID rid;
    table->InsertTuple(tuple, rid);
    // insert into index
    table->InsertEntry(tuple, rid);
  }
  // The row with rowid argv[0] is updated with new values in argv[2] and
  // following parameters.
  else if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    Schema *schema = table->GetSchema();
    Tuple tuple = ConstructTuple(schema, (argv + 2));
    RID rid(sqlite3_value_int64(argv[0]));
    // for update, index always delete and insert
    // because you have no clue key has been updated or not
    table->DeleteEntry(rid);
    // if true, then update succeed, rid keep the same
    // else, delete & insert
    if (table->UpdateTuple(tuple, rid) == false) {
      table->DeleteTuple(rid);
      // rid should be different
      table->InsertTuple(tuple, rid);
    }
    table->InsertEntry(tuple, rid);
  }
  return SQLITE_OK;
}

int VtabBegin(sqlite3_vtab *pVTab) {
  // LOG_DEBUG("VtabBegin");
  // create new transaction(write operation will call this method)
  global_transaction_ = storage_engine_->transaction_manager_->Begin();
  return SQLITE_OK;
}

int VtabCommit(sqlite3_vtab *pVTab) {
  // LOG_DEBUG("VtabCommit");
  auto transaction = GetTransaction();
  if (transaction == nullptr)
    return SQLITE_OK;
  // get global txn manager
  auto transaction_manager = storage_engine_->transaction_manager_;
  // invoke transaction manager to commit(this txn can't fail)
  transaction_manager->Commit(transaction);
  // when commit, delete transaction pointer and set to null
  delete transaction;
  global_transaction_ = nullptr;

  return SQLITE_OK;
}

sqlite3_module VtableModule = {
    0,              /* iVersion */
    VtabCreate,     /* xCreate */
    VtabConnect,    /* xConnect */
    VtabBestIndex,  /* xBestIndex */
    VtabDisconnect, /* xDisconnect */
    VtabDisconnect, /* xDestroy */
    VtabOpen,       /* xOpen - open a cursor */
    VtabClose,      /* xClose - close a cursor */
    VtabFilter,     /* xFilter - configure scan constraints */
    VtabNext,       /* xNext - advance a cursor */
    VtabEof,        /* xEof - check for end of scan */
    VtabColumn,     /* xColumn - read data */
    VtabRowid,      /* xRowid - read data */
    VtabUpdate,     /* xUpdate */
    VtabBegin,      /* xBegin */
    0,              /* xSync */
    VtabCommit,     /* xCommit */
    0,              /* xRollback */
    0,              /* xFindMethod */
    0,              /* xRename */
    0,              /* xSavepoint */
    0,              /* xRelease */
    0,              /* xRollbackTo */
};

#ifdef _WIN32
__declspec(dllexport)
#endif
    extern "C" int sqlite3_vtable_init(sqlite3 *db, char **pzErrMsg,
                                       const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);
  std::string db_file_name = "vtable.db";
  struct stat buffer;
  bool is_file_exist = (stat(db_file_name.c_str(), &buffer) == 0);

  // init storage engine
  storage_engine_ = new StorageEngine(db_file_name);
  // start the logging
  storage_engine_->log_manager_->RunFlushThread();
  // create header page from BufferPoolManager if necessary
  if (!is_file_exist) {
    page_id_t header_page_id;
    storage_engine_->buffer_pool_manager_->NewPage(header_page_id);

    assert(header_page_id == HEADER_PAGE_ID);
    storage_engine_->buffer_pool_manager_->UnpinPage(header_page_id, true);
  }

  int rc = sqlite3_create_module(db, "vtable", &VtableModule, nullptr);
  return rc;
}

/* Helpers */
Schema *ParseCreateStatement(const std::string &sql_base) {
  std::string::size_type n;
  std::vector<Column> v;
  std::string column_name;
  std::string column_type;
  int column_length = 0;
  TypeId type = INVALID;
  // create a copy of the sql query
  std::string sql = sql_base;
  // prepocess, transform sql string into lower case
  std::transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
  std::vector<std::string> tok = StringUtility::Split(sql, ',');
  // iterate through returned result
  for (std::string &t : tok) {
    type = INVALID;
    column_length = 0;
    // whitespace seperate column name and type
    n = t.find_first_of(' ');
    column_name = t.substr(0, n);
    column_type = t.substr(n + 1);
    // deal with varchar(size) situation
    n = column_type.find_first_of('(');
    if (n != std::string::npos) {
      column_length = std::stoi(column_type.substr(n + 1));
      column_type = column_type.substr(0, n);
    }
    if (column_type == "bool" || column_type == "boolean") {
      type = BOOLEAN;
    } else if (column_type == "tinyint") {
      type = TINYINT;
    } else if (column_type == "smallint") {
      type = SMALLINT;
    } else if (column_type == "int" || column_type == "integer") {
      type = INTEGER;
    } else if (column_type == "bigint") {
      type = BIGINT;
    } else if (column_type == "double" || column_type == "float") {
      type = DECIMAL;
    } else if (column_type == "varchar" || column_type == "char") {
      type = VARCHAR;
      column_length = (column_length == 0) ? 32 : column_length;
    }
    // construct each column
    if (type == INVALID) {
      throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE,
                      "unknown type for create table");
    } else if (type == VARCHAR) {
      Column col(type, column_length, column_name);
      v.emplace_back(col);
    } else {
      Column col(type, Type::GetTypeSize(type), column_name);
      v.emplace_back(col);
    }
  }
  Schema *schema = new Schema(v);
  // LOG_DEBUG("%s", schema->ToString().c_str());

  return schema;
}

IndexMetadata *ParseIndexStatement(std::string &sql,
                                   const std::string &table_name,
                                   Schema *schema) {
  std::string::size_type n;
  std::string index_name;
  std::vector<int> key_attrs;
  int column_id = -1;
  // prepocess, transform sql string into lower case
  std::transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
  n = sql.find_first_of(' ');
  // NOTE: must use whitespace to seperate index name and indexed column names
  assert(n != std::string::npos);
  index_name = sql.substr(0, n);
  sql = sql.substr(n + 1);

  std::vector<std::string> tok = StringUtility::Split(sql, ',');
  // iterate through returned result
  for (std::string &t : tok) {
    StringUtility::Trim(t);
    column_id = schema->GetColumnID(t);
    if (column_id != -1)
      key_attrs.emplace_back(column_id);
  }
  if ((int)key_attrs.size() > schema->GetColumnCount())
    throw Exception(EXCEPTION_TYPE_INDEX, "can't create index, format error");

  IndexMetadata *metadata =
      new IndexMetadata(index_name, table_name, schema, key_attrs);

  // LOG_DEBUG("%s", metadata->ToString().c_str());
  return metadata;
}

Tuple ConstructTuple(Schema *schema, sqlite3_value **argv) {
  int column_count = schema->GetColumnCount();
  Value v(TypeId::INVALID);
  std::vector<Value> values;
  // iterate through schema, generate column value to insert
  for (int i = 0; i < column_count; i++) {
    TypeId type = schema->GetType(i);

    switch (type) {
    case TypeId::BOOLEAN:
    case TypeId::INTEGER:
    case TypeId::SMALLINT:
    case TypeId::TINYINT:
      v = Value(type, (int32_t)sqlite3_value_int(argv[i]));
      break;
    case TypeId::BIGINT:
      v = Value(type, (int64_t)sqlite3_value_int64(argv[i]));
      break;
    case TypeId::DECIMAL:
      v = Value(type, sqlite3_value_double(argv[i]));
      break;
    case TypeId::VARCHAR:
      v = Value(type, std::string(reinterpret_cast<const char *>(
                          sqlite3_value_text(argv[i]))));
      break;
    default:
      break;
    } // End of switch
    values.emplace_back(v);
  }
  Tuple tuple(values, schema);

  return tuple;
}

// serve the functionality of index factory
Index *ConstructIndex(IndexMetadata *metadata,
                      BufferPoolManager *buffer_pool_manager,
                      page_id_t root_id) {
  // The size of the key in bytes
  Schema *key_schema = metadata->GetKeySchema();
  int key_size = key_schema->GetLength();
  // for each varchar attribute, we assume the largest size is 16 bytes
  key_size += 16 * key_schema->GetUnlinedColumnCount();

  if (key_size <= 4) {
    return new BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>(
        metadata, buffer_pool_manager, root_id);
  } else if (key_size <= 8) {
    return new BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>(
        metadata, buffer_pool_manager, root_id);
  } else if (key_size <= 16) {
    return new BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>(
        metadata, buffer_pool_manager, root_id);
  } else if (key_size <= 32) {
    return new BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>(
        metadata, buffer_pool_manager, root_id);
  } else {
    return new BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>(
        metadata, buffer_pool_manager, root_id);
  }
}

Transaction *GetTransaction() { return global_transaction_; }

} // namespace cmudb
