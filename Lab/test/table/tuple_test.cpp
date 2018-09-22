/**
 * tuple_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "logging/common.h"
#include "table/table_heap.h"
#include "table/tuple.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {
TEST(TupleTest, TableHeapTest) {
  // test1: parse create sql statement
  std::string createStmt =
      "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);
  Tuple tuple = ConstructTuple(schema);

  // create transaction
  Transaction *transaction = new Transaction(0);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *buffer_pool_manager =
      new BufferPoolManager(50, disk_manager);
  LockManager *lock_manager = new LockManager(true);
  LogManager *log_manager = new LogManager(disk_manager);
  TableHeap *table = new TableHeap(buffer_pool_manager, lock_manager,
                                   log_manager, transaction);

  RID rid;
  std::vector<RID> rid_v;
  for (int i = 0; i < 5000; ++i) {
    table->InsertTuple(tuple, rid, transaction);
    // std::cout << rid << '\n';
    rid_v.push_back(rid);
  }

  TableIterator itr = table->begin(transaction);
  while (itr != table->end()) {
    // std::cout << itr->ToString(schema) << std::endl;
    ++itr;
  }

  // int i = 0;
  std::random_shuffle(rid_v.begin(), rid_v.end());
  for (auto rid : rid_v) {
    // std::cout << i++ << std::endl;
    assert(table->MarkDelete(rid, transaction) == 1);
  }
  remove("test.db"); // remove db file
  remove("test.log");
  delete schema;
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
}

} // namespace cmudb
