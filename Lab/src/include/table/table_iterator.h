/**
 * table_iterator.h
 *
 * For seq scan of table heap
 */

#pragma once

#include <cassert>

#include "common/rid.h"
#include "table/tuple.h"

namespace cmudb {

class TableHeap;

class TableIterator {
  friend class Cursor;

public:
  TableIterator(TableHeap *table_heap, RID rid, Transaction *txn);

  ~TableIterator() { delete tuple_; }

  inline bool operator==(const TableIterator &itr) const {
    return tuple_->rid_.Get() == itr.tuple_->rid_.Get();
  }

  inline bool operator!=(const TableIterator &itr) const {
    return !(*this == itr);
  }

  const Tuple &operator*();

  Tuple *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  TableHeap *table_heap_;
  Tuple *tuple_;
  Transaction *txn_;
};

} // namespace cmudb