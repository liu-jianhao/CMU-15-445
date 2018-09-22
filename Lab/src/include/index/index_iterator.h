/**
 * index_iterator.h
 * For range scan of b+ tree
 */

#pragma once

#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *,
                int, BufferPoolManager *);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  int index_;
  BufferPoolManager *buff_pool_manager_;
};

} // namespace cmudb
