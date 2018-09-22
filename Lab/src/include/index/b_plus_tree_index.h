/**
 * b_plus_tree_index.h
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "index/b_plus_tree.h"
#include "index/index.h"

namespace cmudb {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex : public Index {

public:
  BPlusTreeIndex(IndexMetadata *metadata,
                 BufferPoolManager *buffer_pool_manager,
                 page_id_t root_page_id = INVALID_PAGE_ID);

  ~BPlusTreeIndex() {}

  void InsertEntry(const Tuple &key, RID rid,
                   Transaction *transaction = nullptr) override;

  void DeleteEntry(const Tuple &key,
                   Transaction *transaction = nullptr) override;

  void ScanKey(const Tuple &key, std::vector<RID> &result,
               Transaction *transaction = nullptr) override;

protected:
  // comparator for key
  KeyComparator comparator_;
  // container
  BPlusTree<KeyType, ValueType, KeyComparator> container_;
};

} // namespace cmudb
