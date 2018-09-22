/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
Init(page_id_t page_id, page_id_t parent_id) {
// set page type
  SetPageType(IndexPageType::LEAF_PAGE);
  // set current size: 1 for the first invalid key
  SetSize(0);
  // set page id
  SetPageId(page_id);
  // set parent id
  SetParentPageId(parent_id);
  // set next page id
  SetNextPageId(INVALID_PAGE_ID);

  // set max page size, header is 28bytes
  int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage))/
      (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}

/**
 * Helper methods to set/get next page id
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
GetNextPageId() const {
  return next_page_id_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
const MappingType &BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
GetItem(int index) {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
Insert(const KeyType &key, const ValueType &value,
       const KeyComparator &comparator) {
  // empty or bigger than last value in the page
  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array[GetSize()] = {key, value};
  } else if (comparator(key, array[0].first) < 0) {
    memmove(array + 1, array, static_cast<size_t>(GetSize()*sizeof(MappingType)));
    array[0] = {key, value};
  } else {
    int low = 0, high = GetSize() - 1, mid;
    while (low < high && low + 1 != high) {
      mid = low + (high - low)/2;
      if (comparator(key, array[mid].first) < 0) {
        high = mid;
      } else if (comparator(key, array[mid].first) > 0) {
        low = mid;
      } else {
        // only support unique key
        assert(0);
      }
    }
    memmove(array + high + 1, array + high,
            static_cast<size_t>((GetSize() - high)*sizeof(MappingType)));
    array[high] = {key, value};
  }

  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
MoveHalfTo(BPlusTreeLeafPage *recipient,
           __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  // at least have some key-value pairs
  assert(GetSize() > 0);

  int size = GetSize()/2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyHalfFrom(src, size);
  IncreaseSize(-1*size);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
CopyHalfFrom(MappingType *items, int size) {
  // must be empty leaf page
  assert(IsLeafPage() && GetSize() == 0);
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
Lookup(const KeyType &key, ValueType &value,
       const KeyComparator &comparator) const {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return false;
  }
  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      value = array[mid].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return GetSize();
  }

  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      // delete
      memmove(array + mid, array + mid + 1,
              static_cast<size_t>((GetSize() - mid - 1)*sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
MoveAllTo(BPlusTreeLeafPage *recipient, int, BufferPoolManager *) {
  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
CopyAllFrom(MappingType *items, int size) {
  assert(GetSize() + size <= GetMaxSize());
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                 BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(array, array + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));

  recipient->CopyLastFrom(pair);

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while MoveFirstToEndOf");
  }
  // update relevant key & value pair in parent
  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  // replace key in parent with the moving one
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);

  // unpin parent when we are done
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex,
                  BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
CopyFirstFrom(const MappingType &item, int parentIndex,
              BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  memmove(array + 1, array, GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CopyFirstFrom");
  }
  // get parent
  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  // replace with moving key
  parent->SetKeyAt(parentIndex, item.first);

  // unpin when are done
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************d**********************
 * DEBUG
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[" << GetPageId() << "-" << GetParentPageId() << "]";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << " " << array[entry].first;
    if (verbose) {
      stream << " (" << array[entry].second << ")";
    }
    ++entry;
    stream << " ";
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
