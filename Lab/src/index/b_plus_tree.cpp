/**
 * b_plus_tree.cpp
 */

#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb
{

template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTree<KeyType, ValueType, KeyComparator>::
    BPlusTree(const std::string &name,
              BufferPoolManager *buffer_pool_manager,
              const KeyComparator &comparator,
              page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local bool BPlusTree<KeyType, ValueType, KeyComparator>::root_is_locked = false;

/*
 * Helper function to decide whether current b+tree is empty
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    IsEmpty() const
{
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    GetValue(const KeyType &key, std::vector<ValueType> &result,
             Transaction *transaction)
{

  // 根据key找到叶子节点页面
  auto *leaf = FindLeafPage(key, false, Operation::READONLY, transaction);
  bool ret = false;
  if (leaf != nullptr)
  {
    ValueType value;
    if (leaf->Lookup(key, value, comparator_))
    {
      result.push_back(value);
      ret = true;
    }

    // 释放锁
    UnlockUnpinPages(Operation::READONLY, transaction);

    if (transaction == nullptr)
    {
      auto page_id = leaf->GetPageId();
      buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);

      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    Insert(const KeyType &key, const ValueType &value, Transaction *transaction)
{

  // 互斥锁
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // 如果树为空就新建一棵树
    if (IsEmpty())
    {
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    StartNewTree(const KeyType &key, const ValueType &value)
{
  auto *page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while StartNewTree");
  }
  auto root =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,
                                         KeyComparator> *>(page->GetData());
  // 别忘了要更新根节点页面id
  UpdateRootPageId(true);
  root->Init(root_page_id_, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction)
{
  auto *leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
  if (leaf == nullptr)
  {
    return false;
  }

  ValueType v;
  // 如果树中已经有值了，就返回false
  if (leaf->Lookup(key, v, comparator_))
  {
    UnlockUnpinPages(Operation::INSERT, transaction);
    return false;
  }

  // 不需要分裂就直接插入
  if (leaf->GetSize() < leaf->GetMaxSize())
  {
    leaf->Insert(key, value, comparator_);
  }
  else
  {
    // 分裂出一个新叶子节点页面
    auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
    if (comparator_(key, leaf2->KeyAt(0)) < 0)
    {
      leaf->Insert(key, value, comparator_);
    }
    else
    {
      leaf2->Insert(key, value, comparator_);
    }

    // 更新前后关系
    if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0)
    {
      leaf2->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(leaf2->GetPageId());
    }
    else
    {
      leaf2->SetNextPageId(leaf->GetPageId());
    }

    // 将分裂的节点插入到父节点
    InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
  }

  UnlockUnpinPages(Operation::INSERT, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
N *BPlusTree<KeyType, ValueType, KeyComparator>::
    Split(N *node)
{
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while Split");
  }
  auto new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id);

  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                     BPlusTreePage *new_node, Transaction *transaction)
{
  // 如果old_node是根节点，则需要新生成一个根页面
  if (old_node->IsRootPage())
  {
    auto *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while InsertIntoParent");
    }
    assert(page->GetPinCount() == 1);
    auto root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    root->Init(root_page_id_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // 这时需要更新根节点页面id
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
  }
  else
  {
    auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while InsertIntoParent");
    }
    auto internal =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    
    // 如果父节点还有空间
    if (internal->GetSize() < internal->GetMaxSize())
    {
      internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

      new_node->SetParentPageId(internal->GetPageId());

      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }
    else
    {
      page_id_t page_id;
      auto *page = buffer_pool_manager_->NewPage(page_id);
      if (page == nullptr)
      {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while InsertIntoParent");
      }
      assert(page->GetPinCount() == 1);

      auto *copy =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                                 KeyComparator> *>(page->GetData());
      copy->Init(page_id);
      copy->SetSize(internal->GetSize());
      for (int i = 1, j = 0; i <= internal->GetSize(); ++i, ++j)
      {
        if (internal->ValueAt(i - 1) == old_node->GetPageId())
        {
          copy->SetKeyAt(j, key);
          copy->SetValueAt(j, new_node->GetPageId());
          ++j;
        }
        if (i < internal->GetSize())
        {
          copy->SetKeyAt(j, internal->KeyAt(i));
          copy->SetValueAt(j, internal->ValueAt(i));
        }
      }

      assert(copy->GetSize() == copy->GetMaxSize());
      auto internal2 =
          Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(copy);

      internal->SetSize(copy->GetSize() + 1);
      for (int i = 0; i < copy->GetSize(); ++i)
      {
        internal->SetKeyAt(i + 1, copy->KeyAt(i));
        internal->SetValueAt(i + 1, copy->ValueAt(i));
      }

      if (comparator_(key, internal2->KeyAt(0)) < 0)
      {
        new_node->SetParentPageId(internal->GetPageId());
      }
      else if (comparator_(key, internal2->KeyAt(0)) == 0)
      {
        new_node->SetParentPageId(internal2->GetPageId());
      }
      else
      {
        new_node->SetParentPageId(internal2->GetPageId());
        old_node->SetParentPageId(internal2->GetPageId());
      }

      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      buffer_pool_manager_->UnpinPage(copy->GetPageId(), false);
      buffer_pool_manager_->DeletePage(copy->GetPageId());

      InsertIntoParent(internal, internal2->KeyAt(0), internal2);
    }

    buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty())
  {
    return;
  }

  auto *leaf = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (leaf != nullptr)
  {
    int size_before_deletion = leaf->GetSize();
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion)
    {
      if (CoalesceOrRedistribute(leaf, transaction))
      {
        transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      }
    }
    UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    CoalesceOrRedistribute(N *node, Transaction *transaction)
{
  if (node->IsRootPage())
  {
    return AdjustRoot(node);
  }
  if (node->IsLeafPage())
  {
    if (node->GetSize() >= node->GetMinSize())
    {
      return false;
    }
  }
  else
  {
    if (node->GetSize() > node->GetMinSize())
    {
      return false;
    }
  }

  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CoalesceOrRedistribute");
  }
  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                             KeyComparator> *>(page->GetData());
  int value_index = parent->ValueIndex(node->GetPageId());

  assert(value_index != parent->GetSize());

  int sibling_page_id;
  if (value_index == 0)
  {
    sibling_page_id = parent->ValueAt(value_index + 1);
  }
  else
  {
    sibling_page_id = parent->ValueAt(value_index - 1);
  }

  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CoalesceOrRedistribute");
  }

  // lab3
  page->WLatch();
  transaction->AddIntoPageSet(page);
  auto sibling = reinterpret_cast<N *>(page->GetData());
  bool redistribute = false;

  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    redistribute = true;
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }

  if (redistribute)
  {
    if (value_index == 0)
    {
      Redistribute<N>(sibling, node, 1);
    }
    return false;
  }

  bool ret;
  if (value_index == 0)
  {
    // lab3
    Coalesce<N>(node, sibling, parent, 1, transaction);
    transaction->AddIntoDeletedPageSet(sibling_page_id);
    ret = false;
  }
  else
  {
    Coalesce<N>(sibling, node, parent, value_index, transaction);
    ret = true;
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Coalesce(N *neighbor_node, N *node,
             BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
             int index, Transaction *transaction)
{

  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

  parent->Remove(index);

  if (CoalesceOrRedistribute(parent, transaction))
  {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Redistribute(N *neighbor_node, N *node, int index)
{
  if (index == 0)
  {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
  else
  {
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while Redistribute");
    }
    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    int idx = parent->ValueIndex(node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);

    neighbor_node->MoveLastToFrontOf(node, idx, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    AdjustRoot(BPlusTreePage *old_root_node)
{
  // 如果删除了最后一个节点
  if (old_root_node->IsLeafPage())
  {
    if (old_root_node->GetSize() == 0)
    {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }

  // 删除了还有最后一个节点
  if (old_root_node->GetSize() == 1)
  {
    auto root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    UpdateRootPageId(false);

    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while AdjustRoot");
    }
    auto new_root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::
    Begin()
{
  KeyType key{};
  return IndexIterator<KeyType, ValueType, KeyComparator>(
      FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::
    Begin(const KeyType &key)
{
  auto *leaf = FindLeafPage(key, false);
  int index = 0;
  if (leaf != nullptr)
  {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(
      leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

// **************** lab3 ***********************
/*
 * Unlock all nodes current transaction hold according to op then unpin pages
 * and delete all pages if any
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    UnlockUnpinPages(Operation op, Transaction *transaction)
{
  if (transaction == nullptr)
  {
    return;
  }

  for (auto *page : *transaction->GetPageSet())
  {
    if (op == Operation::READONLY)
    {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    else
    {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();

  for (auto page_id : *transaction->GetDeletedPageSet())
  {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

  if (root_is_locked)
  {
    root_is_locked = false;
    unlockRoot();
  }
}

/*
 * Note: leaf node and internal node have different MAXSIZE
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    isSafe(N *node, Operation op)
{
  if (op == Operation::INSERT)
  {
    return node->GetSize() < node->GetMaxSize();
  }
  else if (op == Operation::DELETE)
  {
    // >=: keep same with `coalesce logic`
    return node->GetSize() > node->GetMinSize() + 1;
  }
  return true;
}
// **************** lab3 ***********************

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
// 这个函数lab2和lab3有着很多不同
template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *
BPlusTree<KeyType, ValueType, KeyComparator>::
    FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction)
{
  // 如果操作不是只读的，就要锁根节点
  if (op != Operation::READONLY)
  {
    lockRoot();
    root_is_locked = true;
  }

  if (IsEmpty())
  {
    return nullptr;
  }

  auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
  if (parent == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while FindLeafPage");
  }

  if (op == Operation::READONLY)
  {
    parent->RLatch();
  }
  else
  {
    parent->WLatch();
  }
  if (transaction != nullptr)
  {
    transaction->AddIntoPageSet(parent);
  }

  auto *node = reinterpret_cast<BPlusTreePage *>(parent->GetData());
  while (!node->IsLeafPage())
  {
    auto internal =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(node);
    page_id_t parent_page_id = node->GetPageId(), child_page_id;
    if (leftMost)
    {
      child_page_id = internal->ValueAt(0);
    }
    else
    {
      child_page_id = internal->Lookup(key, comparator_);
    }

    auto *child = buffer_pool_manager_->FetchPage(child_page_id);
    if (child == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while FindLeafPage");
    }

    if (op == Operation::READONLY)
    {
      child->RLatch();
      UnlockUnpinPages(op, transaction);
    }
    else
    {
      child->WLatch();
    }
    node = reinterpret_cast<BPlusTreePage *>(child->GetData());
    assert(node->GetParentPageId() == parent_page_id);

    // 如果是安全的，就释放父节点那的锁
    if (op != Operation::READONLY && isSafe(node, op))
    {
      UnlockUnpinPages(op, transaction);
    }
    if (transaction != nullptr)
    {
      transaction->AddIntoPageSet(child);
    }
    else
    {
      parent->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      parent = child;
    }
  }
  return reinterpret_cast<BPlusTreeLeafPage<KeyType,
                                            ValueType, KeyComparator> *>(node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method every time root page id is changed.
 * @parameter: insert_record default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    UpdateRootPageId(bool insert_record)
{
  auto *page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while UpdateRootPageId");
  }
  auto *header_page = reinterpret_cast<HeaderPage *>(page->GetData());

  if (insert_record)
  {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  }
  else
  {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTree<KeyType, ValueType, KeyComparator>::
    ToString(bool verbose)
{
  if (IsEmpty())
  {
    return "Empty tree";
  }
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty())
  {
    node = todo.front();
    if (first)
    {
      first = false;
      tree << "| ";
    }
    // leaf page, print all key-value pairs
    if (node->IsLeafPage())
    {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
    }
    else
    {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty())
    {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }
    // unpin node when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    InsertFromFile(const std::string &file_name, Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;

    KeyType index_key{};
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    RemoveFromFile(const std::string &file_name, Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
