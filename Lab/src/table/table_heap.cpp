/**
 * table_heap.cpp
 */

#include <cassert>

#include "common/logger.h"
#include "table/table_heap.h"

namespace cmudb {

// open table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                     LockManager *lock_manager, LogManager *log_manager,
                     page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
      log_manager_(log_manager), first_page_id_(first_page_id) {}

// create table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                     LockManager *lock_manager, LogManager *log_manager,
                     Transaction *txn)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
      log_manager_(log_manager) {
  auto first_page =
      static_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
  assert(first_page != nullptr); // todo: abort table creation?
  first_page->WLatch();
  //LOG_DEBUG("new table page created %d", first_page_id_);

  first_page->Init(first_page_id_, PAGE_SIZE, INVALID_PAGE_ID, log_manager_, txn);
  first_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, true);
}

bool TableHeap::InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn) {
  if (tuple.size_ + 32 > PAGE_SIZE) { // larger than one page size
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto cur_page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (cur_page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  cur_page->WLatch();
  while (!cur_page->InsertTuple(
      tuple, rid, txn, lock_manager_,
      log_manager_)) { // fail to insert due to not enough space
    auto next_page_id = cur_page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // valid next page
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = static_cast<TablePage *>(
          buffer_pool_manager_->FetchPage(next_page_id));
      cur_page->WLatch();
    } else { // create new page
      auto new_page =
          static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if (new_page == nullptr) {
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
        txn->SetState(TransactionState::ABORTED);
        return false;
      }
      new_page->WLatch();
      std::cout << "new table page " << next_page_id << " created" <<
                std::endl;
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, PAGE_SIZE, cur_page->GetPageId(),
                     log_manager_, txn);
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
      cur_page = new_page;
    }
  }
  cur_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
  txn->GetWriteSet()->emplace_back(rid, WType::INSERT, Tuple{}, this);
  return true;
}

bool TableHeap::MarkDelete(const RID &rid, Transaction *txn) {
  // todo: remove empty page
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  txn->GetWriteSet()->emplace_back(rid, WType::DELETE, Tuple{}, this);
  return true;
}

bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid,
                            Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  Tuple old_tuple;
  page->WLatch();
  bool is_updated = page->UpdateTuple(tuple, old_tuple, rid, txn, lock_manager_,
                                      log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_updated);
  if (is_updated && txn->GetState() != TransactionState::ABORTED)
    txn->GetWriteSet()->emplace_back(rid, WType::UPDATE, old_tuple, this);
  return is_updated;
}

void TableHeap::ApplyDelete(const RID &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  lock_manager_->Unlock(txn, rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

void TableHeap::RollbackDelete(const RID &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

// called by tuple iterator
bool TableHeap::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn) {
  auto page = static_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  page->RLatch();
  bool res = page->GetTuple(rid, tuple, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return res;
}

bool TableHeap::DeleteTableHeap() {
  // todo: real delete
  return true;
}

TableIterator TableHeap::begin(Transaction *txn) {
  auto page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->RLatch();
  RID rid;
  // if failed (no tuple), rid will be the result of default
  // constructor, which means eof
  page->GetFirstTupleRid(rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, rid, txn);
}

TableIterator TableHeap::end() {
  return TableIterator(this, RID(INVALID_PAGE_ID, -1), nullptr);
}

} // namespace cmudb
