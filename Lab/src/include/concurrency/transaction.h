/**
 * transaction.h
 */

#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <thread>
#include <unordered_set>

#include "common/config.h"
#include "common/logger.h"
#include "page/page.h"
#include "table/tuple.h"

namespace cmudb {

/**
 * Transaction states:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 **/
enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

enum class WType { INSERT = 0, DELETE, UPDATE };

class TableHeap;

// write set record
class WriteRecord {
public:
  WriteRecord(RID rid, WType wtype, const Tuple &tuple, TableHeap *table)
      : rid_(rid), wtype_(wtype), tuple_(tuple), table_(table) {}

  RID rid_;
  WType wtype_;
  // tuple is only for update operation
  Tuple tuple_;
  // which table
  TableHeap *table_;
};

class Transaction {
public:
  Transaction(Transaction const &) = delete;
  Transaction(txn_id_t txn_id)
      : state_(TransactionState::GROWING),
        thread_id_(std::this_thread::get_id()),
        txn_id_(txn_id), prev_lsn_(INVALID_LSN), shared_lock_set_{new std::unordered_set<RID>},
        exclusive_lock_set_{new std::unordered_set<RID>} {
    // initialize sets
    write_set_.reset(new std::deque<WriteRecord>);
    page_set_.reset(new std::deque<Page *>);
    deleted_page_set_.reset(new std::unordered_set<page_id_t>);
  }

  ~Transaction() {}

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//
  inline std::thread::id GetThreadId() const { return thread_id_; }

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline std::shared_ptr<std::deque<WriteRecord>> GetWriteSet() {
    return write_set_;
  }

  inline std::shared_ptr<std::deque<Page *>> GetPageSet() { return page_set_; }

  inline void AddIntoPageSet(Page *page) { page_set_->push_back(page); }

  inline std::shared_ptr<std::unordered_set<page_id_t>> GetDeletedPageSet() {
    return deleted_page_set_;
  }

  inline void AddIntoDeletedPageSet(page_id_t page_id) {
    deleted_page_set_->insert(page_id);
  }

  inline std::shared_ptr<std::unordered_set<RID>> GetSharedLockSet() {
    return shared_lock_set_;
  }

  inline std::shared_ptr<std::unordered_set<RID>> GetExclusiveLockSet() {
    return exclusive_lock_set_;
  }

  inline TransactionState GetState() { return state_; }

  inline void SetState(TransactionState state) { state_ = state; }

  inline lsn_t GetPrevLSN() { return prev_lsn_; }

  inline void SetPrevLSN(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

private:
  TransactionState state_;

  // thread id, single-threaded transactions
  std::thread::id thread_id_;
  // transaction id
  txn_id_t txn_id_;

  // Below are used by transaction, undo set
  std::shared_ptr<std::deque<WriteRecord>> write_set_;
  // prev lsn
  lsn_t prev_lsn_;

  // Below are used by concurrent index
  // this deque contains page pointer that was latched during index operation
  std::shared_ptr<std::deque<Page *>> page_set_;

  // this set contains page_id that was deleted during index operation
  std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

  // Below are used by lock manager
  // this set contains rid of shared-locked tuples by this transaction
  std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
  // this set contains rid of exclusive-locked tuples by this transaction
  std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};
} // namespace cmudb
