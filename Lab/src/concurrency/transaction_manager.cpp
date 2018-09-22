/**
 * transaction_manager.cpp
 *
 */

#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>

namespace cmudb {

Transaction *TransactionManager::Begin() {
  Transaction *txn = new Transaction(next_txn_id_++);

  if (ENABLE_LOGGING) {
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(log));
  }

  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);
  // truly delete before commit
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // this also release the lock when holding the page latch
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(log));

    // make sure log persist, pre lsn is the last one
    while (txn->GetPrevLSN() > log_manager_->GetPersistentLSN()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      //LOG_DEBUG("Commit(): spin....\n");
    }
    //LOG_DEBUG("txn %d: Commit....", txn->GetTransactionId());
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // rollback before releasing lock
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      LOG_DEBUG("rollback delete");
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      LOG_DEBUG("rollback insert");
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      LOG_DEBUG("rollback update");
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(log));

    // make sure log persist, pre lsn is the last one
    while (txn->GetPrevLSN() > log_manager_->GetPersistentLSN()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      //LOG_DEBUG("Abort(): spin....\n");
    }
    //LOG_DEBUG("txn %d: Abort....", txn->GetTransactionId());
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}
} // namespace cmudb
