/**
 * transaction_manager.h
 *
 */

#pragma once

#include <atomic>
#include <unordered_set>

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "logging/log_manager.h"

namespace cmudb {
class TransactionManager {
public:
  explicit TransactionManager(LockManager *lock_manager,
                              LogManager *log_manager = nullptr)
      : next_txn_id_(0), lock_manager_(lock_manager),
        log_manager_(log_manager) {}

  // disable copy
  TransactionManager(TransactionManager const &) = delete;
  TransactionManager &operator=(TransactionManager const &) = delete;

  Transaction *Begin();
  void Commit(Transaction *txn);
  void Abort(Transaction *txn);

private:
  std::atomic<txn_id_t> next_txn_id_;
  LockManager *lock_manager_;
  LogManager *log_manager_;
};

} // namespace cmudb
