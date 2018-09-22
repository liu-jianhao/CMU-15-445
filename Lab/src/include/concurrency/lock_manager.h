/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockMode { SHARED = 0, EXCLUSIVE };

class LockManager {
  struct Request {
    explicit Request(txn_id_t id, LockMode m, bool g) :
        txn_id(id), mode(m), granted(g) {}
    txn_id_t txn_id;
    LockMode mode = LockMode::SHARED;
    bool granted = false;
  };
  struct Waiting {
    size_t exclusive_cnt = 0;  // how many exclusive requests
    txn_id_t oldest = -1;      // wait-die: txn older than `oldest`(<) can wait or die
    std::list<Request> list;
    //std::mutex mutex_;
    //std::condition_variable cond;
  };
public:
  explicit LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {};

  // disable copy
  LockManager(LockManager const &) = delete;
  LockManager &operator=(LockManager const &) = delete;

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  bool strict_2PL_;
  std::mutex mutex_;
  std::condition_variable cond;
  std::unordered_map<RID, Waiting> lock_table_;
};

} // namespace cmudb
