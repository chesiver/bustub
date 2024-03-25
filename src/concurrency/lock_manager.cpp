//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

using LockMode = LockManager::LockMode;
using LockRequestQueue = LockManager::LockRequestQueue;
using LockRequest = LockManager::LockRequest;

auto CheckIfLockModeCompatible(LockMode current_lock_mode, LockMode expected_lock_mode) -> bool {
  if (current_lock_mode == LockMode::INTENTION_SHARED) {
    return expected_lock_mode == LockMode::INTENTION_SHARED || expected_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           expected_lock_mode == LockMode::SHARED || expected_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return expected_lock_mode == LockMode::INTENTION_SHARED || expected_lock_mode == LockMode::INTENTION_EXCLUSIVE;
  }
  if (current_lock_mode == LockMode::SHARED) {
    return expected_lock_mode == LockMode::INTENTION_SHARED || expected_lock_mode == LockMode::SHARED;
  }
  if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return expected_lock_mode == LockMode::INTENTION_SHARED;
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    return false;
  }
  return false;
}

auto CheckIfTransactionHoldsRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, bool &is_locked,
                                    LockMode &lock_mode) -> void {
  auto it_shared = txn->GetSharedRowLockSet()->find(oid);
  if (it_shared != txn->GetSharedRowLockSet()->end()) {
    auto &locked_row_set = it_shared->second;
    if (locked_row_set.find(rid) != locked_row_set.end()) {
      is_locked = true;
      lock_mode = LockMode::SHARED;
      return;
    }
  }
  auto it_exclusive = txn->GetExclusiveRowLockSet()->find(oid);
  if (it_exclusive != txn->GetExclusiveRowLockSet()->end()) {
    auto &locked_row_set = it_exclusive->second;
    if (locked_row_set.find(rid) != locked_row_set.end()) {
      is_locked = true;
      lock_mode = LockMode::EXCLUSIVE;
      return;
    }
  }
  is_locked = false;
}

auto RemoveTableLockInBookKeeping(Transaction *txn, const table_oid_t &oid, LockMode &current_lock_mode) -> void {
  if (current_lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  }
  if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  }
  if (current_lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

auto AddTableLockInBookKeeping(Transaction *txn, const table_oid_t &oid, LockMode &current_lock_mode) -> void {
  if (current_lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }
  if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }
  if (current_lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

auto RemoveRowLockInBookKeeping(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode &current_lock_mode)
    -> void {
  if (current_lock_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->operator[](oid).erase(rid);
  }
}

auto AddRowLockInBookKeeping(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode &current_lock_mode)
    -> void {
  if (current_lock_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->operator[](oid).insert(rid);
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->operator[](oid).insert(rid);
  }
}

auto FindLockRequestForRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue, const txn_id_t &txn_id, const table_oid_t &oid) -> std::shared_ptr<LockRequest> {
  for (auto lock_req : lock_request_queue->request_queue_) {
    if (lock_req->txn_id_ == txn_id && lock_req->oid_ == oid) {
      return lock_req;
    }
  }
  return nullptr;
}

auto FindLockRequestForRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue, const txn_id_t &txn_id, const table_oid_t &oid, const RID &rid) -> std::shared_ptr<LockRequest> {
  for (auto lock_req : lock_request_queue->request_queue_) {
    if (lock_req->txn_id_ == txn_id && lock_req->oid_ == oid && lock_req->rid_ == rid) {
      return lock_req;
    }
  }
  return nullptr;
}

/* Test */
auto PrintLockRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue) -> void {
  std::stringstream ss;
  ss << "Debug lock_request_queue" << "\n";
  for (auto item : lock_request_queue->request_queue_) {
    ss << fmt::format("[txn_id: {}, oid: {}, lock_mode: {}, granted: {}]", item->txn_id_, item->oid_, (int)item->lock_mode_, item->granted_) << "\n";
  }
  // MY_LOG_DEBUG("{}", ss.str());
}

/* Assume LockRequestQueue is already locked */
auto GrantLocksForLockRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue, txn_id_t txn_id) -> void {
  auto first_not_granted = lock_request_queue->request_queue_.begin();
  while (first_not_granted != lock_request_queue->request_queue_.end() && (*first_not_granted)->granted_) {
    ++first_not_granted;
  }
  while (first_not_granted != lock_request_queue->request_queue_.end()) {
    bool is_compatible = true;
    auto granted = lock_request_queue->request_queue_.begin();
    while (granted != first_not_granted) {
      if (!CheckIfLockModeCompatible((*granted)->lock_mode_, (*first_not_granted)->lock_mode_)) {
        is_compatible = false;
        break;
      }
      ++granted;
    }
    if (is_compatible) {
      (*first_not_granted)->granted_ = true;
      // MY_LOG_DEBUG("Grant lock for txn_id: {}, oid: {}, rid: {}, lock_mode: {}", (*first_not_granted)->txn_id_, (*first_not_granted)->oid_, (*first_not_granted)->rid_.ToString(), (int)(*first_not_granted)->lock_mode_);
      ++first_not_granted;
    } else {
      /* Found fist incompatible. No need to check the rest */
      // MY_LOG_DEBUG("Conflict: {} {} --- {} {}", (*granted)->txn_id_, (int)(*granted)->lock_mode_, (*first_not_granted)->txn_id_, (int)(*first_not_granted)->lock_mode_);
      break;
    }
  }
}

auto CheckIfTransactionHoldsTableLock(Transaction *txn, const table_oid_t &oid, bool &is_locked, LockMode &lock_mode)
    -> void {
  if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
    is_locked = true;
    lock_mode = LockMode::INTENTION_SHARED;
    return;
  }
  if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    is_locked = true;
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return;
  }
  if (txn->GetSharedTableLockSet()->count(oid) > 0) {
    is_locked = true;
    lock_mode = LockMode::SHARED;
    return;
  }
  if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
    is_locked = true;
    lock_mode = LockMode::EXCLUSIVE;
    return;
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
    is_locked = true;
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return;
  }
  is_locked = false;
}

}  // namespace

namespace bustub {

auto LockManager::UpgradeFromCurrentLockIfPossible(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                                   int &current_status) -> void {
  bool is_currently_locked;
  LockMode current_lock_mode;
  CheckIfTransactionHoldsTableLock(txn, oid, is_currently_locked, current_lock_mode);
  if (!is_currently_locked) {
    current_status = 0;
    return;
  }
  if (current_lock_mode == lock_mode) {
    current_status = 1;
    return;
  }
  bool is_upgrade_compatible = false;
  if (current_lock_mode == LockMode::INTENTION_SHARED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
        lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      is_upgrade_compatible = true;
    }
  }
  if (current_lock_mode == LockMode::SHARED) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      is_upgrade_compatible = true;
    }
  }
  if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      is_upgrade_compatible = true;
    }
  }
  if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      is_upgrade_compatible = true;
    }
  }
  if (!is_upgrade_compatible) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table_lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock table_lock_map_lock{table_lock_map_latch_};
    // MY_LOG_DEBUG("After get table_lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    auto it = table_lock_map_.find(oid);
    if (it == table_lock_map_.end()) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = table_lock_map_[oid];
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock lock_request_queue_lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    /* Check if another transaction is upgrading */
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    /* Set upgrading transaction id */
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    /* Execute upgrade */
    auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid);
    if (lock_request == nullptr) {
      throw std::runtime_error(
          fmt::format("cannot find table lock request for txn_id: %d, oid: %d", txn->GetTransactionId(), oid));
    }
    lock_request->lock_mode_ = lock_mode;
    RemoveTableLockInBookKeeping(txn, oid, current_lock_mode);
    AddTableLockInBookKeeping(txn, oid, lock_mode);
    /* Unset upgrading transaction id */
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  current_status = 2;
}

auto LockManager::UpgradeRowLockIfPossible(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                           int &current_status) -> void {
  bool is_currently_locked;
  LockMode current_lock_mode;
  CheckIfTransactionHoldsRowLock(txn, oid, rid, is_currently_locked, current_lock_mode);
  if (!is_currently_locked) {
    current_status = 0;
    return;
  }
  if (current_lock_mode == lock_mode) {
    current_status = 1;
    return;
  }
  bool is_upgrade_compatible = false;
  if (current_lock_mode == LockMode::SHARED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      is_upgrade_compatible = true;
    }
  }
  if (!is_upgrade_compatible) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock row_lock_map_lock{row_lock_map_latch_};
    // MY_LOG_DEBUG("After get table lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    auto it = row_lock_map_.find(rid);
    if (it == row_lock_map_.end()) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = row_lock_map_[rid];
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock lock_request_queue_lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    /* Check if another transaction is upgrading */
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    /* Set upgrading transaction id */
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    /* Execute upgrade */
    auto lock_request = FindLockRequest(txn->GetTransactionId(), oid);
    if (lock_request == nullptr) {
      throw std::runtime_error(
          fmt::format("cannot find row lock request for txn_id: %d, oid: %d", txn->GetTransactionId(), oid));
    }
    lock_request->lock_mode_ = lock_mode;
    RemoveRowLockInBookKeeping(txn, oid, rid, current_lock_mode);
    AddRowLockInBookKeeping(txn, oid, rid, lock_mode);
    /* Unset upgrading transaction id */
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  current_status = 2;
}

auto LockManager::ProcessLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  // MY_LOG_DEBUG("Start of ProcessLockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
  int current_status;
  UpgradeFromCurrentLockIfPossible(txn, lock_mode, oid, current_status);
  if (current_status == 1 || current_status == 2) {
    /* 1: Same lock mode  2: Already upgraded */
    return;
  }
  /* Add lock request. Once this block is completed , lock is held by this txn. */
  std::shared_ptr<bustub::LockManager::LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table_lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock table_lock{table_lock_map_latch_};
    // MY_LOG_DEBUG("After get table_lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = table_lock_map_[oid];
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::unique_lock lock_request_queue_lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_queue->request_queue_.push_back(lock_request);
    // MY_LOG_DEBUG("Before grant locks --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    GrantLocksForLockRequestQueue(lock_request_queue, txn->GetTransactionId());
    // MY_LOG_DEBUG("After grant locks --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    // PrintLockRequestQueue(lock_request_queue);
    lock_request_queue->cv_.wait(lock_request_queue_lock, [&] {
      auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid);
      if (lock_request == nullptr) {
        throw std::runtime_error(
            fmt::format("cannot find table lock request for txn_id: {}, oid: {}", txn->GetTransactionId(), oid));
      }
      return lock_request->granted_;
    });
    AddTableLockInBookKeeping(txn, oid, lock_mode);
  }
  // MY_LOG_DEBUG("End of ProcessLockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
}

auto LockManager::ProcessUnlockTable(Transaction *txn, const table_oid_t &oid) -> void {
  // MY_LOG_DEBUG("Start of ProcessUnlockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
  bool is_currently_locked;
  LockMode current_lock_mode;
  CheckIfTransactionHoldsTableLock(txn, oid, is_currently_locked, current_lock_mode);
  if (!is_currently_locked) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  /* Execute unlock */
  std::shared_ptr<bustub::LockManager::LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::scoped_lock table_lock{table_lock_map_latch_};
    // MY_LOG_DEBUG("After get table lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    lock_request_queue = table_lock_map_[oid];
    if (lock_request_queue == nullptr) {
      throw std::runtime_error(fmt::format("ProcessUnlockTable - cannot find table lock request queue for oid: {}", oid));
    }
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    std::unique_lock lock_request_queue_lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
    lock_request_queue->request_queue_.remove_if([&](std::shared_ptr<LockRequest> lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId() && lock_request->oid_ == oid;
    });
    RemoveTableLockInBookKeeping(txn, oid, current_lock_mode);
    GrantLocksForLockRequestQueue(lock_request_queue, txn->GetTransactionId());
  }
  lock_request_queue->cv_.notify_all();
  // MY_LOG_DEBUG("End of ProcessUnlockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
}

auto LockManager::ProcessLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> void {
  // MY_LOG_DEBUG("Start of ProcessLockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
  int current_status;
  UpgradeRowLockIfPossible(txn, lock_mode, oid, rid, current_status);
  if (current_status == 1 || current_status == 2) {
    /*
      1: Same lock mode
      2: Upgraded
    */
    return;
  }
  /* Add lock request. Once this block is completed, lock is held by this txn */
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    std::scoped_lock row_lock_map_lock{row_lock_map_latch_};
    // MY_LOG_DEBUG("After get table lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    auto it = row_lock_map_.find(rid);
    if (it == row_lock_map_.end()) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
    lock_request_queue = row_lock_map_[rid];
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    std::unique_lock lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_queue->request_queue_.push_back(lock_request);
    GrantLocksForLockRequestQueue(lock_request_queue, txn->GetTransactionId());
    lock_request_queue->cv_.wait(lock, [&] {
      auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid, rid);
      if (lock_request == nullptr) {
        throw std::runtime_error(fmt::format("cannot find row lock request for txn_id: {}, rid: {}",
                                              txn->GetTransactionId(), rid.ToString()));
      }
      return lock_request->granted_;
    });
    AddRowLockInBookKeeping(txn, oid, rid, lock_mode);
  }
  // MY_LOG_DEBUG("End of ProcessLockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
}

auto LockManager::ProcessUnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> void {
  // MY_LOG_DEBUG("Start of ProcessUnlockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
  bool is_currently_locked;
  LockMode current_lock_mode;
  CheckIfTransactionHoldsRowLock(txn, oid, rid, is_currently_locked, current_lock_mode);
  if (!is_currently_locked) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  /* Execute unlock */
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  {
    // MY_LOG_DEBUG("Before get table lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    std::scoped_lock row_lock_map_lock{row_lock_map_latch_};
    // MY_LOG_DEBUG("After get table lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    auto it = row_lock_map_.find(rid);
    if (it == row_lock_map_.end()) {
      throw std::runtime_error(fmt::format("cannot find lock request queue for rid: %s", rid.ToString().c_str()));
    }
    lock_request_queue = it->second;
  }
  {
    // MY_LOG_DEBUG("Before get lock_request_queue lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    std::unique_lock lock{lock_request_queue->latch_};
    // MY_LOG_DEBUG("After get lock_request_queue lock --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
    lock_request_queue->request_queue_.remove_if([&](std::shared_ptr<LockRequest> lock_request) {
      return lock_request->txn_id_ == txn->GetTransactionId() && lock_request->oid_ == oid && lock_request->rid_ == rid;
    });
    RemoveRowLockInBookKeeping(txn, oid, rid, current_lock_mode);
    /* transaction doesn't hold the lock now. Need check if new lock request should be granted */
    GrantLocksForLockRequestQueue(lock_request_queue, txn->GetTransactionId());
    lock_request_queue->cv_.notify_all();
  }
  // MY_LOG_DEBUG("End of ProcessUnlockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (TransactionState::ABORTED == txn->GetState() || TransactionState::COMMITTED == txn->GetState()) {
    throw std::runtime_error("LockTable in aborted/committed transaction");
  }
  /*
    Check if isolation level compatible with lock mode
  */
  if (IsolationLevel::READ_UNCOMMITTED == txn->GetIsolationLevel()) {
    /*
      The transaction is required to take only IX, X locks.
      X, IX locks are allowed in the GROWING state.
      S, IS, SIX locks are never allowed
    */
    if (LockMode::SHARED == lock_mode || LockMode::INTENTION_SHARED == lock_mode ||
        LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (TransactionState::SHRINKING == txn->GetState()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel()) {
    /*
      The transaction is required to take all locks.
      All locks are allowed in the GROWING state
      Only IS, S locks are allowed in the SHRINKING state
    */
    if (TransactionState::SHRINKING == txn->GetState()) {
      if (LockMode::SHARED != lock_mode && LockMode::INTENTION_SHARED != lock_mode) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel()) {
    /*
      The transaction is required to take all locks.
      All locks are allowed in the GROWING state
      No locks are allowed in the SHRINKING state
    */
    if (TransactionState::SHRINKING == txn->GetState()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  ProcessLockTable(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  /* Only happes in ReleaseLocks (Committed / Aborted) */
  if (TransactionState::ABORTED == txn->GetState() || TransactionState::COMMITTED == txn->GetState()) {
    if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetSharedTableLockSet()->count(oid) > 0) {
      txn->GetSharedTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetExclusiveTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    return false;
  }
  if (IsolationLevel::READ_UNCOMMITTED == txn->GetIsolationLevel()) {
    /*
      Unlocking X locks should set the transaction state to SHRINKING.
      S locks are not permitted under READ_UNCOMMITTED.
      The behaviour upon unlocking an S lock under this isolation level is undefined.
    */
    if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    } else {
      /* Undefined */
      LOG_ERROR("Undefined behavior when unlock S locks in read_uncommited isolation level");
      return false;
    }
  }
  if (IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel()) {
    /*
      Unlocking X locks should set the transaction state to SHRINKING.
      Unlocking S locks does not affect transaction state.
    */
    if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    } else if (txn->GetSharedTableLockSet()->count(oid) > 0) {
    } else if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
    } else if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }
  if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel()) {
    /*
      Unlocking S/X locks should set the transaction state to SHRINKING
    */
    if (txn->GetSharedTableLockSet()->count(oid) > 0) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    } else if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
    } else if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }
  ProcessUnlockTable(txn, oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /*
    Row locking should not support Intention locks
  */
  if (LockMode::INTENTION_SHARED == lock_mode || LockMode::INTENTION_EXCLUSIVE == lock_mode ||
      LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (LockMode::SHARED == lock_mode) {
    if (txn->GetIntentionSharedTableLockSet()->count(oid) == 0 && txn->GetSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  if (LockMode::EXCLUSIVE == lock_mode) {
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  ProcessLockRow(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  if (TransactionState::ABORTED == txn->GetState() || TransactionState::COMMITTED == txn->GetState()) {
    if (txn->GetSharedRowLockSet()->count(oid) > 0) {
      txn->GetSharedRowLockSet()->erase(oid);
      return true;
    }
    if (txn->GetExclusiveRowLockSet()->count(oid) > 0) {
      txn->GetExclusiveRowLockSet()->erase(oid);
      return true;
    }
    return false;
  }
  bool is_currently_locked;
  LockMode current_lock_mode;
  CheckIfTransactionHoldsRowLock(txn, oid, rid, is_currently_locked, current_lock_mode);
  if (current_lock_mode == LockMode::SHARED) {
    /* Undefined behavior */
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      return false;
    }
    /* Shrinking */
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }
  if (current_lock_mode == LockMode::EXCLUSIVE) {
    /* Shrinking in all 3 isolation levels */
    if (txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  if (!is_currently_locked) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  ProcessUnlockRow(txn, oid, rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
