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

/* ------- Debug ------ */
auto PrintLockRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue) -> void {
  std::stringstream ss;
  ss << "Debug lock_request_queue"
     << "\n";
  for (auto item : lock_request_queue->request_queue_) {
    ss << fmt::format("[txn_id: {}, oid: {}, lock_mode: {}, granted: {}]", item->txn_id_, item->oid_,
                      (int)item->lock_mode_, item->granted_)
       << "\n";
  }
  // MY_LOG_DEBUG("{}", ss.str());
}

auto PrintEdgeList(const std::vector<std::pair<txn_id_t, txn_id_t>> &edge_list) -> void {
  std::stringstream ss;
  ss << "Debug EdgeList" << "\n";
  for (auto &item : edge_list) {
    ss << fmt::format("[from: {}, to: {}]", item.first, item.second) << "\n";
  }
  // MY_LOG_DEBUG("{}", ss.str());
}

auto PrintTableLockSet(const std::unordered_set<table_oid_t> &table_lock_set) -> void {
  std::stringstream ss;
  ss << "Debug TableLockSet: ";
  for (auto &oid : table_lock_set) {
    ss << fmt::format("oid: {}", oid) << ",";
  }
  ss << "\n";
  // MY_LOG_DEBUG("{}", ss.str());
}
/* ------- End of Debug ------ */

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

auto FindLockRequestForRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue, const txn_id_t &txn_id,
                                    const table_oid_t &oid) -> std::shared_ptr<LockRequest> {
  for (auto lock_req : lock_request_queue->request_queue_) {
    if (lock_req->txn_id_ == txn_id && lock_req->oid_ == oid) {
      return lock_req;
    }
  }
  return nullptr;
}

auto FindLockRequestForRequestQueue(std::shared_ptr<LockRequestQueue> lock_request_queue, const txn_id_t &txn_id,
                                    const table_oid_t &oid, const RID &rid) -> std::shared_ptr<LockRequest> {
  for (auto lock_req : lock_request_queue->request_queue_) {
    if (lock_req->txn_id_ == txn_id && lock_req->oid_ == oid && lock_req->rid_ == rid) {
      return lock_req;
    }
  }
  return nullptr;
}

auto FilterLockRequestsIfTransanctionAborted(
  std::list<std::shared_ptr<LockRequest>> &queue, 
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> &adj_list
) -> void {
  /* Need remove waits_for edge here */
  for (const auto &item: queue) {
    txn_id_t txn_id = item->txn_id_;
    Transaction *txn = TransactionManager::GetTransaction(txn_id);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto &p: adj_list) {
        auto &adj = p.second;
        auto it = std::find(adj.begin(), adj.end(), txn_id);
        if (it != adj.end()) {
          adj.erase(it);
        }
      }
    }
  }
  queue.remove_if([](std::shared_ptr<LockRequest> lock_request) {
    txn_id_t txn_id = lock_request->txn_id_;
    Transaction *txn = TransactionManager::GetTransaction(txn_id);
    return txn->GetState() == TransactionState::ABORTED;
  });
}

/* Assume LockRequestQueue is already locked */
auto GrantLocksForLockRequestQueue(
  LockManager *lock_manager, 
  std::shared_ptr<LockRequestQueue> lock_request_queue,
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> &adj_list,
  txn_id_t txn_id
) -> void {
  FilterLockRequestsIfTransanctionAborted(lock_request_queue->request_queue_, adj_list);
  bool has_lock_granted = false;
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
      has_lock_granted = true;
      // MY_LOG_DEBUG("Grant lock for txn_id: {}, oid: {}, rid: {}, lock_mode: {}", (*first_not_granted)->txn_id_, (*first_not_granted)->oid_, (*first_not_granted)->rid_.ToString(), (int)(*first_not_granted)->lock_mode_);
      ++first_not_granted;
    } else {
      /* Found fist incompatible. No need to check the rest */
      // MY_LOG_DEBUG("Conflict: {} {} --- {} {}", (*granted)->txn_id_, (int)(*granted)->lock_mode_, (*first_not_granted)->txn_id_, (int)(*first_not_granted)->lock_mode_);
      /* Need to add wait dependency here. */
      lock_manager->AddEdge((*first_not_granted)->txn_id_, (*granted)->txn_id_);
      /* Remember that if multiple transactions hold a lock on the same object, a single transaction may be waiting on
       * multiple transactions. */
      auto it = ++granted;
      while (it != first_not_granted) {
        if (!CheckIfLockModeCompatible((*it)->lock_mode_, (*first_not_granted)->lock_mode_)) {
          lock_manager->AddEdge((*first_not_granted)->txn_id_, (*it)->txn_id_);
          // MY_LOG_DEBUG("AddEdge: {} {}", (*first_not_granted)->txn_id_, (*it)->txn_id_);
        }
        ++it;
      }
      // MY_LOG_DEBUG("End of Conflict");
      break;
    }
  }
  // MY_LOG_DEBUG("has_lock_granted: {}", has_lock_granted);
  if (has_lock_granted) {
    lock_request_queue->cv_.notify_all();
  }
  PrintEdgeList(lock_manager->GetEdgeList());
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

auto dfs(txn_id_t u, const std::unordered_map<txn_id_t, std::vector<txn_id_t>> &adj_list,
         std::unordered_map<txn_id_t, int> &node_status) -> bool {
  node_status[u] = 1;
  if (adj_list.find(u) == adj_list.end()) {
    node_status[u] = 2;
    return false;
  }
  for (txn_id_t v : adj_list.at(u)) {
    if (node_status[v] == 1) {
      /* Found cycle */
      return true;
    }
    if (node_status[v] == 2) {
      continue;
    }
    if (dfs(v, adj_list, node_status)) {
      return true;
    }
  }
  node_status[u] = 2;
  return false;
}

}  /* namespace bustub */

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
    auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid);
    if (lock_request == nullptr) {
      throw std::runtime_error(
          fmt::format("upgrade - cannot find row lock request for txn_id: %d, oid: %d", txn->GetTransactionId(), oid));
    }
    lock_request->lock_mode_ = lock_mode;
    RemoveRowLockInBookKeeping(txn, oid, rid, current_lock_mode);
    AddRowLockInBookKeeping(txn, oid, rid, lock_mode);
    /* Unset upgrading transaction id */
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  current_status = 2;
}

auto LockManager::ProcessLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // MY_LOG_DEBUG("Start of ProcessLockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
  int current_status;
  UpgradeFromCurrentLockIfPossible(txn, lock_mode, oid, current_status);
  if (current_status == 1 || current_status == 2) {
    /* 1: Same lock mode  2: Already upgraded */
    return true;
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
    // PrintLockRequestQueue(lock_request_queue);
    bool possibly_aborted = false;
    lock_request_queue->cv_.wait(lock_request_queue_lock, [&] {
      // MY_LOG_DEBUG("Before grant locks --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
      GrantLocksForLockRequestQueue(this, lock_request_queue, waits_for_, txn->GetTransactionId());
      // MY_LOG_DEBUG("After grant locks --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
      auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid);
      if (lock_request == nullptr) {
        // throw std::runtime_error(fmt::format("cannot find table lock request for txn_id: {}, oid: {}", txn->GetTransactionId(), oid));
        possibly_aborted = true;
      }
      return possibly_aborted || lock_request->granted_;
    });
    if (possibly_aborted) {
      return false;
    }
    AddTableLockInBookKeeping(txn, oid, lock_mode);
  }
  // MY_LOG_DEBUG("End of ProcessLockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
  return true;
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
      throw std::runtime_error(
          fmt::format("ProcessUnlockTable - cannot find table lock request queue for oid: {}", oid));
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
  }
  lock_request_queue->cv_.notify_all();
  // MY_LOG_DEBUG("End of ProcessUnlockTable --- txn_id: {}, oid: {}", txn->GetTransactionId(), oid);
}

auto LockManager::ProcessLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // MY_LOG_DEBUG("Start of ProcessLockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
  int current_status;
  UpgradeRowLockIfPossible(txn, lock_mode, oid, rid, current_status);
  if (current_status == 1 || current_status == 2) {
    /*
      1: Same lock mode
      2: Upgraded
    */
    return true;
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
    bool possibly_aborted = false;
    lock_request_queue->cv_.wait(lock, [&] {
      GrantLocksForLockRequestQueue(this, lock_request_queue, waits_for_, txn->GetTransactionId());
      auto lock_request = FindLockRequestForRequestQueue(lock_request_queue, txn->GetTransactionId(), oid, rid);
      if (lock_request == nullptr) {
        // throw std::runtime_error(fmt::format("lock - cannot find row lock request for txn_id: {}, rid: {}", txn->GetTransactionId(), rid.ToString()));
        possibly_aborted = true;
      }
      return possibly_aborted || lock_request->granted_;
    });
    if (possibly_aborted) {
      return false;
    }
    AddRowLockInBookKeeping(txn, oid, rid, lock_mode);
  }
  // MY_LOG_DEBUG("End of ProcessLockRow --- txn_id: {}, oid: {}, rid: {}", txn->GetTransactionId(), oid, rid.ToString());
  return true;
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
  }
  lock_request_queue->cv_.notify_all();
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
  return ProcessLockTable(txn, lock_mode, oid);
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
  return ProcessLockRow(txn, lock_mode, oid, rid);
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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> &adj = waits_for_[t1];
  int n = adj.size(), p = 0;
  adj.resize(n + 1);
  while (p < n && adj[p] < t2) {
    p += 1;
  }
  if (p < n && adj[p] == t2) {
    return;
  }
  for (int i = n; i > p; i -= 1) {
    adj[i] = adj[i - 1];
  }
  adj[p] = t2;
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &adj = waits_for_[t1];
  auto it = std::find(adj.begin(), adj.end(), t2);
  if (it != adj.end()) {
    adj.erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_map<txn_id_t, int> node_status;
  for (const auto &item : waits_for_) {
    txn_id_t u = item.first;
    if (node_status[u] != 0) {
      continue;
    }
    if (dfs(u, waits_for_, node_status)) {
      /* Find youngest transaction */
      txn_id_t highest = INT32_MIN;
      for (const auto &item: node_status) {
        highest = std::max(highest, item.first);
      }
      *txn_id = highest;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &item : waits_for_) {
    txn_id_t u = item.first;
    for (txn_id_t v : item.second) {
      edges.emplace_back(u, v);
    }
  }
  return edges;
}

auto LockManager::NotifyLockRequestQueueForAbortedTransaction(Transaction *txn) -> void {
  txn->LockTxn();
  std::unordered_set<RID> row_lock_set;
  for (const auto &s_row_lock_set : *txn->GetSharedRowLockSet()) {
    for (auto rid : s_row_lock_set.second) {
      row_lock_set.insert(rid);
    }
  }
  for (const auto &x_row_lock_set : *txn->GetExclusiveRowLockSet()) {
    for (auto rid : x_row_lock_set.second) {
      row_lock_set.insert(rid);
    }
  }
  std::unordered_set<table_oid_t> table_lock_set;
  for (auto oid : *txn->GetSharedTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (table_oid_t oid : *(txn->GetIntentionSharedTableLockSet())) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetIntentionExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetSharedIntentionExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  txn->UnlockTxn();
  // PrintTableLockSet(table_lock_set);
  std::scoped_lock table_lock_map_lock{table_lock_map_latch_};
  for (auto oid : table_lock_set) {
    auto it = table_lock_map_.find(oid);
    if (it != table_lock_map_.end()) {
      // MY_LOG_DEBUG("Find table lock request queue for oid: {}", oid);
      auto lock_request_queue = it->second;
      lock_request_queue->cv_.notify_all();
    }
  }
  std::scoped_lock row_lock_map_lock{row_lock_map_latch_};
  for (auto rid : row_lock_set) {
    auto it = row_lock_map_.find(rid);
    if (it != row_lock_map_.end()) {
      // MY_LOG_DEBUG("Find row lock request queue for rid: {}", rid.ToString());
      auto lock_request_queue = it->second;
      lock_request_queue->cv_.notify_all();
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::scoped_lock waits_for_lock{waits_for_latch_};
      txn_id_t txn_id;
      if (HasCycle(&txn_id)) {
        // MY_LOG_DEBUG("Has cycle --- txn_id: {}", txn_id);
        /* Need break cycle */
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        // MY_LOG_DEBUG("Test !!!!!: txn is null: {}", txn == nullptr);
        txn->SetState(TransactionState::ABORTED);
        NotifyLockRequestQueueForAbortedTransaction(txn);
      }
    }
  }
}

}  // namespace bustub
