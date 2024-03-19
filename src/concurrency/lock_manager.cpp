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

auto LockManager::UpgradeTableLockIfCompatible(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /* If upgradable, remove the old lock */
  if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
    if (LockMode::INTENTION_SHARED == lock_mode) {
      return true;
    }
    if (LockMode::SHARED == lock_mode) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    }
    if (LockMode::EXCLUSIVE == lock_mode) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    }
    if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    }
  }
  if (txn->GetSharedTableLockSet()->count(oid) > 0) {
    if (LockMode::INTENTION_SHARED == lock_mode) {
      return false;
    }
    if (LockMode::SHARED == lock_mode) {
      return true;
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      return false;
    }
    if (LockMode::EXCLUSIVE == lock_mode) {
      txn->GetSharedTableLockSet()->erase(oid);
    }
    if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      txn->GetSharedTableLockSet()->erase(oid);
    }
  }
  if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    if (LockMode::INTENTION_SHARED == lock_mode) {
      return false;
    }
    if (LockMode::SHARED == lock_mode) {
      return false;
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      return true;
    }
    if (LockMode::EXCLUSIVE == lock_mode) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    }
    if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    }
  }
  if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
    if (LockMode::INTENTION_SHARED == lock_mode) {
      return false;
    }
    if (LockMode::SHARED == lock_mode) {
      return false;
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      return false;
    }
    if (LockMode::EXCLUSIVE == lock_mode) {
      return true;
    }
    if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      return false;
    }
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
    if (LockMode::INTENTION_SHARED == lock_mode) {
      return false;
    }
    if (LockMode::SHARED == lock_mode) {
      return false;
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      return false;
    }
    if (LockMode::EXCLUSIVE == lock_mode) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    }
    if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
      return true;
    }
  }
  /* Add lock */
  if (LockMode::INTENTION_SHARED == lock_mode) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }
  if (LockMode::SHARED == lock_mode) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }
  if (LockMode::EXCLUSIVE == lock_mode) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  if (LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (TransactionState::ABORTED == txn->GetState() || TransactionState::COMMITTED == txn->GetState()) {
    LOG_ERROR("LockTable in aborted/committed transaction");
    return false;
  }
  // LOG_INFO("Test in LockTable --- id: %d, lock_mode: %d", txn->GetTransactionId(), (int)lock_mode);
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
    if (LockMode::EXCLUSIVE == lock_mode) {
      /* Removal due to lock upgrade */
      if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      }
      txn->GetExclusiveTableLockSet()->insert(oid);
    }
    if (LockMode::INTENTION_EXCLUSIVE == lock_mode) {
      /* Ignore due to lock downgrade */
      if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
        return true;
      }
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    }
    return true;
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
    if (!UpgradeTableLockIfCompatible(txn, lock_mode, oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    return true;
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
    if (!UpgradeTableLockIfCompatible(txn, lock_mode, oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    return true;
  }
  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // LOG_INFO("Test in UnlockTable --- trnsaction id: %d --- oid: %d", txn->GetTransactionId(), oid);
  /* Only happes in ReleaseLocks (Committed / Aborted)*/
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
      txn->GetExclusiveTableLockSet()->erase(oid);
      txn->SetState(TransactionState::SHRINKING);
      return true;
    }
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    /*
      Undefined
    */
    LOG_ERROR("Undefined behavior when unlock S locks in read_uncommited isolation level");
    return false;
  }
  if (IsolationLevel::READ_COMMITTED == txn->GetIsolationLevel()) {
    /*
      Unlocking X locks should set the transaction state to SHRINKING.
      Unlocking S locks does not affect transaction state.
    */
    if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetExclusiveTableLockSet()->erase(oid);
      txn->SetState(TransactionState::SHRINKING);
      return true;
    }
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetSharedTableLockSet()->count(oid) > 0) {
      txn->GetSharedTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (IsolationLevel::REPEATABLE_READ == txn->GetIsolationLevel()) {
    /*
      Unlocking S/X locks should set the transaction state to SHRINKING
    */
    if (txn->GetSharedTableLockSet()->count(oid) > 0) {
      txn->GetSharedTableLockSet()->erase(oid);
      txn->SetState(TransactionState::SHRINKING);
      return true;
    }
    if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetExclusiveTableLockSet()->erase(oid);
      txn->SetState(TransactionState::SHRINKING);
      return true;
    }
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      return true;
    }
    if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      return true;
    }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return false;
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
    /* Ignore in exclusively lock set*/
    if (txn->GetExclusiveRowLockSet()->operator[](oid).count(rid) > 0) {
      return true;
    }
    txn->GetSharedRowLockSet()->operator[](oid).insert(rid);
    return true;
  }
  if (LockMode::EXCLUSIVE == lock_mode) {
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    /* Lock upgrade */
    if (txn->GetSharedRowLockSet()->operator[](oid).count(rid) > 0) {
      txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
    }
    txn->GetExclusiveRowLockSet()->operator[](oid).insert(rid);
    return true;
  }
  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetSharedRowLockSet()->operator[](oid).count(rid) > 0) {
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
    txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
    return true;
  }
  if (txn->GetExclusiveRowLockSet()->operator[](oid).count(rid) > 0) {
    /* Shrinking in all 3 isolation levels */
    if (txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
    txn->GetExclusiveRowLockSet()->operator[](oid).erase(rid);
    return true;
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
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
