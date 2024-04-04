//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  it_ = std::make_unique<TableIterator>(table_info->table_->Begin(exec_ctx_->GetTransaction()));
  it_end_ = std::make_unique<TableIterator>(table_info->table_->End());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto oid = plan_->GetTableOid();
  auto lock_manager = exec_ctx_->GetLockManager();

  bool success = lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
  if (!success) {
    throw ExecutionException("lock table failed in SeqScanExecutor");
  }
  
  if (*it_ == *it_end_) {

    success = lock_manager->UnlockTable(txn, oid);
    if (!success) {
      throw ExecutionException("unlock table failed in SeqScanExecutor");
    }
    return false;

  }

  *rid = (*it_)->GetRid();

  success = lock_manager->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid);
  if (!success) {
    throw ExecutionException("lock row failed in SeqScanExecutor");
  }

  *tuple = *(*it_);
  ++(*it_);

  success = lock_manager->UnlockTable(txn, oid);
  if (!success) {
    throw ExecutionException("unlock table failed in SeqScanExecutor");
  }

  success = lock_manager->UnlockRow(txn, oid, *rid);
  if (!success) {
    throw ExecutionException("unlock row failed in SeqScanExecutor");
  }
  
  return true;
}

}  // namespace bustub
