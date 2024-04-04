//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  MY_LOG_DEBUG("Start of InsertExecutor::Next");

  auto txn = exec_ctx_->GetTransaction();
  auto oid = plan_->TableOid();
  auto lock_manager = exec_ctx_->GetLockManager();

  int cnt = 0;
  Tuple inserted_tuple;
  RID inserted_rid;
  RID inserted_new_rid;

  bool lock_success = lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
  if (!lock_success) {
    throw ExecutionException("lock table failed in InsertExecutor");
  }

  MY_LOG_DEBUG("After LockTable");

  while (child_executor_->Next(&inserted_tuple, &inserted_rid)) {

    lock_success = lock_manager->LockRow(txn, LockManager::LockMode::SHARED, oid, inserted_rid);
    if (!lock_success) {
      throw ExecutionException("lock row failed in InsertExecutor");
    }

    auto t1 = txn->GetSharedRowLockSet()->at(oid);
    MY_LOG_DEBUG("After LockRow: {}", t1.find(inserted_rid) != t1.end());


    bool insert_success = table_info_->table_->InsertTuple(inserted_tuple, &inserted_new_rid, exec_ctx_->GetTransaction());

    auto t4 = txn->GetSharedRowLockSet()->at(oid);
    MY_LOG_DEBUG("After InsertTuple: {}", t4.find(inserted_rid) != t4.end());

    if (!insert_success) {
      lock_success = lock_manager->UnlockRow(txn, oid, inserted_rid);
      if (!lock_success) {
        throw ExecutionException("unlock row failed in InsertExecutor");
      }
      continue;
    }

    auto t3 = txn->GetSharedRowLockSet()->at(oid);
    MY_LOG_DEBUG("Before update index: {}", t3.find(inserted_rid) != t3.end());
    
    for (IndexInfo *index_info : index_infos_) {
      const Tuple &key_tuple =
          inserted_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, inserted_new_rid, exec_ctx_->GetTransaction());
    }

    MY_LOG_DEBUG("After update index");

    auto t2 = txn->GetSharedRowLockSet()->at(oid);
    MY_LOG_DEBUG("Before unlock: {}", t2.find(inserted_rid) != t2.end());

    lock_success = lock_manager->UnlockRow(txn, oid, inserted_rid);

    MY_LOG_DEBUG("unlock success: {}", lock_success);

    if (!lock_success) {
      throw ExecutionException("unlock row failed in InsertExecutor");
    }

    MY_LOG_DEBUG("After UnlockRow");

    cnt += 1;
  }

  std::vector<Value> values = {ValueFactory::GetIntegerValue(cnt)};
  *tuple = Tuple{values, &plan_->OutputSchema()};

  lock_success = lock_manager->UnlockTable(txn, oid);
  if (!lock_success) {
    throw ExecutionException("unlock table failed in InsertExecutor");
  }

  bool ret = cnt > 0 || first_time_;
  first_time_ = false;

  MY_LOG_DEBUG("End of InsertExecutor::Next");

  return ret;
}

}  // namespace bustub
