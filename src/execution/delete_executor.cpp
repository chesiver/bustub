//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  first_time_ = true;
  child_executor_->Init();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto oid = plan_->TableOid();
  auto lock_manager = exec_ctx_->GetLockManager();

  int cnt = 0;
  Tuple deleted_tuple;
  RID deleted_rid;

  bool lock_success = lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  if (!lock_success) {
    throw ExecutionException("lock table failed in InsertExecutor");
  }

  while (child_executor_->Next(&deleted_tuple, &deleted_rid)) {

    lock_success = lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, deleted_rid);
    if (!lock_success) {
      throw ExecutionException("lock row failed in InsertExecutor");
    }

    bool delete_success = table_info_->table_->MarkDelete(deleted_rid, exec_ctx_->GetTransaction());

    if (!delete_success) {

      lock_success = lock_manager->UnlockRow(txn, oid, deleted_rid);
      if (!lock_success) {
        throw ExecutionException("unlock row failed in InsertExecutor");
      }

      continue;
    }

    for (IndexInfo *index_info : index_infos_) {
      const Tuple &key_tuple =
          deleted_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, deleted_rid, exec_ctx_->GetTransaction());
    }

    lock_success = lock_manager->UnlockRow(txn, oid, deleted_rid);
    if (!lock_success) {
      throw ExecutionException("unlock row failed in InsertExecutor");
    }

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
  return ret;
}

}  // namespace bustub
