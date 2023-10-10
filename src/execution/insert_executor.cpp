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
  int cnt = 0;
  Tuple inserted_tuple;
  RID inserted_rid;
  while (child_executor_->Next(&inserted_tuple, &inserted_rid)) {
    bool insert_success = table_info_->table_->InsertTuple(inserted_tuple, &inserted_rid, exec_ctx_->GetTransaction());
    if (!insert_success) {
      continue;
    }
    for (IndexInfo *index_info : index_infos_) {
      const Tuple &key_tuple =
          inserted_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, inserted_rid, exec_ctx_->GetTransaction());
    }
    cnt += 1;
  }
  std::vector<Value> values = {ValueFactory::GetIntegerValue(cnt)};
  *tuple = Tuple{values, &plan_->OutputSchema()};
  bool ret = cnt > 0 || first_time_;
  first_time_ = false;
  return ret;
}

}  // namespace bustub
