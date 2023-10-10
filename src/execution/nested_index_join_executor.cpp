//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/logger.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  child_executor_ = std::move(child_executor);
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  Catalog *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info_->table_name_);
  for (const auto &column : GetOutputSchema().GetColumns()) {
    auto col_op = child_executor_->GetOutputSchema().TryGetColIdx(column.GetName());
    if (col_op.has_value()) {
      left_col_idx_map_[column.GetName()] = col_op.value();
    } else {
      col_op = plan_->InnerTableSchema().TryGetColIdx(column.GetName());
      right_col_idx_map_[column.GetName()] = col_op.value();
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> fetched_rids;
  Tuple left_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    auto extracted_value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    auto extracted_key_tuple = Tuple{{extracted_value}, &index_info_->key_schema_};
    index_info_->index_->ScanKey(extracted_key_tuple, &fetched_rids, exec_ctx_->GetTransaction());
    if (!fetched_rids.empty()) {
      Tuple right_tuple;
      table_info_->table_->GetTuple(fetched_rids[0], &right_tuple, exec_ctx_->GetTransaction());
      std::vector<Value> values;
      for (const auto &column : GetOutputSchema().GetColumns()) {
        auto it = left_col_idx_map_.find(column.GetName());
        if (it != left_col_idx_map_.end()) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), it->second));
        } else {
          values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), right_col_idx_map_[column.GetName()]));
        }
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (const auto &column : GetOutputSchema().GetColumns()) {
        auto it = left_col_idx_map_.find(column.GetName());
        if (it != left_col_idx_map_.end()) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), it->second));
        } else {
          values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
        }
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
