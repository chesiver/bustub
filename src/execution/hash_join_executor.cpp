//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  left_executor_ = std::move(left_child);
  right_executor_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Schema output_schema = GetOutputSchema();
  for (const auto &column : output_schema.GetColumns()) {
    auto col_op = left_executor_->GetOutputSchema().TryGetColIdx(column.GetName());
    if (col_op.has_value()) {
      left_col_idx_map_[column.GetName()] = col_op.value();
    } else {
      col_op = right_executor_->GetOutputSchema().TryGetColIdx(column.GetName());
      right_col_idx_map_[column.GetName()] = col_op.value();
    }
  }
  RID tmp;
  left_executor_->Next(&left_tuple_, &tmp);
  left_key_value_ = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple_, plan_->GetLeftPlan()->OutputSchema());
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  FMT_MAYBE_UNUSED RID tmp_rid;
  while (true) {
    while (!right_executor_->Next(&right_tuple, &tmp_rid)) {
      if (!left_executor_->Next(&left_tuple_, &tmp_rid)) {
        return false;
      }
      left_key_value_ = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple_, plan_->GetLeftPlan()->OutputSchema());
      right_executor_->Init();
    }
    Value right_key_value = plan_->RightJoinKeyExpression().Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
    if (left_key_value_.CompareEquals(right_key_value) == CmpBool::CmpTrue) {
      std::vector<Value> values;
      for (const auto &column : GetOutputSchema().GetColumns()) {
        auto it = left_col_idx_map_.find(column.GetName());
        if (it != left_col_idx_map_.end()) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), it->second));
        } else {
          values.push_back(
              right_tuple.GetValue(&right_executor_->GetOutputSchema(), right_col_idx_map_[column.GetName()]));
        }
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      break;
    }
  }
  return true;
}

}  // namespace bustub
