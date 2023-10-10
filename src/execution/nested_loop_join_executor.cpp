//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/logger.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
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
  left_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_DEBUG("Start");
  Tuple right_tuple;
  FMT_MAYBE_UNUSED RID tmp_rid_left;
  FMT_MAYBE_UNUSED RID tmp_rid_right;
  while (true) {
    while (!right_executor_->Next(&right_tuple, &tmp_rid_right)) {
      if (!left_matched_ && plan_->GetJoinType() == JoinType::LEFT) {
        // LOG_DEBUG("Left not matched");
        std::vector<Value> values;
        for (const auto &column : GetOutputSchema().GetColumns()) {
          auto it = left_col_idx_map_.find(column.GetName());
          if (it != left_col_idx_map_.end()) {
            values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), it->second));
          } else {
            values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
          }
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        left_matched_ = true;
        return true;
      }
      if (!left_executor_->Next(&left_tuple_, &tmp_rid_left)) {
        return false;
      }
      left_matched_ = false;
      right_executor_->Init();
    }
    auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                 plan_->GetRightPlan()->OutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      left_matched_ = true;
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
