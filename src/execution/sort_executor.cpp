#include "execution/executors/sort_executor.h"
#include "common/logger.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  auto order_by = plan_->GetOrderBy();
  auto cmp = [&](const Tuple &left, const Tuple &right) {
    for (auto &[type, expr] : order_by) {
      auto left_val = expr->Evaluate(&left, child_executor_->GetOutputSchema());
      auto right_val = expr->Evaluate(&right, child_executor_->GetOutputSchema());
      if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
        if (left_val.CompareLessThan(right_val) == CmpBool::CmpTrue) {
          return true;
        }
        if (left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue) {
          return true;
        }
        if (left_val.CompareLessThan(right_val) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    return false;
  };
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), cmp);
  sorted_tuples_iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *sorted_tuples_iter_;
  ++sorted_tuples_iter_;
  return true;
}
}  // namespace bustub
