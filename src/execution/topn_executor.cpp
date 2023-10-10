#include "execution/executors/topn_executor.h"
#include "common/logger.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void TopNExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  auto cmp = [&](const Tuple &left, const Tuple &right) {
    for (auto &[type, expr] : plan_->GetOrderBy()) {
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
  std::priority_queue<Tuple, std::vector<Tuple>, std::function<bool(const Tuple &, const Tuple &)>> top_n(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    top_n.push(tuple);
    if (top_n.size() > plan_->GetN()) {
      top_n.pop();
    }
  }
  while (!top_n.empty()) {
    sorted_tuples_.push_back(top_n.top());
    top_n.pop();
  }
  std::reverse(sorted_tuples_.begin(), sorted_tuples_.end());
  sorted_tuples_iter_ = sorted_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *sorted_tuples_iter_;
  ++sorted_tuples_iter_;
  return true;
}

}  // namespace bustub
