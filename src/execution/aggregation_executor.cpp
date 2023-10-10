//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  child_ = std::move(child);
}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();
  Tuple cur_tuple;
  RID cur_id;
  while (child_->Next(&cur_tuple, &cur_id)) {
    auto agg_key = MakeAggregateKey(&cur_tuple);
    auto agg_val = MakeAggregateValue(&cur_tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }
  if (plan_->GetGroupBys().empty()) {
    aht_.ResetIfEmpty();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto agg_key = aht_iterator_.Key();
  auto agg_value = aht_iterator_.Val();
  std::vector<Value> values = agg_key.group_bys_;
  values.insert(values.end(), agg_value.aggregates_.begin(), agg_value.aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
