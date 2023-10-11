#include "optimizer/optimizer.h"
#include "execution/plans/hash_join_plan.h"

namespace bustub {
  

auto Optimizer::OptimizesHashJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
    std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::HashJoin) {
    return optimized_plan;
  }

  auto hash_plan = dynamic_cast<HashJoinPlanNode &>(*optimized_plan);
  
  if (hash_plan.GetLeftPlan()->GetType() == PlanType::SeqScan || hash_plan.GetLeftPlan()->GetType() == PlanType::MockScan) {
    
  }

  return optimized_plan;
}
    
}