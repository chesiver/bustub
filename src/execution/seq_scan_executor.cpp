//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  it_ = std::make_unique<TableIterator>(table_info->table_->Begin(exec_ctx_->GetTransaction()));
  it_end_ = std::make_unique<TableIterator>(table_info->table_->End());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*it_ == *it_end_) {
    return false;
  }
  *tuple = *(*it_);
  *rid = (*it_)->GetRid();
  ++(*it_);
  return true;
}

}  // namespace bustub
