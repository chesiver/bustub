//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  it_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(tree_->GetBeginIterator());
  table_info_ = catalog->GetTable(index_info->table_name_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*it_ == tree_->GetEndIterator()) {
    return false;
  }
  auto item = *(*it_);
  *rid = item.second;
  bool fetch_success = table_info_->table_->GetTuple(item.second, tuple, exec_ctx_->GetTransaction());
  BUSTUB_ASSERT(fetch_success, "fail to get tuple by index key");
  ++(*it_);
  return true;
}
}  // namespace bustub
