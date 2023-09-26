/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *leaf, int idx) {
  buffer_pool_manager_ = buffer_pool_manager;
  current_tree_page_ = leaf;
  idx_ = idx;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return idx_ >= current_tree_page_->GetSize() && current_tree_page_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (idx_ >= current_tree_page_->GetSize()) {
    page_id_t next_page_id = current_tree_page_->GetNextPageId();
    auto *next_tree_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    current_tree_page_ = next_tree_page;
    idx_ = 0;
  }
  return current_tree_page_->GetEntries()[idx_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (idx_ >= current_tree_page_->GetSize()) {
    page_id_t next_page_id = current_tree_page_->GetNextPageId();
    auto *next_tree_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    current_tree_page_ = next_tree_page;
    idx_ = 0;
  } else {
    idx_ += 1;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
