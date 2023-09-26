#include <string>
#include <tuple>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DownToLeaf(const KeyType &key, Transaction *transaction) -> LeafPage * {
  // LOG_DEBUG("Start %lld", key.ToString());
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    int p = 1;
    int q = cur_tree_page_as_internal->GetSize();
    while (p < q) {
      int mid = (p + q) / 2;
      if (comparator_(cur_tree_page_as_internal->GetMappingType()[mid].first, key) <= 0) {
        p = mid + 1;
      } else {
        q = mid;
      }
    }
    p -= 1;
    page_id_t pid = cur_tree_page_as_internal->GetMappingType()[p].second;
    buffer_pool_manager_->UnpinPage(cur_tree_page_as_internal->GetPageId(), false);
    cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid)->GetData());
    BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  }
  auto *cur_tree_page_as_leaf = reinterpret_cast<LeafPage *>(cur_tree_page);
  return cur_tree_page_as_leaf;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInLeaf(LeafPage *leaf, const KeyType &key, std::vector<ValueType> *result,
                                  Transaction *transaction) -> bool {
  int p = 0;
  int q = leaf->GetSize();
  while (p < q) {
    int mid = (p + q) / 2;
    if (comparator_(leaf->GetMappingType()[mid].first, key) < 0) {
      p = mid + 1;
    } else {
      q = mid;
    }
  }
  if (p < leaf->GetSize() && comparator_(leaf->GetMappingType()[p].first, key) == 0) {
    *result = std::vector<ValueType>{leaf->GetMappingType()[p].second};
    return true;
  }
  return false;
}
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  LeafPage *leaf_page = DownToLeaf(key, transaction);
  bool found = SearchInLeaf(leaf_page, key, result, transaction);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertOnLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("Start %lld", key.ToString());
  int p = 0;
  int q = leaf->GetSize();
  while (p < q) {
    int mid = (p + q) / 2;
    if (comparator_(leaf->GetMappingType()[mid].first, key) <= 0) {
      p = mid + 1;
    } else {
      q = mid;
    }
  }
  for (int i = leaf->GetSize() - 1; i >= p; i -= 1) {
    leaf->GetMappingType()[i + 1] = leaf->GetMappingType()[i];
  }
  leaf->GetMappingType()[p] = std::make_pair(key, value);
  leaf->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertOnParent(BPlusTreePage *left, const KeyType &key, BPlusTreePage *right) {
  // LOG_DEBUG("Start left: %d - key: %lld - right: %d", left->GetPageId(), key.ToString(), right->GetPageId());
  // Is Root
  if (left->IsRootPage()) {
    // LOG_DEBUG("Create Root Page: %d - %lld - %d", left->GetPageId(), key.ToString(), right->GetPageId());
    // Create root page
    page_id_t new_root_page_id;
    Page *root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    BUSTUB_ASSERT(root_page != nullptr, "Fail to create new Page - Check if all pages are pinned");
    // Create B+ tree page
    auto *new_root = reinterpret_cast<InternalPage *>(root_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetSize(2);
    new_root->GetMappingType()[0] = std::make_pair(KeyType{}, left->GetPageId());
    new_root->GetMappingType()[1] = std::make_pair(key, right->GetPageId());
    left->SetParentPageId(new_root_page_id);
    right->SetParentPageId(new_root_page_id);
    // Set root
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(false);
    // Unpin
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
    return;
  }
  // Normal
  auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left->GetParentPageId()));
  int size = parent->GetSize();
  if (size < parent->GetMaxSize()) {
    // Insert directly
    // LOG_DEBUG("Insert directly: %lld", key.ToString());
    int i = size;
    while (i > 0 && comparator_(parent->GetMappingType()[i - 1].first, key) > 0) {
      parent->GetMappingType()[i] = parent->GetMappingType()[i - 1];
      i -= 1;
    }
    parent->GetMappingType()[i] = std::make_pair(key, right->GetPageId());
    parent->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    // Split
    // LOG_DEBUG("Split: %lld", key.ToString());
    std::vector<std::pair<KeyType, page_id_t>> tmp(size + 1);
    int i = size;
    while (i > 0 && comparator_(parent->GetMappingType()[i - 1].first, key) > 0) {
      tmp[i] = parent->GetMappingType()[i - 1];
      i -= 1;
    }
    tmp[i--] = std::make_pair(key, right->GetPageId());
    while (i >= 0) {
      tmp[i] = parent->GetMappingType()[i];
      i -= 1;
    }
    // left
    int half = (size + 1 + 1) / 2;
    parent->SetSize(half);
    for (int i = 0; i < half; i += 1) {
      parent->GetMappingType()[i] = tmp[i];
    }
    // right
    page_id_t next_to_parent_page_id;
    Page *next_to_parent_page = buffer_pool_manager_->NewPage(&next_to_parent_page_id);
    auto *next_to_parent_tree_page = reinterpret_cast<InternalPage *>(next_to_parent_page->GetData());
    BUSTUB_ASSERT(next_to_parent_tree_page != nullptr, "Fail to create new Page - Check if all pages are pinned");
    next_to_parent_tree_page->Init(next_to_parent_page_id, parent->GetParentPageId(), internal_max_size_);
    next_to_parent_tree_page->SetSize(size + 1 - half);
    // redirect parent
    for (int i = half; i < size + 1; i += 1) {
      auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp[i].second)->GetData());
      child->SetParentPageId(next_to_parent_page_id);
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
    for (int i = half; i < size + 1; i += 1) {
      next_to_parent_tree_page->GetMappingType()[i - half] = tmp[i];
    }
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    // recursive
    const KeyType &parent_split_key = tmp[half].first;
    InsertOnParent(parent, parent_split_key, next_to_parent_tree_page);
  }
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_DEBUG("Enter: %lld", key.ToString());
  // records_.push_back(key.ToString());
  // if (records_.size() % 50 == 0) {
  //   std::ostringstream o;
  //   for (int64_t num : records_) {
  //     o << num << ' ';
  //   }
  //   LOG_INFO("keys: %s", o.str().c_str());
  //   records_.clear();
  // }
  // Search leaf
  LeafPage *cur_tree_page;
  if (IsEmpty()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    // LOG_DEBUG("Creat Root Page When Empty: %lld - %d", key.ToString(), root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "Fail to create new Page - Check if all pages are pinned");
    cur_tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    cur_tree_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(true);
  } else {
    cur_tree_page = DownToLeaf(key, transaction);
  }
  // Check duplicate
  // LOG_DEBUG("Check Duplicate %lld", key.ToString());
  std::vector<ValueType> tmp;
  if (SearchInLeaf(cur_tree_page, key, &tmp, transaction)) {
    buffer_pool_manager_->UnpinPage(cur_tree_page->GetPageId(), false);
    return false;
  }
  // Insert
  // LOG_DEBUG("Insert: %lld", key.ToString());
  int size = cur_tree_page->GetSize();
  if (size < cur_tree_page->GetMaxSize() - 1) {
    InsertOnLeaf(cur_tree_page, key, value);
    buffer_pool_manager_->UnpinPage(cur_tree_page->GetPageId(), true);
    return true;
  }
  // Split
  // LOG_DEBUG("Split: %lld", key.ToString());
  std::vector<std::pair<KeyType, ValueType>> tmp_entries(size + 1);
  int idx = 0;
  while (idx < size && comparator_(cur_tree_page->GetMappingType()[idx].first, key) < 0) {
    tmp_entries[idx] = cur_tree_page->GetMappingType()[idx];
    idx += 1;
  }
  tmp_entries[idx++] = std::make_pair(key, value);
  while (idx <= size) {
    tmp_entries[idx] = cur_tree_page->GetMappingType()[idx - 1];
    idx += 1;
  }
  // Left
  int half = (size + 1 + 1) / 2;
  cur_tree_page->SetSize(half);
  for (int i = 0; i < half; i += 1) {
    cur_tree_page->GetMappingType()[i] = tmp_entries[i];
  }
  // Right
  page_id_t next_to_cur_page_id;
  Page *next_to_cur_page = buffer_pool_manager_->NewPage(&next_to_cur_page_id);
  BUSTUB_ASSERT(next_to_cur_page != nullptr, "Fail to create new Page - Check if all pages are pinned");
  auto *next_to_cur_tree_page = reinterpret_cast<LeafPage *>(next_to_cur_page->GetData());
  next_to_cur_tree_page->Init(next_to_cur_page_id, cur_tree_page->GetParentPageId(), leaf_max_size_);
  next_to_cur_tree_page->SetSize(size + 1 - half);
  for (int i = half; i < size + 1; i += 1) {
    next_to_cur_tree_page->GetMappingType()[i - half] = tmp_entries[i];
  }
  // Redirect next page id
  next_to_cur_tree_page->SetNextPageId(cur_tree_page->GetNextPageId());
  cur_tree_page->SetNextPageId(next_to_cur_page_id);
  // Recursive
  const KeyType &parent_split_key = next_to_cur_tree_page->GetMappingType()[0].first;
  InsertOnParent(cur_tree_page, parent_split_key, next_to_cur_tree_page);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
auto RemoveEntryLocally(int size, MappingType *entries, const KeyType &key, const KeyComparator &comparator) -> bool {
  int idx = 0;
  for (; idx < size && comparator(entries[idx].first, key) != 0; idx += 1) {
  }
  if (idx >= size) {
    return false;
  }
  for (int i = idx; i < size; i += 1) {
    entries[i] = entries[i + 1];
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto FindSibling(int size, MappingType *parentEntries, const ValueType &value) -> std::tuple<int, int, bool, KeyType> {
  int idx = 0;
  for (; idx < size && parentEntries[idx].second != value; idx += 1) {
  }
  BUSTUB_ASSERT(idx < size, "Cannot find sibling");
  if (idx > 0) {
    return {idx, idx - 1, true, parentEntries[idx].first};
  }
  return {idx, idx + 1, false, parentEntries[idx + 1].first};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLeafEntry(LeafPage *cur, const KeyType &key, Transaction *transaction) {
  // LOG_DEBUG("Page id: %d - key: %lld", cur->GetPageId(), key.ToString());
  // Delete in current node
  bool removed = RemoveEntryLocally(cur->GetSize(), cur->GetMappingType(), key, comparator_);
  if (!removed) {
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
    return;
  }
  cur->IncreaseSize(-1);
  // If too few pointers
  if (cur->GetSize() < cur->GetMinSize()) {
    // If Root - No sibling
    if (cur->IsRootPage()) {
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      return;
    }
    // Find previous / next child
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->GetParentPageId())->GetData());
    // LOG_DEBUG("Parent page id: %d - key: %lld", parent->GetPageId(), key.ToString());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] =
        FindSibling<KeyType, page_id_t, KeyComparator>(parent->GetSize(), parent->GetMappingType(), cur->GetPageId());
    // LOG_DEBUG("splitKey: %lld", splitKey.ToString());
    int sibling_page_id = parent->GetMappingType()[siblingIdxInParent].second;
    auto *sibling = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(sibling_page_id)->GetData());
    // LOG_DEBUG("Sibling page id: %d - key: %lld", sibling->GetPageId(), key.ToString());
    // If fit in same tree page
    if (cur->GetSize() + sibling->GetSize() <= cur->GetMaxSize()) {
      // Swap if sibling is after parent
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling, cur);
      }
      // LOG_DEBUG("Merge: %d - %lld - %d", cur->GetPageId(), splitKey.ToString(), sibling->GetPageId());
      for (int i = 0; i < cur->GetSize(); i += 1) {
        sibling->GetMappingType()[sibling->GetSize() + i] = cur->GetMappingType()[i];
      }
      sibling->IncreaseSize(cur->GetSize());
      sibling->SetNextPageId(cur->GetNextPageId());
      // Unpin
      page_id_t cur_page_id = cur->GetPageId();
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      // Recursive
      RemoveInternalEntry(parent, splitKey, transaction);
      // Delete current node
      bool deleted = buffer_pool_manager_->DeletePage(cur_page_id);
      BUSTUB_ASSERT(deleted, "Cannot delete current page");
    } else {
      // Redistribution
      if (isPredecessor) {
        // Predecessor
        MappingType last = sibling->GetMappingType()[sibling->GetSize() - 1];
        sibling->IncreaseSize(-1);
        for (int i = cur->GetSize(); i > 0; i -= 1) {
          cur->GetMappingType()[i] = cur->GetMappingType()[i - 1];
        }
        cur->GetMappingType()[0] = last;
        cur->IncreaseSize(1);
        // replace split key in parent
        parent->GetMappingType()[curIndexInParent].first = last.first;
      } else {
        // Successor
        MappingType first = sibling->GetMappingType()[0];
        for (int i = 0; i < sibling->GetSize() - 1; i += 1) {
          sibling->GetMappingType()[i] = sibling->GetMappingType()[i + 1];
        }
        sibling->IncreaseSize(-1);
        cur->GetMappingType()[cur->GetSize()] = first;
        cur->IncreaseSize(1);
        // replace split key in parent
        parent->GetMappingType()[siblingIdxInParent].first = sibling->GetMappingType()[0].first;
      }
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    return;
  }
  buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternalEntry(InternalPage *cur, const KeyType &key, Transaction *transaction) {
  // LOG_DEBUG("Start Page id: %d - key: %lld", cur->GetPageId(), key.ToString());
  // Delete in current node
  bool removed =
      RemoveEntryLocally<KeyType, page_id_t, KeyComparator>(cur->GetSize(), cur->GetMappingType(), key, comparator_);
  if (!removed) {
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
    return;
  }
  cur->IncreaseSize(-1);
  // If root
  if (cur->IsRootPage()) {
    // If only one child
    if (cur->GetSize() == 1) {
      // LOG_DEBUG("Only one child - page id: %d - key: %lld", cur->GetPageId(), key.ToString());
      // Redirect new root if current one is not leaf page
      auto *child = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage(cur->GetMappingType()[0].second)->GetData());
      // Reset root
      root_page_id_ = child->GetPageId();
      child->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false);
      // Unpin
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      // delete page
      buffer_pool_manager_->DeletePage(cur->GetPageId());
    }
    return;
  }
  // If too few pointers
  if (cur->GetSize() < cur->GetMinSize()) {
    // Find previous / next child
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->GetParentPageId())->GetData());
    // LOG_DEBUG("Parent page id: %d - key: %lld", parent->GetPageId(), key.ToString());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] =
        FindSibling<KeyType, page_id_t, KeyComparator>(parent->GetSize(), parent->GetMappingType(), cur->GetPageId());
    // LOG_DEBUG("splitKey: %lld", splitKey.ToString());
    int sibling_page_id = parent->GetMappingType()[siblingIdxInParent].second;
    auto *sibling = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(sibling_page_id)->GetData());
    // LOG_DEBUG("Sibling page id: %d - key: %lld", sibling->GetPageId(), key.ToString());
    // If fit in same tree page
    if (cur->GetSize() + sibling->GetSize() <= cur->GetMaxSize()) {
      // Swap if sibling is after parent
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling, cur);
      }
      // LOG_DEBUG("Merge: %d - %lld - %d", cur->GetPageId(), splitKey.ToString(), sibling->GetPageId());
      for (int i = 0; i < cur->GetSize(); i += 1) {
        sibling->GetMappingType()[sibling->GetSize() + i] = cur->GetMappingType()[i];
        // Redirect children's parent after merge
        auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(
            buffer_pool_manager_->FetchPage(cur->GetMappingType()[i].second)->GetData());
        child_tree_page->SetParentPageId(sibling->GetPageId());
        buffer_pool_manager_->UnpinPage(child_tree_page->GetPageId(), true);
      }
      sibling->IncreaseSize(cur->GetSize());
      // Unpin
      page_id_t cur_page_id = cur->GetPageId();
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      // Recursive
      RemoveInternalEntry(parent, splitKey, transaction);
      // Delete current node
      bool deleted = buffer_pool_manager_->DeletePage(cur_page_id);
      BUSTUB_ASSERT(deleted, "Cannot delete current page");
    } else {
      // Redistribution
      if (isPredecessor) {
        // Predecessor
        // LOG_DEBUG("Redistribution predecessor");
        std::pair<KeyType, page_id_t> last = sibling->GetMappingType()[sibling->GetSize() - 1];
        sibling->IncreaseSize(-1);
        for (int i = cur->GetSize(); i > 0; i -= 1) {
          cur->GetMappingType()[i] = cur->GetMappingType()[i - 1];
        }
        cur->GetMappingType()[0] = last;
        cur->GetMappingType()[1].first = splitKey;
        cur->IncreaseSize(1);
        // redirect child
        auto *child_tree_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(last.second)->GetData());
        child_tree_page->SetParentPageId(cur->GetPageId());
        buffer_pool_manager_->UnpinPage(child_tree_page->GetPageId(), true);
        // replace split key in parent
        parent->GetMappingType()[curIndexInParent].first = last.first;
      } else {
        // Successor
        // LOG_DEBUG("Redistribution successor");
        std::pair<KeyType, page_id_t> first = sibling->GetMappingType()[0];
        for (int i = 0; i < sibling->GetSize() - 1; i += 1) {
          sibling->GetMappingType()[i] = sibling->GetMappingType()[i + 1];
        }
        sibling->IncreaseSize(-1);
        cur->GetMappingType()[cur->GetSize()] = first;
        cur->IncreaseSize(1);
        // redirect child
        auto *child_tree_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(first.second)->GetData());
        child_tree_page->SetParentPageId(cur->GetPageId());
        buffer_pool_manager_->UnpinPage(child_tree_page->GetPageId(), true);
        // replace split key in parent
        parent->GetMappingType()[siblingIdxInParent].first = sibling->GetMappingType()[0].first;
      }
      // Unpin
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    return;
  }
  buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  // LOG_DEBUG("Start: %lld", key.ToString());
  LeafPage *cur = DownToLeaf(key, transaction);
  RemoveLeafEntry(cur, key, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
