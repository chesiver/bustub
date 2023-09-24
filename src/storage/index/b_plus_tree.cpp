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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::downToLeaf(const KeyType &key, Transaction *transaction) -> LeafPage* {
   BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
   while (!cur_page->IsLeafPage()) {
    InternalPage *cur_page_as_internal = static_cast<InternalPage*>(cur_page);
    std::pair<KeyType, page_id_t> *entries = cur_page_as_internal->GetMappingType();
    int size = cur_page_as_internal->GetSize();
    int p = 1, q = size;
    while (p < q) {
      int mid = (p + q) / 2;
      if (comparator_(entries[mid].first, key) <= 0) {
        p = mid + 1;
      } else {
        q = mid;
      }
    }
    page_id_t pid = entries[p - 1].second;
    cur_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(pid)->GetData());
  }
  LeafPage *cur_page_as_leaf = reinterpret_cast<LeafPage*>(cur_page);
  return cur_page_as_leaf;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::searchInLeaf(LeafPage *leaf, const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  MappingType *entries = leaf->GetMappingType();
  int size = leaf->GetSize();
  int p = 0, q = size;
  while (p < q) {
    int mid = (p + q) / 2;
    if (comparator_(entries[mid].first, key) < 0) {
      p = mid + 1;
    } else {
      q = mid;
    }
  }
  if (p < size && comparator_(entries[p].first, key) == 0) {
    *result = std::vector<ValueType>{entries[p].second};
    return true;
  } else {
    return false;
  }
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
  LeafPage *leafPage = downToLeaf(key, transaction);
  return searchInLeaf(leafPage, key, result, transaction);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::insertOnLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, const KeyType &key, const ValueType &value) {
  LOG_DEBUG("Start %lld", key.ToString());
  std::pair<KeyType, ValueType> *entries = leaf->GetMappingType();
  int size = leaf->GetSize();
  int p = 0, q = size;
  while (p < q) {
    int mid = (p + q) / 2;
    if (comparator_(entries[mid].first, key) <= 0) {
      p = mid + 1;
    } else {
      q = mid;
    }
  }
  for (int i = size - 1; i >= p; i -= 1) {
    entries[i + 1] = entries[i];
  }
  entries[p] = std::make_pair(key, value);
  leaf->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::insertOnParent(BPlusTreePage *left, const KeyType &key, BPlusTreePage *right) {
  LOG_DEBUG("Start %lld", key.ToString());
  // Is Root
  if (left->GetPageId() == root_page_id_) {
    LOG_DEBUG("Create Root Page: %d - %lld - %d", left->GetPageId(), key.ToString(), right->GetPageId());
    // Create root page
    page_id_t new_root_page_id;
    Page *root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    // Create B+ tree page
    InternalPage *new_root = reinterpret_cast<InternalPage*>(root_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetSize(2);
    new_root->GetMappingType()[0] = std::make_pair(KeyType{}, left->GetPageId());
    new_root->GetMappingType()[1] = std::make_pair(key, right->GetPageId());
    left->SetParentPageId(new_root_page_id);
    right->SetParentPageId(new_root_page_id);
    // Set root
    root_page_id_ = new_root_page_id;
    return;
  }
  // Normal
  InternalPage *parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(left->GetParentPageId()));
  std::pair<KeyType, page_id_t> *entries = parent->GetMappingType();
  int size = parent->GetSize();
  if (size < parent->GetMaxSize()) {
    // Insert directly
    LOG_DEBUG("Insert Directly: %lld", key.ToString());
    int i = size;
    while (i >= 0 && comparator_(entries[i - 1].first, key) > 0) {
      entries[i] = entries[i - 1];
      i -= 1;
    }
    entries[i] = std::make_pair(key, right->GetPageId());
    parent->IncreaseSize(1);
  } else {
    // Split
    LOG_DEBUG("Split: %lld", key.ToString());
    std::vector<std::pair<KeyType, page_id_t>> tmp(size + 1);
    int i = 0;
    while (i == 0 || (i < size && comparator_(entries[i].first, key) < 0)) {
      tmp[i] = entries[i];
      i += 1;
    }
    tmp[i++] = std::make_pair(key, right->GetPageId());
    while (i <= size) {
      tmp[i] = entries[i - 1];
      i += 1;
    }
    for (int i = 0; i < size + 1; i += 1) {
       LOG_DEBUG("tmp: %lld", tmp[i].first.ToString());
    }
    // left
    int half = (size + 1 + 1) / 2;
    parent->SetSize(half);
    for (int i = 0; i < half; i += 1) {
      entries[i] = tmp[i];
    }
    // right
    page_id_t next_to_parent_page_id;
    Page *next_to_parent_page = buffer_pool_manager_->NewPage(&next_to_parent_page_id);
    InternalPage *next_to_parent = reinterpret_cast<InternalPage*>(next_to_parent_page->GetData());
    next_to_parent->Init(next_to_parent_page_id, parent->GetParentPageId(), internal_max_size_);
    next_to_parent->SetSize(size + 1 - half);
    // redirect parent
    for (int i = half; i < size + 1; i += 1) {
      BPlusTreePage *child = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(tmp[i].second)->GetData());
      child->SetParentPageId(next_to_parent_page_id);
    }
    for (int i = half; i < size + 1; i += 1) {
        next_to_parent->GetMappingType()[i - half] = tmp[i];
    }
    // recursive
    const KeyType& parentSplitKey = next_to_parent->GetMappingType()[0].first;
    insertOnParent(parent, parentSplitKey, next_to_parent);
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
  LOG_DEBUG("Enter: %lld", key.ToString());
  // Search leaf
  LeafPage* cur_tree_page;
  if (IsEmpty()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    cur_tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
    cur_tree_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    cur_tree_page = downToLeaf(key, transaction);
  }
  // Check duplicate
  LOG_DEBUG("Check Duplicate %lld", key.ToString());
  std::vector<ValueType> tmp;
  if (searchInLeaf(cur_tree_page, key, &tmp, transaction)) {
    return false;
  }
  // Insert
  LOG_DEBUG("Insert: %lld", key.ToString());
  int size = cur_tree_page->GetSize();
  std::pair<KeyType, ValueType> *entries = cur_tree_page->GetMappingType();
  if (size < cur_tree_page->GetMaxSize()) {
    insertOnLeaf(cur_tree_page, key, value);
  } else {
    // Split
    LOG_DEBUG("Split: %lld", key.ToString());
    std::vector<std::pair<KeyType, ValueType>> tmp(size + 1);
    int i = 0;
    while (i < size && comparator_(entries[i].first, key) < 0) {
      tmp[i] = entries[i];
      i += 1;
    }
    tmp[i++] = std::make_pair(key, value);
    while (i <= size) {
      tmp[i] = entries[i - 1];
      i += 1;
    }
    // left
    int half = (size + 1 + 1) / 2;
    LOG_DEBUG("half: %d", half);
    cur_tree_page->SetSize(half);
    for (int i = 0; i < half; i += 1) {
      entries[i] = tmp[i];
    }
    // right
    page_id_t next_to_cur_page_id;
    Page *next_to_cur_page = buffer_pool_manager_->NewPage(&next_to_cur_page_id);
    LeafPage *next_to_cur = reinterpret_cast<LeafPage*>(next_to_cur_page->GetData());
    next_to_cur->Init(next_to_cur_page_id, cur_tree_page->GetParentPageId(), leaf_max_size_);
    next_to_cur->SetSize(size + 1 - half);
    for (int i = half; i < size + 1; i += 1) {
        next_to_cur->GetMappingType()[i - half] = tmp[i];
    }
    next_to_cur->SetNextPageId(cur_tree_page->GetNextPageId());
    cur_tree_page->SetNextPageId(next_to_cur_page_id);
    // recursive
    const KeyType& parentSplitKey = next_to_cur->GetMappingType()[0].first;
    insertOnParent(cur_tree_page, parentSplitKey, next_to_cur);
  }
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
bool removeEntryLocally(int size, MappingType *entries, const KeyType &key, const KeyComparator &comparator) {
  int idx = 0;
  for (; idx < size && comparator(entries[idx].first, key) != 0; idx += 1);
  if (idx >= size) {
    return false;
  }
  for (int i = idx; i < size; i += 1) {
    entries[i] = entries[i + 1];
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
std::tuple<int, int, bool, KeyType> findSibling(int size, MappingType *parentEntries, const ValueType &value) {
  int idx = 0;
  for (; idx < size && parentEntries[idx].second != value; idx += 1);
  BUSTUB_ASSERT(idx < size, "Cannot find sibling");
  if (idx > 0) {
    return {idx, idx - 1, true, parentEntries[idx].first};
  } else {
    return {idx, idx + 1, false, parentEntries[idx + 1].first};
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::removeLeafEntry(LeafPage *cur, const KeyType &key, Transaction *transaction) {
  LOG_DEBUG("Page id: %d - key: %lld", cur->GetPageId(), key.ToString());
  // Delete in current node
  bool removed = removeEntryLocally(cur->GetSize(), cur->GetMappingType(), key, comparator_);
  if (!removed) {
    return;
  }
  cur->IncreaseSize(-1);
  // If too few pointers
  if (cur->GetSize() < cur->GetMinSize()) {
    // If Root - No sibling
    if (cur->IsRootPage()) {
      return;
    }
    // Find previous / next child
    InternalPage *parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(cur->GetParentPageId())->GetData());
    LOG_DEBUG("Parent page id: %d - key: %lld", parent->GetPageId(), key.ToString());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] = findSibling<KeyType, page_id_t, KeyComparator>(parent->GetSize(), parent->GetMappingType(), cur->GetPageId());
    int siblingPageId = parent->GetMappingType()[siblingIdxInParent].second;
    LeafPage *sibling = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(siblingPageId)->GetData());
    LOG_DEBUG("Sibling page id: %d - key: %lld", sibling->GetPageId(), key.ToString());
    // If fit in same tree page
    if (cur->GetSize() + sibling->GetSize() <= cur->GetMaxSize()) {
      // Swap if sibling is after parent
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling, cur);
      }
      LOG_DEBUG("Sibling page id: %d - key: %lld", sibling->GetPageId(), key.ToString());
      // Merge
      for (int i = 0; i < cur->GetSize(); i += 1) {
        sibling->GetMappingType()[sibling->GetSize() + i] = cur->GetMappingType()[i];
      }
      sibling->IncreaseSize(cur->GetSize());
      sibling->SetNextPageId(cur->GetNextPageId());
      // Recursive
      removeInternalEntry(parent, splitKey, transaction);
      // Delete current node
      buffer_pool_manager_->DeletePage(cur->GetPageId());
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
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::removeInternalEntry(InternalPage *cur, const KeyType &key, Transaction *transaction) {
  // Delete in current node
  bool removed = removeEntryLocally<KeyType, page_id_t, KeyComparator>(cur->GetSize(), cur->GetMappingType(), key, comparator_);
  if (!removed) {
    return;
  }
  cur->IncreaseSize(-1);
  // If root
  if (cur->IsRootPage()) {
    // If only one child
    if (cur->GetSize() == 1) {
      // redirect new root if current one is not lead page
      BPlusTreePage *child = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(cur->GetMappingType()[0].second)->GetData());
      root_page_id_ = child->GetPageId();
      child->SetParentPageId(INVALID_PAGE_ID);
      // delete page
      buffer_pool_manager_->DeletePage(cur->GetPageId());
    }
    return;
  }
  // If too few pointers
  if (cur->GetSize() < cur->GetMinSize()) {
    // Find previous / next child
    InternalPage *parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(cur->GetParentPageId())->GetData());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] = findSibling<KeyType, page_id_t, KeyComparator>(parent->GetSize(), parent->GetMappingType(), cur->GetPageId());
    int siblingPageId = parent->GetMappingType()[siblingIdxInParent].second;
    InternalPage *sibling = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(siblingPageId)->GetData());
    // If fit in same tree page
    if (cur->GetSize() + sibling->GetSize() <= cur->GetMaxSize()) {
      // Swap if sibling is after parent
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling, cur);
      }
      // Merge
      for (int i = 0; i < cur->GetSize(); i += 1) {
        if (i == 0) {
          sibling->GetMappingType()[sibling->GetSize() + i] = std::make_pair(splitKey, cur->GetMappingType()[i].second);
        } else {
          sibling->GetMappingType()[sibling->GetSize() + i] = cur->GetMappingType()[i];
        }
        // Redirect children's parent after merge
        BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(cur->GetMappingType()[i].second)->GetData());
        childTreePage->SetParentPageId(sibling->GetPageId());
      }
      sibling->IncreaseSize(cur->GetSize());
      // Recursive
      removeInternalEntry(parent, splitKey, transaction);
      // Delete current node
      buffer_pool_manager_->DeletePage(cur->GetPageId());
    } else {
      // Redistribution
      if (isPredecessor) {
        // Predecessor
        std::pair<KeyType, page_id_t> last = sibling->GetMappingType()[sibling->GetSize() - 1];
        sibling->IncreaseSize(-1);
        for (int i = cur->GetSize(); i > 0; i -= 1) {
          cur->GetMappingType()[i] = cur->GetMappingType()[i - 1];
        }
        cur->GetMappingType()[0] = last;
        cur->GetMappingType()[1].first = splitKey;
        cur->IncreaseSize(1);
        // replace split key in parent
        parent->GetMappingType()[curIndexInParent].first = last.first;
      } else {
        // Successor
        std::pair<KeyType, page_id_t> first = sibling->GetMappingType()[0];
        for (int i = 0; i < sibling->GetSize() - 1; i += 1) {
          sibling->GetMappingType()[i] = sibling->GetMappingType()[i + 1];
        }
        sibling->IncreaseSize(-1);
        cur->GetMappingType()[cur->GetSize()] = first;
        cur->IncreaseSize(1);
        // replace split key in parent
        parent->GetMappingType()[siblingIdxInParent].first = sibling->GetMappingType()[0].first;
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  LOG_DEBUG("Start: %lld", key.ToString());
  LeafPage *cur = downToLeaf(key, transaction);
  removeLeafEntry(cur, key, transaction);
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
