#include <string>
#include <tuple>

#include "common/exception.h"
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
auto FindEntriesWithKeyLargerThan(int size, MappingType *entries, const KeyType &key, KeyComparator comparator) -> int {
  int p = 0;
  int q = size;
  while (p < q) {
    int mid = (p + q) / 2;
    if (comparator(entries[mid].first, key) <= 0) {
      p = mid + 1;
    } else {
      q = mid;
    }
  }
  return p;
}

auto GetPageFromTransactionPageSet(page_id_t page_id, Transaction *transaction) -> Page * {
  auto pages = transaction->GetPageSet();
  for (auto it = pages->rbegin(); it != pages->rend(); ++it) {
    Page *page = *it;
    if (page->GetPageId() == page_id) {
      return page;
    }
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeEntries(BPlusTreePage *tree_page, Operation op) -> bool {
  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() < tree_page->GetMaxSize() - 1;
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }
  if (op == Operation::Remove) {
    if (tree_page->IsRootPage()) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    return tree_page->GetSize() > tree_page->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreatePageWithSpin(page_id_t *page_id) -> Page * {
  Page *page;
  while ((page = buffer_pool_manager_->NewPage(page_id)) == nullptr) {
    // MY_LOG_DEBUG("All frames are pinned when create new page. Spin....");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleasePage(Page *page, bool dirty_flag) {
  page_id_t page_id = page->GetPageId();
  if (dirty_flag) {
    // MY_LOG_DEBUG("write: page id: {} --- page pin count: {}", page->GetPageId(), page->GetPinCount());
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
  } else {
    // MY_LOG_DEBUG("read: page id: {} --- page pin count: {}", page->GetPageId(), page->GetPinCount());
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction) {
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      // MY_LOG_DEBUG("Released root latch");
      root_latch_.WUnlock();
    } else {
      ReleasePage(page, true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DownToLeafForRead(const KeyType &key, Transaction *transaction) -> Page * {
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // MY_LOG_DEBUG("Fetched for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  BUSTUB_ASSERT(cur_page != nullptr, "cannot fetch page");
  cur_page->RLatch();
  // MY_LOG_DEBUG("Obtain read latch for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    int p = FindEntriesWithKeyLargerThan<KeyType, page_id_t, KeyComparator>(
        cur_tree_page_as_internal->GetSize(), cur_tree_page_as_internal->GetEntries(), key, comparator_);
    p -= 1;
    page_id_t next_pid = cur_tree_page_as_internal->GetEntries()[p].second;
    /* Special root latch unlocked */
    if (cur_tree_page->IsRootPage()) {
      root_latch_.RUnlock();
    }
    /* Next */
    Page *next_page = buffer_pool_manager_->FetchPage(next_pid);
    BUSTUB_ASSERT(next_page != nullptr, "cannot fetch page");
    // MY_LOG_DEBUG("fetched for page id: {} - key: {}", next_pid, key.ToString());
    next_page->RLatch();
    // MY_LOG_DEBUG("Obtain read latch for page id: {} - key: {}", next_page->GetPageId(), key.ToString());
    /* Unpin */
    ReleasePage(cur_page, false);
    cur_page = next_page;
    cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }
  return cur_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticDownToLeafForWrite(const KeyType &key, Transaction *transaction) -> Page * {
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  if (cur_tree_page->IsLeafPage()) {
    cur_page->WLatch();
    root_latch_.RUnlock();
    transaction->AddIntoPageSet(cur_page);
    return cur_page;
  }
  // MY_LOG_DEBUG("Fetched for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  BUSTUB_ASSERT(cur_page != nullptr, "cannot fetch page");
  cur_page->RLatch();
  // MY_LOG_DEBUG("Obtain read latch for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    int p = FindEntriesWithKeyLargerThan<KeyType, page_id_t, KeyComparator>(
        cur_tree_page_as_internal->GetSize(), cur_tree_page_as_internal->GetEntries(), key, comparator_);
    p -= 1;
    page_id_t next_pid = cur_tree_page_as_internal->GetEntries()[p].second;
    /* Special root latch unlocked */
    if (cur_tree_page->IsRootPage()) {
      root_latch_.RUnlock();
    }
    /* Next */
    Page *next_page = buffer_pool_manager_->FetchPage(next_pid);
    BUSTUB_ASSERT(next_page != nullptr, "cannot fetch page");
    auto *next_tree_page = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    // MY_LOG_DEBUG("fetched for page id: {} - key: {}", next_pid, key.ToString());
    if (next_tree_page->IsLeafPage()) {
      next_page->WLatch();
      transaction->AddIntoPageSet(next_page);
    } else {
      next_page->RLatch();
    }
    /* Unpin */
    ReleasePage(cur_page, false);
    cur_page = next_page;
    cur_tree_page = next_tree_page;
  }
  return cur_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DownToLeafForWrite(const KeyType &key, Transaction *transaction, Operation op) -> Page * {
  /* Optimism */
  {
    // MY_LOG_DEBUG("Start optimism - key: {}", key.ToString());
    Page *cur_page = OptimisticDownToLeafForWrite(key, transaction);
    BUSTUB_ASSERT(cur_page != nullptr, "cannot fetch page");
    auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    if (IsSafeEntries(cur_tree_page, op)) {
      // MY_LOG_DEBUG("Is safe by optimism - key: {}", key.ToString());
      return cur_page;
    }
    // MY_LOG_DEBUG("Not safe. Prepare to release - key: {}", key.ToString());
    ReleaseWLatches(transaction);
  }
  // MY_LOG_DEBUG("Not safe by optimism. Start second round - key: {}", key.ToString());
  root_latch_.WLock();
  // MY_LOG_DEBUG("Obtain root write latch - key: {}", key.ToString());
  transaction->AddIntoPageSet(nullptr);
  // MY_LOG_DEBUG("Second round. Obtain root write latch - key: {}", key.ToString());
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // MY_LOG_DEBUG("Fetched for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  BUSTUB_ASSERT(cur_page != nullptr, "cannot fetch page");
  cur_page->WLatch();
  transaction->AddIntoPageSet(cur_page);
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    int p = FindEntriesWithKeyLargerThan<KeyType, page_id_t, KeyComparator>(
        cur_tree_page_as_internal->GetSize(), cur_tree_page_as_internal->GetEntries(), key, comparator_);
    p -= 1;
    page_id_t next_pid = cur_tree_page_as_internal->GetEntries()[p].second;
    cur_page = buffer_pool_manager_->FetchPage(next_pid);
    // MY_LOG_DEBUG("fetched for page id: {} - key: {}", next_pid, key.ToString());
    BUSTUB_ASSERT(cur_page != nullptr, "cannot fetch page");
    cur_page->WLatch();
    cur_tree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    if (IsSafeEntries(cur_tree_page, op)) {
      // MY_LOG_DEBUG("is safe for page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
      ReleaseWLatches(transaction);
    }
    transaction->AddIntoPageSet(cur_page);
  }
  return cur_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInLeaf(LeafPage *leaf, const KeyType &key, std::vector<ValueType> *result,
                                  Transaction *transaction) -> bool {
  int p = FindEntriesWithKeyLargerThan(leaf->GetSize(), leaf->GetEntries(), key, comparator_) - 1;
  if (p < 0 || comparator_(leaf->GetEntries()[p].first, key) != 0) {
    return false;
  }
  *result = std::vector<ValueType>{};
  ValueType v = leaf->GetEntries()[p].second;
  result->push_back(v);
  return true;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // MY_LOG_DEBUG("Enter {}", key.ToString());
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }
  /* Search leaf. Will latch on leaf */
  Page *leaf_page = DownToLeafForRead(key, transaction);
  auto leaf_tree_page = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // MY_LOG_DEBUG("Find leaf page id: {} - key: {}", leaf_page->GetPageId(), key.ToString());
  bool found = SearchInLeaf(leaf_tree_page, key, result, transaction);
  /* Special release for root latch */
  if (leaf_tree_page->IsRootPage()) {
    root_latch_.RUnlock();
  }
  ReleasePage(leaf_page, false);
  // MY_LOG_DEBUG("Finish {}", key.ToString());
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void SplitEntries(int size, MappingType *left, MappingType *right, const MappingType &entry, KeyComparator comparator) {
  std::vector<MappingType> tmp(size + 1);
  int idx = size;
  while (idx > 0 && comparator(left[idx - 1].first, entry.first) > 0) {
    tmp[idx] = left[idx - 1];
    idx -= 1;
  }
  tmp[idx--] = entry;
  while (idx >= 0) {
    tmp[idx] = left[idx];
    idx -= 1;
  }
  int half = (size + 1) / 2;
  for (int i = 0; i < half; i += 1) {
    left[i] = tmp[i];
  }
  for (int i = half; i < size + 1; i += 1) {
    right[i - half] = tmp[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertOnLeaf(LeafPage *leaf, const KeyType &key, const ValueType &value) {
  int p = FindEntriesWithKeyLargerThan(leaf->GetSize(), leaf->GetEntries(), key, comparator_);
  for (int i = leaf->GetSize() - 1; i >= p; i -= 1) {
    leaf->GetEntries()[i + 1] = leaf->GetEntries()[i];
  }
  leaf->GetEntries()[p] = std::make_pair(key, value);
  leaf->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertOnParent(Page *left_page, const KeyType &key, Page *right_page, Transaction *transaction) {
  // MY_LOG_DEBUG("Start left: {} - key: {} - right: {}", left_page->GetPageId(), key.ToString(),
  // right_page->GetPageId());
  auto left_tree_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
  auto right_tree_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
  /* Is Root */
  if (left_tree_page->IsRootPage()) {
    /* Create root page */
    page_id_t new_root_page_id;
    Page *root_page = CreatePageWithSpin(&new_root_page_id);
    // MY_LOG_DEBUG("Create root page id: {} - key: {}", new_root_page_id, key.ToString());
    BUSTUB_ASSERT(root_page != nullptr, "Fail to create root page - Check if all pages are pinned");
    /* Create B+ tree page */
    auto *new_root = reinterpret_cast<InternalPage *>(root_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetSize(2);
    new_root->GetEntries()[0] = std::make_pair(KeyType{}, left_tree_page->GetPageId());
    new_root->GetEntries()[1] = std::make_pair(key, right_tree_page->GetPageId());
    left_tree_page->SetParentPageId(new_root_page_id);
    right_tree_page->SetParentPageId(new_root_page_id);
    /* Set root */
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(false);
    /* Unpin */
    buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
    return;
  }
  // Normal
  Page *parent_page = GetPageFromTransactionPageSet(left_tree_page->GetParentPageId(), transaction);
  auto *parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int size = parent_tree_page->GetSize();
  if (size < parent_tree_page->GetMaxSize()) {
    // MY_LOG_DEBUG("Insert directly: {}", key.ToString());
    /* Insert directly */
    int i = size;
    while (i > 0 && comparator_(parent_tree_page->GetEntries()[i - 1].first, key) > 0) {
      parent_tree_page->GetEntries()[i] = parent_tree_page->GetEntries()[i - 1];
      i -= 1;
    }
    parent_tree_page->GetEntries()[i] = std::make_pair(key, right_tree_page->GetPageId());
    parent_tree_page->IncreaseSize(1);
    // MY_LOG_DEBUG("Finish insert directly: {}", key.ToString());
    return;
  }
  // MY_LOG_DEBUG("Split before insert: {}", key.ToString());
  /* Split */
  /* Create right page */
  page_id_t next_to_parent_page_id;
  Page *next_to_parent_page = CreatePageWithSpin(&next_to_parent_page_id);
  // MY_LOG_DEBUG("Create next to current page id: {} - key: {}", next_to_parent_page_id, key.ToString());
  BUSTUB_ASSERT(next_to_parent_page != nullptr, "Fail to create new right page - Check if all pages are pinned");
  /* Lock right page */
  next_to_parent_page->WLatch();
  auto *next_to_parent_tree_page = reinterpret_cast<InternalPage *>(next_to_parent_page->GetData());
  next_to_parent_tree_page->Init(next_to_parent_page_id, parent_tree_page->GetParentPageId(), internal_max_size_);
  /* Split entries */
  SplitEntries<KeyType, page_id_t, KeyComparator>(size, parent_tree_page->GetEntries(),
                                                  next_to_parent_tree_page->GetEntries(),
                                                  std::make_pair(key, right_tree_page->GetPageId()), comparator_);
  parent_tree_page->SetSize((size + 1) / 2);
  next_to_parent_tree_page->SetSize(size + 1 - parent_tree_page->GetSize());
  /* Split key */
  const KeyType parent_split_key = next_to_parent_tree_page->GetEntries()[0].first;
  /* Redirect parent - acquire child's latch */
  /* Redirect parent */
  for (int i = 0; i < next_to_parent_tree_page->GetSize(); i += 1) {
    Page *child_page = buffer_pool_manager_->FetchPage(next_to_parent_tree_page->GetEntries()[i].second);
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_tree_page->SetParentPageId(next_to_parent_page_id);
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
  /* Recursive */
  InsertOnParent(parent_page, parent_split_key, next_to_parent_page, transaction);
  /* Release */
  ReleasePage(next_to_parent_page, true);
  // MY_LOG_DEBUG("Finish split for insert: {}", key.ToString());
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
  // MY_LOG_DEBUG("Enter: {}", key.ToString());
  /* Lock */
  root_latch_.RLock();
  // MY_LOG_DEBUG("Obtain root read latch - key: {}", key.ToString());
  Page *cur_page;
  LeafPage *cur_tree_page;
  if (IsEmpty()) {
    root_latch_.RUnlock();
    root_latch_.WLock();
    // MY_LOG_DEBUG("Obtain root write latch - key: {}", key.ToString());
    transaction->AddIntoPageSet(nullptr);
    if (IsEmpty()) {
      cur_page = CreatePageWithSpin(&root_page_id_);
      BUSTUB_ASSERT(cur_page != nullptr, "Fail to create new Page - Check if all pages are pinned");
      // MY_LOG_DEBUG("Creat root page when empty - key: {} - root page id: {}", key.ToString(), root_page_id_);
      cur_page->WLatch();
      transaction->AddIntoPageSet(cur_page);
      cur_tree_page = reinterpret_cast<LeafPage *>(cur_page->GetData());
      cur_tree_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      UpdateRootPageId(true);
    } else {
      /* When waiting for write latch. Other threads insert some keys */
      ReleaseWLatches(transaction);
      root_latch_.RLock();
      // MY_LOG_DEBUG("Obtain root read latch - key: {}", key.ToString());
      cur_page = DownToLeafForWrite(key, transaction, Operation::Insert);
      cur_tree_page = reinterpret_cast<LeafPage *>(cur_page->GetData());
    }
  } else {
    cur_page = DownToLeafForWrite(key, transaction, Operation::Insert);
    cur_tree_page = reinterpret_cast<LeafPage *>(cur_page->GetData());
  }
  // MY_LOG_DEBUG("Found leaf page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  /* Check duplicate */
  std::vector<ValueType> tmp;
  if (SearchInLeaf(cur_tree_page, key, &tmp, transaction)) {
    ReleaseWLatches(transaction);
    // MY_LOG_DEBUG("Found duplicate - key: {}. Return", key.ToString());
    return false;
  }
  /* Insert */
  // MY_LOG_DEBUG("Do insert - key: {}", key.ToString());
  int size = cur_tree_page->GetSize();
  if (size < cur_tree_page->GetMaxSize() - 1) {
    // MY_LOG_DEBUG("Directly insert - key: {}", key.ToString());
    InsertOnLeaf(cur_tree_page, key, value);
    ReleaseWLatches(transaction);
    // MY_LOG_DEBUG("Finish Directly insert - key: {}", key.ToString());
    return true;
  }
  // MY_LOG_DEBUG("Split before insert - key: {}", key.ToString());
  /* Split */
  /* Create right page */
  page_id_t next_to_cur_page_id;
  Page *next_to_cur_page = CreatePageWithSpin(&next_to_cur_page_id);
  BUSTUB_ASSERT(next_to_cur_page != nullptr, "Fail to create new Page - Check if all pages are pinned");
  // MY_LOG_DEBUG("Create next to current page id: {}", next_to_cur_page_id);
  /* Lock right page */
  next_to_cur_page->WLatch();
  /* Init right page */
  auto *next_to_cur_tree_page = reinterpret_cast<LeafPage *>(next_to_cur_page->GetData());
  next_to_cur_tree_page->Init(next_to_cur_page_id, cur_tree_page->GetParentPageId(), leaf_max_size_);
  /* Split entries */
  SplitEntries(size, cur_tree_page->GetEntries(), next_to_cur_tree_page->GetEntries(), std::make_pair(key, value),
               comparator_);
  cur_tree_page->SetSize((size + 1) / 2);
  next_to_cur_tree_page->SetSize(size + 1 - cur_tree_page->GetSize());
  /* Split key */
  const KeyType parent_split_key = next_to_cur_tree_page->GetEntries()[0].first;
  /* Redirect next page id */
  next_to_cur_tree_page->SetNextPageId(cur_tree_page->GetNextPageId());
  cur_tree_page->SetNextPageId(next_to_cur_page_id);
  /* Recursive */
  InsertOnParent(cur_page, parent_split_key, next_to_cur_page, transaction);
  /* Release */
  ReleasePage(next_to_cur_page, true);
  ReleaseWLatches(transaction);
  // MY_LOG_DEBUG("Finish Split for insert: {}", key.ToString());
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
  BUSTUB_ASSERT(idx < size, "Cannot find index in parent");
  if (idx < size - 1) {
    return {idx, idx + 1, false, parentEntries[idx + 1].first};
  }
  return {idx, idx - 1, true, parentEntries[idx].first};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLeafEntry(Page *cur_page, const KeyType &key, Transaction *transaction) {
  // MY_LOG_DEBUG("Page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  /* Delete in current node */
  auto cur_tree_page = reinterpret_cast<LeafPage *>(cur_page->GetData());
  bool removed = RemoveEntryLocally(cur_tree_page->GetSize(), cur_tree_page->GetEntries(), key, comparator_);
  if (!removed) {
    return;
  }
  cur_tree_page->IncreaseSize(-1);
  /* If too few pointers */
  if (cur_tree_page->GetSize() < cur_tree_page->GetMinSize()) {
    /* If Root - No sibling */
    if (cur_tree_page->IsRootPage()) {
      /* Set to be empty is current size is zero */
      if (cur_tree_page->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(false);
        transaction->AddIntoDeletedPageSet(cur_tree_page->GetPageId());
      }
      return;
    }
    /* Find previous / next child */
    Page *parent_page = GetPageFromTransactionPageSet(cur_tree_page->GetParentPageId(), transaction);
    // MY_LOG_DEBUG("Parent page id: {} - key: {}", parent_page->GetPageId(), key.ToString());
    /* Get index */
    auto *parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] =
        FindSibling<KeyType, page_id_t, KeyComparator>(parent_tree_page->GetSize(), parent_tree_page->GetEntries(),
                                                       cur_tree_page->GetPageId());
    // MY_LOG_DEBUG("splitKey: {}", splitKey.ToString());
    /* Get sibling page */
    page_id_t sibling_page_id = parent_tree_page->GetEntries()[siblingIdxInParent].second;
    Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    /* Must put write lock on sibling*/
    sibling_page->WLatch();
    auto *sibling_tree_page = reinterpret_cast<LeafPage *>(sibling_page->GetData());
    // MY_LOG_DEBUG("Sibling page id: {} - key: {}", sibling_tree_page->GetPageId(), key.ToString());
    /* If fit in same tree page */
    if (cur_tree_page->GetSize() + sibling_tree_page->GetSize() < cur_tree_page->GetMaxSize()) {
      /* Swap if sibling is after parent */
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling_page, cur_page);
        std::swap(sibling_tree_page, cur_tree_page);
      }
      // MY_LOG_DEBUG("Merge: {} - {} - {}", cur_page->GetPageId(), splitKey.ToString(), sibling_page->GetPageId());
      for (int i = 0; i < cur_tree_page->GetSize(); i += 1) {
        sibling_tree_page->GetEntries()[sibling_tree_page->GetSize() + i] = cur_tree_page->GetEntries()[i];
      }
      sibling_tree_page->IncreaseSize(cur_tree_page->GetSize());
      sibling_tree_page->SetNextPageId(cur_tree_page->GetNextPageId());
      /* Set not transaction latched page */
      Page *not_transaction_latched_page = isPredecessor ? sibling_page : cur_page;
      /* Recursive */
      RemoveInternalEntry(parent_page, splitKey, transaction);
      /* Add node to delete set */
      // MY_LOG_DEBUG("Add deleted page id: {} to deleted set", cur_page->GetPageId());
      transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
      /* Release not transaction latched page */
      ReleasePage(not_transaction_latched_page, true);
    } else {
      /* Redistribution */
      if (isPredecessor) {
        /* Predecessor */
        MappingType last = sibling_tree_page->GetEntries()[sibling_tree_page->GetSize() - 1];
        sibling_tree_page->IncreaseSize(-1);
        for (int i = cur_tree_page->GetSize(); i > 0; i -= 1) {
          cur_tree_page->GetEntries()[i] = cur_tree_page->GetEntries()[i - 1];
        }
        cur_tree_page->GetEntries()[0] = last;
        cur_tree_page->IncreaseSize(1);
        /* Replace split key in parent */
        parent_tree_page->GetEntries()[curIndexInParent].first = last.first;
      } else {
        /* Successor */
        MappingType first = sibling_tree_page->GetEntries()[0];
        for (int i = 0; i < sibling_tree_page->GetSize() - 1; i += 1) {
          sibling_tree_page->GetEntries()[i] = sibling_tree_page->GetEntries()[i + 1];
        }
        sibling_tree_page->IncreaseSize(-1);
        cur_tree_page->GetEntries()[cur_tree_page->GetSize()] = first;
        cur_tree_page->IncreaseSize(1);
        /* Replace split key in parent */
        parent_tree_page->GetEntries()[siblingIdxInParent].first = sibling_tree_page->GetEntries()[0].first;
      }
      /* Unpin sibling */
      ReleasePage(sibling_page, true);
    }
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternalEntry(Page *cur_page, const KeyType &key, Transaction *transaction) {
  // MY_LOG_DEBUG("Start Page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
  /* Delete in current node */
  auto *cur_tree_page = reinterpret_cast<InternalPage *>(cur_page);
  bool removed = RemoveEntryLocally<KeyType, page_id_t, KeyComparator>(cur_tree_page->GetSize(),
                                                                       cur_tree_page->GetEntries(), key, comparator_);
  BUSTUB_ASSERT(removed, "cannot find key in internal entries");
  cur_tree_page->IncreaseSize(-1);
  // MY_LOG_DEBUG("Current page id: {} - size: {}", cur_page->GetPageId(), cur_tree_page->GetSize());
  /* If root */
  if (cur_tree_page->IsRootPage()) {
    /* If only one child */
    if (cur_tree_page->GetSize() == 1) {
      // MY_LOG_DEBUG("Only one child - page id: {} - key: {}", cur_page->GetPageId(), key.ToString());
      /* Child should be already locked */
      Page *child_page = buffer_pool_manager_->FetchPage(cur_tree_page->GetEntries()[0].second);
      auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      /* Reset root */
      root_page_id_ = child_tree_page->GetPageId();
      child_tree_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false);
      /* Unpin */
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      /* Add to deleted pages */
      transaction->AddIntoDeletedPageSet(cur_tree_page->GetPageId());
    }
    return;
  }
  /* If too few pointers */
  if (cur_tree_page->GetSize() < cur_tree_page->GetMinSize()) {
    /* Find previous / next child */
    Page *parent_page = GetPageFromTransactionPageSet(cur_tree_page->GetParentPageId(), transaction);
    BUSTUB_ASSERT(parent_page != nullptr, "cannot fetch parent page");
    // MY_LOG_DEBUG("Parent page id: {} - key: {}", parent_page->GetPageId(), key.ToString());
    /* Get index */
    auto *parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    auto [curIndexInParent, siblingIdxInParent, isPredecessor, splitKey] =
        FindSibling<KeyType, page_id_t, KeyComparator>(parent_tree_page->GetSize(), parent_tree_page->GetEntries(),
                                                       cur_tree_page->GetPageId());
    // MY_LOG_DEBUG("splitKey: {}", splitKey.ToString());
    /* Lock on sibling page */
    int sibling_page_id = parent_tree_page->GetEntries()[siblingIdxInParent].second;
    Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    sibling_page->WLatch();
    auto *sibling_tree_page = reinterpret_cast<InternalPage *>(sibling_page->GetData());
    // MY_LOG_DEBUG("Sibling page id: {} - key: {}", sibling_page->GetPageId(), key.ToString());
    /* If fit in same tree page */
    if (cur_tree_page->GetSize() + sibling_tree_page->GetSize() <= cur_tree_page->GetMaxSize()) {
      // Swap if sibling is after parent
      if (!isPredecessor) {
        std::swap(siblingIdxInParent, curIndexInParent);
        std::swap(sibling_page, cur_page);
        std::swap(sibling_tree_page, cur_tree_page);
      }
      // MY_LOG_DEBUG("Merge: {} - {} - {}", cur_page->GetPageId(), splitKey.ToString(), sibling_page->GetPageId());
      for (int i = 0; i < cur_tree_page->GetSize(); i += 1) {
        sibling_tree_page->GetEntries()[sibling_tree_page->GetSize() + i] = cur_tree_page->GetEntries()[i];
        // Redirect children's parent after merge
        auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(
            buffer_pool_manager_->FetchPage(cur_tree_page->GetEntries()[i].second)->GetData());
        child_tree_page->SetParentPageId(sibling_tree_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_tree_page->GetPageId(), true);
      }
      sibling_tree_page->IncreaseSize(cur_tree_page->GetSize());
      /* Set not transaction latched page */
      Page *not_transaction_latched_page = isPredecessor ? sibling_page : cur_page;
      /* Recursive */
      RemoveInternalEntry(parent_page, splitKey, transaction);
      /* Add deleted page */
      transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
      // MY_LOG_DEBUG("Merge end - key: {} - deleted_page_id: {}", key.ToString(), cur_page->GetPageId());
      /* Release not transaction latched page */
      ReleasePage(not_transaction_latched_page, true);
    } else {
      /* Redistribution */
      if (isPredecessor) {
        /* Predecessor */
        // MY_LOG_DEBUG("Redistribution predecessor");
        std::pair<KeyType, page_id_t> last = sibling_tree_page->GetEntries()[sibling_tree_page->GetSize() - 1];
        sibling_tree_page->IncreaseSize(-1);
        for (int i = cur_tree_page->GetSize(); i > 0; i -= 1) {
          cur_tree_page->GetEntries()[i] = cur_tree_page->GetEntries()[i - 1];
        }
        cur_tree_page->GetEntries()[0] = last;
        cur_tree_page->GetEntries()[1].first = splitKey;
        cur_tree_page->IncreaseSize(1);
        /* Redirect child */
        Page *child_page = buffer_pool_manager_->FetchPage(last.second);
        auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_tree_page->SetParentPageId(cur_page->GetPageId());
        /* Unpin child */
        buffer_pool_manager_->UnpinPage(child_tree_page->GetPageId(), true);
        /* Replace split key in parent */
        parent_tree_page->GetEntries()[curIndexInParent].first = last.first;
      } else {
        // Successor
        // MY_LOG_DEBUG("Redistribution successor");
        std::pair<KeyType, page_id_t> first = sibling_tree_page->GetEntries()[0];
        for (int i = 0; i < sibling_tree_page->GetSize() - 1; i += 1) {
          sibling_tree_page->GetEntries()[i] = sibling_tree_page->GetEntries()[i + 1];
        }
        sibling_tree_page->IncreaseSize(-1);
        cur_tree_page->GetEntries()[cur_tree_page->GetSize()] = first;
        cur_tree_page->IncreaseSize(1);
        /* Redirect child */
        Page *child_page = buffer_pool_manager_->FetchPage(first.second);
        auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_tree_page->SetParentPageId(cur_tree_page->GetPageId());
        /* Unpin child */
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
        // replace split key in parent
        parent_tree_page->GetEntries()[siblingIdxInParent].first = sibling_tree_page->GetEntries()[0].first;
      }
      /* Unpin sibling */
      ReleasePage(sibling_page, true);
    }
    // MY_LOG_DEBUG("Redistribution end - key: {}", key.ToString());
    return;
  }
  // MY_LOG_DEBUG("Normal end - key: {}", key.ToString());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // MY_LOG_DEBUG("Enter: {}", key.ToString());
  /* Lock */
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return;
  }
  Page *cur_page = DownToLeafForWrite(key, transaction, Operation::Remove);
  RemoveLeafEntry(cur_page, key, transaction);
  ReleaseWLatches(transaction);
  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    // MY_LOG_DEBUG("Delete page id: {}", page_id);
    buffer_pool_manager_->DeletePage(page_id);
  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr);
  }
  /* Find leftmost leaf */
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    page_id_t first_pid = cur_tree_page_as_internal->GetEntries()[0].second;
    buffer_pool_manager_->UnpinPage(cur_tree_page_as_internal->GetPageId(), false);
    cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(first_pid)->GetData());
    BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  }
  auto *cur_tree_page_as_leaf = reinterpret_cast<LeafPage *>(cur_tree_page);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, cur_tree_page_as_leaf);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr);
  }
  auto *cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  while (!cur_tree_page->IsLeafPage()) {
    auto *cur_tree_page_as_internal = reinterpret_cast<InternalPage *>(cur_tree_page);
    int p = FindEntriesWithKeyLargerThan<KeyType, page_id_t, KeyComparator>(
        cur_tree_page_as_internal->GetSize(), cur_tree_page_as_internal->GetEntries(), key, comparator_);
    p -= 1;
    page_id_t pid = cur_tree_page_as_internal->GetEntries()[p].second;
    buffer_pool_manager_->UnpinPage(cur_tree_page_as_internal->GetPageId(), false);
    cur_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid)->GetData());
    BUSTUB_ASSERT(cur_tree_page != nullptr, "Current tree page cannot be null");
  }
  auto *cur_tree_page_as_leaf = reinterpret_cast<LeafPage *>(cur_tree_page);
  int idx = 0;
  while (idx < cur_tree_page_as_leaf->GetSize() &&
         comparator_(cur_tree_page_as_leaf->GetEntries()[idx].first, key) < 0) {
    idx += 1;
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, cur_tree_page_as_leaf, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr); }

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
