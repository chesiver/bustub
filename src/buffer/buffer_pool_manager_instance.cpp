//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::GetFreePageAndFlushIfDirty() -> frame_id_t {
  // Get free frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // if all frames are currently in use and not evictable (in another word, pinned)
    if (!replacer_->Evict(&frame_id)) {
      LOG_DEBUG("Cannot evict frame id - GetFreePageAndFlushIfDirty");
      return -1;
    }
  }
  // Create page object if necessary
  Page *page = pages_ + frame_id;
  if (page == nullptr) {
    page = new Page();
  }
  // Flush if frame is dirty. Reset metadata.
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].ResetMemory();
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    // Remove page id to frame id mapping
    page_table_->Remove(pages_[frame_id].page_id_);
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  }
  return frame_id;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  latch_.lock();
  LOG_DEBUG("Start");
  // Try get free frame
  frame_id_t frame_id = GetFreePageAndFlushIfDirty();
  if (frame_id == -1) {
    latch_.unlock();
    return nullptr;
  }
  pages_[frame_id].page_id_ = *page_id = AllocatePage();
  pages_[frame_id].pin_count_ = 1;
  // Pin frame && Add access count
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  // Map page id to frame id
  page_table_->Insert(*page_id, frame_id);
  Page *page = &pages_[frame_id];
  LOG_DEBUG("End page id: %d - frame id: %d", *page_id, frame_id);
  latch_.unlock();
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  latch_.lock();
  LOG_DEBUG("Start page id: %d", page_id);
  // First search for page_id in the buffer pool
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    LOG_DEBUG("Found page id: %d - frame id - %d", page_id, frame_id);
    Page *page = pages_ + frame_id;
    latch_.unlock();
    return page;
  }
  // Not found.
  LOG_DEBUG("Not found page id: %d", page_id);
  frame_id = GetFreePageAndFlushIfDirty();
  if (frame_id == -1) {
    latch_.unlock();
    return nullptr;
  }
  // Read page from disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // Pin frame. Add access count
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  // Map page id to frame id
  page_table_->Insert(page_id, frame_id);
  Page *page = &pages_[frame_id];
  LOG_DEBUG("Ens page id: %d - frame id: %d", page_id, frame_id);
  latch_.unlock();
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  LOG_DEBUG("Start page id: %d", page_id);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    LOG_DEBUG("Cannot find frame id for page id: %d", page_id);
    return false;
  }
  if (pages_[frame_id].GetPinCount() <= 0) {
    latch_.unlock();
    LOG_DEBUG("Pin count is already 0 for page id: %d", page_id);
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  if (pages_[frame_id].GetPinCount() == 0) {
    LOG_DEBUG("Remove for page id: %d - frame id - %d", page_id, frame_id);
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  LOG_DEBUG("End page id: %d", page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  latch_.lock();
  LOG_DEBUG("Start page id: %d", page_id);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  // Remove page id to frame id mapping
  page_table_->Remove(pages_[frame_id].page_id_);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  LOG_DEBUG("Ens page id: %d - frame id: %d", page_id, frame_id);
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  LOG_DEBUG("Start");
  for (frame_id_t fid = 0; static_cast<size_t>(fid) < pool_size_; fid += 1) {
    disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    pages_[fid].ResetMemory();
    pages_[fid].is_dirty_ = false;
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    pages_[fid].pin_count_ = 0;
  }
  LOG_DEBUG("End");
  latch_.unlock();
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  latch_.lock();
  LOG_DEBUG("Start page id: %d", page_id);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");
  frame_id_t frame_id;
  // If page_id is not in the buffer pool, do nothing and return true
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    return true;
  }
  // Pinned. Cannot be deleted
  if (pages_[frame_id].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  // After deleting the page from the page table, stop tracking the frame in the replacer
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);
  // Add the frame back to the free list
  free_list_.push_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  // Remove page id to frame id mapping
  page_table_->Remove(page_id);
  // Deallocate disk page
  DeallocatePage(page_id);
  LOG_DEBUG("End page id: %d", page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
