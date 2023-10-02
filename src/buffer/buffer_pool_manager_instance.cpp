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

#include <fmt/color.h>
#include <fmt/std.h>

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
    if (!replacer_->Evict(&frame_id)) {
      return -1;
    }
  }
  // Flush if frame is dirty. Reset metadata.
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  page_table_->Remove(pages_[frame_id].page_id_);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  return frame_id;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  /* Try get free frame with metadata reset */
  frame_id_t frame_id = GetFreePageAndFlushIfDirty();
  if (frame_id == -1) {
    return nullptr;
  }
  pages_[frame_id].page_id_ = *page_id = AllocatePage();
  /* Pin frame && Add access count */
  pages_[frame_id].pin_count_ = 1;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  /* Map page id to frame id */
  page_table_->Insert(*page_id, frame_id);
  Page *page = pages_ + frame_id;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");

  /* First search for page_id in the buffer pool */
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = pages_ + frame_id;
    page->pin_count_ += 1;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    return page;
  }
  /* Not found */
  frame_id = GetFreePageAndFlushIfDirty();
  if (frame_id == -1) {
    return nullptr;
  }
  /* Read page from disk */
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  /* Map page id to frame id */
  page_table_->Insert(page_id, frame_id);
  Page *page = pages_ + frame_id;
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (frame_id_t fid = 0; static_cast<size_t>(fid) < pool_size_; fid += 1) {
    disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    pages_[fid].ResetMemory();
    pages_[fid].is_dirty_ = false;
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    pages_[fid].pin_count_ = 0;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "cannot be invalid page id");

  frame_id_t frame_id;
  /* If page_id is not in the buffer pool, do nothing and return true */
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  /* Pinned. Cannot be deleted */
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  /* After deleting the page from the page table, stop tracking the frame in the replacer */
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);
  /* Add the frame back to the free list */
  free_list_.push_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  /* Remove page id to frame id mapping */
  page_table_->Remove(page_id);
  /* Deallocate disk page*/
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
