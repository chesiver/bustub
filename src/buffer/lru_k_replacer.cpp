//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), access_records_(num_frames), evictable_frames_(num_frames, true) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  size_t inf_earliest_timestamp = SIZE_MAX;
  size_t earliest_timestamp = SIZE_MAX;
  for (size_t fid = 0; fid < this->replacer_size_; fid += 1) {
    const auto &records = this->access_records_[fid];
    if (this->evictable_frames_[fid]) {
      if (!records.empty()) {
        if (records.size() < this->k_ && records.front() < inf_earliest_timestamp) {
          inf_earliest_timestamp = records.front();
          *frame_id = fid;
        }
        if (records.size() == this->k_ && inf_earliest_timestamp == SIZE_MAX && records.front() < earliest_timestamp) {
          earliest_timestamp = records.front();
          *frame_id = fid;
        }
      }
    }
  }
  if (inf_earliest_timestamp != SIZE_MAX || earliest_timestamp != SIZE_MAX) {
    latch_.unlock();
    this->Remove(*frame_id);
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < this->replacer_size_, "frame id not valid");
  auto &records = this->access_records_[frame_id];
  if (records.empty()) {
    this->curr_size_ += 1;
  }
  if (records.size() >= this->k_) {
    records.pop_front();
  }
  size_t t =
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  records.push_back(t);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < this->replacer_size_, "frame id not valid");
  if (this->evictable_frames_[frame_id] && !set_evictable) {
    this->curr_size_ -= 1;
  }
  if (!this->evictable_frames_[frame_id] && set_evictable) {
    this->curr_size_ += 1;
  }
  this->evictable_frames_[frame_id] = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < this->replacer_size_, "frame id not valid");
  BUSTUB_ASSERT(this->evictable_frames_[frame_id], "non-evictable frame cannot be removed");
  if (!this->access_records_[frame_id].empty()) {
    this->curr_size_ -= 1;
  }
  this->access_records_[frame_id].clear();
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t s = this->curr_size_;
  latch_.unlock();
  return s;
}
}  // namespace bustub
