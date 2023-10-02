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
#include "common/logger.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), access_records_(num_frames), evictable_frames_(num_frames, true) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t inf_earliest_timestamp = SIZE_MAX;
  size_t earliest_timestamp = SIZE_MAX;
  for (size_t fid = 0; fid < replacer_size_; fid += 1) {
    const auto &records = access_records_[fid];
    if (evictable_frames_[fid]) {
      if (!records.empty()) {
        if (records.size() < k_ && records.front() < inf_earliest_timestamp) {
          inf_earliest_timestamp = records.front();
          *frame_id = fid;
        }
        if (records.size() == k_ && inf_earliest_timestamp == SIZE_MAX && records.front() < earliest_timestamp) {
          earliest_timestamp = records.front();
          *frame_id = fid;
        }
      }
    }
  }
  if (inf_earliest_timestamp != SIZE_MAX || earliest_timestamp != SIZE_MAX) {
    RemoveInternal(*frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < replacer_size_, "frame id not valid");
  auto &records = access_records_[frame_id];
  if (records.empty()) {
    curr_size_ += 1;
  }
  if (records.size() >= k_) {
    records.pop_front();
  }
  size_t t =
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  records.push_back(t);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < replacer_size_, "frame id not valid");
  if (evictable_frames_[frame_id] && !set_evictable) {
    curr_size_ -= 1;
  }
  if (!evictable_frames_[frame_id] && set_evictable) {
    curr_size_ += 1;
  }
  evictable_frames_[frame_id] = set_evictable;
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < replacer_size_, "frame id not valid");
  BUSTUB_ASSERT(evictable_frames_[frame_id], "non-evictable frame cannot be removed");
  if (!access_records_[frame_id].empty()) {
    curr_size_ -= 1;
  }
  access_records_[frame_id].clear();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  RemoveInternal(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub
