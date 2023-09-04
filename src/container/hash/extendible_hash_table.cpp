//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "common/logger.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      this->dir_ = { std::make_shared<Bucket>(bucket_size) };
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  this->latch_.lock();
  int idx = this->IndexOf(key);
  bool success = this->dir_[idx]->Find(key, value);
  this->latch_.unlock();
  return success;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  this->latch_.lock();
  int idx = this->IndexOf(key);
  bool success = this->dir_[idx]->Remove(key);
  this->latch_.unlock();
  return success;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  this->latch_.lock();
  LOG_DEBUG("Insert Start");
  int idx;
  while (true) {
    idx = this->IndexOf(key);
    LOG_DEBUG("Try insert with idx: %d", idx);
    if (!this->dir_[idx]->IsFull()) {
      break;
    } 
    this->RedistributeBucket(idx);
  }
  this->dir_[idx]->Insert(key, value);
  LOG_DEBUG("Insert End\n");
  this->latch_.unlock();
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(int bucket_idx) {
  LOG_DEBUG("RedistributeBucket for index - %d", bucket_idx);
  std::shared_ptr<Bucket> bucket = this->dir_[bucket_idx];
  // increase depth if necessary
  if (bucket->GetDepth() == this->global_depth_) {
    this->global_depth_ += 1;
    // expand dir array
    int original_size = this->dir_.size();
    this->dir_.resize(2 * original_size);
    for (int i = 0; i < original_size; i += 1) {
      this->dir_[i | (1 << (global_depth_ - 1))] = this->dir_[i];
    }
  }
  std::shared_ptr<Bucket> split_bucket_0 = std::make_shared<Bucket>(this->bucket_size_, bucket->GetDepth() + 1);
  std::shared_ptr<Bucket> split_bucket_1 = std::make_shared<Bucket>(this->bucket_size_, bucket->GetDepth() + 1);
  // redistribute
  for (const auto &item: bucket->GetItems()) {
    int idx = this->IndexOf(item.first);
    if (idx & (1 << (global_depth_ - 1))) {
      split_bucket_1->GetItems().push_back(item);
    } else {
      split_bucket_0->GetItems().push_back(item);
    }
  }
  this->dir_[bucket_idx] = split_bucket_0;
  this->dir_[bucket_idx | (1 << (global_depth_ - 1))] = split_bucket_1;
  LOG_DEBUG("After RedistributeBucket for index: %d - size: %d - %d %d - %d %d", bucket_idx, (int)bucket->GetItems().size(), bucket_idx, bucket_idx | (1 << (global_depth_ - 1)), (int)split_bucket_0->GetItems().size(), (int)split_bucket_1->GetItems().size());
  // final
  this->num_buckets_ += 1;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = this->list_.begin(); it != this->list_.end(); ++it) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = this->list_.begin(); it != this->list_.end(); ++it) {
    if (it->first == key) {
      this->list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (this->list_.size() >= this->size_) {
    return false;
  }
  for (auto it = this->list_.begin(); it != this->list_.end(); ++it) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  this->list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
