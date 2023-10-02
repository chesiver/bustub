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

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_ = {std::make_shared<Bucket>(bucket_size)};
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  int idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  int idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::shared_mutex> lock(latch_);
  int idx;
  while (true) {
    idx = IndexOf(key);
    if (!dir_[idx]->IsFull()) {
      break;
    }
    RedistributeBucket(dir_[idx]);
  }
  dir_[idx]->Insert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
  int bucket_idx = bucket->GetIndex();
  int local_depth = bucket->GetDepth();
  // increase depth if necessary
  if (local_depth == global_depth_) {
    global_depth_ += 1;
    // expand dir array
    int original_size = dir_.size();
    dir_.resize(2 * original_size);
    for (int i = 0; i < original_size; i += 1) {
      dir_[i | (1 << (global_depth_ - 1))] = dir_[i];
    }
  }
  int sub_index1 = bucket_idx;
  int sub_index2 = bucket_idx | (1 << local_depth);
  std::shared_ptr<Bucket> split_bucket_0 = std::make_shared<Bucket>(bucket_size_, local_depth + 1, sub_index1);
  std::shared_ptr<Bucket> split_bucket_1 = std::make_shared<Bucket>(bucket_size_, local_depth + 1, sub_index2);
  // redistribute
  for (const auto &item : bucket->GetItems()) {
    int idx = IndexOf(item.first);
    if ((idx & (1 << (local_depth))) != 0) {
      split_bucket_1->GetItems().push_back(item);
    } else {
      split_bucket_0->GetItems().push_back(item);
    }
  }
  for (int i = 0, prefix_limit = (1 << (global_depth_ - local_depth)); i < prefix_limit; i += 1) {
    if (i % 2 == 0) {
      dir_[(i << local_depth) | bucket_idx] = split_bucket_0;
    } else {
      dir_[(i << local_depth) | bucket_idx] = split_bucket_1;
    }
  }
  // final
  num_buckets_ += 1;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth, int bucket_index)
    : size_(array_size), depth_(depth), bucket_index_(bucket_index) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (list_.size() >= size_) {
    return false;
  }
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
