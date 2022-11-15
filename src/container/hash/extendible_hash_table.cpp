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

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  for (int i = 0; i < (1 << global_depth_); ++i) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
  }
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
  size_t bucket_no = IndexOf(key);
  std::unique_lock<std::mutex> lk(latch_);
  return dir_[bucket_no]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t bucket_no = IndexOf(key);
  std::unique_lock<std::mutex> lk(latch_);
  return dir_[bucket_no]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::mutex> lk(latch_);
  RawInsert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RawInsert(const K &key, const V &value) {
  size_t bucket_no = IndexOf(key);
  bool status = dir_[bucket_no]->Insert(key, value);
  if (!status) {
    RedistributeBucket(bucket_no);
    RawInsert(key, value);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t bucket_no) -> void {
  auto bucket = dir_[bucket_no];
  std::unordered_map<K, V> bucket_data(bucket->GetItems().begin(), bucket->GetItems().end());
  bucket->GetItems().clear();
  bucket->IncrementDepth();
  int local_depth = bucket->GetDepth();

  if (local_depth > global_depth_) {
    Grow();
  }
  int other_idx = bucket_no ^ (1 << (local_depth - 1)); /* find pairing index */
  dir_[other_idx].reset();
  dir_[other_idx] =
      std::make_shared<Bucket>(bucket_size_, local_depth); /* change directory to points to newly splitted bucket*/
  int index_diff = 1 << local_depth;
  int dir_size = 1 << global_depth_;
  for (int i = other_idx - index_diff; i >= 0; i -= index_diff) {
    dir_[i] = dir_[other_idx];
  }
  for (int i = other_idx + index_diff; i < dir_size; i += index_diff) {
    dir_[i] = dir_[other_idx];
  }
  for (auto it = bucket_data.begin(); it != bucket_data.end(); ++it) {
    RawInsert(it->first, it->second);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Grow() -> void {
  for (int i = 0; i < (1 << global_depth_); ++i) {
    dir_.push_back(dir_[i]);
  }
  global_depth_++;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  if (list_.count(key)) {
    value = list_[key];
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  if (list_.count(key)) {
    list_.erase(key);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (list_.count(key)) {
    list_[key] = value;
    return true;
  }
  if (IsFull()) {
    return false;
  }
  list_[key] = value;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
