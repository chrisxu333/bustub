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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto &it : frame_lookup_) {
    delete it.second;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id_) -> bool {
  std::unique_lock<std::mutex> lk(mutex_);
  // prioritize eviction from history list
  if (!history_list_.empty()) {
    for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
      if ((*it)->evictable_) {
        *frame_id_ = (*it)->frame_id_;
        history_list_.remove(*it);
        curr_size_--;
        delete frame_lookup_[*frame_id_];
        frame_lookup_.erase(*frame_id_);
        return true;
      }
    }
  }
  // if history list is not hit, come to cache list for search
  if (!cache_list_.empty()) {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
      if ((*it)->evictable_) {
        *frame_id_ = (*it)->frame_id_;
        cache_list_.remove(*it);
        curr_size_--;
        delete frame_lookup_[*frame_id_];
        frame_lookup_.erase(*frame_id_);
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id_) {
  if (static_cast<size_t>(frame_id_) > replacer_size_) {
    throw std::invalid_argument("Invalid frame id");
  }
  std::unique_lock<std::mutex> lk(mutex_);
  if (frame_lookup_.count(frame_id_) == 0) { /* first time access, insert into history list */
    Info *info = new Info(frame_id_);
    history_list_.push_back(info);
    frame_lookup_[frame_id_] = info;
  } else { /* old access, check access time */
    Info *info = frame_lookup_[frame_id_];
    info->access_time_++;
    if (info->access_time_ == k_) { /* needs to move to cache list */
      history_list_.remove(info);
      cache_list_.push_back(info);
    } else if (info->access_time_ > k_) {
      cache_list_.remove(info);
      cache_list_.push_back(info);
    } else { /* re-insert at the top of the history_list_ */
      history_list_.remove(info);
      history_list_.push_back(info);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id_, bool set_evictable) {
  if (static_cast<size_t>(frame_id_) > replacer_size_) {
    throw std::invalid_argument("Invalid frame id");
  }
  std::unique_lock<std::mutex> lk(mutex_);
  if (frame_lookup_.count(frame_id_) == 1) {
    if (frame_lookup_[frame_id_]->evictable_ == set_evictable) {
      return;
    }
    curr_size_ = set_evictable ? curr_size_ + 1 : curr_size_ - 1;
    frame_lookup_[frame_id_]->evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id_) {
  std::unique_lock<std::mutex> lk(mutex_);
  if (frame_lookup_.count(frame_id_) == 1) {
    if (frame_lookup_[frame_id_]->evictable_) {
      if (frame_lookup_[frame_id_]->access_time_ >= k_) {
        // remove it from the cache list
        cache_list_.remove(frame_lookup_[frame_id_]);
      } else {
        history_list_.remove(frame_lookup_[frame_id_]);
      }
      delete frame_lookup_[frame_id_];
      frame_lookup_.erase(frame_id_);
      curr_size_--;
    } else {
      throw std::invalid_argument("Invalid frame id");
    }
  } else {
    return;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
