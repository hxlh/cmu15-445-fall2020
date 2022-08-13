//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { this->num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(latch_);
  if (lru_hash_.empty()) {
    return false;
  }

  frame_id_t victim_frame = this->lru_list_.back();
  lru_hash_.erase(victim_frame);
  this->lru_list_.pop_back();
  *frame_id = victim_frame;

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (this->lru_hash_.count(frame_id) == 0) {
    return;
  }
  this->lru_list_.erase(this->lru_hash_[frame_id]);
  this->lru_hash_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (this->lru_hash_.count(frame_id) != 0) {
    return;
  }

  while (this->Size() >= this->num_pages_) {
    auto victim_frame = this->lru_list_.back();
    this->lru_list_.pop_back();
    this->lru_hash_.erase(victim_frame);
  }
  this->lru_list_.push_front(frame_id);
  this->lru_hash_[frame_id] = this->lru_list_.begin();
}

size_t LRUReplacer::Size() { return lru_hash_.size(); }

}  // namespace bustub
