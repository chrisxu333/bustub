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

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lk(latch_);
  Page *new_page = nullptr;
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    /* get usable frame id */
    frame_id = free_list_.front();
    free_list_.pop_front();
    /* get the corresponding page */
    new_page = &pages_[frame_id];
    /* reset page data */
    page_id_t new_page_id = AllocatePage();
    *page_id = new_page_id;            // set param
    new_page->page_id_ = new_page_id;  // set new page id
    new_page->ResetMemory();           // reset memory
    new_page->is_dirty_ = true;        // set page to dirty
    new_page->pin_count_ = 1;          // set pin count to 1
    /* record entry in page table */
    page_table_->Insert(new_page_id, frame_id);
    /* record access in replacer */
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    /* get the corresponding page */
    new_page = &pages_[frame_id];
    /* if page is dirty, flush it to disk first */
    if (new_page->IsDirty()) {
      disk_manager_->WritePage(new_page->GetPageId(), new_page->GetData());
    }
    /* remove old entry in page table */
    page_table_->Remove(new_page->GetPageId());
    /* reset page data */
    page_id_t new_page_id = AllocatePage();
    *page_id = new_page_id;            // set param
    new_page->page_id_ = new_page_id;  // set new page id
    new_page->is_dirty_ = true;        // set page to dirty
    new_page->ResetMemory();           // clean page data
    new_page->pin_count_ = 1;          // set pin count
    /* record entry in page table */
    page_table_->Insert(new_page_id, frame_id);
    /* record access in replacer */
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }
  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::unique_lock<std::mutex> lk(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;
  /* find existing page in buffer pool */
  if (page_table_->Find(page_id, frame_id)) {
    /* record a new access in replacer */
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page = &pages_[frame_id];
    page->pin_count_++;  // increment pin count
  } else {
    if (!free_list_.empty()) {
      /* grab empty frame_id from free list */
      frame_id = free_list_.front();
      free_list_.pop_front();
      page = &pages_[frame_id];
      /* set page data */
      page->page_id_ = page_id;  // set page id
      page->is_dirty_ = false;   // set page to clean
      page->pin_count_ = 1;      // set pin count
      page->ResetMemory();       // reset memory
      /* fetch content from disk_manager */
      disk_manager_->ReadPage(page_id, page->data_);
      /* record entry in page table */
      page_table_->Insert(page_id, frame_id);
      /* record a new access in replacer */
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
      /* get the corresponding page */
      page = &pages_[frame_id];
      /* remove old entry in page table */
      page_table_->Remove(page->GetPageId());
      /* if page is dirty, flush it to disk first */
      if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      /* set page data */
      page->page_id_ = page_id;                       // set page id
      page->is_dirty_ = false;                        // set page to clean
      page->pin_count_ = 1;                           // set pin count
      page->ResetMemory();                            // reset memory
      disk_manager_->ReadPage(page_id, page->data_);  // get page data from disk
      /* record entry in page table */
      page_table_->Insert(page_id, frame_id);
      /* record access in replacer */
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    }
  }
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  frame_id_t frame_id;
  bool status = page_table_->Find(page_id, frame_id);
  if (!status) { /* return false if page is not in buffer pool */
    return false;
  }
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) { /* return false if pin count is already zero */
    return false;
  }
  /* decrement pin count */
  page->pin_count_--;
  /* set status */
  page->is_dirty_ = is_dirty;
  /* update replacer evictable status */
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  frame_id_t frame_id;
  bool status = page_table_->Find(page_id, frame_id);
  if (!status) { /* return false if page is not in buffer pool */
    return false;
  }
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  char test[4096];
  disk_manager_->ReadPage(page->GetPageId(), test);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::unique_lock<std::mutex> lk(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::unique_lock<std::mutex> lk(latch_);
  bool status = page_table_->Find(page_id, frame_id);
  if (!status) { /* return true if page is not in buffer pool */
    return true;
  }
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    return false;
  }
  /* remove entry from page table */
  page_table_->Remove(page_id);
  /* reset memory and metadata */
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  /* stop the replacer from tracking this frame_id */
  replacer_->Remove(frame_id);
  /* add back to free list */
  free_list_.push_back(frame_id);
  /* deallocate */
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
