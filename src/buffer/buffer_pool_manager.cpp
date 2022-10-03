//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
// 爬取测试文件
#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include <list>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    // lab add
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;

    free_list_.emplace_back(static_cast<int>(i));
  }

  // // 爬取测试文件
  // char *buffer;
  // buffer = getcwd(nullptr, 0);
  // std::cout << "文件路径" << buffer << std::endl;
  // {
  //   // 1、打开文件目录
  //   DIR *dirStream;

  //   const char *path = "../../test/concurrency";
  //   dirStream = opendir(path);
  //   // 2、接下来是读取文件信息
  //   struct dirent *dirInfo;
  //   std::vector<std::string> name;
  //   while ((dirInfo = readdir(dirStream)) != nullptr) {
  //     name.emplace_back(dirInfo->d_name);
  //   }  // 注意此时dirStream 已经指向文件尾了
  //   // 3、最后关闭文件目录
  //   closedir(dirStream);
  //   for (const auto &iter : name) {
  //     std::cout << iter << std::endl;
  //   }

  //   {
  //     std::cout << "grading_lock_manager_detection_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_lock_manager_detection_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }

  //   {
  //     std::cout << "grading_lock_manager_2_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_lock_manager_2_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }

  //   {
  //     std::cout << "grading_lock_manager_1_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_lock_manager_1_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }

  //   {
  //     std::cout << "grading_lock_manager_3_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_lock_manager_3_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }

  //   {
  //     std::cout << "grading_transaction_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_transaction_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }

  //   {
  //     std::cout << "grading_rollback_test.cpp --------------------------" << std::endl;
  //     std::ifstream in(std::string(path) + "/grading_rollback_test.cpp", std::ios::in);
  //     std::istreambuf_iterator<char> beg(in);
  //     std::istreambuf_iterator<char> end;
  //     std::string strdata(beg, end);
  //     in.close();
  //     std::cout << strdata << std::endl;
  //   }
  // }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock lock(latch_);
  // 1.1
  auto frame_iter = page_table_.find(page_id);
  if (frame_iter != page_table_.end()) {
    frame_id_t frame_id = frame_iter->second;
    Page *page = &pages_[frame_id];
    // pin
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }
  // 1.2
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    if (pages_[frame_id].pin_count_ != 0) {
      LOG_ERROR("FetchPgImp: !free_list_.empty() replace_page->pin_count_ != 0\n");
    }
  } else {
    // 找不到牺牲页
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].pin_count_ != 0) {
      LOG_ERROR("FetchPgImp: !replacer_->Victim(&frame_id) replace_page->pin_count_ != 0\n");
    }
  }

  // 2
  Page *replace_page = &pages_[frame_id];
  if (replace_page->IsDirty()) {
    disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());
  }

  // 3
  // LOG_INFO("FetchPgImp page_id %d 被移除\n", replace_page->page_id_);
  page_table_.erase(replace_page->GetPageId());
  page_table_[page_id] = frame_id;
  // LOG_INFO("FetchPgImp page_id %d 被添加\n", page_id);
  // 4
  replace_page->page_id_ = page_id;
  replace_page->is_dirty_ = false;
  // 无论是从free list还是lru list中获取的frame都是pin_count==0
  replace_page->pin_count_ = 1;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(replace_page->GetPageId(), replace_page->GetData());

  return replace_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  auto pg_iter = page_table_.find(page_id);
  if (pg_iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = pg_iter->second;
  Page *pg = &pages_[frame_id];
  // 当is_dirty为false而pg->is_dirty为true防止覆盖
  if (is_dirty) {
    pg->is_dirty_ = true;
  }

  if (pg->GetPinCount() <= 0) {
    // LOG_INFO("UnpinPgImp: (pg->pin_count_ <= 0) return false");
    return false;
  }

  pg->pin_count_--;
  if (pg->GetPinCount() == 0) {
    if (pg->pin_count_ != 0) {
      LOG_ERROR("UnpinPgImp: pg->pin_count_!=0\n");
    }
    replacer_->Unpin(frame_id);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto page = &pages_[iter->second];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock(latch_);
  // 从buffer pool中找一个buffer位置
  // 2
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    if (pages_[frame_id].pin_count_ != 0) {
      LOG_ERROR("NewPgImp: !free_list_.empty() replace_page->pin_count_ != 0\n");
    }
  } else {
    // 1
    // 找不到牺牲页
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].pin_count_ != 0) {
      LOG_ERROR("NewPgImp: !replacer_->Victim(&frame_id) replace_page->pin_count_ != 0\n");
    }
  }
  Page *replace_page = &pages_[frame_id];
  if (replace_page->IsDirty()) {
    disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());
  }

  // 3
  // Update P's metadata
  page_table_.erase(replace_page->GetPageId());
  auto new_page_id = disk_manager_->AllocatePage();
  page_table_[new_page_id] = frame_id;
  replace_page->page_id_ = new_page_id;
  replace_page->pin_count_ = 1;
  replace_page->is_dirty_ = false;
  replacer_->Pin(frame_id);
  // zero out memory
  replace_page->ResetMemory();
  disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());

  // 4
  // Set the page ID output parameter
  *page_id = new_page_id;
  return replace_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock(latch_);
  // 1
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return true;
  }
  frame_id_t frame_id = iter->second;
  // 2
  Page *pg = &pages_[frame_id];
  if (pg->GetPinCount() > 0) {
    return false;
  }
  // 清理
  if (pg->IsDirty()) {
    disk_manager_->WritePage(pg->GetPageId(), pg->GetData());
  }
  disk_manager_->DeallocatePage(page_id);
  // 3
  // LOG_INFO("DeletePgImp page_id %d 被移除\n", page_id);
  page_table_.erase(page_id);
  // 因为要放入free list，因此从lru中移除
  replacer_->Pin(frame_id);
  // reset metadata
  pg->page_id_ = INVALID_PAGE_ID;
  pg->is_dirty_ = false;
  pg->pin_count_ = 0;
  // 放回free list
  free_list_.push_back(frame_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    auto page = &pages_[i];
    if (page->page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
