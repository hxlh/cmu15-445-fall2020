/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *page, int index, BufferPoolManager *buffer_manager) {
  page_ = page;
  index_ = index;
  buffer_manager_ = buffer_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_manager_->UnpinPage(page_->GetPageId(), false); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return (page_->GetNextPageId() == INVALID_PAGE_ID && index_ >= page_->GetSize()); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_++;
  // 获取下一页
  if (index_ >= page_->GetSize() && page_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = buffer_manager_->FetchPage(page_->GetNextPageId());
    LeafPage *next_node = reinterpret_cast<LeafPage *>(next_page->GetData());
    buffer_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = next_node;
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
