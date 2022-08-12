//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/* 查询该key接下来要到哪个子树去查找
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // 找到比key更大的key index
  assert(GetSize() > 0);
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return ValueAt(left - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  // 算上无效的第一个key
  SetSize(2);
}
/* 插入到old_value之后
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  auto old_index = ValueIndex(old_value);
  // 其他key则向后移动一位
  for (int i = GetSize(); i > old_index + 1; i--) {
    array[i] = array[i - 1];
  }
  array[old_index + 1] = MappingType(new_key, new_value);

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto start = GetMinSize();
  auto item = &array[start];
  auto size = GetSize() - start;
  recipient->CopyNFrom(item, size, buffer_pool_manager);
  IncreaseSize(-size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array + GetSize());
  // set page_id
  for (int i = GetSize(); i < GetSize() + size; i++) {
    Page *p = buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
    bp->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(p->GetPageId(), true);
  }

  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  IncreaseSize(-1);
  for (int i = index; i < GetSize(); i++) {
    array[i] = array[i + 1];
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/* 合并操作,当前node为被删除的node需要被recipient合并
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 需要将从父node获取的分隔key作为第一个key去移动到recipient
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = pair;

  Page *p = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  bp->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(p->GetPageId(), true);

  IncreaseSize(1);
}

/*
删除算法：Remove
1. 如果当前的树为空，则立即返回: IsEmpty
2. 否则，找到待删除的key所在的叶子页面： FindLeafPage
3. 从叶子页面中删除对应的记录： RemoveAndDeleteRecord
4. 如果叶子页面中记录条数发生下溢，则需要进行合并或重分配操作： CoalesceOrRedistribute

合并或重分配操作： CoalesceOrRedistribute
1. 如果当前结点是根页面，则需要调整根：AdjustRoot
  1.1
如果是从根页面中删除最后一个key值后，且根中只剩下一个子节点，则使其子结点成为该树的新的根节点并删除原来的根结点（此时根节点为内部结点）。
  1.2 如果是从根结点删除最后一个key后，整个B+树中不存在元素，则删除该根结点（此时根结点为叶节点）。
2. 否找，找到当前结点和兄妹结点（默认找先左兄妹，对于该层最左边结点找到其右兄妹）
3. 如果当前结点和兄妹结点中的元素个数之和发生上溢出，则需要进行重分配： Redistribute

  3.1 如果是叶节点：
    - 当前结点位于该层的最左边，则把其兄妹结点的第一个元素移动到该结点的末尾： MoveFirstToEndOf
    - 否找，则把其兄妹结点的最后一个元素移动到该结点的首都： MoveLastToFrontOf
  3.2 如果是内部结点：
    注意：叶节点和内部结点的移动操作不完全相同。内部结点需要从父节点中找到middle_key来分割指针
    - 当前结点位于该层的最左边，则把其兄妹结点的第一个元素移动到该结点的末尾： MoveFirstToEndOf
    - 否找，则把其兄妹结点的最后一个元素移动到该结点的首都： MoveLastToFrontOf
  3.3 设置当前结点的父节点中的middle_index为当前结点和兄妹结点的之间的new_key
4. 否找，就需要进行合并： Coalesce
  4.1
如果当前结点位于兄妹结点的前面，则交换当前结点和兄妹结点的指针，使得指向当前结点的指针始终位于指向其兄妹结点的后面。 4.2
把当前结点的所有元素都移动到兄妹结点上，然后删除当前结点: MoveAllTo
    - 叶子结点, 注意只有当前结点的next_page_id不等于兄妹结点的page_id才需要改变兄妹的next_page_id
    - 内部结点
    注意：叶节点和内部结点的移动操作不完全相同
  4.3 从父节点中删除引导至当前结点的键值对。
  4.4 当父节点的发生下溢出时，对父节点递归的使用合并或重分配操作
*/

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // 因为recipient第一个key为无效值，而CopyFirstFrom需要后移所有键值，因此需要先设置
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = pair;

  Page *p = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  bp->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(p->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
