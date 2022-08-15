//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPage(key, false);

  if (page == nullptr) {
    return false;
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool found = leaf->Lookup(key, &value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (found) {
    result->push_back(value);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 创建新page
  auto new_page_id = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  // 更新root
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);
  // 插入
  LeafPage *root_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  root_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_leaf->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // find the right leaf page
  Page *page = FindLeafPage(key);
  assert(page != nullptr);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  // key是否已存在
  ValueType un_value;
  bool exist = leaf_page->Lookup(key, &un_value, comparator_);
  if (exist) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  int newsz = leaf_page->Insert(key, value, comparator_);
  // leaf已满需要分裂,leaf page 需要>=来保证与internal page有相同的key数量
  if (newsz >= leaf_page->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  auto new_pid = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_pid);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  if (node->IsLeafPage()) {
    LeafPage *old_leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_node);
    new_leaf->Init(new_pid, INVALID_PAGE_ID, leaf_max_size_);
    old_leaf->MoveHalfTo(new_leaf);
    // leaf page需要更新next_page_id
    new_leaf->SetNextPageId(old_leaf->GetNextPageId());
    old_leaf->SetNextPageId(new_leaf->GetPageId());
  } else {
    InternalPage *old_internal = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(new_pid, INVALID_PAGE_ID, internal_max_size_);
    old_internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }

  return new_node;
}
/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 根节点分裂需要创建新节点作为根节点
  if (old_node->IsRootPage()) {
    page_id_t new_root_pid = INVALID_PAGE_ID;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_pid);
    if (new_root_page == nullptr) {
      throw std::runtime_error("out of memory");
    }
    // 更新root id
    root_page_id_ = new_root_pid;
    UpdateRootPageId(0);
    // init
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->Init(new_root_pid, INVALID_PAGE_ID, internal_max_size_);
    // 填充新Root
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改父id
    old_node->SetParentPageId(new_root_node->GetPageId());
    new_node->SetParentPageId(new_root_node->GetPageId());

    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }

  // 非根节点，找父节点插入,满了则递归分裂
  Page *ppage = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  assert(ppage != nullptr);

  InternalPage *pnode = reinterpret_cast<InternalPage *>(ppage->GetData());
  int newsz = pnode->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 给new_node添加parent_id
  new_node->SetParentPageId(pnode->GetPageId());
  // 递归分裂，internal page本身第一位的key为空占一个size，因此不用>=
  if (newsz >= pnode->GetMaxSize()) {
    InternalPage *new_pnode = Split(pnode);
    InsertIntoParent(pnode, new_pnode->KeyAt(0), new_pnode, transaction);
    buffer_pool_manager_->UnpinPage(new_pnode->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(pnode->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf_page = FindLeafPage(key);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (old_size == new_size) {
    // 删除失败
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return;
  }
  // 删除成功
  CoalesceOrRedistribute(leaf_node, transaction);

  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // 处理根节点被Delete后,并且根节点情况与leaf和internal不同，不必满足size<min_size
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // 处理叶节点或中间节点
  // 是否需要满足size<min_size
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  // 需要重新分配或合并
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 找一个兄弟节点
  int index = parent_node->ValueIndex(node->GetPageId());
  int neighbor_index = index == 0 ? 1 : index - 1;
  auto neighbor_id = parent_node->ValueAt(neighbor_index);
  auto neighbor_page = buffer_pool_manager_->FetchPage(neighbor_id);
  N *neighbor_node = reinterpret_cast<N *>(neighbor_page->GetData());

  // 重新分配,因为最小值=最大值/2，所以当node的size小于最小值时,需要满足
  // sibling's size + input* page's size >= page's max size
  // >=的原因是假设max_size=4,min_size=2,leaf当size为4时就需要分裂，而当
  // neighbor_node size=3,node size=1时,若选择 > 会导致进入合并操作，而合并后size=4，
  // 但却没有分裂，导致key数量溢出
  bool target_be_deleted = false;
  if (neighbor_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(neighbor_node, node, index);
  } else {
    // 合并
    Coalesce(&neighbor_node, &node, &parent_node, index, transaction);
    target_be_deleted = true;
  }

  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);

  return target_be_deleted;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // 由于需要保证key的顺序，因此无论neighbor_node是node的左或右兄弟
  // 都是neighbor_node合并到node中
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    if (index == 0) {
      // neighbor在node右边
      leaf_neighbor_node->MoveAllTo(leaf_node);
      leaf_node->SetNextPageId(leaf_neighbor_node->GetNextPageId());
      (*parent)->Remove(1);
    } else {
      // neighbor在node左边
      leaf_node->MoveAllTo(leaf_neighbor_node);
      leaf_neighbor_node->SetNextPageId(leaf_node->GetNextPageId());
      (*parent)->Remove(index);
    }

  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    if (index == 0) {
      // neighbor在node右边
      internal_neighbor_node->MoveAllTo(internal_node, (*parent)->KeyAt(1), buffer_pool_manager_);
      (*parent)->Remove(1);
    } else {
      // neighbor在node左边
      internal_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
      (*parent)->Remove(index);
    }
  }

  return CoalesceOrRedistribute((*parent), transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // 从兄弟节点拿一个节点过来
  // index==0 则说明neighbor_node是node后继节点，即node是第一个node所以只能向右兄弟拿
  Page *p_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *p_node = reinterpret_cast<InternalPage *>(p_page->GetData());
  if (node->IsLeafPage()) {
    LeafPage *node_leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == 0) {
      // move sibling page's first key & value pair into end of input "node"
      neighbor_leaf->MoveFirstToEndOf(node_leaf);
      // change parent keys
      // node是第一个node所以只能向右兄弟拿
      p_node->SetKeyAt(1, neighbor_leaf->KeyAt(0));
    } else {
      neighbor_leaf->MoveLastToFrontOf(node_leaf);
      p_node->SetKeyAt(index, node_leaf->KeyAt(0));
    }
  } else {
    InternalPage *node_internal = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      neighbor_internal->MoveFirstToEndOf(node_internal, p_node->KeyAt(1), buffer_pool_manager_);
      p_node->SetKeyAt(1, neighbor_internal->KeyAt(0));
    } else {
      neighbor_internal->MoveLastToFrontOf(node_internal, p_node->KeyAt(index), buffer_pool_manager_);
      p_node->SetKeyAt(index, node_internal->KeyAt(0));
    }
  }

  buffer_pool_manager_->UnpinPage(p_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case1 old_root_node是internal,将仅剩的一个child更新为root
  if ((!old_root_node->IsLeafPage()) && old_root_node->GetSize() == 1) {
    InternalPage *old_root_internal = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_pid = old_root_internal->ValueAt(0);

    root_page_id_ = new_root_pid;
    UpdateRootPageId(0);

    Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_pid);
    InternalPage *new_root_internal = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_internal->SetParentPageId(INVALID_PAGE_ID);

    return true;
  }
  // case2 old_root_node是leaf
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  auto page = FindLeafPage(KeyType(), true);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(node, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key);
  LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
  int index = node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(node, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  auto page = FindLeafPageEx(KeyType(), FindOp::RightMost);
  LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
  int index = node->GetSize();
  return INDEXITERATOR_TYPE(node, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (leftMost) {
    return FindLeafPageEx(key, FindOp::LeftMost);
  }
  return FindLeafPageEx(key, FindOp::None);
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageEx(const KeyType &key, FindOp op) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if (IsEmpty()) {
    return nullptr;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(page != nullptr);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    InternalPage *in_node = reinterpret_cast<InternalPage *>(node);
    auto next_pid = INVALID_PAGE_ID;
    // 查找leaf方式
    switch (op) {
      case FindOp::None:
        next_pid = in_node->Lookup(key, comparator_);
        break;
      case FindOp::LeftMost:
        next_pid = in_node->ValueAt(0);
        break;
      case FindOp::RightMost:
        next_pid = in_node->ValueAt(in_node->GetSize() - 1);
        break;
      default:
        throw std::runtime_error("FindLeafPageEx: error enum type");
        break;
    }
    assert(next_pid != INVALID_PAGE_ID);

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // 下一轮
    page = buffer_pool_manager_->FetchPage(next_pid);
    assert(page != nullptr);

    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
