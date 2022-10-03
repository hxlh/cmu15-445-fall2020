//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    for (const auto &val : plan_->RawValues()) {
      Tuple tuple(val, &table_->schema_);
      InsertTableAndIndexs(&tuple);
    }
    return false;
  }

  child_executor_->Init();
  std::vector<Tuple> tuple_vec;
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      tuple_vec.push_back(tuple);
    }
  } catch (Exception &e) {
    // TODO(student): handle exceptions
    LOG_INFO("%s", e.what());
    throw e;
  }

  for (auto &tuple : tuple_vec) {
    InsertTableAndIndexs(&tuple);
  }

  return false;
}

void InsertExecutor::InsertTableAndIndexs(Tuple *tuple) {
  RID rid;
  auto success = table_->table_->InsertTuple(*tuple, &rid, exec_ctx_->GetTransaction());
  if (!success) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertTuple out of memory");
  }
  // lock
  auto txn = exec_ctx_->GetTransaction();
  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      exec_ctx_->GetLockManager()->LockExclusive(txn, rid);
    }
  } catch (const std::exception &e) {
    throw e;
  }

  // add to set
  // InsertTuple函数中已经将rid加入WriteSet，这里无需加入
  // txn->GetWriteSet()->emplace_back(TableWriteRecord(rid, WType::INSERT, *tuple, &(*table_->table_)));

  for (auto index : indexs_) {
    // KeyFromTuple根据index生成一个tuple
    index->index_->InsertEntry(tuple->KeyFromTuple(table_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
                               rid, exec_ctx_->GetTransaction());

    // add to set
    txn->GetIndexWriteSet()->emplace_back(
        IndexWriteRecord(rid, table_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
  }
}

}  // namespace bustub
