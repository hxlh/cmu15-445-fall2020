//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_meta_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iter_(table_meta_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {}

// 不能直接把TableHeapIterator返回的Tuple作为最终结果输出，plan中的out_schema可能仅仅是TableHeapIterator返回的Tuple的一个projection
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (table_iter_ == table_meta_->table_->End()) {
    return false;
  }

  // 只获取想要的columns
  auto out_schema = plan_->OutputSchema();
  auto &out_columns = out_schema->GetColumns();

  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (auto &column_iter : out_columns) {
    values.emplace_back(column_iter.GetExpr()->Evaluate(&(*table_iter_), &table_meta_->schema_));
  }

  auto oringal_rid = table_iter_->GetRid();
  auto tmp_tuple = Tuple(values, out_schema);
  table_iter_++;

  // 判断谓词（条件）是否满足，不满足则跳过该tuple，即只返回满足条件的元组
  auto predicate = plan_->GetPredicate();
  if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, out_schema).GetAs<bool>()) {
    *rid = oringal_rid;
    *tuple = tmp_tuple;
    return true;
  }

  return Next(tuple, rid);
}

}  // namespace bustub
