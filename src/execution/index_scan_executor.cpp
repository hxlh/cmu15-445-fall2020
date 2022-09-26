//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      bpt_index_(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      index_iter_(bpt_index_->GetBeginIterator()) {
  auto index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (index_iter_.isEnd()) {
    return false;
  }
  auto &kv = *index_iter_;
  auto oringal_tmp_tuple = Tuple();
  table_meta_->table_->GetTuple(kv.second, &oringal_tmp_tuple, exec_ctx_->GetTransaction());

  index_iter_.operator++();

  // 只获取想要的columns
  auto out_schema = plan_->OutputSchema();
  auto &out_columns = out_schema->GetColumns();
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (auto &column_iter : out_columns) {
    values.emplace_back(column_iter.GetExpr()->Evaluate(&oringal_tmp_tuple, &table_meta_->schema_));
  }

  // 构建投影过后的元组
  auto oringal_rid = oringal_tmp_tuple.GetRid();
  auto tmp_tuple = Tuple(values, out_schema);

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
