//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_meta_->name_);

  child_executor_->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 假设索引无重复key
  Tuple outer_tuple;
  RID outer_rid;
  while (child_executor_->Next(&outer_tuple, &outer_rid)) {
    Tuple index_key = outer_tuple.KeyFromTuple(*plan_->OuterTableSchema(), index_info_->key_schema_,
                                               index_info_->index_->GetKeyAttrs());
    std::vector<bustub::RID> result;
    // 搜索key是否在索引中，并且取出rid
    index_info_->index_->ScanKey(index_key, &result, exec_ctx_->GetTransaction());
    if (!result.empty()) {
      Tuple inner_tuple;
      table_meta_->table_->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction());

      // 构建输出
      auto out_schema = plan_->OutputSchema();
      std::vector<Value> values;
      auto &out_columns = out_schema->GetColumns();
      values.reserve(out_schema->GetColumnCount());
      for (auto &column_iter : out_columns) {
        Value val = column_iter.GetExpr()->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), &inner_tuple,
                                                        plan_->InnerTableSchema());
        values.emplace_back(std::move(val));
      }

      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }

  return false;
}

}  // namespace bustub
