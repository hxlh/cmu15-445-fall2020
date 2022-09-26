//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

// 更新的元组由child_executor提供
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();

  try {
    // Tuple before_update_tuple;
    // Tuple after_update_tuple;

    while (child_executor_->Next(tuple, rid)) {
      // // 获取更新前的完整tuple，用于删除索引
      // table_info_->table_->GetTuple(*rid, &before_update_tuple, exec_ctx_->GetTransaction());
      // // 更新后的tuple
      // *tuple = GenerateUpdatedTuple(*tuple);
      // // 更新tuple
      // auto updated = table_info_->table_->UpdateTuple(*tuple, *rid, exec_ctx_->GetTransaction());
      // if (!updated) {
      //   throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateExecutor::Next | UpdateTuple return false");
      // }
      // // 更新索引
      // // 获取更新后完整的tuple，用于插入索引
      // table_info_->table_->GetTuple(*rid, &after_update_tuple, exec_ctx_->GetTransaction());
      // for (auto index : indexs_) {
      //   auto b =
      //       before_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      //   index->index_->DeleteEntry(b, *rid, exec_ctx_->GetTransaction());

      //   auto a =
      //       after_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      //   index->index_->InsertEntry(a, *rid, exec_ctx_->GetTransaction());
      // }
      auto new_tuple = GenerateUpdatedTuple(*tuple);
      table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
      for (auto index : indexs_) {
        index->index_->DeleteEntry((*tuple).KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_,
                                                         index->index_->GetKeyAttrs()),
                                   *rid, exec_ctx_->GetTransaction());
        index->index_->InsertEntry(
            new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      }
    }
  } catch (Exception &e) {
    // TODO(student): handle exceptions
    LOG_INFO("%s", e.what());
    throw e;
  }
  return false;
}
}  // namespace bustub
