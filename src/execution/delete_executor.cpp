//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();

  try {
    while (child_executor_->Next(tuple, rid)) {
      table_meta_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
      // 从索引中删除
      for (auto index : indexs_) {
        index->index_->DeleteEntry(
            tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
      }
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    throw e;
  }
  return false;
}

}  // namespace bustub
