//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
  offset_ = plan_->GetOffset();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid) && offset_ > 0) {
    offset_--;
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (offset_ > 0) {
    return false;
  }
  try {
    Tuple tmp_tuple;
    RID tmp_rid;
    while (child_executor_->Next(&tmp_tuple, &tmp_rid) && count_ < plan_->GetLimit()) {
      *tuple = tmp_tuple;
      *rid = tmp_rid;

      count_++;
      return true;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    throw e;
  }
  return false;
}

}  // namespace bustub
