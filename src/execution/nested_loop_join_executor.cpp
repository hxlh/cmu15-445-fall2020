//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  // 先将right全部取出
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(std::move(tuple));
  }
  right_tuple_iter_ = right_tuples_.begin();
  non_any_tuple_ = !left_executor_->Next(&left_tuple_, &left_rid_);
}
// 每次取出一个left_tuple与全部right_tuple比较
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 判断初始化后left_tuple_是否有效
  if (non_any_tuple_) {
    return false;
  }
  do {
    while (right_tuple_iter_ != right_tuples_.end()) {
      if (plan_->Predicate()
              ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &(*right_tuple_iter_),
                             right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        // 匹配成功，构建输出结果
        auto out_schema = plan_->OutputSchema();
        std::vector<Value> values;
        auto &out_columns = out_schema->GetColumns();
        values.reserve(out_schema->GetColumnCount());
        for (auto &column_iter : out_columns) {
          values.emplace_back(column_iter.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                                  &(*right_tuple_iter_),
                                                                  right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(values, out_schema);
        *rid = tuple->GetRid();

        right_tuple_iter_++;
        return true;
      }
      right_tuple_iter_++;
    }
    right_tuple_iter_ = right_tuples_.begin();
  } while (left_executor_->Next(&left_tuple_, &left_rid_));

  return false;
}

}  // namespace bustub
