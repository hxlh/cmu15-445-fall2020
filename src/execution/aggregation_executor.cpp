//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  try {
    child_->Init();
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
      // 计算聚合值
      aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    throw e;
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    // 过滤条件
    auto having = plan_->GetHaving();
    if (having == nullptr ||
        having->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      // 构建tuple
      std::vector<Value> res;
      res.reserve(plan_->OutputSchema()->GetColumnCount());
      for (auto &column : plan_->OutputSchema()->GetColumns()) {
        res.emplace_back(
            column.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
      }
      *tuple = Tuple(res, plan_->OutputSchema());

      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }

  return false;
}

}  // namespace bustub
