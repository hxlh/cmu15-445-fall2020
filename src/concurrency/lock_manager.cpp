//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <mutex>  // NOLINT
#include <set>
#include <stack>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(latch_);

  // 被唤醒后先检查当前事务的状态
  // 只有growing阶段可以加锁，并且未提交读隔离级别也需要加锁
check:
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  // 未提交读不需要加锁
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  auto &lock_queue = lock_table_[rid];
  // 加入请求队列
  lock_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  auto &myreq = lock_queue.request_queue_.back();
  // 检查是否批准
  for (const auto &req : lock_queue.request_queue_) {
    if (req.lock_mode_ == LockMode::EXCLUSIVE && req.granted_) {
      // rid已经被上了排它锁则等待
      lock_queue.cv_.wait(locker);
      // 唤醒后重新检查状态
      goto check;
    }
  }

  // 批准生效
  myreq.granted_ = true;
  // 将rid加入事务的锁表
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(latch_);

  // 被唤醒后先检查当前事务的状态
  // 只有growing阶段可以加锁，并且未提交读隔离级别也需要加锁
check:
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (txn->IsExclusiveLocked(rid) || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return true;
  }

  auto &lock_queue = lock_table_[rid];
  lock_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  auto &myreq = lock_queue.request_queue_.back();

  for (const auto &req : lock_queue.request_queue_) {
    if (req.granted_) {
      // rid已经被上了锁则等待
      lock_queue.cv_.wait(locker);
      // 唤醒后重新检查状态
      goto check;
    }
  }

  myreq.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(latch_);
  // 被唤醒后先检查当前事务的状态
  // 只有growing阶段可以加锁，并且未提交读隔离级别也需要加锁
  bool apply = false;

check:
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (txn->IsExclusiveLocked(rid) || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return true;
  }

  auto &lock_queue = lock_table_[rid];
  if (lock_queue.upgrading_ && (!apply)) {
    // 已经有一个事务已经在等待升级他们的锁
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  // 发出申请并等待其他共享锁解锁
  if (!apply) {
    apply = true;
  }
  // 还有其他共享锁或者排它锁
  for (const auto &req : lock_queue.request_queue_) {
    if (req.granted_ && req.txn_id_ != txn->GetTransactionId()) {
      lock_queue.cv_.wait(locker);
      goto check;
    }
  }

  // for (auto &req : lock_queue.request_queue_) {
  //   if (req.txn_id_ == txn->GetTransactionId() && req.) {
  //     req.lock_mode_ = LockMode::EXCLUSIVE;
  //     break;
  //   }
  // }
  for (auto &iter : lock_queue.request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.lock_mode_ = LockMode::EXCLUSIVE;
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(latch_);

  auto &lock_queue = lock_table_[rid];
  auto req_iter = lock_queue.request_queue_.begin();
  auto found = false;
  auto lock_mode = LockMode::SHARED;
  while (req_iter != lock_queue.request_queue_.end()) {
    if (req_iter->txn_id_ == txn->GetTransactionId()) {
      lock_mode = req_iter->lock_mode_;
      lock_queue.request_queue_.erase(req_iter);
      found = true;
      break;
    }
    req_iter++;
  }
  if (found) {
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->erase(rid);

    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::REPEATABLE_READ:
        if (txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        break;
    }

    // 解锁后检查
    if (lock_mode == LockMode::EXCLUSIVE) {
      lock_queue.cv_.notify_all();
    } else {
      /** 共享锁解锁后可能有情况：
       * (1)所有锁都被释放
       * (2)只有一个共享锁没释放，并且该共享锁在等待升级
       */
      int cnt = 0;
      for (auto &&req : lock_queue.request_queue_) {
        if (req.granted_) {
          cnt += 1;
        }
        if (cnt > 1) {
          break;
        }
      }
      if (cnt <= 1) {
        lock_queue.cv_.notify_all();
      }
    }

    return true;
  }
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  for (auto iter = v.begin(); iter != v.end(); iter++) {
    if (*iter == t2) {
      v.erase(iter);
      break;
    }
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  auto order_id = std::vector<int>();
  for (auto &&i : waits_for_) {
    order_id.emplace_back(i.first);
  }
  std::sort(order_id.begin(), order_id.end());

  auto stk = std::stack<int>();
  for (auto &&tmp_txn_id : order_id) {
    auto visited = std::set<int>();
    stk.push(tmp_txn_id);

    for (; !stk.empty();) {
      auto id = stk.top();
      stk.pop();
      if (visited.find(id) == visited.end()) {
        visited.emplace(id);
      } else {
        // printf("final :%d\n",id);
        *txn_id = *visited.rbegin();

        return true;
      }
      auto v = waits_for_[id];
      // 同样需要从更年轻的事务开始dfs，这里降序排序方便入栈
      std::sort(v.begin(), v.end(), [](txn_id_t a, txn_id_t b) { return a > b; });
      if (!v.empty()) {
        for (auto &&i : v) {
          stk.push(i);
        }
      }
    }
  }

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::unique_lock<std::mutex> l(latch_);
  auto res = std::vector<std::pair<txn_id_t, txn_id_t>>();
  for (auto &&i : waits_for_) {
    for (auto &&k : i.second) {
      res.emplace_back(std::pair<txn_id_t, txn_id_t>{i.first, k});
    }
  }
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      auto txn2rid = ScanWaitingTxn();
      if (txn2rid.empty()) {
        continue;
      }
      txn_id_t txn_id = -1;
      auto found = HasCycle(&txn_id);
      if (found) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        // 此时该事务阻塞在某个加锁操作中，需要唤醒，事务上的锁在abort函数中会自动解锁
        // 删除请求
        auto &q = lock_table_[txn2rid[txn_id]];
        for (auto iter = q.request_queue_.begin(); iter != q.request_queue_.end(); iter++) {
          if (iter->txn_id_ == txn->GetTransactionId()) {
            q.request_queue_.erase(iter);
            break;
          }
        }
        // 唤醒阻塞线程执行abort，解锁其他元组
        q.cv_.notify_all();
      }
    }
  }
}

std::unordered_map<txn_id_t, RID> LockManager::ScanWaitingTxn() {
  std::unordered_map<txn_id_t, RID> res;
  auto map = std::unordered_map<txn_id_t, std::set<txn_id_t>>();
  // 扫描每一个元组
  // LOG_INFO("Scan Start: \n");
  for (auto &&pair : lock_table_) {
    // 扫描每一个元组上的等待队列
    for (const auto &req : pair.second.request_queue_) {
      auto txn1 = TransactionManager::GetTransaction(req.txn_id_);
      if ((!req.granted_) && txn1->GetState() != TransactionState::ABORTED) {
        // 当前事务想获取的元组，用于后面唤醒用
        res[req.txn_id_] = pair.first;
        // 将等待队列上的已上锁的事务2(即事务1需要等待的事务)加入set去重
        for (const auto &i : pair.second.request_queue_) {
          auto txn2 = TransactionManager::GetTransaction(req.txn_id_);
          if (i.granted_ && txn2->GetState() != TransactionState::ABORTED) {
            map[req.txn_id_].emplace(i.txn_id_);
            // LOG_INFO("txn:%d waiting for txn:%d in rid:%d\n", req.txn_id_, i.txn_id_, pair.first.GetSlotNum());
          }
        }
      }
    }
  }
  // LOG_INFO("Scan End: \n");
  // 构建
  if (!map.empty()) {
    waits_for_.clear();
    for (auto &&pair : map) {
      for (auto &&i : pair.second) {
        AddEdge(pair.first, i);
      }
    }
  }

  return res;
}

}  // namespace bustub
