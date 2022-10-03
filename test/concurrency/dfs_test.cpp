#include <gtest/gtest.h>
#include <set>
#include <stack>
#include <unordered_map>
#include <vector>

// 找出是否有环，有则找出最大的id
bool HasCycle(int *youth_id, std::unordered_map<int, std::vector<int>> wait_for) {
  auto order_id = std::vector<int>();
  for (auto &&i : wait_for) {
    order_id.emplace_back(i.first);
  }
  std::sort(order_id.begin(), order_id.end());

  auto stk = std::stack<int>();
  for (auto &&txn_id : order_id) {
    auto visited = std::set<int>();
    stk.push(txn_id);

    for (; !stk.empty();) {
      auto id = stk.top();
      stk.pop();
      if (visited.find(id) == visited.end()) {
        visited.emplace(id);
      } else {
        printf("final :%d\n", id);
        *youth_id = *visited.rbegin();
        return true;
      }
      auto v = wait_for[id];
      if (v.size() > 0) {
        for (auto &&i : v) {
          stk.push(i);
        }
      }
    }
  }

  return false;
}

TEST(DFSTEST, dfs) {
  auto map = std::unordered_map<int, std::vector<int>>();
  // 1 --> 2 --> 3 -->4 --> 1
  map[1] = {2};
  map[2] = {6};
  map[3] = {4};
  map[4] = {6};
  map[6] = {};

  int txn = -1;

  printf("HasCircle: %d,txn: %d\n", HasCycle(&txn, map), txn);
  auto smap = std::unordered_map<int, std::set<int>>();
  smap[1].emplace(1);
  smap[1].emplace(1);
}
