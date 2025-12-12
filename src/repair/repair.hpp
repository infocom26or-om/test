#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <chrono>

// 前向声明（避免循环依赖）
class MemcachedClient;
class Placement;

/*
 Repair 类：负责按 placement 对应的 repair 策略进行修复驱动。
  - 构造时传入 PC 参数 (k1,m1,k2,m2)
  - set_strategy(int) 选择 1..7 的 placement/repair 策略
  - repair_and_set_group_first(...) 为主入口
*/
class Repair {
public:
    Repair(int k1, int m1, int k2, int m2);

    // failed_set: 用来表示故障发生的位置/索引
    // placement: placement 映射表（包含 block -> rack/server 信息）
    // client: memcached 客户端（用于读写）
    // encoded_blocks: 当前 in-memory 编码块映射（注意：该参数为 const 引用，如果策略需要更新内存映射请把写回独立处理）
    // repair_time: 输出参数，修复总耗时（ms）
    bool repair_and_set(const std::unordered_set<int>& failed_set,
                        Placement& placement,
                        MemcachedClient& client,
                        const std::unordered_map<int, std::string>& encoded_blocks,
                        double& repair_time);


    void set_strategy(int strategy) { strategy_ = strategy; }

    // 枚举 恰好 r 个故障块的所有组合
    std::vector<std::vector<int>> enumerate_failures_exact(int num_blocks, int r);


private:
    int k1_, m1_, k2_, m2_;
    int strategy_;

    // ------- 公共辅助 I/O 函数 -------
    // 从 placement + memcached 读取一个 block 的字符串内容（返回 true 表示成功）
    bool get_block_from_placement(int block_id, Placement& placement, MemcachedClient& client, std::string& out_value);

    // 把字符串写回 placement 指定的位置
    bool put_block_to_placement(int block_id, Placement& placement, MemcachedClient& client, const std::string& value);


    bool read_blocks(const std::vector<int>& block_ids, Placement& placement, MemcachedClient& client,
                     std::unordered_map<int, std::string>& out_map);

    bool write_blocks(const std::unordered_map<int, std::string>& write_map, Placement& placement, MemcachedClient& client);

    // ---------------- count failed blocks -----------------
    int count_failed_in_row(const std::unordered_set<int>& failed, int row) const {
        int cnt = 0;
        int col_count = k1_ + m1_;
        for (int c = 0; c < col_count; ++c) {
            int bid = row * col_count + c;
            if (failed.count(bid)) cnt++;
        }
        return cnt;
    }

    int count_failed_in_column(const std::unordered_set<int>& failed, int col) const {
        int cnt = 0;
        int row_count = k2_ + m2_;
        int col_count = k1_ + m1_;
        for (int r = 0; r < row_count; ++r) {
            int bid = r * col_count + col;
            if (failed.count(bid)) cnt++;
        }
        return cnt;
    }

    // ------- strategy 协调入口（每个 strategy 在单独的文件中实现该函数） -------
    // 这里仅声明（实现放在 repair_strategyX.cpp）
    bool repair_strategy1(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy2(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy3(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy4(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy5(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy6(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    bool repair_strategy7(const std::unordered_set<int>& failed_block_ids,
                          Placement& placement,
                          MemcachedClient& client,
                          const std::unordered_map<int, std::string>& encoded_blocks,
                          double& repair_time_ms);

    // 小工具：将 unordered_set 转为 vector（有序）
    static std::vector<int> set_to_sorted_vector(const std::unordered_set<int>& s);
};
