#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <chrono>
#include <memory>

// 前向声明
class MemcachedClient;
class Placement;

// 定义修复动作
struct RepairAction {
    enum Type { ROW, COL } type;
    int index;          // 行号 或 列号
    int cost;           // 跨机架传输代价
    int recovered_mask; // 这一步能修好哪些块（在 failed_set 中的下标掩码）
};

class Repair {
public:
    Repair(int k1, int m1, int k2, int m2);

    // 设置策略 (1-7)
    void set_strategy(int strategy) { strategy_ = strategy; }

    // 主修复入口：自动规划最优路径并执行
    // 返回 true 表示成功，repair_time 输出毫秒耗时
    bool repair_and_set(const std::unordered_set<int>& failed_set,
                        Placement& placement,
                        MemcachedClient& client,
                        double& repair_time);

private:
    int k1_, m1_, k2_, m2_;
    int strategy_;

    // --- 路径规划 ---
    std::vector<RepairAction> plan_optimal_repair(
        const std::vector<int>& failed_ids,
        const Placement& placement);

    int calculate_cost(const std::vector<int>& peer_ids, 
                       int target_rack_id, 
                       const std::unordered_set<int>& current_failures,
                       const Placement& placement);

    // --- 辅助工具 ---
    void get_rc(int block_id, int& r, int& c) const;
    int get_block_id(int r, int c) const;
    std::vector<int> get_row_peers(int r) const;
    std::vector<int> get_col_peers(int c) const;

    // --- 执行层 ---
    bool execute_repair_plan(const std::vector<RepairAction>& plan,
                             const std::vector<int>& failed_ids,
                             Placement& placement,
                             MemcachedClient& client);

    bool perform_row_repair(int row_idx, 
                            const std::vector<int>& failed_ids, 
                            Placement& placement, 
                            MemcachedClient& client);

    bool perform_col_repair(int col_idx, 
                            const std::vector<int>& failed_ids, 
                            Placement& placement, 
                            MemcachedClient& client);

    // --- 解码运算 (Jerasure wrapper) ---
    // 输入：survivors (id -> data), needed_ids (丢失的id)
    // 输出：recovered (id -> data)
    // k, m: RS 码参数 (行是 k1,m1; 列是 k2,m2)
    bool decode_rs(const std::unordered_map<int, std::string>& survivors,
                   const std::vector<int>& needed_ids,
                   int k, int m,
                   int block_size,
                   bool is_row, // true 用行矩阵，false 用列矩阵
                   std::unordered_map<int, std::string>& out_recovered);
};