#include "repair.hpp"
#include "placement.hpp"
#include "memcached_client.hpp"
#include "repair_common.hpp"     // col_repair_one / row_repair_one / decode interface etc.
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <iostream>

//
// Strategy-1: "One block per rack" placement
// Repair logic:
//  - Prefer column repair if (k2 < k1), otherwise row repair
//  - Loop until all failed blocks are repaired or no progress
//

bool Repair::repair_strategy1(std::unordered_set<int>& failed_set,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& inner_time_ms) 
{
    auto t0 = std::chrono::high_resolution_clock::now();

    int row_cnt = k2_ + m2_; // 总行数
    int col_cnt = k1_ + m1_; // 总列数

    bool prefer_col_first = (k2_ < k1_); // 优先列修复或行修复

    bool progress = true;
    while (!failed_set.empty() && progress) {
        progress = false;

        if (prefer_col_first) {
            // ==================== 优先列修复 ====================
            // 遍历列修复
            for (int c = 0; c < col_cnt; ++c) {
                int t = count_failed_in_column(failed_set, c);
                if (t > 0 && t <= m2_) {
                    if (!col_repair_one(failed_set, c, placement, client, encoded_blocks))
                        return false;
                    progress = true;
                }
            }

            if (failed_set.empty()) break;

            // 找到行内故障块 ≤ m1 且最多的行，修复
            int best_row = -1;
            int max_s = 0;
            for (int r = 0; r < row_cnt; ++r) {
                int s = count_failed_in_row(failed_set, r);
                if (s > 0 && s <= m1_ && s > max_s) {
                    max_s = s;
                    best_row = r;
                }
            }
            if (best_row >= 0) {
                if (!row_repair_one(failed_set, best_row, placement, client, encoded_blocks))
                    return false;
                progress = true;
            }
        } 
        else {
            // ==================== 优先行修复 ====================
            // 遍历行修复
            for (int r = 0; r < row_cnt; ++r) {
                int s = count_failed_in_row(failed_set, r);
                if (s > 0 && s <= m1_) {
                    if (!row_repair_one(failed_set, r, placement, client, encoded_blocks))
                        return false;
                    progress = true;
                }
            }

            if (failed_set.empty()) break;

            // 找出列内故障块 ≤ m2 且最多的列，只修一列
            int best_col = -1;
            int max_t = 0;
            for (int c = 0; c < col_cnt; ++c) {
                int t = count_failed_in_column(failed_set, c);
                if (t > 0 && t <= m2_ && t > max_t) {
                    max_t = t;
                    best_col = c;
                }
            }
            if (best_col >= 0) {
                if (!col_repair_one(failed_set, best_col, placement, client, encoded_blocks))
                    return false;
                progress = true;
            }
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    inner_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // 返回是否所有故障块修复完成
    return failed_set.empty();
}


// row repair
bool Repair::row_repair_one(std::unordered_set<int>& failed_set,
                            int row,
                            Placement& placement,
                            MemcachedClient& client,
                            const std::unordered_map<int, std::string>& encoded_blocks) 
{
    int col_count = k1_ + m1_;     ///////////////////

    //  找出行内故障块
    std::vector<int> row_failed;
    for (int c = 0; c < col_count; ++c) {
        int bid = row * col_count + c;
        if (failed_set.count(bid)) row_failed.push_back(bid);
    }
    int s = row_failed.size();
    if (s == 0) return true; // 没有故障块

    // 选择主机架（第一个故障块的 rack）
    int host_rack = placement.get(row_failed[0]).rack;

    // 选择辅助块：行内剩余幸存块
    std::vector<int> helper_blocks;
    for (int c = 0; c < col_count; ++c) {
        int bid = row * col_count + c;
        if (!failed_set.count(bid)) helper_blocks.push_back(bid);
        if ((int)helper_blocks.size() >= k1_ + s - 1) break;
    }
    if ((int)helper_blocks.size() < k1_ + s - 1) {
        std::cerr << "[Repair] Not enough helper blocks for row " << row << "\n";
        return false;
    }

    // 4️⃣ 解码（伪代码）
    // decode row_failed using helper_blocks，解码结果放在 host_rack
    // 这里调用实际的 decode 函数

    // 5️⃣ 重新分配解码块到原 rack
    // 假设 decode 得到 s 个块，更新 failed_set
    for (int bid : row_failed) failed_set.erase(bid);

    return true;
}

// column repair
bool Repair::col_repair_one(std::unordered_set<int>& failed_set,
                            int col,
                            Placement& placement,
                            MemcachedClient& client,
                            const std::unordered_map<int, std::string>& encoded_blocks) 
{
    int row_count = k2_ + m2_;
    int col_count = k1_ + m1_;

    // 1️⃣ 找出列内故障块
    std::vector<int> col_failed;
    for (int r = 0; r < row_count; ++r) {
        int bid = r * col_count + col;
        if (failed_set.count(bid)) col_failed.push_back(bid);
    }
    int t = col_failed.size();
    if (t == 0) return true;

    // 2️⃣ 主机架
    int host_rack = placement.get(col_failed[0]).rack;

    // 3️⃣ 辅助块：列内剩余幸存块
    std::vector<int> helper_blocks;
    for (int r = 0; r < row_count; ++r) {
        int bid = r * col_count + col;
        if (!failed_set.count(bid)) helper_blocks.push_back(bid);
        if ((int)helper_blocks.size() >= k2_ + t - 1) break;
    }
    if ((int)helper_blocks.size() < k2_ + t - 1) {
        std::cerr << "[Repair] Not enough helper blocks for column " << col << "\n";
        return false;
    }

    // 4️⃣ 解码
    // decode col_failed using helper_blocks，解码结果放在 host_rack

    // 5️⃣ 更新 failed_set
    for (int bid : col_failed) failed_set.erase(bid);

    return true;
}
