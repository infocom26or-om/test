#include "repair.hpp"
#include "memcached_client.hpp"
#include "placement.hpp"
#include <iostream>
#include <algorithm>
#include <cassert>

// ----------------- ctor -----------------
Repair::Repair(int k1, int m1, int k2, int m2)
    : k1_(k1), m1_(m1), k2_(k2), m2_(m2), strategy_(1) {}

// ----------------- helper: enumerate combinations (recursive) -----------------
static void comb_recursive(int n, int r, int start, 
                           std::vector<int>& cur, 
                           std::vector<std::vector<int>>& out) 
{
    if ((int)cur.size() == r) {
        out.push_back(cur);
        return;
    }
    for (int i = start; i < n; ++i) {
        cur.push_back(i);
        comb_recursive(n, r, i + 1, cur, out);
        cur.pop_back();
    }
}

std::vector<std::vector<int>> 
Repair::enumerate_failures_exact(int num_blocks, int r)
{
    std::vector<std::vector<int>> all;
    if (r <= 0 || r > num_blocks) return all;

    std::vector<int> cur;
    comb_recursive(num_blocks, r, 0, cur, all);

    return all;
}

// ----------------- get/put low-level helpers -----------------
bool Repair::get_block_from_placement(int block_id, Placement& placement, MemcachedClient& client, std::string& out_value) {
    try {
        const PlacementEntry& entry = placement.get(block_id);
        bool ok = client.get(entry.server_ip, entry.port, "block_" + std::to_string(block_id), out_value);
        if (!ok) {
            // read failed (may be simulated failure)
            return false;
        }
        return true;
    } catch (const std::out_of_range& e) {
        std::cerr << "[Repair] get_block_from_placement: placement.get() out_of_range for block " << block_id << std::endl;
        return false;
    }
}

bool Repair::put_block_to_placement(int block_id, Placement& placement, MemcachedClient& client, const std::string& value) {
    try {
        const PlacementEntry& entry = placement.get(block_id);
        bool ok = client.set(entry.server_ip, entry.port, "block_" + std::to_string(block_id), value);
        if (!ok) {
            std::cerr << "[Repair] put_block_to_placement: memcached set failed for block " << block_id
                      << " -> " << entry.server_ip << ":" << entry.port << std::endl;
            return false;
        }
        return true;
    } catch (const std::out_of_range& e) {
        std::cerr << "[Repair] put_block_to_placement: placement.get() out_of_range for block " << block_id << std::endl;
        return false;
    }
}

// read multiple blocks into out_map (only those successfully read are present)
bool Repair::read_blocks(const std::vector<int>& block_ids, Placement& placement, MemcachedClient& client,
                         std::unordered_map<int, std::string>& out_map) {
    out_map.clear();
    for (int bid : block_ids) {
        std::string val;
        if (get_block_from_placement(bid, placement, client, val)) {
            out_map[bid] = val;
        } else {
            // mark not present by not inserting
        }
    }
    return true;
}

bool Repair::write_blocks(const std::unordered_map<int, std::string>& write_map, Placement& placement, MemcachedClient& client) {
    bool all_ok = true;
    for (const auto& kv : write_map) {
        int bid = kv.first;
        const std::string& v = kv.second;
        if (!put_block_to_placement(bid, placement, client, v)) {
            all_ok = false;
        }
    }
    return all_ok;
}

// ----------------- top-level dispatch: repair_and_set_group_first -----------------
// This function uses strategy_ to dispatch to the corresponding repair_strategyX.
// repair_time is measured in milliseconds and returned via reference.
bool Repair::repair_and_set(const std::unordered_set<int>& failed_set,
                            Placement& placement,
                            MemcachedClient& client,
                            const std::unordered_map<int, std::string>& encoded_blocks,
                            double& repair_time)
{
    auto t0 = std::chrono::high_resolution_clock::now();

    bool ok = false;
    double inner_time_ms = 0.0;

    switch (strategy_) {
        case 1:
            ok = repair_strategy1(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 2:
            ok = repair_strategy2(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 3:
            ok = repair_strategy3(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 4:
            ok = repair_strategy4(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 5:
            ok = repair_strategy5(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 6:
            ok = repair_strategy6(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        case 7:
            ok = repair_strategy7(failed_set, placement, client, encoded_blocks, inner_time_ms);
            break;
        default:
            std::cerr << "[Repair] Invalid strategy_=" << strategy_ << std::endl;
            ok = false;
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    repair_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

    (void)inner_time_ms;
    return ok;
}


// 统计第 col 列中有多少 failed block
static int count_failed_in_column(const std::unordered_set<int>& failed,
                                  const Placement& placement,
                                  int col, int k1, int m1, int k2, int m2)
{
    int cnt = 0;
    int total_cols = k1 + m1;   // 列数
    int total_rows = k2 + m2;   // 行数
    for (int r = 0; r < total_rows; ++r) {
        int bid = r * total_cols + col;   // 注意：乘的是 total_cols
        if (failed.count(bid)) ++cnt;
    }
    return cnt;
}

// 统计第 row 行中有多少 failed block
static int count_failed_in_row(const std::unordered_set<int>& failed,
                               const Placement& placement,
                               int row, int k1, int m1, int k2, int m2)
{
    int cnt = 0;
    int total_cols = k1 + m1;   // 列数
    int total_rows = k2 + m2;   // 行数
    for (int c = 0; c < total_cols; ++c) {
        int bid = row * total_cols + c;
        if (failed.count(bid)) ++cnt;
    }
    return cnt;
}




// ----------------- strategy placeholders -----------------
// Each of these functions should be implemented in files repair_strategyX.cpp.
// Here we provide small default stubs that return false and print a hint.
// You should replace/implement them with your strategy-specific code that:
//  - uses failed_block_ids input
//  - calls read_blocks(...) to fetch needed surviving blocks
//  - constructs linear system (A * X = B) if needed
//  - calls gf256 solver or Jerasure to compute X
//  - writes recovered blocks with put_block_to_placement(...)
bool Repair::repair_strategy1(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy1] Not implemented. Please implement strategy-specific logic in repair_strategy1.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy2(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy2] Not implemented. Please implement strategy-specific logic in repair_strategy2.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy3(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy3] Not implemented. Please implement strategy-specific logic in repair_strategy3.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy4(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy4] Not implemented. Please implement strategy-specific logic in repair_strategy4.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy5(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy5] Not implemented. Please implement strategy-specific logic in repair_strategy5.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy6(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy6] Not implemented. Please implement strategy-specific logic in repair_strategy6.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}

bool Repair::repair_strategy7(const std::unordered_set<int>& failed_block_ids,
                              Placement& placement,
                              MemcachedClient& client,
                              const std::unordered_map<int, std::string>& encoded_blocks,
                              double& repair_time_ms) {
    std::cerr << "[Repair::strategy7] Not implemented. Please implement strategy-specific logic in repair_strategy7.cpp\n";
    (void)failed_block_ids; (void)placement; (void)client; (void)encoded_blocks; (void)repair_time_ms;
    return false;
}
