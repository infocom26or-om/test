#include "repair.hpp"
#include "placement.hpp"
#include "memcached_client.hpp"

#include <iostream>
#include <algorithm>
#include <vector>
#include <cmath>
#include <cstring>
#include <future> // 用于并发读取
#include <jerasure.h>
#include <jerasure/reed_sol.h>

// 构造函数
Repair::Repair(int k1, int m1, int k2, int m2)
    : k1_(k1), m1_(m1), k2_(k2), m2_(m2), strategy_(1) {}

// ---------------------------------------------------------
// 辅助函数：坐标转换
// ---------------------------------------------------------
void Repair::get_rc(int block_id, int& r, int& c) const {
    int cols = k1_ + m1_;
    r = block_id / cols;
    c = block_id % cols;
}

int Repair::get_block_id(int r, int c) const {
    return r * (k1_ + m1_) + c;
}

std::vector<int> Repair::get_row_peers(int r) const {
    std::vector<int> peers;
    int cols = k1_ + m1_;
    for (int c = 0; c < cols; ++c) {
        peers.push_back(get_block_id(r, c));
    }
    return peers;
}

std::vector<int> Repair::get_col_peers(int c) const {
    std::vector<int> peers;
    int rows = k2_ + m2_;
    for (int r = 0; r < rows; ++r) {
        peers.push_back(get_block_id(r, c));
    }
    return peers;
}

// ---------------------------------------------------------
// 核心逻辑 1：代价计算 (机架感知)
// ---------------------------------------------------------
int Repair::calculate_cost(const std::vector<int>& peer_ids, 
                           int target_rack_id, 
                           const std::unordered_set<int>& current_failures,
                           const Placement& placement) 
{
    int cost = 0;
    for (int bid : peer_ids) {
        // 如果该块也是坏的，它无法提供数据，不计入读取代价
        // (实际解码时如果缺块太多会失败，但这里只算 Cost)
        if (current_failures.count(bid)) continue;

        try {
            auto entry = placement.get(bid);
            if (entry.rack_id != target_rack_id) {
                cost++; // 跨机架 +1
            }
        } catch (...) {
            // 块不存在 mapping 中？忽略
        }
    }
    return cost;
}

// ---------------------------------------------------------
// 核心逻辑 2：Dijkstra 路径规划
// ---------------------------------------------------------
std::vector<RepairAction> Repair::plan_optimal_repair(
    const std::vector<int>& failed_ids,
    const Placement& placement)
{
    int n = failed_ids.size();
    int target_mask = (1 << n) - 1;
    
    // min_cost[mask]: 达到 mask 状态的最小代价
    std::vector<int> min_cost(1 << n, 999999);
    // parent[mask]: 记录路径 {prev_mask, action}
    std::vector<std::pair<int, RepairAction>> parent(1 << n);

    min_cost[0] = 0;

    for (int mask = 0; mask < target_mask; ++mask) {
        if (min_cost[mask] == 999999) continue;

        // 1. 找出当前 mask 下还没修好的块下标
        std::vector<int> missing_indices;
        for (int i = 0; i < n; ++i) {
            if (!((mask >> i) & 1)) missing_indices.push_back(i);
        }

        // 2. 尝试修复所有涉及的行和列
        std::unordered_set<int> rows_to_try, cols_to_try;
        for (int idx : missing_indices) {
            int r, c;
            get_rc(failed_ids[idx], r, c);
            rows_to_try.insert(r);
            cols_to_try.insert(c);
        }

        // --- 尝试行修复 ---
        for (int r : rows_to_try) {
            int new_recovered_bits = 0;
            // 统计这一行能修好哪些块
            for (int i = 0; i < n; ++i) {
                int br, bc;
                get_rc(failed_ids[i], br, bc);
                if (br == r && !((mask >> i) & 1)) {
                    new_recovered_bits |= (1 << i);
                }
            }

            // 检查行是否可修（缺失数 <= m1）
            // 这里的“缺失”是指【在当前 mask 下还坏着的】+【原本就坏了的】?
            // 不，RS 解码只关心有没有 k 个好块。
            // 只要 (总列数 - 当前坏块数) >= k1 就能修。
            // 当前坏块数 = 该行中所有 failed_ids 里且 mask 没覆盖的块。
            int current_bad_in_row = 0;
            for (int i = 0; i < n; ++i) {
                int br, bc;
                get_rc(failed_ids[i], br, bc);
                if (br == r && !((mask >> i) & 1)) current_bad_in_row++;
            }
            // 如果该行还有其他非 failed_ids 的坏块我们没法处理，这里假设只有 failed_ids 是坏的
            if (current_bad_in_row > m1_) continue; // 坏太多，修不了

            // 计算 Cost
            // 目标机架：假设以该行第一个坏块的机架为修复点（In-place repair）
            int first_bad_idx = -1;
            for(int i=0; i<n; ++i) { if((new_recovered_bits>>i)&1) { first_bad_idx=i; break; } }
            int target_rack = -1;
            if (first_bad_idx != -1) {
                target_rack = placement.get(failed_ids[first_bad_idx]).rack_id;
            }

            // 构造当前坏块集合 (用于 calculate_cost 排除坏块)
            std::unordered_set<int> current_failures_set;
            for(int i=0; i<n; ++i) if(!((mask>>i)&1)) current_failures_set.insert(failed_ids[i]);

            std::vector<int> row_peers = get_row_peers(r);
            int cost = calculate_cost(row_peers, target_rack, current_failures_set, placement);

            // 更新 Dijkstra
            int next_mask = mask | new_recovered_bits;
            if (min_cost[mask] + cost < min_cost[next_mask]) {
                min_cost[next_mask] = min_cost[mask] + cost;
                parent[next_mask] = {mask, {RepairAction::ROW, r, cost, new_recovered_bits}};
            }
        }

        // --- 尝试列修复 (逻辑同上) ---
        for (int c : cols_to_try) {
            int new_recovered_bits = 0;
            for (int i = 0; i < n; ++i) {
                int br, bc;
                get_rc(failed_ids[i], br, bc);
                if (bc == c && !((mask >> i) & 1)) {
                    new_recovered_bits |= (1 << i);
                }
            }

            int current_bad_in_col = 0;
            for (int i = 0; i < n; ++i) {
                int br, bc;
                get_rc(failed_ids[i], br, bc);
                if (bc == c && !((mask >> i) & 1)) current_bad_in_col++;
            }
            if (current_bad_in_col > m2_) continue;

            int first_bad_idx = -1;
            for(int i=0; i<n; ++i) { if((new_recovered_bits>>i)&1) { first_bad_idx=i; break; } }
            int target_rack = -1;
            if (first_bad_idx != -1) {
                target_rack = placement.get(failed_ids[first_bad_idx]).rack_id;
            }

            std::unordered_set<int> current_failures_set;
            for(int i=0; i<n; ++i) if(!((mask>>i)&1)) current_failures_set.insert(failed_ids[i]);

            std::vector<int> col_peers = get_col_peers(c);
            int cost = calculate_cost(col_peers, target_rack, current_failures_set, placement);

            int next_mask = mask | new_recovered_bits;
            if (min_cost[mask] + cost < min_cost[next_mask]) {
                min_cost[next_mask] = min_cost[mask] + cost;
                parent[next_mask] = {mask, {RepairAction::COL, c, cost, new_recovered_bits}};
            }
        }
    }

    // 回溯路径
    std::vector<RepairAction> plan;
    int curr = target_mask;
    if (min_cost[curr] == 999999) return plan; // 无法修复

    while (curr > 0) {
        auto p = parent[curr];
        plan.push_back(p.second);
        curr = p.first;
    }
    std::reverse(plan.begin(), plan.end());
    return plan;
}

// ---------------------------------------------------------
// 核心逻辑 3：解码运算 (RS Decode via Jerasure)
// ---------------------------------------------------------
bool Repair::decode_rs(const std::unordered_map<int, std::string>& survivors,
                       const std::vector<int>& needed_ids,
                       int k, int m, // 对于行：k=k1, m=m1
                       int block_size,
                       bool is_row,
                       std::unordered_map<int, std::string>& out_recovered)
{
    if (survivors.size() < (size_t)k) return false;

    // 1. 准备 Jerasure 矩阵
    // Encoder 使用 reed_sol_vandermonde_coding_matrix(k, m+1, 8)
    // 并且取了 Row 1 到 Row m (跳过 Row 0) 作为校验矩阵
    // 生成完整的生成矩阵 G ( (k+m) x k )
    // Top k is Identity
    // Bottom m is Vandermonde
    
    int total_blocks = k + m;
    int* matrix = reed_sol_vandermonde_coding_matrix(k, m + 1, 8); // (m+1) x k
    
    // 我们需要构建一个 vector 版本的生成矩阵 G_full (k+m) x k
    // 用于挑选行
    std::vector<int> G_full((k + m) * k);
    
    // 填充数据部分 (Identity)
    for (int r = 0; r < k; ++r) {
        for (int c = 0; c < k; ++c) {
            G_full[r * k + c] = (r == c) ? 1 : 0;
        }
    }
    // 填充校验部分
    // Encoder: R[p] 对应 matrix 的第 p+1 行 (p=0..m-1)
    for (int p = 0; p < m; ++p) {
        for (int c = 0; c < k; ++c) {
            // matrix 是 (m+1) x k，按行优先存储
            // 我们要取第 p+1 行
            G_full[(k + p) * k + c] = matrix[(p + 1) * k + c];
        }
    }
    free(matrix); // 用完释放

    // 2. 挑选幸存块对应的行，构建解码矩阵
    // 我们需要 k 个幸存块
    std::vector<int> survivor_ids;
    for (const auto& kv : survivors) {
        if (survivor_ids.size() < k) survivor_ids.push_back(kv.first);
    }

    std::vector<int> decoding_matrix(k * k);
    std::vector<char*> data_ptrs(k);
    std::vector<std::string> survivor_data_storage(k); // 保持数据存活

    // 注意：survivor_ids 存的是全局 block_id
    // 我们需要转成 0..(k+m)-1 的局部索引
    // 假设 get_row_peers/col_peers 返回的顺序就是 0..k+m-1
    // 我们需要知道每个 survivor_id 对应这一行/列的第几个块
    
    int row_start_idx = -1; // 这一行/列的起始 block_id
    // 简单粗暴方法：block_id % (k+m) ? 不行，行列不一样
    // 我们需要利用 is_row 判断
    int cols = k1_ + m1_;
    int rows = k2_ + m2_;
    
    // 为了映射 block_id -> local_index (0..k+m-1)
    auto get_local_idx = [&](int bid) {
        int r, c;
        get_rc(bid, r, c);
        if (is_row) return c; // 行修复，列号就是索引
        else return r;        // 列修复，行号就是索引
    };

    for (int i = 0; i < k; ++i) {
        int bid = survivor_ids[i];
        int local_idx = get_local_idx(bid);
        
        // 拷贝 G_full 的第 local_idx 行到 decoding_matrix 的第 i 行
        for (int j = 0; j < k; ++j) {
            decoding_matrix[i * k + j] = G_full[local_idx * k + j];
        }
        
        // 准备数据指针
        survivor_data_storage[i] = survivors.at(bid);
        data_ptrs[i] = (char*)survivor_data_storage[i].data();
    }

    // 3. 求逆矩阵 (Jerasure)
    // jerasure_invert_matrix 需要 int*
    std::vector<int> inverted_matrix(k * k);
    if (jerasure_invert_matrix(decoding_matrix.data(), inverted_matrix.data(), k, 8) == -1) {
        std::cerr << "[Repair] Singular matrix, cannot decode!" << std::endl;
        return false;
    }

    // 4. 解码出原始 k 个数据块
    // data_ptrs 现在指向幸存块，inverted_matrix * survivors = original_data_blocks
    // 结果存哪里？
    std::vector<std::string> recovered_data_blocks(k, std::string(block_size, 0));
    std::vector<char*> recovered_data_ptrs(k);
    for(int i=0; i<k; ++i) recovered_data_ptrs[i] = (char*)recovered_data_blocks[i].data();

    jerasure_matrix_dotprod(k, 8, inverted_matrix.data(), 
                            (int*)1, k, // row_k=1, col_k=k? dotprod定义有点怪，通常用 jerasure_matrix_encode 做乘法
                            data_ptrs.data(), recovered_data_ptrs.data(), block_size);
    // jerasure_matrix_dotprod: k x k matrix * k x 1 vector (of blocks) -> k x 1 vector
    // src_rows = k, src_cols = k. 
    // jerasure_matrix_dotprod(k, w, matrix_row, src_ids, dest_id, ...) 不是这个API
    // 正确API: jerasure_matrix_encode(k, m, w, matrix, data_ptrs, coding_ptrs, size)
    // 这里 k=k(inputs), m=k(outputs). matrix 是 k*k.
    jerasure_matrix_encode(k, k, 8, inverted_matrix.data(), data_ptrs.data(), recovered_data_ptrs.data(), block_size);

    // 现在 recovered_data_blocks 里是原始的 k 个数据块 (local index 0..k-1)
    
    // 5. 我们可能需要的是 Parity 块，或者 Data 块
    // needed_ids 是我们需要恢复的。
    for (int needed_bid : needed_ids) {
        int local_idx = get_local_idx(needed_bid);
        
        if (local_idx < k) {
            // 是数据块，直接拿
            out_recovered[needed_bid] = recovered_data_blocks[local_idx];
        } else {
            // 是校验块，需要重新编码
            // parity = G_row * data
            // G_row 是 G_full 的第 local_idx 行
            std::vector<int> coding_row(k);
            for(int j=0; j<k; ++j) coding_row[j] = G_full[local_idx * k + j];
            
            std::string parity_block(block_size, 0);
            char* p_ptr = (char*)parity_block.data();
            
            // 计算点积: coding_row (1xk) * data_blocks (kx1)
            // Jerasure 没有直接的 dotprod for blocks，但可以用 matrix_encode (m=1)
            jerasure_matrix_encode(k, 1, 8, coding_row.data(), recovered_data_ptrs.data(), &p_ptr, block_size);
            
            out_recovered[needed_bid] = parity_block;
        }
    }

    return true;
}

// ---------------------------------------------------------
// 执行层：Perform Row/Col Repair
// ---------------------------------------------------------
bool Repair::perform_row_repair(int row_idx, 
                                const std::vector<int>& failed_ids, 
                                Placement& placement, 
                                MemcachedClient& client)
{
    // 1. 确定需要读哪些块（该行所有幸存块）
    std::vector<int> all_blocks = get_row_peers(row_idx);
    std::unordered_set<int> failed_set(failed_ids.begin(), failed_ids.end());
    std::vector<int> survivors;
    std::vector<int> needed;
    
    // 过滤出该行的需要修复块和幸存块
    for (int bid : all_blocks) {
        if (failed_set.count(bid)) needed.push_back(bid);
        else survivors.push_back(bid);
    }
    
    if (needed.empty()) return true; // 没啥要修的

    // 2. 并发读取 (Parallel Fetch)
    std::unordered_map<int, std::string> survivor_data;
    std::vector<std::future<bool>> futures;
    std::mutex data_mutex;

    // 只需要读 k1 个就够了解码了，优化一下？
    // 为了简单，读所有活着的，或者读前 k1 个活着的
    if (survivors.size() > (size_t)k1_) survivors.resize(k1_);

    for (int bid : survivors) {
        futures.push_back(std::async(std::launch::async, [&, bid]() {
            std::string val;
            try {
                auto entry = placement.get(bid);
                if (client.get(entry.server_ip, entry.port, "block_" + std::to_string(bid), val)) {
                    std::lock_guard<std::mutex> lock(data_mutex);
                    survivor_data[bid] = val;
                    return true;
                }
            } catch(...) {}
            return false;
        }));
    }

    for (auto& f : futures) f.wait();

    // 3. 解码
    if (survivor_data.empty()) return false;
    int block_size = survivor_data.begin()->second.size();
    
    std::unordered_map<int, std::string> recovered;
    if (!decode_rs(survivor_data, needed, k1_, m1_, block_size, true, recovered)) {
        std::cerr << "[Repair] Row decode failed for row " << row_idx << std::endl;
        return false;
    }

    // 4. 写回 (Write Back)
    for (const auto& kv : recovered) {
        int bid = kv.first;
        try {
            auto entry = placement.get(bid);
            client.set(entry.server_ip, entry.port, "block_" + std::to_string(bid), kv.second);
        } catch(...) {}
    }
    
    return true;
}

bool Repair::perform_col_repair(int col_idx, 
                                const std::vector<int>& failed_ids, 
                                Placement& placement, 
                                MemcachedClient& client)
{
    // 逻辑同 Row Repair，只是参数换成 k2, m2, is_row=false
    std::vector<int> all_blocks = get_col_peers(col_idx);
    std::unordered_set<int> failed_set(failed_ids.begin(), failed_ids.end());
    std::vector<int> survivors;
    std::vector<int> needed;

    for (int bid : all_blocks) {
        if (failed_set.count(bid)) needed.push_back(bid);
        else survivors.push_back(bid);
    }
    if (needed.empty()) return true;

    if (survivors.size() > (size_t)k2_) survivors.resize(k2_);

    std::unordered_map<int, std::string> survivor_data;
    std::vector<std::future<bool>> futures;
    std::mutex data_mutex;

    for (int bid : survivors) {
        futures.push_back(std::async(std::launch::async, [&, bid]() {
            std::string val;
            try {
                auto entry = placement.get(bid);
                if (client.get(entry.server_ip, entry.port, "block_" + std::to_string(bid), val)) {
                    std::lock_guard<std::mutex> lock(data_mutex);
                    survivor_data[bid] = val;
                    return true;
                }
            } catch(...) {}
            return false;
        }));
    }
    for (auto& f : futures) f.wait();

    if (survivor_data.empty()) return false;
    int block_size = survivor_data.begin()->second.size();

    std::unordered_map<int, std::string> recovered;
    // 注意 k=k2, m=m2, is_row=false
    if (!decode_rs(survivor_data, needed, k2_, m2_, block_size, false, recovered)) {
        std::cerr << "[Repair] Col decode failed for col " << col_idx << std::endl;
        return false;
    }

    for (const auto& kv : recovered) {
        int bid = kv.first;
        try {
            auto entry = placement.get(bid);
            client.set(entry.server_ip, entry.port, "block_" + std::to_string(bid), kv.second);
        } catch(...) {}
    }

    return true;
}

// ---------------------------------------------------------
// 主入口
// ---------------------------------------------------------
bool Repair::repair_and_set(const std::unordered_set<int>& failed_set,
                            Placement& placement,
                            MemcachedClient& client,
                            double& repair_time)
{
    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<int> failed_vec(failed_set.begin(), failed_set.end());
    
    // 1. 规划路径 (Dijkstra)
    auto plan = plan_optimal_repair(failed_vec, placement);
    
    if (plan.empty()) {
        std::cerr << "[Repair] No valid repair plan found!" << std::endl;
        return false;
    }

    // 2. 依次执行
    for (const auto& action : plan) {
        bool ok = false;
        if (action.type == RepairAction::ROW) {
            // 注意：Dijkstra 规划的是“修整行”，这可能包含多个 failed_ids
            // perform_row_repair 会自动处理该行所有在 failed_vec 里的块
            ok = perform_row_repair(action.index, failed_vec, placement, client);
        } else {
            ok = perform_col_repair(action.index, failed_vec, placement, client);
        }
        if (!ok) return false;
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    repair_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
    
    return true;
}