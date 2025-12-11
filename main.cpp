#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <random>
#include <chrono>

#include "encoder/encoder.hpp"
#include "placement/placement.hpp"
#include "repair/repair.hpp"
#include "memcached_client.hpp"
#include "block_manager.hpp"
#include "gf256_solver/gf256_solver.hpp"

// 生成随机文件（或直接生成 blocks）
std::vector<std::string> generate_random_blocks(int n_blocks, int block_size) {
    std::vector<std::string> blocks(n_blocks, std::string(block_size, 0));
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(0, 255);
    for (int i = 0; i < n_blocks; ++i) {
        for (int b = 0; b < block_size; ++b) {
            blocks[i][b] = static_cast<char>(dist(rng));
        }
    }
    return blocks;
}

int main() {
    init_tables(); // gf256

    int k1, m1, k2, m2, BLOCK_SIZE, strategy;
    int rack_count, servers_per_rack;
    std::cout << "Enter k1 m1 k2 m2 BLOCK_SIZE strategy(1-7): ";
    std::cin >> k1 >> m1 >> k2 >> m2 >> BLOCK_SIZE >> strategy;
    std::cout << "Enter : rack_count and servers_per_rack";
    std::cin >> rack_count >> servers_per_rack;

    if (k1 <= 0 || k2 <= 0) {
        std::cerr << "Invalid k1/k2\n"; return 1;
    }

    int data_blocks = k1 * k2;
    int total_blocks = data_blocks + (k1 * m1) + (k2 * m2) + (m1 * m2);

    std::cout << "[INFO] data_blocks=" << data_blocks << " total_blocks=" << total_blocks << "\n";

    // 1) 生成原始数据块
    auto data_vec = generate_random_blocks(data_blocks, BLOCK_SIZE);

    // 2) encoding 
    Encoder encoder;
    auto encoded_map = encoder.encode(data_vec, k1, m1, k2, m2, BLOCK_SIZE);
    std::cout << "[INFO] Encoding done. Encoded blocks = "
              << encoded_map.size() << "\n";

    // 3) Placement init
    Placement placement(k1, m1, k2, m2,
                        strategy,
                        rack_count,
                        servers_per_rack,
                        11211,
                        true);

    placement.init();              // 设置 IP并清理
    placement.generate_mapping();  // 生成放置策略 mapping

    // 4) Memcached client
    MemcachedClient memc_client;

    // 5) 写入全部块
    int ok_cnt = placement.write_all_blocks(encoded_map, memc_client);

    std::cout << "[INFO] All blocks written to memcached: "
            << ok_cnt << " / " << encoded_map.size() << "\n";

    // 6) 枚举故障组合（单/二/三块）并调用修复（这里假设 Repair 提供 enumerate_failures)
    Repair repair(k1, m1, k2, m2);
    repair.set_strategy(strategy);

    // 简单：枚举单/二/三块组合的示例（完整枚举请用你的枚举工具）
    std::vector<std::vector<int>> failure_list;
    // single failures
    for (int i = 0; i < data_blocks; ++i) failure_list.push_back({i});
    // pairs (示例前100对以免爆炸)
    for (int i = 0; i < data_blocks && failure_list.size() < 2000; ++i)
        for (int j = i+1; j < data_blocks && failure_list.size() < 2000; ++j)
            failure_list.push_back({i,j});
    // triples (示例有限制)
    for (int i = 0; i < data_blocks && failure_list.size() < 3000; ++i)
        for (int j = i+1; j < data_blocks && failure_list.size() < 3000; ++j)
            for (int k = j+1; k < data_blocks && failure_list.size() < 3000; ++k)
                failure_list.push_back({i,j,k});

    int successful = 0;
    double total_time = 0.0;

    for (const auto &failset : failure_list) {
        // Simulate failures: remove these blocks from memcached (or mark as failed in placement)
        // For safety we will not actually delete objects from memcached; instead we pass the failed set to repair
        double repair_time_ms = 0.0;
        std::unordered_map<int, std::string> recovered;

        bool ok = repair.repair_and_set_group_first( (failset.size() == 1 ? failset[0] : failset[0]),
                                                     placement,
                                                     memc_client,
                                                     /*k*/ k1*k2, /*l*/ m1, /*g*/ m2,
                                                     BLOCK_SIZE,
                                                     recovered,
                                                     pc_matrix,
                                                     repair_time_ms);
        // Note: above call signature may differ — adapt to your repair.hpp full signature;
        // it's important that repair receives: failed set (or placement), memc client, parameters, encoded matrix

        if (ok) {
            successful++;
            total_time += repair_time_ms;
        } else {
            // log
        }
    }

    std::cout << "===== Summary =====\n";
    std::cout << "Tested combinations: " << failure_list.size() << "\n";
    std::cout << "Successful repairs: " << successful << "\n";
    if (successful) std::cout << "Avg repair time (ms): " << (total_time / successful) << "\n";

    return 0;
}
