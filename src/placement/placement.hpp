#pragma once

#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include <cassert>

#include "memcached_client.hpp"

struct PlacementEntry {
    int block_id;
    int row;
    int col;
    int rack;
    int server_index;
};

class Placement {
public:
    Placement(int k1, int m1, int k2, int m2,
              int strategy,
              int rack_count,
              int servers_per_rack,
              int base_port = 11211,
              bool use_single_vm = true);

    // 初始化策略、生成 IP 列表等
    void init();

    // 生成完整映射表（调用 strategyN_generate）
    void generate_mapping();

    // 写入单个 block
    bool write_block(const PlacementEntry& e,
                     const std::string& data,
                     MemcachedClient& client);

    // 写入全部 block
    int write_all_blocks(
        const std::unordered_map<int, std::string>& encoded_map,
        MemcachedClient& client
    );

    // 查 mapping
    const PlacementEntry& get(int block_id) const;

private:
    // 参数
    int k1_, m1_, k2_, m2_;
    int strategy_;
    int rack_count_;
    int servers_per_rack_;
    int base_port_;
    bool use_single_vm_;

    // 单机测试：所有 rack 使用 127.0.0.1
    std::vector<std::string> rack_ips_;

    // 放置总表
    std::unordered_map<int, PlacementEntry> placement_map_;

private:
    // 辅助：计算 block 的 row/col（按照 encoder flatten 顺序）
    void blockid_to_rowcol(int block_id, int &row, int &col) const;

    // 默认 rack ip 填充
    void fill_default_rack_ips();

    // 7 种策略
    void strategy1_generate();
    void strategy2_generate();
    void strategy3_generate();
    void strategy4_generate();
    void strategy5_generate();
    void strategy6_generate();
    void strategy7_generate();
};
