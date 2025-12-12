#include "placement.hpp"

// ---------------------------------------------------------
// 构造函数
// ---------------------------------------------------------
Placement::Placement(int k1, int m1, int k2, int m2,
                     int strategy,
                     int rack_count,
                     int servers_per_rack,
                     int base_port,
                     bool use_single_vm)
    : k1_(k1), m1_(m1), k2_(k2), m2_(m2),
      strategy_(strategy),
      rack_count_(rack_count),
      servers_per_rack_(servers_per_rack),
      base_port_(base_port),
      use_single_vm_(use_single_vm) 
{
    rack_ips_.resize(rack_count_);
}

// ---------------------------------------------------------
// init：设置 rack IP，清理表
// ---------------------------------------------------------
void Placement::init() {
    fill_default_rack_ips();
    placement_map_.clear();
}

// ---------------------------------------------------------
// 默认：所有 rack 的 IP 都设置为 127.0.0.1（单机测试）
// ---------------------------------------------------------
void Placement::fill_default_rack_ips() {
    for (int i = 0; i < rack_count_; i++) {
        rack_ips_[i] = "127.0.0.1";
    }
}

// ---------------------------------------------------------
// block_id → row,col（保持 encoder flatten 一致性）
// ---------------------------------------------------------
void Placement::blockid_to_rowcol(int block_id, int &row, int &col) const {
    int total_cols = k1_ + m1_;
    row = block_id / total_cols;
    col = block_id % total_cols;
}

// ---------------------------------------------------------
// 主入口：根据 strategy 生成完整 mapping 表
// ---------------------------------------------------------
void Placement::generate_mapping() {
    switch (strategy_) {
        case 1: strategy1_generate(); break;
        case 2: strategy2_generate(); break;
        case 3: strategy3_generate(); break;
        case 4: strategy4_generate(); break;
        case 5: strategy5_generate(); break;
        case 6: strategy6_generate(); break;
        case 7: strategy7_generate(); break;

        default:
            std::cerr << "[Placement] Invalid strategy " << strategy_ << std::endl;
            break;
    }
}


// ---------------------------------------------------------
// 写入单个 block
// ---------------------------------------------------------
bool Placement::write_block(const PlacementEntry& e,
                            const std::string& data,
                            MemcachedClient& client)
{
    const std::string& ip = rack_ips_[e.rack];
    int port = base_port_ + e.server_index;

    std::string key = "block_" + std::to_string(e.block_id);

    return client.set(ip, port, key, data);
}

// ---------------------------------------------------------
// 写入全部 block
// ---------------------------------------------------------
int Placement::write_all_blocks(
    const std::unordered_map<int, std::string>& encoded_map,
    MemcachedClient& client)
{
    int success = 0;

    for (const auto& kv : encoded_map) {
        int block_id = kv.first;
        const std::string& data = kv.second;

        if (placement_map_.count(block_id) == 0) {
            std::cerr << "[Placement] Missing mapping for block " << block_id << "\n";
            continue;
        }

        const PlacementEntry& e = placement_map_.at(block_id);

        if (write_block(e, data, client))
            success++;
    }

    std::cout << "[Placement] Successfully wrote " << success 
              << " / " << encoded_map.size() << " blocks.\n";

    return success;
}

// ---------------------------------------------------------
// 查 mapping
// ---------------------------------------------------------
const PlacementEntry& Placement::get(int block_id) const {
    return placement_map_.at(block_id);
}
