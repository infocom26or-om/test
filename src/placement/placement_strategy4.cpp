#include "placement.hpp"

// ---------------------------------------------------------
// strategy4：每 m1 列为一组放在同一个 rack，组内 block 在 rack 的不同 server 轮转
// 例如 m1=2: 列 0,1 -> rack0; 列 2,3 -> rack1; 列 4,5 -> rack2; ...
// ---------------------------------------------------------
void Placement::strategy4_generate() {
    placement_map_.clear();

    // sanity
    if (m1_ <= 0) {
        std::cerr << "[Placement-Strategy4] ERROR: m1 must be > 0\n";
        return;
    }

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    // 需要的组数 = ceil(cols / m1_)
    int groups = (cols + m1_ - 1) / m1_;
    if (rack_count_ < 1) {
        std::cerr << "[Placement-Strategy4] ERROR: rack_count must be >= 1\n";
        return;
    }

    // 每 rack 的 server 轮转指针
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; ++id) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        // determine which group this column belongs to
        int group = col / m1_;               // group index (0..groups-1)
        int rack = group % rack_count_;      // map group -> rack (wrap if fewer racks)

        int server_index = rack_next_srv[rack] % servers_per_rack_;
        rack_next_srv[rack]++;

        PlacementEntry e;
        e.block_id = id;
        e.row = row;
        e.col = col;
        e.rack = rack;
        e.server_index = server_index;

        placement_map_[id] = e;
    }

    std::cout << "[Placement] Strategy4 done: " << total_blocks
              << " blocks placed into " << groups << " column-groups (m1=" << m1_ << ").\n";
}

