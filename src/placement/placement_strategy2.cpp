#include "placement.hpp"

// ---------------------------------------------------------
// strategy2：每一列（k2+m2行块）放同一 rack，server 轮转
// ---------------------------------------------------------
void Placement::strategy2_generate() {
    placement_map_.clear();

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    if (rack_count_ < cols) {
        std::cerr << "[Placement-Strategy2] ERROR: rack_count("
                  << rack_count_ << ") < total columns("
                  << cols << "). Each column needs 1 rack.\n";
        return;
    }

    // 每 rack 的 server round-robin 索引
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; id++) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        int rack = col; // 列映射到 rack

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

    std::cout << "[Placement] Strategy2 done: " << total_blocks << " blocks placed.\n";
}

