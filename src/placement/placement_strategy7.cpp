#include "placement.hpp"

// ---------------------------------------------------------
// strategy7：Diagonal Shift（对角策略）
// rack = (row + col) % rack_count_
// 每个 rack 内 server 使用轮转分配
// ---------------------------------------------------------
void Placement::strategy7_generate() {
    placement_map_.clear();

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    if (rows <= 0 || cols <= 0 || rack_count_ <= 0) {
        std::cerr << "[Placement-Strategy7] ERROR: invalid parameters\n";
        return;
    }

    // 每 rack 的 server 轮转
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; ++id) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        // Diagonal mapping
        int rack = (row + col) % rack_count_;

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

    std::cout << "[Placement] Strategy7 done: " << total_blocks
              << " blocks diagonal-shift mapped into " << rack_count_
              << " racks.\n";
}
