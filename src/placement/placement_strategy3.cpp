#include "placement.hpp"

// ---------------------------------------------------------
// strategy3：每一行（k1+m1列块）放在同一个 rack
// ---------------------------------------------------------
void Placement::strategy3_generate() {
    placement_map_.clear();

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    if (rack_count_ < rows) {
        std::cerr << "[Placement-Strategy3] ERROR: rack_count("
                  << rack_count_ << ") < total rows("
                  << rows << "). Each row needs 1 rack.\n";
        return;
    }

    // 每 rack 的 server 轮转指针
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; id++) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        int rack = row;  // 映射：行 -> rack

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

    std::cout << "[Placement] Strategy3 done: " << total_blocks << " blocks placed.\n";
}
