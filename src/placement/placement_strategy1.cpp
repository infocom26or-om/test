#include "placement.hpp"

// ---------------------------------------------------------
// strategy1：每个 block 依次放不同 rack + 不同 server
// ---------------------------------------------------------
void Placement::strategy1_generate() {
    placement_map_.clear();

    int total_blocks = (k1_ + m1_) * (k2_ + m2_);

    for (int id = 0; id < total_blocks; id++) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        PlacementEntry e;
        e.block_id = id;
        e.row      = row;
        e.col      = col;
        e.rack     = id;
        e.server_index = 0;

        placement_map_[id] = e;
    }
    std::cout << "[Placement] Strategy1 done: " << total_blocks << " blocks placed.\n";
}
