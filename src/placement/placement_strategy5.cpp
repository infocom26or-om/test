#include "placement.hpp"

// ---------------------------------------------------------
// strategy5：每 m2 行为一组放在同一个 rack，组内 block 在 rack 的不同 server 轮转
// 例如 m2=2: 行 0,1 -> rack0; 行 2,3 -> rack1; 行 4,5 -> rack2; ...
// ---------------------------------------------------------
void Placement::strategy5_generate() {
    placement_map_.clear();

    // sanity
    if (m2_ <= 0) {
        std::cerr << "[Placement-Strategy5] ERROR: m2 must be > 0\n";
        return;
    }

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    // 需要的组数 = ceil(rows / m2_)
    int groups = (rows + m2_ - 1) / m2_;
    if (rack_count_ < 1) {
        std::cerr << "[Placement-Strategy5] ERROR: rack_count must be >= 1\n";
        return;
    }

    // 每 rack 的 server 轮转指针
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; ++id) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        // determine which group this row belongs to
        int group = row / m2_;               // group index (0..groups-1)
        int rack  = group % rack_count_;     // map group -> rack (wrap if fewer racks)

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

    std::cout << "[Placement] Strategy5 done: " << total_blocks
              << " blocks placed into " << groups << " row-groups (m2=" << m2_ << ").\n";
}
