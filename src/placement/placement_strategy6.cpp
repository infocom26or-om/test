#include "placement.hpp"

// ---------------------------------------------------------
// strategy6：每 (m2+1)×(m1+1) 个 block 为一组放在同一个 rack
// 但组左上角的块( row%(m2+1)==0 且 col%(m1+1)==0 )统一放在最后一个 rack
// ---------------------------------------------------------
void Placement::strategy6_generate() {
    placement_map_.clear();

    int rows = k2_ + m2_;
    int cols = k1_ + m1_;
    int total_blocks = rows * cols;

    if (rows <= 0 || cols <= 0) {
        std::cerr << "[Placement-Strategy6] ERROR: invalid rows/cols\n";
        return;
    }

    // group size
    int h = m2_ + 1;  // group height
    int w = m1_ + 1;  // group width

    if (h <= 0 || w <= 0) {
        std::cerr << "[Placement-Strategy6] ERROR: invalid group size\n";
        return;
    }

    // number of group blocks in grid
    int group_rows = (rows + h - 1) / h;
    int group_cols = (cols + w - 1) / w;
    int normal_group_count = group_rows * group_cols;

    // total racks needed = normal groups + 1 (special rack)
    if (rack_count_ < normal_group_count + 1) {
        std::cerr << "[Placement-Strategy6] ERROR: not enough racks. "
                  << "Need at least " << (normal_group_count + 1)
                  << ", but have " << rack_count_ << "\n";
        return;
    }

    // 每 rack 的 server 轮转指针
    std::vector<int> rack_next_srv(rack_count_, 0);

    for (int id = 0; id < total_blocks; ++id) {
        int row, col;
        blockid_to_rowcol(id, row, col);

        bool is_special = (row % h == 0) && (col % w == 0);

        int rack;
        if (is_special) {
            // 所有特殊块放最后一个 rack
            rack = normal_group_count;
        } else {
            int group_r = row / h;
            int group_c = col / w;
            int group_id = group_r * group_cols + group_c;
            rack = group_id;
        }

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

    std::cout << "[Placement] Strategy6 done: " << total_blocks
              << " blocks, " << normal_group_count
              << " normal groups + 1 special group, "
              << "h=" << h << " w=" << w << ".\n";
}
