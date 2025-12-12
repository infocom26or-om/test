#pragma once

#include <vector>
#include <string>
#include <unordered_map>

// Encoder for Product Code PC(k1, m1, k2, m2)
// Data layout:
//   - data: k2 rows x k1 cols
//   - row parity: k2 rows x m1 cols (right side)
//   - col parity (for data columns): m2 rows x k1 cols (bottom left)
//   - cross parity (shared): m2 rows x m1 cols (bottom right)  <-- same physical blocks
//
// flatten order (block_id increasing):
// 1) data: r=0..k2-1, c=0..k1-1   => id = r*k1 + c
// 2) row parity: r=0..k2-1, p=0..m1-1 => id = base_row + r*m1 + p
// 3) col parity (for data columns): q=0..m2-1, c=0..k1-1 => id = base_col + q*k1 + c
// 4) cross parity S: q=0..m2-1, p=0..m1-1 => id = base_cross + q*m1 + p
//
// Requires Jerasure (reed_sol_vandermonde_coding_matrix) and gf256 solver (init_tables(), gf256_mul, gf256_pow, etc.)
class Encoder {
public:
    // Encode data_blocks (length == k1 * k2). Each string may be shorter than block_size (will be zero-padded).
    // Returns mapping block_id -> block bytes (std::string of length block_size).
    std::unordered_map<int, std::string> encode(
        const std::vector<std::string>& data_blocks,
        int k1, int m1, int k2, int m2,
        int block_size);

private:
    // helpers use uint8_t vectors internally
    void reshape_data(const std::vector<std::string>& data_blocks,
                      std::vector<std::vector<std::vector<uint8_t>>>& D,
                      int k1, int k2, int block_size);

    void generate_row_parity(const std::vector<std::vector<std::vector<uint8_t>>>& D,
                             std::vector<std::vector<std::vector<uint8_t>>>& R,
                             int k1, int m1, int k2, int block_size);

    void generate_col_parity_for_data(const std::vector<std::vector<std::vector<uint8_t>>>& D,
                                      std::vector<std::vector<std::vector<uint8_t>>>& C,
                                      int k1, int k2, int m2, int block_size);

    void generate_cross_parity_from_R(const std::vector<std::vector<std::vector<uint8_t>>>& R,
                                      std::vector<std::vector<std::vector<uint8_t>>>& S,
                                      int k2, int m1, int m2, int block_size);

    std::unordered_map<int, std::string> flatten_blocks(
        const std::vector<std::vector<std::vector<uint8_t>>>& D,
        const std::vector<std::vector<std::vector<uint8_t>>>& R,
        const std::vector<std::vector<std::vector<uint8_t>>>& C,
        const std::vector<std::vector<std::vector<uint8_t>>>& S,
        int k1, int m1, int k2, int m2, int block_size);
};
