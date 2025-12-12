#include "encoder.hpp"

#include <jerasure.h>
#include <jerasure/reed_sol.h>

#include "gf256_solver.hpp"

#include <cassert>
#include <cstring>
#include <algorithm>
#include <iostream>

// reshape_data:
// Input: data_blocks vector length == k1 * k2
// D[r][c] -> r in [0..k2-1], c in [0..k1-1]
// Each D[r][c] is block_size bytes (zero-padded if input shorter)
void Encoder::reshape_data(const std::vector<std::string>& data_blocks,
                           std::vector<std::vector<std::vector<uint8_t>>>& D,
                           int k1, int k2, int block_size) {
    assert((int)data_blocks.size() == k1 * k2);
    D.assign(k2, std::vector<std::vector<uint8_t>>(k1, std::vector<uint8_t>(block_size, 0)));

    for (int r = 0; r < k2; ++r) {
        for (int c = 0; c < k1; ++c) {
            int idx = r * k1 + c;
            const std::string& s = data_blocks[idx];
            size_t copy_len = std::min<size_t>(s.size(), (size_t)block_size);
            if (copy_len > 0) {
                memcpy(D[r][c].data(), s.data(), copy_len);
            }
            // rest already zero
        }
    }
}

// generate_row_parity:
// Use Reed-Sol Vandermonde coefficients for row parity:
// Build a (m1+1) x k1 matrix with reed_sol_vandermonde_coding_matrix(k1, m1+1, 8)
// and use rows 1..m1 as parity rows (consistent with prior Jerasure usage)
void Encoder::generate_row_parity(const std::vector<std::vector<std::vector<uint8_t>>>& D,
                                  std::vector<std::vector<std::vector<uint8_t>>>& R,
                                  int k1, int m1, int k2, int block_size) {
    R.assign(k2, std::vector<std::vector<uint8_t>>(m1, std::vector<uint8_t>(block_size, 0)));

    if (m1 == 0) return;

    int *vand_row = reed_sol_vandermonde_coding_matrix(k1, m1 + 1, 8); // (m1+1) * k1
    // use rows 1..m1 (skip row 0 which would be all-ones)
    for (int r = 0; r < k2; ++r) {
        for (int p = 0; p < m1; ++p) {
            for (int c = 0; c < k1; ++c) {
                int idx = (p + 1) * k1 + c; // row (p+1) in vandermonde
                uint8_t coef = static_cast<uint8_t>(vand_row[idx] & 0xFF);
                for (int b = 0; b < block_size; ++b) {
                    R[r][p][b] ^= gf256_mul(coef, D[r][c][b]);
                }
            }
        }
    }
    delete[] vand_row;
}

// generate_col_parity_for_data:
// For data columns only: use reed_sol_vandermonde_coding_matrix(k2, m2+1, 8)
// use rows 1..m2 as parity rows to produce C[q][c] for q in 0..m2-1, c in 0..k1-1
void Encoder::generate_col_parity_for_data(const std::vector<std::vector<std::vector<uint8_t>>>& D,
                                           std::vector<std::vector<std::vector<uint8_t>>>& C,
                                           int k1, int k2, int m2, int block_size) {
    C.assign(m2, std::vector<std::vector<uint8_t>>(k1, std::vector<uint8_t>(block_size, 0)));

    if (m2 == 0) return;

    int *vand_col = reed_sol_vandermonde_coding_matrix(k2, m2 + 1, 8); // (m2+1) * k2
    for (int q = 0; q < m2; ++q) {
        for (int c = 0; c < k1; ++c) {
            for (int r = 0; r < k2; ++r) {
                int idx = (q + 1) * k2 + r; // row (q+1)
                uint8_t coef = static_cast<uint8_t>(vand_col[idx] & 0xFF);
                for (int b = 0; b < block_size; ++b) {
                    C[q][c][b] ^= gf256_mul(coef, D[r][c][b]);
                }
            }
        }
    }
    delete[] vand_col;
}

// generate_cross_parity_from_R:
// Compute S[q][p] = column-parity applied to R[:,p]
// Use same column Vandermonde as used for data columns: reed_sol_vandermonde_coding_matrix(k2, m2+1, 8)
void Encoder::generate_cross_parity_from_R(const std::vector<std::vector<std::vector<uint8_t>>>& R,
                                           std::vector<std::vector<std::vector<uint8_t>>>& S,
                                           int k2, int m1, int m2, int block_size) {
    S.assign(m2, std::vector<std::vector<uint8_t>>(m1, std::vector<uint8_t>(block_size, 0)));

    if (m2 == 0 || m1 == 0) return;

    int *vand_col = reed_sol_vandermonde_coding_matrix(k2, m2 + 1, 8); // (m2+1) * k2
    for (int q = 0; q < m2; ++q) {
        for (int p = 0; p < m1; ++p) {
            for (int r = 0; r < k2; ++r) {
                int idx = (q + 1) * k2 + r; // row (q+1)
                uint8_t coef = static_cast<uint8_t>(vand_col[idx] & 0xFF);
                for (int b = 0; b < block_size; ++b) {
                    S[q][p][b] ^= gf256_mul(coef, R[r][p][b]);
                }
            }
        }
    }
    delete[] vand_col;
}

// Flatten the 2D PC matrix in true row-major order:
// Matrix shape = (k2 + m2) rows × (k1 + m1) cols
// Row 0..k2-1: [ D | R ]
// Row k2..k2+m2-1: [ C | S ]
std::unordered_map<int, std::string> Encoder::flatten_blocks(
    const std::vector<std::vector<std::vector<uint8_t>>>& D,  // k2 x k1
    const std::vector<std::vector<std::vector<uint8_t>>>& R,  // k2 x m1
    const std::vector<std::vector<std::vector<uint8_t>>>& C,  // m2 x k1
    const std::vector<std::vector<std::vector<uint8_t>>>& S,  // m2 x m1
    int k1, int m1, int k2, int m2, int block_size)
{
    std::unordered_map<int, std::string> result;
    int id = 0;

    int total_rows = k2 + m2;
    int total_cols = k1 + m1;

    for (int r = 0; r < total_rows; ++r) {
        for (int c = 0; c < total_cols; ++c) {

            const uint8_t* ptr = nullptr;

            if (r < k2 && c < k1) {
                // data block
                ptr = D[r][c].data();
            } 
            else if (r < k2 && c >= k1) {
                // row parity block
                int p = c - k1;
                ptr = R[r][p].data();
            } 
            else if (r >= k2 && c < k1) {
                // column parity block
                int q = r - k2;
                ptr = C[q][c].data();
            } 
            else {
                // cross parity block
                int q = r - k2;
                int p = c - k1;
                ptr = S[q][p].data();
            }

            result[id++] = std::string(reinterpret_cast<const char*>(ptr), block_size);
        }
    }

    return result;
}


// top-level encode driver
std::unordered_map<int, std::string> Encoder::encode(
    const std::vector<std::string>& data_blocks,
    int k1, int m1, int k2, int m2,
    int block_size) {

    // Ensure GF tables prepared before calling encode (main should call init_tables())
    // reshape D[r][c]
    std::vector<std::vector<std::vector<uint8_t>>> D;
    reshape_data(data_blocks, D, k1, k2, block_size);

    // R[r][p] row parity (k2 x m1)
    std::vector<std::vector<std::vector<uint8_t>>> R;
    generate_row_parity(D, R, k1, m1, k2, block_size);

    // C[q][c] column parity for data columns (m2 x k1)
    std::vector<std::vector<std::vector<uint8_t>>> C;
    generate_col_parity_for_data(D, C, k1, k2, m2, block_size);

    // S[q][p] cross parity (m2 x m1), computed from R's columns using same column coefficients
    std::vector<std::vector<std::vector<uint8_t>>> S;
    generate_cross_parity_from_R(R, S, k2, m1, m2, block_size);

    return flatten_blocks(D, R, C, S, k1, m1, k2, m2, block_size);
}
