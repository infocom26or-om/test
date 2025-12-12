// gf256_solver.cpp
#include "gf256_solver.hpp"
#include <cassert>
#include <iostream>

static const int GF_SIZE = 256;
static uint8_t gf_log[GF_SIZE];
static uint8_t gf_exp[2 * GF_SIZE];

void init_tables() {
    int poly = 0x11d;
    gf_exp[0] = 1;
    for (int i = 1; i < 512; ++i) {
        gf_exp[i] = gf_exp[i - 1] << 1;
        if (gf_exp[i - 1] & 0x80)
            gf_exp[i] ^= poly;
    }

    for (int i = 0; i < 255; ++i)
        gf_log[gf_exp[i]] = i;
}

uint8_t gf256_mul(uint8_t a, uint8_t b) {
    if (a == 0 || b == 0) return 0;
    int log_a = gf_log[a];
    int log_b = gf_log[b];
    int sum = log_a + log_b;
    return gf_exp[sum % 255];
}

uint8_t gf256_inv(uint8_t a) {
    assert(a != 0);
    return gf_exp[(255 - gf_log[a]) % 255];
}

// 计算 a^n (a != 0) 在 GF(256) 中的幂
uint8_t gf256_pow(uint8_t a, int n) {
    if (n == 0) return 1;   // 任何数的0次幂为1
    if (a == 0) return 0;   // 0的任何正次幂为0

    int log_a = gf_log[a];
    if (log_a == 0 && a != 1) return 0;  // a=1情况除外，0为非法输入

    int log_result = (log_a * n) % 255;
    if (log_result < 0) log_result += 255;

    return gf_exp[log_result];
}

bool gf256_gaussian_elimination(const std::vector<std::vector<int>>& A,
                                const std::vector<std::vector<uint8_t>>& B,
                                std::vector<std::vector<uint8_t>>& X) {
    int n = A.size();        // 方程数量
    if (n == 0) return false;
    int m = B[0].size();     // 每个方程对应的数据长度（通常是 block_size）

    X.assign(n, std::vector<uint8_t>(m, 0));

    // 拷贝矩阵 A 到 uint8_t 类型的 mat 中
    std::vector<std::vector<uint8_t>> mat(n, std::vector<uint8_t>(n));
    for (int i = 0; i < n; ++i)
        for (int j = 0; j < n; ++j)
            mat[i][j] = static_cast<uint8_t>(A[i][j] & 0xFF);

    std::vector<std::vector<uint8_t>> rhs = B;  // 拷贝 RHS

    for (int i = 0; i < n; ++i) {
        // 找到第 i 列非 0 的 pivot 行
        int pivot = -1;
        for (int r = i; r < n; ++r) {
            if (mat[r][i] != 0) {
                pivot = r;
                break;
            }
        }

        if (pivot == -1) {
            std::cerr << "[GF(256)] Singular matrix at column " << i << std::endl;
            return false;
        }

        // 若 pivot 不是当前行，则交换
        if (pivot != i) {
            std::swap(mat[i], mat[pivot]);
            std::swap(rhs[i], rhs[pivot]);
        }

        // 归一化 pivot 行
        uint8_t inv = gf256_inv(mat[i][i]);
        for (int j = 0; j < n; ++j)
            mat[i][j] = gf256_mul(mat[i][j], inv) & 0xFF;
        for (int j = 0; j < m; ++j)
            rhs[i][j] = gf256_mul(rhs[i][j], inv) & 0xFF;

        // 消元：将其他行第 i 列清零
        for (int r = 0; r < n; ++r) {
            if (r == i) continue;
            uint8_t factor = mat[r][i];
            for (int j = 0; j < n; ++j) {
                mat[r][j] = mat[r][j] ^ gf256_mul(factor, mat[i][j]) & 0xFF;
            }
            for (int j = 0; j < m; ++j) {
                rhs[r][j] = rhs[r][j] ^ gf256_mul(factor, rhs[i][j]) & 0xFF;
            }
        }
    }

    X = rhs;
    return true;
}
