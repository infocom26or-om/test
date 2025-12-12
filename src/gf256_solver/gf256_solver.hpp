// gf256_solver.hpp
#ifndef GF256_SOLVER_HPP
#define GF256_SOLVER_HPP

#include <vector>
#include <cstdint>

void init_tables();

uint8_t gf256_mul(uint8_t a, uint8_t b);
uint8_t gf256_inv(uint8_t a);

// 计算 a^n 在 GF(256) 上的幂
uint8_t gf256_pow(uint8_t a, int n);

bool gf256_gaussian_elimination(const std::vector<std::vector<int>>& A,
                                const std::vector<std::vector<uint8_t>>& B,
                                std::vector<std::vector<uint8_t>>& X);

#endif // GF256_SOLVER_HPP
