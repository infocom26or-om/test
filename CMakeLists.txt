cmake_minimum_required(VERSION 3.10)
project(PC_System)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# === Manually set Jerasure ===
set(JERASURE_INCLUDE_DIR "/usr/local/include")
set(JERASURE_LIBRARY "/usr/local/lib/libJerasure.so")

# === Manually set GF-Complete ===
set(GALOIS_LIBRARY "/usr/local/lib/libgf_complete.so")

# === Manually set libmemcached ===
set(MEMCACHED_INCLUDE_DIR "/usr/include")  # 或 /usr/local/include 看你系统的情况
set(MEMCACHED_LIBRARY "/usr/lib/x86_64-linux-gnu/libmemcached.so")


# Add include directories
include_directories(
    ${JERASURE_INCLUDE_DIR}
    ${MEMCACHED_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/src
)

# === 源文件 ===
file(GLOB ENCODER_SRC "src/encoder/*.cpp")
file(GLOB PLACEMENT_SRC "src/placement/*.cpp")
file(GLOB REPAIR_SRC "src/repair/*.cpp")
file(GLOB GF256_SRC "src/gf256_solver/*.cpp")
file(GLOB UTIL_SRC "src/*.cpp")
set(OTHER src/memcached_client.cpp src/block_manager.cpp)

add_executable(PC_System
    main.cpp
    ${ENCODER_SRC}
    ${PLACEMENT_SRC}
    ${REPAIR_SRC}
    ${GF256_SRC}
    ${OTHER}
)

# Link libraries
target_link_libraries(ALRC_System
    ${JERASURE_LIBRARY}
    ${GALOIS_LIBRARY}
    ${MEMCACHED_LIBRARY}
)
