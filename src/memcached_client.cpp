#include "memcached_client.hpp"
#include <iostream>

MemcachedClient::MemcachedClient() {}

MemcachedClient::~MemcachedClient() {
    for (auto& kv : server_map) {
        memcached_free(kv.second);
    }
}

memcached_st* MemcachedClient::get_or_create_client(const std::string& server_ip, int port) {
    std::string server_key = server_ip + ":" + std::to_string(port);
    if (server_map.find(server_key) == server_map.end()) {
        memcached_st* memc = memcached_create(NULL);
        if (!memc) {
            std::cerr << "Failed to create memcached client for " << server_key << std::endl;
            return nullptr;
        }
        memcached_server_add(memc, server_ip.c_str(), port);
        server_map[server_key] = memc;
    }
    return server_map[server_key];
}

bool MemcachedClient::set(const std::string& server_ip, int port,
                          const std::string& key, const std::string& value) {
    memcached_st* memc = get_or_create_client(server_ip, port);
    if (!memc) return false;

    memcached_return rc = memcached_set(memc, key.c_str(), key.length(),
                                        value.c_str(), value.length(),
                                        (time_t)0, 0);
    if (rc != MEMCACHED_SUCCESS) {
        std::cerr << "Memcached SET failed on " << server_ip << ":" << port
                  << " for key=" << key << ": " << memcached_strerror(memc, rc) << std::endl;
    }
    return rc == MEMCACHED_SUCCESS;
}

bool MemcachedClient::get(const std::string& server_ip, int port,
                          const std::string& key, std::string& value_out) {
    memcached_st* memc = get_or_create_client(server_ip, port);
    if (!memc) return false;

    size_t value_length;
    uint32_t flags;
    memcached_return rc;
    char* result = memcached_get(memc, key.c_str(), key.length(),
                                 &value_length, &flags, &rc);
    if (rc == MEMCACHED_SUCCESS && result != nullptr) {
        value_out.assign(result, value_length);
        free(result);
        return true;
    } else {
        std::cerr << "Memcached GET failed on " << server_ip << ":" << port
                  << " for key=" << key << ": " << memcached_strerror(memc, rc) << std::endl;
        return false;
    }
}
