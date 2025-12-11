
//memcached_client.hpp
#pragma once
#include <string>
#include <unordered_map>
#include <libmemcached/memcached.h>

class MemcachedClient {
private:
    std::unordered_map<std::string, memcached_st*> server_map;

    memcached_st* get_or_create_client(const std::string& server_ip, int port);

public:
    MemcachedClient();
    ~MemcachedClient();

    bool set(const std::string& server_ip, int port,
             const std::string& key, const std::string& value);

    bool get(const std::string& server_ip, int port,
             const std::string& key, std::string& value_out);
};

