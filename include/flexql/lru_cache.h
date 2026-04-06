#pragma once
#include "flexql/types.h"
#include <string>
#include <list>
#include <unordered_map>
#include <mutex>

namespace flexql {

class LRUCache {
public:
    explicit LRUCache(size_t capacity = 4096);
    
    // Returns true if found, sets out_res
    bool get(const std::string& query, QueryResult& out_res);
    
    // Puts result in cache
    void put(const std::string& query, const QueryResult& res);
    
    // Invalidates all queries containing table_name (simple string match)
    void invalidateTable(const std::string& table_name);

private:
    struct CacheItem {
        std::string query;
        QueryResult res;
    };

    size_t capacity_;
    std::list<CacheItem> list_;
    std::unordered_map<std::string, std::list<CacheItem>::iterator> map_;
    mutable std::mutex mutex_;
};

} // namespace flexql
