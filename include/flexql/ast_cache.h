#pragma once
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include "flexql/parser.h"

namespace flexql {

class ASTCache {
public:
    ASTCache(size_t capacity = 1000);
    ~ASTCache() = default;

    bool get(const std::string& query, ASTNode& out_ast);
    void put(const std::string& query, const ASTNode& ast);
    void clear();

private:
    size_t capacity_;
    std::mutex mutex_;
    std::list<std::string> lru_list_;
    
    struct CacheItem {
        ASTNode ast;
        std::list<std::string>::iterator list_it;
    };
    
    std::unordered_map<std::string, CacheItem> cache_map_;
};

} // namespace flexql
