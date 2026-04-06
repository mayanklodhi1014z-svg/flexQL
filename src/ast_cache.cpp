#include "flexql/ast_cache.h"

namespace flexql {

ASTCache::ASTCache(size_t capacity) : capacity_(capacity) {}

bool ASTCache::get(const std::string& query, ASTNode& out_ast) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_map_.find(query);
    if (it == cache_map_.end()) {
        return false;
    }
    
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.list_it);
    out_ast = it->second.ast;
    return true;
}

void ASTCache::put(const std::string& query, const ASTNode& ast) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_map_.find(query);
    if (it != cache_map_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second.list_it);
        it->second.ast = ast;
        return;
    }
    
    if (cache_map_.size() >= capacity_) {
        const std::string& lru_query = lru_list_.back();
        cache_map_.erase(lru_query);
        lru_list_.pop_back();
    }
    
    lru_list_.push_front(query);
    cache_map_[query] = {ast, lru_list_.begin()};
}

void ASTCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_map_.clear();
    lru_list_.clear();
}

} // namespace flexql
