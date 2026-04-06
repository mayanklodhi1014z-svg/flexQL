#include "flexql/lru_cache.h"

namespace flexql {

LRUCache::LRUCache(size_t capacity) : capacity_(capacity) {}

bool LRUCache::get(const std::string& query, QueryResult& out_res) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(query);
    if (it == map_.end()) {
        return false;
    }
    
    list_.splice(list_.begin(), list_, it->second);
    out_res = it->second->res;
    return true;
}

void LRUCache::put(const std::string& query, const QueryResult& res) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(query);
    if (it != map_.end()) {
        it->second->res = res;
        list_.splice(list_.begin(), list_, it->second);
        return;
    }

    if (list_.size() >= capacity_) {
        map_.erase(list_.back().query);
        list_.pop_back();
    }
    list_.push_front({query, res});
    map_[query] = list_.begin();
}

void LRUCache::invalidateTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = list_.begin();
    
    std::string t_upper = table_name;
    for (char& c : t_upper) c = toupper(c);

    while (it != list_.end()) {
        std::string q_upper = it->query;
        for (char& c : q_upper) c = toupper(c);
        
        if (q_upper.find(t_upper) != std::string::npos) {
            map_.erase(it->query);
            it = list_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace flexql
