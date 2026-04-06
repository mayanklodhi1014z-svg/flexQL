#pragma once
#include <atomic>
#include <string>
#include <vector>
#include <functional>

namespace flexql {

class LockFreeHashMap {
private:
    struct KVNode {
        std::string key;
        size_t value;
        std::atomic<KVNode*> next;

        KVNode(const std::string& k, size_t v, KVNode* n)
            : key(k), value(v), next(n) {}
    };

    std::vector<std::atomic<KVNode*>> buckets_;

public:
    // Pre-allocate millions of buckets to minimize hash collisions
    // Uses ~134MB of RAM for 16M buckets
    LockFreeHashMap(size_t bucket_count = 16777216) : buckets_(bucket_count) {
        for (size_t i = 0; i < bucket_count; ++i) {
            buckets_[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    ~LockFreeHashMap() {
        for (size_t i = 0; i < buckets_.size(); ++i) {
            KVNode* curr = buckets_[i].load(std::memory_order_relaxed);
            while (curr) {
                KVNode* next = curr->next.load(std::memory_order_relaxed);
                delete curr;
                curr = next;
            }
        }
    }

    // Completely Lock-Free insertion via atomic CPU CAS operations
    void insert(const std::string& key, size_t value) {
        size_t idx = std::hash<std::string>{}(key) % buckets_.size();
        KVNode* new_node = new KVNode(key, value, nullptr);

        while (true) {
            KVNode* head = buckets_[idx].load(std::memory_order_acquire);
            new_node->next.store(head, std::memory_order_relaxed);
            
            // Atomic instruction executes in 1 nanosecond; multiple threads will 
            // naturally retry safely without ever pausing if they collide.
            if (buckets_[idx].compare_exchange_weak(
                    head, new_node, 
                    std::memory_order_release, 
                    std::memory_order_relaxed)) {
                break;
            }
        }
    }

    // Lock-Free read matching
    bool get(const std::string& key, size_t& out_value) const {
        size_t idx = std::hash<std::string>{}(key) % buckets_.size();
        KVNode* curr = buckets_[idx].load(std::memory_order_acquire);
        
        while (curr) {
            if (curr->key == key) {
                out_value = curr->value;
                return true;
            }
            curr = curr->next.load(std::memory_order_acquire);
        }
        return false;
    }

    void clear() {
        for (size_t i = 0; i < buckets_.size(); ++i) {
            KVNode* head = buckets_[i].exchange(nullptr, std::memory_order_acq_rel);
            while (head) {
                KVNode* next = head->next.load(std::memory_order_relaxed);
                delete head;
                head = next;
            }
        }
    }
    
    // Not safely implemented for iterating during concurrent writes, just stubbed
    size_t size() const { return 0; }
};

} // namespace flexql
