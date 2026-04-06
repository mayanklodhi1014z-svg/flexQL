#pragma once
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <algorithm>
#include <iostream>

namespace flexql {

// A True Cache-Aligned B+ Tree implementation replacing std::multimap.
// Node capacity is set to roughly align with standard 4KB memory pages for typical types
// bringing range query pointer dereferences from O(N) down to O(log_B N + K).
template<typename KeyType, size_t MaxKeys = 127>
class BTreeIndex {
private:
    struct Node {
        bool is_leaf;
        std::vector<KeyType> keys;
        Node() : is_leaf(false) { keys.reserve(MaxKeys + 1); }
        virtual ~Node() = default;
    };

    struct LeafNode : public Node {
        std::vector<size_t> values;
        LeafNode* next;
        LeafNode* prev;
        LeafNode() : Node(), next(nullptr), prev(nullptr) {
            this->is_leaf = true;
            values.reserve(MaxKeys + 1);
        }
    };

    struct InternalNode : public Node {
        std::vector<Node*> children;
        InternalNode() : Node() {
            children.reserve(MaxKeys + 2);
        }
        ~InternalNode() {
            for (Node* child : children) delete child;
        }
    };

    Node* root_;
    LeafNode* head_;
    LeafNode* tail_;
    mutable std::shared_mutex mutex_;
    size_t size_;

    void insertInternal(const KeyType& key, size_t value, Node* node, Node*& new_child, KeyType& split_key) {
        if (node->is_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            auto it = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key);
            size_t idx = std::distance(leaf->keys.begin(), it);
            
            leaf->keys.insert(leaf->keys.begin() + idx, key);
            leaf->values.insert(leaf->values.begin() + idx, value);

            if (leaf->keys.size() > MaxKeys) {
                LeafNode* new_leaf = new LeafNode();
                size_t mid = leaf->keys.size() / 2;
                
                new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
                new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
                
                leaf->keys.resize(mid);
                leaf->values.resize(mid);
                
                new_leaf->next = leaf->next;
                new_leaf->prev = leaf;
                if (leaf->next) leaf->next->prev = new_leaf;
                else tail_ = new_leaf;
                leaf->next = new_leaf;

                split_key = new_leaf->keys[0];
                new_child = new_leaf;
            } else {
                new_child = nullptr;
            }
            return;
        }

        InternalNode* internal = static_cast<InternalNode*>(node);
        auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
        size_t idx = std::distance(internal->keys.begin(), it);
        
        Node* child_split = nullptr;
        KeyType up_key;
        insertInternal(key, value, internal->children[idx], child_split, up_key);

        if (child_split) {
            internal->keys.insert(internal->keys.begin() + idx, up_key);
            internal->children.insert(internal->children.begin() + idx + 1, child_split);

            if (internal->keys.size() > MaxKeys) {
                InternalNode* new_internal = new InternalNode();
                size_t mid = internal->keys.size() / 2;

                split_key = internal->keys[mid];
                
                new_internal->keys.assign(internal->keys.begin() + mid + 1, internal->keys.end());
                new_internal->children.assign(internal->children.begin() + mid + 1, internal->children.end());
                
                internal->keys.resize(mid);
                internal->children.resize(mid + 1);
                
                new_child = new_internal;
            } else {
                new_child = nullptr;
            }
        } else {
            new_child = nullptr;
        }
    }

    LeafNode* findLeaf(const KeyType& target) const {
        if (!root_) return nullptr;
        Node* current = root_;
        while (!current->is_leaf) {
            InternalNode* internal = static_cast<InternalNode*>(current);
            auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), target);
            size_t idx = std::distance(internal->keys.begin(), it);
            current = internal->children[idx];
        }
        return static_cast<LeafNode*>(current);
    }

public:
    BTreeIndex() : root_(new LeafNode()), head_(static_cast<LeafNode*>(root_)), tail_(static_cast<LeafNode*>(root_)), size_(0) {}
    
    ~BTreeIndex() {
        delete root_;
    }

    void insert(const KeyType& key, size_t row_idx) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        Node* new_child = nullptr;
        KeyType split_key;
        insertInternal(key, row_idx, root_, new_child, split_key);
        
        if (new_child) {
            InternalNode* new_root = new InternalNode();
            new_root->keys.push_back(split_key);
            new_root->children.push_back(root_);
            new_root->children.push_back(new_child);
            root_ = new_root;
        }
        size_++;
    }

    void build() {}

    bool get(const KeyType& target, size_t& out_val) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = findLeaf(target);
        if (!leaf) return false;
        for (size_t i = 0; i < leaf->keys.size(); ++i) {
            if (leaf->keys[i] == target) {
                out_val = leaf->values[i];
                return true;
            }
            if (leaf->keys[i] > target) return false;
        }
        return false;
    }

    void findEqual(const KeyType& target, std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = findLeaf(target);
        if (!leaf) return;
        
        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] == target) result.push_back(leaf->values[i]);
                else if (leaf->keys[i] > target) return;
            }
            leaf = leaf->next;
        }
    }

    void findGreater(const KeyType& target, std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = findLeaf(target);
        if (!leaf) return;

        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] > target) result.push_back(leaf->values[i]);
            }
            leaf = leaf->next;
        }
    }

    void findGreaterEqual(const KeyType& target, std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = findLeaf(target);
        if (!leaf) return;

        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] >= target) result.push_back(leaf->values[i]);
            }
            leaf = leaf->next;
        }
    }

    void findLess(const KeyType& target, std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = head_;
        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] < target) result.push_back(leaf->values[i]);
                else return;
            }
            leaf = leaf->next;
        }
    }

    void findLessEqual(const KeyType& target, std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = head_;
        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] <= target) result.push_back(leaf->values[i]);
                else return;
            }
            leaf = leaf->next;
        }
    }

    void getAllSorted(std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = head_;
        while (leaf) {
            for (size_t val : leaf->values) result.push_back(val);
            leaf = leaf->next;
        }
    }

    void getAllSortedDesc(std::vector<size_t>& result) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        LeafNode* leaf = tail_;
        while (leaf) {
            for (auto it = leaf->values.rbegin(); it != leaf->values.rend(); ++it) {
                result.push_back(*it);
            }
            leaf = leaf->prev;
        }
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        delete root_;
        root_ = new LeafNode();
        head_ = static_cast<LeafNode*>(root_);
        tail_ = static_cast<LeafNode*>(root_);
        size_ = 0;
    }

    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return size_;
    }

    void rebuild(std::function<KeyType(size_t)> key_extractor, size_t total_rows) {
        clear();
        for (size_t r = 0; r < total_rows; ++r) {
            try {
                insert(key_extractor(r), r);
            } catch (...) {
                continue;
            }
        }
    }
};

}  // namespace flexql
