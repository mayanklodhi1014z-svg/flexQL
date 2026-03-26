#pragma once
#include <string>
#include <vector>
#include <shared_mutex>
#include <unordered_map>
#include <atomic>
#include "flexql/lockfree_hashmap.h"

namespace flexql {

enum class ColumnType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

struct ColumnDef {
    std::string name;
    ColumnType type;
    bool is_primary_key = false;
    bool is_not_null = false;
};

class Table {
public:
    Table(const std::string& name, const std::vector<ColumnDef>& schema)
        : name_(name), schema_(schema), row_count_(0) {
        for (size_t i = 0; i < schema.size(); ++i) {
            if (schema[i].is_primary_key) {
                pk_col_ = i;
                break;
            }
        }
        columns_data_.resize(schema.size());
    }
    ~Table() = default;

    const std::string& name() const { return name_; }
    void insertRowFast(const std::string* row_data, size_t num_cols, time_t expires_at = 0);
    void reserveRows(size_t additional);
    
    const std::vector<ColumnDef>& schema() const { return schema_; }
    std::shared_mutex& mutex() { return table_mutex_; }
    
    int primary_key_col() const { return pk_col_; }

    size_t rowCount() const { return row_count_.load(std::memory_order_acquire); }
    size_t maxCapacity() const { return current_capacity_.load(std::memory_order_acquire); }

    const std::vector<std::string>& getColumn(size_t col_idx) const {
        return columns_data_[col_idx];
    }

    bool getRowByPK(const std::string& pk_val, std::vector<std::string>& out_row) const;

    const std::vector<time_t>& getExpirations() const {
        return expiration_timestamps_;
    }

private:
    friend class StorageEngine;
    friend class Executor;

    std::string name_;
    std::vector<ColumnDef> schema_;
    int pk_col_ = -1;

    std::vector<std::vector<std::string>> columns_data_;
    std::vector<time_t> expiration_timestamps_;
    
    LockFreeHashMap primary_index_;

    std::atomic<size_t> row_count_{0};
    std::atomic<size_t> current_capacity_{0}; // Protects against unsafe vector bounds reads
    std::mutex resize_mutex_;
    std::shared_mutex table_mutex_;

public:
    void rebuildPrimaryIndex() {
        // Simple rebuild, assuming it's only called on single thread during WAL load
        if (pk_col_ < 0 || columns_data_.empty()) return;
        size_t limit = row_count_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < limit; ++i) {
            primary_index_.insert(columns_data_[pk_col_][i], i);
        }
    }
};

} // namespace flexql
