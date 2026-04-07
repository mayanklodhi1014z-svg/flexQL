#pragma once
#include <string>
#include <vector>
#include <filesystem>
#include <shared_mutex>
#include <unordered_map>
#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <functional>
#include "flexql/lockfree_hashmap.h"
#include "flexql/btree_index.h"

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
    int varchar_max_length = -1;
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
        int_columns_.resize(schema.size());
        int_nulls_.resize(schema.size());
        decimal_columns_.resize(schema.size());
        decimal_nulls_.resize(schema.size());
        int_range_indexes_.resize(schema.size(), nullptr);
        decimal_range_indexes_.resize(schema.size(), nullptr);
        try {
            std::cout << "Loading table: " << name_ << std::endl;
            for (const auto& entry : std::filesystem::directory_iterator("data/sst")) {
                if (entry.path().filename().string().find(name_ + "_") == 0) {
                    sst_files_.push_back(entry.path().string());
                    std::cout << "Added SST for table " << name_ << ": " << entry.path().string() << std::endl;
                }
            }
        } catch(...) {}
    }
    ~Table() {
        // Clean up dynamically allocated range indexes
        for (auto idx : int_range_indexes_) {
            delete idx;
        }
        for (auto idx : decimal_range_indexes_) {
            delete idx;
        }
    }

    const std::string& name() const { return name_; }
    void insertRowFast(const std::string* row_data, size_t num_cols, time_t expires_at = 0);
    void reserveRows(size_t additional);
    
    // LSM-Tree operations
    void flushToSSTable(const std::string& directory);
    void scanSSTables(const std::function<void(const std::vector<std::string>& row_data, time_t expiration)>& callback) const;
    
    const std::vector<ColumnDef>& schema() const { return schema_; }
    std::shared_mutex& mutex() { return table_mutex_; }
    
    int primary_key_col() const { return pk_col_; }

    size_t rowCount() const { return row_count_.load(std::memory_order_acquire); }
    size_t maxCapacity() const { return current_capacity_.load(std::memory_order_acquire); }

    const std::vector<std::string>& getColumn(size_t col_idx) const {
        return columns_data_[col_idx];
    }

    // Typed accessors for executors/WHERE/ORDER BY
    int32_t getInt(size_t col_idx, size_t row_idx) const {
        if (col_idx < int_columns_.size() && row_idx < int_columns_[col_idx].size()) {
            if (col_idx < int_nulls_.size() && row_idx < int_nulls_[col_idx].size() && int_nulls_[col_idx][row_idx]) {
                return 0;
            }
            return int_columns_[col_idx][row_idx];
        }
        return 0;
    }
    double getDecimal(size_t col_idx, size_t row_idx) const {
        if (col_idx < decimal_columns_.size() && row_idx < decimal_columns_[col_idx].size()) {
            if (col_idx < decimal_nulls_.size() && row_idx < decimal_nulls_[col_idx].size() && decimal_nulls_[col_idx][row_idx]) {
                return 0.0;
            }
            return decimal_columns_[col_idx][row_idx];
        }
        return 0.0;
    }
    const std::string& getString(size_t col_idx, size_t row_idx) const {
        return columns_data_[col_idx][row_idx];
    }

    bool isNull(size_t col_idx, size_t row_idx) const {
        if (col_idx < int_nulls_.size() && row_idx < int_nulls_[col_idx].size() && int_nulls_[col_idx][row_idx]) {
            return true;
        }
        if (col_idx < decimal_nulls_.size() && row_idx < decimal_nulls_[col_idx].size() && decimal_nulls_[col_idx][row_idx]) {
            return true;
        }
        return row_idx < columns_data_[col_idx].size() && columns_data_[col_idx][row_idx] == "NULL";
    }

    bool getRowIndexByPK(const std::string& pk_val, size_t& out_row_idx) const;
    bool getRowByPK(const std::string& pk_val, std::vector<std::string>& out_row) const;

    const std::vector<time_t>& getExpirations() const {
        return expiration_timestamps_;
    }

    // Range index operations
    void buildRangeIndexes();
    void getRangeIndexResults(size_t col_idx, const std::string& op, 
                             const std::string& value, std::vector<size_t>& row_indices) const;
    void getRangeIndexOrdered(size_t col_idx, bool desc, std::vector<size_t>& row_indices) const;

private:
    friend class StorageEngine;
    friend class Executor;

    std::string name_;
    std::vector<ColumnDef> schema_;
    int pk_col_ = -1;

    // Typed storage: VARCHAR/DATETIME use columns_data_, INT/DECIMAL use their own vectors
    std::vector<std::vector<std::string>> columns_data_;
    std::vector<std::vector<int32_t>> int_columns_;
    std::vector<std::vector<uint8_t>> int_nulls_;
    std::vector<std::vector<double>> decimal_columns_;
    std::vector<std::vector<uint8_t>> decimal_nulls_;
    std::vector<time_t> expiration_timestamps_;
    std::vector<std::string> sst_files_; // LSM-Tree written SST files
    
    BTreeIndex<std::string> primary_index_;
    
    // Optional range indexes for non-PK columns (one per numeric column)
    std::vector<BTreeIndex<int32_t>*> int_range_indexes_;       // Range indexes for INT columns
    std::vector<BTreeIndex<double>*> decimal_range_indexes_;    // Range indexes for DECIMAL columns

    std::atomic<size_t> row_count_{0};
    std::atomic<bool> range_indexes_dirty_{true};
    std::atomic<size_t> current_capacity_{0}; // Protects against unsafe vector bounds reads
    std::mutex resize_mutex_;
    std::shared_mutex table_mutex_;

public:
    void rebuildPrimaryIndex() {
        // Simple rebuild, assuming it's only called on single thread during WAL load
        if (pk_col_ < 0 || columns_data_.empty()) return;
        primary_index_.clear();
        size_t limit = row_count_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < limit; ++i) {
            primary_index_.insert(columns_data_[pk_col_][i], i);
        }
    }

    void clearData() {
        for (auto& col : columns_data_) {
            col.clear();
        }
        for (auto& col : int_columns_) {
            col.clear();
        }
        for (auto& col : int_nulls_) {
            col.clear();
        }
        for (auto& col : decimal_columns_) {
            col.clear();
        }
        for (auto& col : decimal_nulls_) {
            col.clear();
        }
        expiration_timestamps_.clear();
        primary_index_.clear();
        
        // Clear range indexes
        for (auto idx : int_range_indexes_) {
            if (idx) idx->clear();
        }
        for (auto idx : decimal_range_indexes_) {
            if (idx) idx->clear();
        }
        range_indexes_dirty_.store(true, std::memory_order_release);
        
        row_count_.store(0, std::memory_order_release);
        current_capacity_.store(0, std::memory_order_release);
    }
};

} // namespace flexql
