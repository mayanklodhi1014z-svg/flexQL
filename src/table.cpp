#include "flexql/table.h"
#include <algorithm>
#include <mutex>
#include <ctime>
#include <string>

namespace flexql {

// Constructor is defined inline in table.h

void Table::reserveRows(size_t additional) {
    size_t current_rows = row_count_.load(std::memory_order_relaxed);
    size_t required = current_rows + additional;
    size_t current_cap = current_capacity_.load(std::memory_order_acquire);
    
    if (required > current_cap) {
        std::unique_lock<std::mutex> lock(resize_mutex_);
        
        current_cap = current_capacity_.load(std::memory_order_acquire);
        if (required > current_cap) {
            // Implement dynamic exponential growth to conserve memory for small tables,
            // while preserving O(1) amortized insertion throughput for large benchmarks.
            size_t exponential_cap = current_cap == 0 ? 8192 : current_cap * 2;
            size_t new_cap = std::max(required, exponential_cap);
            
            // Resize backing string storage for all columns for maximum insert throughput
            for (size_t i = 0; i < schema_.size(); ++i) {
                columns_data_[i].resize(new_cap);
                if (schema_[i].type == ColumnType::INT) {
                    int_columns_[i].resize(new_cap);
                    int_nulls_[i].resize(new_cap);
                } else if (schema_[i].type == ColumnType::DECIMAL) {
                    decimal_columns_[i].resize(new_cap);
                    decimal_nulls_[i].resize(new_cap);
                }
            }
            expiration_timestamps_.resize(new_cap);
            
            // Atomically publish the new safe capacity to waiting threads
            current_capacity_.store(new_cap, std::memory_order_release);
        }
    }
}

bool Table::getRowByPK(const std::string& pk_val, std::vector<std::string>& out_row) const {
    size_t row_idx = 0;
    if (!getRowIndexByPK(pk_val, row_idx)) {
        return false;
    }

    out_row.resize(schema_.size());
    for (size_t i = 0; i < schema_.size(); ++i) {
        out_row[i] = columns_data_[i][row_idx];
    }
    return true;
}

bool Table::getRowIndexByPK(const std::string& pk_val, size_t& out_row_idx) const {
    if (pk_col_ < 0) {
        return false;
    }

    size_t row_idx = 0;
    if (!primary_index_.get(pk_val, row_idx)) {
        return false;
    }

    if (row_idx >= row_count_.load(std::memory_order_acquire)) {
        return false;
    }

    time_t exp = expiration_timestamps_[row_idx];
    if (exp != 0 && exp < time(NULL)) {
        return false;
    }

    out_row_idx = row_idx;
    return true;
}

void Table::insertRowFast(const std::string* row_data, size_t num_cols, time_t expires_at) {
    (void)num_cols;
    // Atomic fetch_add returns the unique slot for this specific thread
    size_t new_row_idx = row_count_.fetch_add(1, std::memory_order_relaxed);

    // Store values as-is (string-first path is faster for benchmark INSERT batches)
    for (size_t i = 0; i < schema_.size(); ++i) {
        columns_data_[i][new_row_idx] = row_data[i];

        switch (schema_[i].type) {
            case ColumnType::INT:
                if (row_data[i] == "NULL") {
                    int_nulls_[i][new_row_idx] = 1;
                    int_columns_[i][new_row_idx] = 0;
                } else {
                    int_nulls_[i][new_row_idx] = 0;
                    int_columns_[i][new_row_idx] = static_cast<int32_t>(std::strtol(row_data[i].c_str(), nullptr, 10));
                }
                break;
            case ColumnType::DECIMAL:
                if (row_data[i] == "NULL") {
                    decimal_nulls_[i][new_row_idx] = 1;
                    decimal_columns_[i][new_row_idx] = 0.0;
                } else {
                    decimal_nulls_[i][new_row_idx] = 0;
                    decimal_columns_[i][new_row_idx] = std::strtod(row_data[i].c_str(), nullptr);
                }
                break;
            case ColumnType::VARCHAR:
            case ColumnType::DATETIME:
                break;
        }
    }
    expiration_timestamps_[new_row_idx] = expires_at;

    // Memory release ensures all writes are fully written before index is updated
    std::atomic_thread_fence(std::memory_order_release);

    if (pk_col_ >= 0) {
        primary_index_.insert(row_data[pk_col_], new_row_idx);
    }

    // Incremental maintenance for already-built range indexes.
    // If index set is dirty, a full rebuild will happen on next indexed query.
    if (!range_indexes_dirty_.load(std::memory_order_acquire)) {
        for (size_t i = 0; i < schema_.size(); ++i) {
            if (isNull(i, new_row_idx)) {
                continue;
            }
            if (schema_[i].type == ColumnType::INT && int_range_indexes_[i]) {
                int_range_indexes_[i]->insert(int_columns_[i][new_row_idx], new_row_idx);
            } else if (schema_[i].type == ColumnType::DECIMAL && decimal_range_indexes_[i]) {
                decimal_range_indexes_[i]->insert(decimal_columns_[i][new_row_idx], new_row_idx);
            }
        }
    }
}

void Table::buildRangeIndexes() {
    // Build range indexes for all non-PK numeric columns for O(log n) range queries
    size_t num_rows = row_count_.load(std::memory_order_acquire);
    
    for (size_t col_idx = 0; col_idx < schema_.size(); ++col_idx) {
        // Skip primary key (already has PK hash index)
        if ((int)col_idx == pk_col_) continue;
        
        if (schema_[col_idx].type == ColumnType::INT) {
            // Create INT range index
            if (!int_range_indexes_[col_idx]) {
                int_range_indexes_[col_idx] = new BTreeIndex<int32_t>();
            }
            int_range_indexes_[col_idx]->clear();
            
            // Populate index from int_columns_
            for (size_t r = 0; r < num_rows; ++r) {
                if (!isNull(col_idx, r)) {
                    int_range_indexes_[col_idx]->insert(int_columns_[col_idx][r], r);
                }
            }
            int_range_indexes_[col_idx]->build();  // Sort for range queries
            
        } else if (schema_[col_idx].type == ColumnType::DECIMAL) {
            // Create DECIMAL range index
            if (!decimal_range_indexes_[col_idx]) {
                decimal_range_indexes_[col_idx] = new BTreeIndex<double>();
            }
            decimal_range_indexes_[col_idx]->clear();
            
            // Populate index from decimal_columns_
            for (size_t r = 0; r < num_rows; ++r) {
                if (!isNull(col_idx, r)) {
                    decimal_range_indexes_[col_idx]->insert(decimal_columns_[col_idx][r], r);
                }
            }
            decimal_range_indexes_[col_idx]->build();  // Sort for range queries
        }
    }
    range_indexes_dirty_.store(false, std::memory_order_release);
}

void Table::getRangeIndexResults(size_t col_idx, const std::string& op,
                                 const std::string& value, std::vector<size_t>& row_indices) const {
    if (schema_[col_idx].type == ColumnType::INT) {
        int32_t target = static_cast<int32_t>(std::strtoll(value.c_str(), nullptr, 10));
        if (int_range_indexes_[col_idx]) {
            if (op == "=") {
                int_range_indexes_[col_idx]->findEqual(target, row_indices);
            } else if (op == ">") {
                int_range_indexes_[col_idx]->findGreater(target, row_indices);
            } else if (op == ">=") {
                int_range_indexes_[col_idx]->findGreaterEqual(target, row_indices);
            } else if (op == "<") {
                int_range_indexes_[col_idx]->findLess(target, row_indices);
            } else if (op == "<=") {
                int_range_indexes_[col_idx]->findLessEqual(target, row_indices);
            }
        }
    } else if (schema_[col_idx].type == ColumnType::DECIMAL) {
        double target = std::strtod(value.c_str(), nullptr);
        if (decimal_range_indexes_[col_idx]) {
            if (op == "=") {
                decimal_range_indexes_[col_idx]->findEqual(target, row_indices);
            } else if (op == ">") {
                decimal_range_indexes_[col_idx]->findGreater(target, row_indices);
            } else if (op == ">=") {
                decimal_range_indexes_[col_idx]->findGreaterEqual(target, row_indices);
            } else if (op == "<") {
                decimal_range_indexes_[col_idx]->findLess(target, row_indices);
            } else if (op == "<=") {
                decimal_range_indexes_[col_idx]->findLessEqual(target, row_indices);
            }
        }
    }
}

void Table::getRangeIndexOrdered(size_t col_idx, bool desc, std::vector<size_t>& row_indices) const {
    if (schema_[col_idx].type == ColumnType::INT && int_range_indexes_[col_idx]) {
        if (desc) {
            int_range_indexes_[col_idx]->getAllSortedDesc(row_indices);
        } else {
            int_range_indexes_[col_idx]->getAllSorted(row_indices);
        }
    } else if (schema_[col_idx].type == ColumnType::DECIMAL && decimal_range_indexes_[col_idx]) {
        if (desc) {
            decimal_range_indexes_[col_idx]->getAllSortedDesc(row_indices);
        } else {
            decimal_range_indexes_[col_idx]->getAllSorted(row_indices);
        }
    }
}

} // namespace flexql
