#include "flexql/storage_engine.h"
#include <mutex>
#include <vector>

namespace flexql {

bool StorageEngine::createTable(const std::string& name, const std::vector<ColumnDef>& schema) {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    if (tables_.find(name) != tables_.end()) {
        return false;
    }
    tables_[name] = std::make_unique<Table>(name, schema);
    return true;
}

bool StorageEngine::dropTable(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    return tables_.erase(name) > 0;
}

bool StorageEngine::truncateTable(const std::string& name) {
    Table* table = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(tables_mutex_);
        auto it = tables_.find(name);
        if (it == tables_.end()) return false;
        table = it->second.get();
    }

    std::unique_lock<std::shared_mutex> tlock(table->mutex());
    table->clearData();
    return true;
}

Table* StorageEngine::getTable(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void StorageEngine::sweepExpiredRows() {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    for (auto& pair : tables_) {
        std::unique_lock<std::shared_mutex> tlock(pair.second->mutex(), std::try_to_lock);
        if (!tlock.owns_lock()) {
            continue;
        }
        
        Table* t = pair.second.get();
        if (t->columns_data_.empty()) continue;

        std::vector<size_t> rows_to_keep;
        time_t now = time(NULL);
        
        bool cleaned_any = false;
        size_t num_rows = t->rowCount();
        for (size_t r = 0; r < num_rows; ++r) {
            if (t->expiration_timestamps_[r] != 0 && t->expiration_timestamps_[r] < now) {
                cleaned_any = true;
            } else {
                rows_to_keep.push_back(r);
            }
        }
        if (!cleaned_any) continue;

        size_t safe_cap = t->maxCapacity();

        for (size_t c = 0; c < t->schema().size(); ++c) {
            std::vector<std::string> new_col;
            new_col.reserve(safe_cap);
            for (size_t r : rows_to_keep) {
                new_col.push_back(t->columns_data_[c][r]);
            }
            new_col.resize(safe_cap);
            t->columns_data_[c] = std::move(new_col);
            
            // Also compact typed columns (INT/DECIMAL)
            if (t->schema()[c].type == flexql::ColumnType::INT) {
                std::vector<int32_t> new_int_col;
                std::vector<uint8_t> new_int_nulls;
                new_int_col.reserve(safe_cap);
                new_int_nulls.reserve(safe_cap);
                for (size_t r : rows_to_keep) {
                    new_int_col.push_back(t->int_columns_[c][r]);
                    new_int_nulls.push_back(t->int_nulls_[c][r]);
                }
                new_int_col.resize(safe_cap);
                new_int_nulls.resize(safe_cap);
                t->int_columns_[c] = std::move(new_int_col);
                t->int_nulls_[c] = std::move(new_int_nulls);
            } else if (t->schema()[c].type == flexql::ColumnType::DECIMAL) {
                std::vector<double> new_dec_col;
                std::vector<uint8_t> new_dec_nulls;
                new_dec_col.reserve(safe_cap);
                new_dec_nulls.reserve(safe_cap);
                for (size_t r : rows_to_keep) {
                    new_dec_col.push_back(t->decimal_columns_[c][r]);
                    new_dec_nulls.push_back(t->decimal_nulls_[c][r]);
                }
                new_dec_col.resize(safe_cap);
                new_dec_nulls.resize(safe_cap);
                t->decimal_columns_[c] = std::move(new_dec_col);
                t->decimal_nulls_[c] = std::move(new_dec_nulls);
            }
        }
        
        std::vector<time_t> new_exp;
        new_exp.reserve(safe_cap);
        for (size_t r : rows_to_keep) {
            new_exp.push_back(t->expiration_timestamps_[r]);
        }
        new_exp.resize(safe_cap);
        t->expiration_timestamps_ = std::move(new_exp);

        t->row_count_.store(rows_to_keep.size(), std::memory_order_release);

        if (t->primary_key_col() >= 0) {
            t->rebuildPrimaryIndex();
        }
        
        // Rebuild range indexes after compaction
        t->buildRangeIndexes();
        t->range_indexes_dirty_.store(false, std::memory_order_release);
    }
}

} // namespace flexql
