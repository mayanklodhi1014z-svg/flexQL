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
        std::unique_lock<std::shared_mutex> tlock(pair.second->mutex());
        
        Table* t = pair.second.get();
        if (t->columns_data_.empty()) continue;

        std::vector<size_t> rows_to_keep;
        time_t now = time(NULL);
        size_t scan_limit = t->rowCount();
        
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
                new_col.push_back(std::move(t->getColumn(c)[r]));
            }
            new_col.resize(safe_cap);
            t->columns_data_[c] = std::move(new_col);
        }
        
        std::vector<time_t> new_exp;
        new_exp.reserve(safe_cap);
        for (size_t r : rows_to_keep) {
            new_exp.push_back(t->expiration_timestamps_[r]);
        }
        new_exp.resize(safe_cap);
        t->expiration_timestamps_ = std::move(new_exp);

        if (t->primary_key_col() >= 0) {
            t->rebuildPrimaryIndex();
        }
    }
}

} // namespace flexql
