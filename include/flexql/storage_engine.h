#pragma once
#include "flexql/table.h"
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <string>

namespace flexql {

class StorageEngine {
public:
    StorageEngine() = default;
    ~StorageEngine() = default;

    bool createTable(const std::string& name, const std::vector<ColumnDef>& schema);
    bool dropTable(const std::string& name);
    Table* getTable(const std::string& name);

    void sweepExpiredRows();

private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    std::shared_mutex tables_mutex_;
};

} // namespace flexql
