#pragma once
#include <string>
#include <vector>
#include <memory>
#include "flexql/table.h"

namespace flexql {

enum class StatementType {
    CREATE_TABLE,
    DROP_TABLE,
    INSERT,
    SELECT,
    UNKNOWN
};

struct ASTNode {
    StatementType type = StatementType::UNKNOWN;
    
    // Common
    std::string table_name;

    // For CREATE TABLE
    std::vector<ColumnDef> schema;

    // For INSERT (single row)
    std::vector<std::string> insert_values;
    // For INSERT (batch rows flattened)
    std::vector<std::string> batch_values;
    int num_columns_per_row = 0;
    time_t expires_at = 0;

    // For DROP TABLE
    bool if_exists = false;

    // For SELECT
    std::vector<std::string> select_columns; // "*" means all
    std::string where_column;
    std::string where_value;
    std::string join_table;
    std::string join_on_col1;
    std::string join_on_col2;    
};

class Parser {
public:
    static ASTNode parse(const std::string& query);
};

} // namespace flexql
