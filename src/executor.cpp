#include "flexql/executor.h"
#include <fstream>
#include <iostream>

namespace flexql {

Executor::Executor(StorageEngine& storage) : storage_(storage) {
    wal_out_.open("data/wal/server.log", std::ios::app);
    wal_thread_ = std::thread([this]() {
        while (true) {
            std::vector<std::string> local_queue;
            {
                std::unique_lock<std::mutex> lock(wal_queue_mutex_);
                wal_cv_.wait(lock, [this]() { return !wal_queue_.empty() || !wal_running_; });
                if (!wal_running_ && wal_queue_.empty()) break;
                local_queue.swap(wal_queue_);
            }
            std::lock_guard<std::mutex> lock(wal_mutex_);
            if (wal_out_) {
                for (const auto& q : local_queue) {
                    wal_out_ << q << "\n";
                }
                wal_out_.flush();
            }
        }
    });
}

Executor::~Executor() {
    {
        std::unique_lock<std::mutex> lock(wal_queue_mutex_);
        wal_running_ = false;
    }
    wal_cv_.notify_one();
    if (wal_thread_.joinable()) wal_thread_.join();
}

void Executor::appendToWal(const std::string& raw_query) {
    std::unique_lock<std::mutex> lock(wal_queue_mutex_);
    wal_queue_.push_back(raw_query);
    wal_cv_.notify_one();
}

void Executor::replayWal() {
    std::ifstream in("data/wal/server.log");
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            execute(line, true);
        }
    }
}

void Executor::sweepExpiredRows() {
    storage_.sweepExpiredRows();
}

QueryResult Executor::execute(const std::string& query_string, bool is_replay) {
    ASTNode ast;
    
    // Multi-level AST caching: Avoid re-parsing massive regex structures for identical queries.
    // We skip caching massively long batch inserts so we don't thrash the AST cache RAM footprint.
    bool string_is_cacheable = query_string.size() < 4096;
    
    if (!string_is_cacheable || !ast_cache_.get(query_string, ast)) {
        ast = Parser::parse(query_string);
        if (string_is_cacheable && ast.type != StatementType::UNKNOWN) {
            ast_cache_.put(query_string, ast);
        }
    }
    
    switch (ast.type) {
        case StatementType::CREATE_TABLE:
            return executeCreate(ast, is_replay, query_string);
        case StatementType::DROP_TABLE:
            return executeDrop(ast, is_replay, query_string);
        case StatementType::INSERT:
            return executeInsert(ast, is_replay, query_string);
        case StatementType::SELECT:
            if (is_replay) return {Status::ERROR, "SELECT in WAL", {}, {}};
            return executeSelect(ast, query_string);
        case StatementType::UNKNOWN:
        default:
            return {Status::ERROR, "Syntax error / Unknown command", {}, {}};
    }
}

QueryResult Executor::executeCreate(const ASTNode& ast, bool is_replay, const std::string& raw_query) {
    if (storage_.createTable(ast.table_name, ast.schema)) {
        if (!is_replay) appendToWal(raw_query);
        cache_.invalidateTable(ast.table_name);
        return {Status::OK, "", {}, {}};
    }
    return {Status::ERROR, "Failed to create table (already exists?)", {}, {}};
}

QueryResult Executor::executeDrop(const ASTNode& ast, bool is_replay, const std::string& raw_query) {
    bool dropped = storage_.dropTable(ast.table_name);
    if (!dropped && !ast.if_exists) {
        return {Status::ERROR, "Table not found", {}, {}};
    }
    if (!is_replay) appendToWal(raw_query);
    cache_.invalidateTable(ast.table_name);
    return {Status::OK, "", {}, {}};
}

QueryResult Executor::executeInsert(const ASTNode& ast, bool is_replay, const std::string& raw_query) {
    Table* t = storage_.getTable(ast.table_name);
    if (!t) return {Status::ERROR, "Table not found", {}, {}};

    // Because the table now uses a completely Lock-Free Hash Map and Atomic Row tracking, 
    // all writer threads can acquire a SHARED lock simultaneously!
    std::shared_lock<std::shared_mutex> lock(t->mutex());
    
    const std::vector<std::string>& rows = ast.batch_values.empty() ? ast.insert_values : ast.batch_values;
    size_t num_cols = ast.batch_values.empty() ? ast.insert_values.size() : ast.num_columns_per_row;
    
    if (num_cols == 0) return {Status::ERROR, "Empty insert", {}, {}};
    size_t num_rows = rows.size() / num_cols;
    
    // Pre-allocate memory for the entire batch to avoid repeated vector resizing
    t->reserveRows(num_rows);
    
    for (size_t r = 0; r < num_rows; ++r) {
        if (num_cols != t->schema().size()) {
            return {Status::ERROR, "Insert failed (column count mismatch)", {}, {}};
        }
        t->insertRowFast(&rows[r * num_cols], num_cols, ast.expires_at);
    }
    
    if (!is_replay) appendToWal(raw_query);
    cache_.invalidateTable(ast.table_name);
    return {Status::OK, "", {}, {}};
}

QueryResult Executor::executeSelect(const ASTNode& ast, const std::string& raw_query) {
    QueryResult cached_res;
    if (cache_.get(raw_query, cached_res)) {
        return cached_res;
    }

    Table* t = storage_.getTable(ast.table_name);
    if (!t) return {Status::ERROR, "Table not found", {}, {}};

    std::shared_lock<std::shared_mutex> lock(t->mutex());
    QueryResult res;
    res.status = Status::OK;
    const auto& schema = t->schema();
    
    std::vector<size_t> col_indices;
    if (ast.select_columns.size() == 1 && ast.select_columns[0] == "*") {
        for (size_t i = 0; i < schema.size(); ++i) {
            res.columns.push_back(schema[i].name);
            col_indices.push_back(i);
        }
    } else {
        for (const auto& cname : ast.select_columns) {
            res.columns.push_back(cname);
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == cname) col_indices.push_back(i);
            }
        }
    }

    std::vector<size_t> row_indices;
    bool full_scan = true;
    bool has_expiring_data = false;

    if (!ast.where_column.empty() && t->primary_key_col() >= 0) {
        if (t->schema()[t->primary_key_col()].name == ast.where_column) {
            std::vector<std::string> target_row;
            if (t->getRowByPK(ast.where_value, target_row)) {
                // If a row is found by PK, we directly add it to results.
                // We need to ensure only selected columns are returned.
                std::vector<std::string> selected_row_data;
                for (size_t idx : col_indices) {
                    selected_row_data.push_back(target_row[idx]);
                }
                res.rows.push_back(selected_row_data);
                full_scan = false; // No need for full scan if PK lookup was successful
            } else {
                // PK not found, return empty result
                res.status = Status::OK;
                cache_.put(raw_query, res);
                return res; 
            }
        }
    }

    if (full_scan && !t->columns_data_.empty()) {
        size_t num_rows = t->columns_data_[0].size();
        for (size_t r = 0; r < num_rows; ++r) row_indices.push_back(r);
    }

    // Check for JOIN
    Table* t2 = nullptr;
    std::shared_lock<std::shared_mutex> lock2;
    std::string t1_join_col, t2_join_col;
    int t1_jcol_idx = -1, t2_jcol_idx = -1;

    if (!ast.join_table.empty()) {
        t2 = storage_.getTable(ast.join_table);
        if (!t2) return {Status::ERROR, "Join table not found", {}, {}};
        lock2 = std::shared_lock<std::shared_mutex>(t2->mutex());
        
        // Match columns
        std::string j1 = ast.join_on_col1;
        std::string j2 = ast.join_on_col2;
        
        // Strip table prefixes if present (e.g. t1.id -> id)
        if (j1.find('.') != std::string::npos) j1 = j1.substr(j1.find('.') + 1);
        if (j2.find('.') != std::string::npos) j2 = j2.substr(j2.find('.') + 1);

        for (size_t i = 0; i < schema.size(); ++i) {
            if (schema[i].name == j1) t1_jcol_idx = i;
            if (schema[i].name == j2) t1_jcol_idx = i;
        }

        for (size_t i = 0; i < t2->schema().size(); ++i) {
            if (t2->schema()[i].name == j1) t2_jcol_idx = i;
            if (t2->schema()[i].name == j2) t2_jcol_idx = i;
        }
        
        if (t1_jcol_idx == -1 || t2_jcol_idx == -1) {
            return {Status::ERROR, "Join columns not found", {}, {}};
        }

        // Add t2 columns to result header
        for (const auto& c : t2->schema()) {
            res.columns.push_back(ast.join_table + "." + c.name);
        }
    }

    std::string eval_where_col = ast.where_column;
    if (eval_where_col.find('.') != std::string::npos) {
        eval_where_col = eval_where_col.substr(eval_where_col.find('.') + 1);
    }

    for (size_t r : row_indices) {
        if (t->expiration_timestamps_[r] != 0) {
            if (t->expiration_timestamps_[r] < time(NULL)) continue; 
            has_expiring_data = true;
        }

        bool pass = true;
        if (full_scan && !eval_where_col.empty()) {
            pass = false;
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == eval_where_col) {
                    if (t->getColumn(i)[r] == ast.where_value) pass = true;
                    break;
                }
            }
        }

        if (pass) {
            std::vector<std::string> row_out;
            for (size_t idx : col_indices) {
                row_out.push_back(t->getColumn(idx)[r]);
            }
            
            // Execute JOIN if active
            if (t2) {
                std::string match_val = t->getColumn(t1_jcol_idx)[r];
                std::vector<std::string> t2_row;
                // If the join column on table 2 is its Primary Key, we can use the $O(1)$ Hash Map!
                if (t2->primary_key_col() == t2_jcol_idx && t2->getRowByPK(match_val, t2_row)) {
                    for (const auto& val : t2_row) row_out.push_back(val);
                    res.rows.push_back(row_out);
                } else {
                    // Fallback to nested loop scan for non-PK joins
                    size_t t2_max = t2->rowCount();
                    bool joined = false;
                    for (size_t r2 = 0; r2 < t2_max; ++r2) {
                        if (t2->getColumn(t2_jcol_idx)[r2] == match_val) {
                            for (size_t c2 = 0; c2 < t2->schema().size(); ++c2) {
                                row_out.push_back(t2->getColumn(c2)[r2]);
                            }
                            res.rows.push_back(row_out);
                            joined = true;
                            // Reset row_out back to base size for potential multiple matches
                            row_out.resize(col_indices.size()); 
                        }
                    }
                }
            } else {
                res.rows.push_back(row_out);
            }
        }
    }

    if (res.status == Status::OK && !has_expiring_data) {
        // Prevent OOM by not caching massive results (e.g. 10M row full scans)
        if (res.rows.size() < 10000) {
            cache_.put(raw_query, res);
        }
    }

    return res;
}

} // namespace flexql
