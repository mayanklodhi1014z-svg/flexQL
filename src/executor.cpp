#include "flexql/executor.h"
#include <fstream>
#include <algorithm>
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
        case StatementType::DELETE_ROWS:
            return executeDelete(ast, is_replay, query_string);
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

QueryResult Executor::executeDelete(const ASTNode& ast, bool is_replay, const std::string& raw_query) {
    Table* t = storage_.getTable(ast.table_name);
    if (!t) return {Status::ERROR, "Table not found", {}, {}};

    std::unique_lock<std::shared_mutex> lock(t->mutex());
    t->clearData();
    
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
    std::vector<std::string> output_col_names;

    // Check for JOIN
    Table* t2 = nullptr;
    std::shared_lock<std::shared_mutex> lock2;
    int t1_jcol_idx = -1, t2_jcol_idx = -1;

    if (!ast.join_table.empty()) {
        t2 = storage_.getTable(ast.join_table);
        if (!t2) return {Status::ERROR, "Join table not found", {}, {}};
        lock2 = std::shared_lock<std::shared_mutex>(t2->mutex());
        
        std::string j1 = ast.join_on_col1;
        std::string j2 = ast.join_on_col2;
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
    }

    // Determine output columns
    if (ast.select_columns.size() == 1 && ast.select_columns[0] == "*") {
        for (size_t i = 0; i < schema.size(); ++i) {
            res.columns.push_back(schema[i].name);
            col_indices.push_back(i);
        }
        if (t2) {
            for (const auto& c : t2->schema()) {
                res.columns.push_back(ast.join_table + "." + c.name);
            }
        }
    } else {
        for (const auto& cname : ast.select_columns) {
            bool found = false;
            std::string c_clean = cname;
            if (c_clean.find('.') != std::string::npos) {
                c_clean = c_clean.substr(c_clean.find('.') + 1);
            }
            
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == c_clean || ast.table_name + "." + schema[i].name == cname) {
                    res.columns.push_back(cname);
                    col_indices.push_back(i);
                    found = true;
                    break;
                }
            }
            if (!found && t2) {
                for (size_t i = 0; i < t2->schema().size(); ++i) {
                    if (t2->schema()[i].name == c_clean || ast.join_table + "." + t2->schema()[i].name == cname) {
                        res.columns.push_back(cname);
                        col_indices.push_back(schema.size() + i); // Map t2 columns beyond schema.size()
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                return {Status::ERROR, "Unknown column " + cname, {}, {}};
            }
        }
    }


    // Add sort column to logic if needed
    bool hidden_sort_col = false;
    int sort_schema_idx = -1;
    if (!ast.order_by_column.empty()) {
        std::string clean_sort = ast.order_by_column;
        if (clean_sort.find('.') != std::string::npos) clean_sort = clean_sort.substr(clean_sort.find('.') + 1);
        
        bool found_in_select = false;
        for (const auto& c : res.columns) {
            std::string cname = c;
            if (cname.find('.') != std::string::npos) cname = cname.substr(cname.find('.') + 1);
            if (cname == clean_sort) { found_in_select = true; break; }
        }
        
        if (!found_in_select) {
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == clean_sort) {
                    sort_schema_idx = i;
                    col_indices.push_back(i);
                    res.columns.push_back(ast.order_by_column);
                    hidden_sort_col = true;
                    break;
                }
            }
            if (!hidden_sort_col && t2) {
                for (size_t i = 0; i < t2->schema().size(); ++i) {
                    if (t2->schema()[i].name == clean_sort) {
                        col_indices.push_back(schema.size() + i);
                        res.columns.push_back(ast.order_by_column);
                        hidden_sort_col = true;
                        break;
                    }
                }
            }
        }
    }

    // Helper for where logic

    std::string eval_where_col = ast.where_column;
    std::string eval_where_val = ast.where_value;
    std::string eval_where_op = ast.where_operator.empty() ? "=" : ast.where_operator;
    int where_col_idx = -1;
    bool where_is_t2 = false;
    ColumnType where_col_type = ColumnType::VARCHAR;

    if (!eval_where_col.empty()) {
        std::string clean_col = eval_where_col;
        if (clean_col.find('.') != std::string::npos) {
            clean_col = clean_col.substr(clean_col.find('.') + 1);
        }
        
        for (size_t i = 0; i < schema.size(); ++i) {
            if (schema[i].name == clean_col) {
                where_col_idx = i;
                where_col_type = schema[i].type;
                break;
            }
        }
        
        if (where_col_idx == -1 && t2) {
            for (size_t i = 0; i < t2->schema().size(); ++i) {
                if (t2->schema()[i].name == clean_col) {
                    where_col_idx = i;
                    where_col_type = t2->schema()[i].type;
                    where_is_t2 = true;
                    break;
                }
            }
        }
    }

    auto evaluate_condition = [&](const std::string& val_str) -> bool {
        if (where_col_type == ColumnType::INT || where_col_type == ColumnType::DECIMAL) {
            try {
                double val = std::stod(val_str);
                double target = std::stod(eval_where_val);
                if (eval_where_op == "=") return val == target;
                if (eval_where_op == ">") return val > target;
                if (eval_where_op == "<") return val < target;
                if (eval_where_op == ">=") return val >= target;
                if (eval_where_op == "<=") return val <= target;
            } catch (...) { return false; }
        } else {
            if (eval_where_op == "=") return val_str == eval_where_val;
            if (eval_where_op == ">") return val_str > eval_where_val;
            if (eval_where_op == "<") return val_str < eval_where_val;
            if (eval_where_op == ">=") return val_str >= eval_where_val;
            if (eval_where_op == "<=") return val_str <= eval_where_val;
        }
        return false;
    };    std::vector<size_t> row_indices;
    if (!t->columns_data_.empty()) {
        if (where_col_idx != -1 && !where_is_t2 && where_col_idx == t->primary_key_col() && eval_where_op == "=") {
            size_t r;
            if (t->getRowIndexByPK(ast.where_value, r)) {
                if (!(t->expiration_timestamps_[r] != 0 && t->expiration_timestamps_[r] < time(NULL))) {
                    row_indices.push_back(r);
                }
            }
        } else {
            size_t num_rows = t->rowCount();
            for (size_t r = 0; r < num_rows; ++r) {
                if (t->expiration_timestamps_[r] != 0 && t->expiration_timestamps_[r] < time(NULL)) continue;
                if (where_col_idx != -1 && !where_is_t2) {
                    if (!evaluate_condition(t->getColumn(where_col_idx)[r])) continue;
                }
                row_indices.push_back(r);
            }
        }
    }

    bool has_expiring_data = false;
    for (size_t r : row_indices) {
        if (t->expiration_timestamps_[r] != 0) has_expiring_data = true;

        if (t2) {
            std::string match_val = t->getColumn(t1_jcol_idx)[r];
            std::vector<std::string> t2_row;
            if (t2->primary_key_col() == t2_jcol_idx && t2->getRowByPK(match_val, t2_row)) {
                if (where_col_idx != -1 && where_is_t2) {
                    if (!evaluate_condition(t2_row[where_col_idx])) continue;
                }
                std::vector<std::string> row_out;
                for (size_t idx : col_indices) {
                    if (idx < schema.size()) row_out.push_back(t->getColumn(idx)[r]);
                    else row_out.push_back(t2_row[idx - schema.size()]);
                }
                res.rows.push_back(row_out);
            } else {
                size_t t2_max = t2->rowCount();
                for (size_t r2 = 0; r2 < t2_max; ++r2) {
                    if (t2->getColumn(t2_jcol_idx)[r2] == match_val) {
                        if (where_col_idx != -1 && where_is_t2) {
                            if (!evaluate_condition(t2->getColumn(where_col_idx)[r2])) continue;
                        }
                        std::vector<std::string> row_out;
                        for (size_t idx : col_indices) {
                            if (idx < schema.size()) row_out.push_back(t->getColumn(idx)[r]);
                            else row_out.push_back(t2->getColumn(idx - schema.size())[r2]);
                        }
                        res.rows.push_back(row_out);
                    }
                }
            }
        } else {
            std::vector<std::string> row_out;
            for (size_t idx : col_indices) {
                row_out.push_back(t->getColumn(idx)[r]);
            }
            res.rows.push_back(row_out);
        }
    }

    if (!ast.order_by_column.empty()) {
        int sort_idx = -1;
        ColumnType sort_type = ColumnType::VARCHAR;
        std::string clean_sort = ast.order_by_column;
        if (clean_sort.find('.') != std::string::npos) clean_sort = clean_sort.substr(clean_sort.find('.') + 1);

        for (size_t i = 0; i < res.columns.size(); ++i) {
            std::string cname = res.columns[i];
            if (cname.find('.') != std::string::npos) cname = cname.substr(cname.find('.') + 1);
            if (cname == clean_sort) {
                sort_idx = i;
                if (col_indices[i] < schema.size()) sort_type = schema[col_indices[i]].type;
                else sort_type = t2->schema()[col_indices[i] - schema.size()].type;
                break;
            }
        }


        if (sort_idx != -1) {
            std::sort(res.rows.begin(), res.rows.end(), [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
                if (sort_type == ColumnType::INT || sort_type == ColumnType::DECIMAL) {
                    try {
                        double va = std::stod(a[sort_idx]);
                        double vb = std::stod(b[sort_idx]);
                        return ast.order_by_desc ? (va > vb) : (va < vb);
                    } catch (...) {}
                }
                return ast.order_by_desc ? (a[sort_idx] > b[sort_idx]) : (a[sort_idx] < b[sort_idx]);
            });
        }
        
        if (hidden_sort_col) {
            res.columns.pop_back();
            for (auto& r : res.rows) {
                if (!r.empty()) r.pop_back();
            }
        }
    }

    if (res.status == Status::OK
 && !has_expiring_data && res.rows.size() < 10000) {
        std::vector<std::string> tbls = {ast.table_name};
        if(!ast.join_table.empty()) tbls.push_back(ast.join_table);
        cache_.put(raw_query, res);
    }

    return res;
}

} // namespace flexql
