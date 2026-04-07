#include "flexql/parser.h"
#include <regex>
#include <iostream>
#include <cctype>

namespace flexql {

static std::string trim(std::string s) {
    while (!s.empty() && isspace(static_cast<unsigned char>(s.front()))) s.erase(0, 1);
    while (!s.empty() && isspace(static_cast<unsigned char>(s.back()))) s.pop_back();
    return s;
}

static std::string upper(std::string s) {
    for (char& c : s) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    return s;
}

static void parseValueTuple(const std::string& vals_str, size_t start_idx, size_t end_idx, std::vector<std::string>& out_values, int& out_cols) {
    bool in_quotes = false;
    std::string current_val;
    current_val.reserve(32);
    int cols = 0;
    for (size_t i = start_idx; i < end_idx; ++i) {
        char c = vals_str[i];
        if (c == '\'' || c == '"') {
            in_quotes = !in_quotes;
            continue; 
        }
        if (c == ',' && !in_quotes) {
            size_t s = 0;
            while(s < current_val.size() && isspace(current_val[s])) s++;
            size_t e = current_val.size();
            while(e > s && isspace(current_val[e-1])) e--;
            out_values.push_back(current_val.substr(s, e-s));
            current_val.clear();
            cols++;
        } else {
            current_val += c;
        }
    }
    size_t s = 0;
    while(s < current_val.size() && isspace(current_val[s])) s++;
    size_t e = current_val.size();
    while(e > s && isspace(current_val[e-1])) e--;
    if (s < e || cols > 0) { 
        out_values.push_back(current_val.substr(s, e-s));
        cols++;
    }
    out_cols = cols;
}

ASTNode Parser::parse(const std::string& query) {
    ASTNode node;
    
    std::string q = query;
    while (!q.empty() && (q.back() == ';' || q.back() == '\n' || q.back() == '\r' || q.back() == ' ')) {
        q.pop_back();
    }

    // Fast path for benchmark-style INSERT batches (uppercase SQL, huge payloads).
    // This avoids building a full uppercase copy for multi-megabyte INSERT statements.
    if (q.rfind("INSERT INTO", 0) == 0) {
        node.type = StatementType::INSERT;

        size_t after_into = 11;
        while (after_into < q.size() && isspace(static_cast<unsigned char>(q[after_into]))) after_into++;
        size_t tbl_end = after_into;
        while (tbl_end < q.size() && !isspace(static_cast<unsigned char>(q[tbl_end]))) tbl_end++;
        node.table_name = q.substr(after_into, tbl_end - after_into);

        size_t vals_pos = q.find("VALUES", tbl_end);
        if (vals_pos == std::string::npos) return node;

        size_t after_values = vals_pos + 6;
        while (after_values < q.size() && isspace(static_cast<unsigned char>(q[after_values]))) after_values++;

        std::string remainder = q.substr(after_values);

        size_t expires_pos = remainder.rfind("EXPIRES");
        std::string tuples_str;
        if (expires_pos != std::string::npos) {
            std::string after_exp = remainder.substr(expires_pos + 7);
            while (!after_exp.empty() && isspace(static_cast<unsigned char>(after_exp.front()))) after_exp.erase(0, 1);
            if (!after_exp.empty() && isdigit(static_cast<unsigned char>(after_exp[0]))) {
                node.expires_at = std::stoll(after_exp);
                node.has_expires = true;
                tuples_str = remainder.substr(0, expires_pos);
            } else {
                tuples_str = remainder;
            }
        } else {
            tuples_str = remainder;
        }

        while (!tuples_str.empty() && isspace(static_cast<unsigned char>(tuples_str.back()))) tuples_str.pop_back();

        // Reserve aggressively for big batches: count tuple starts.
        size_t tuple_count = 0;
        for (char ch : tuples_str) {
            if (ch == '(') tuple_count++;
        }
        if (tuple_count > 0) {
            node.batch_values.reserve(tuple_count * 5); // benchmark BIG_USERS has 5 columns
        }

        size_t pos = 0;
        int first_cols = 0;
        while (pos < tuples_str.size()) {
            size_t open = tuples_str.find('(', pos);
            if (open == std::string::npos) break;
            size_t close = std::string::npos;
            bool in_q = false;
            for (size_t i = open + 1; i < tuples_str.size(); ++i) {
                if (tuples_str[i] == '\'' || tuples_str[i] == '"') in_q = !in_q;
                if (tuples_str[i] == ')' && !in_q) { close = i; break; }
            }
            if (close == std::string::npos) break;

            int cols = 0;
            parseValueTuple(tuples_str, open + 1, close, node.batch_values, cols);
            if (first_cols == 0) first_cols = cols;
            pos = close + 1;
        }

        node.num_columns_per_row = first_cols;
        if (!node.batch_values.empty() && first_cols > 0) {
            for (int i = 0; i < first_cols; ++i) {
                if (static_cast<size_t>(i) < node.batch_values.size()) {
                    node.insert_values.push_back(node.batch_values[i]);
                }
            }
        }
        return node;
    }

    std::string q_upper = upper(q);

    // DROP TABLE [IF EXISTS] <name>
    if (q_upper.find("DROP TABLE") == 0) {
        node.type = StatementType::DROP_TABLE;
        std::string rest = q.substr(10);
        while (!rest.empty() && isspace(rest.front())) rest.erase(0, 1);
        
        std::string rest_upper = rest;
        for (char& c : rest_upper) c = toupper(c);
        
        if (rest_upper.find("IF EXISTS") == 0) {
            node.if_exists = true;
            rest = rest.substr(9);
            while (!rest.empty() && isspace(rest.front())) rest.erase(0, 1);
        }
        node.table_name = rest;
        return node;
    }

    std::smatch match;

    // DELETE FROM <name>
    if (q_upper.find("DELETE FROM") == 0) {
        node.type = StatementType::DELETE_ROWS;
        std::string rest = trim(q.substr(11));
        size_t end_name = rest.find_first_of(" \t\r\n");
        node.table_name = (end_name == std::string::npos) ? rest : rest.substr(0, end_name);
        return node;
    }

    std::regex create_rgx(R"(CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)(?:\s*\((.*)\))?)", std::regex::icase);
    if (std::regex_match(q, match, create_rgx)) {
        node.type = StatementType::CREATE_TABLE;
        node.if_not_exists = match[1].matched;
        node.table_name = match[2];
        
        std::string cols_str = match[3];
        if (!cols_str.empty()) {
            std::vector<std::string> col_defs;
            std::string current;
            current.reserve(cols_str.size());
            int paren_depth = 0;
            bool in_quotes = false;

            for (char c : cols_str) {
                if (c == '\'' || c == '"') {
                    in_quotes = !in_quotes;
                }
                if (!in_quotes) {
                    if (c == '(') paren_depth++;
                    else if (c == ')' && paren_depth > 0) paren_depth--;
                }

                if (c == ',' && !in_quotes && paren_depth == 0) {
                    std::string trimmed = trim(current);
                    if (!trimmed.empty()) col_defs.push_back(trimmed);
                    current.clear();
                    continue;
                }
                current.push_back(c);
            }

            std::string trimmed_tail = trim(current);
            if (!trimmed_tail.empty()) col_defs.push_back(trimmed_tail);

            for (const auto& col_def : col_defs) {

            std::vector<std::string> tokens;
            size_t token_start = 0;
            while (token_start < col_def.length()) {
                while (token_start < col_def.length() && isspace(static_cast<unsigned char>(col_def[token_start]))) token_start++;
                if (token_start >= col_def.length()) break;
                size_t token_end = token_start;
                while (token_end < col_def.length() && !isspace(static_cast<unsigned char>(col_def[token_end]))) token_end++;
                tokens.push_back(col_def.substr(token_start, token_end - token_start));
                token_start = token_end;
            }

            if (tokens.size() >= 2) {
                ColumnDef def;
                def.name = tokens[0];
                std::string type_str = upper(tokens[1]);
                size_t paren_pos = type_str.find('(');
                if (paren_pos != std::string::npos) {
                    type_str = type_str.substr(0, paren_pos);
                }

                if (type_str == "INT") def.type = ColumnType::INT;
                else if (type_str == "DECIMAL") def.type = ColumnType::DECIMAL;
                else if (type_str == "VARCHAR" || type_str == "TEXT") {
                    def.type = ColumnType::VARCHAR;

                    std::smatch varchar_match;
                    std::regex varchar_len_rgx(R"(VARCHAR\s*\(\s*(\d+)\s*\))", std::regex::icase);
                    if (std::regex_search(col_def, varchar_match, varchar_len_rgx) && varchar_match.size() > 1) {
                        try {
                            def.varchar_max_length = std::stoi(varchar_match[1].str());
                        } catch (...) {
                            def.varchar_max_length = -1;
                        }
                    }
                }
                else if (type_str == "DATETIME") def.type = ColumnType::DATETIME;
                else def.type = ColumnType::VARCHAR;

                for (size_t i = 2; i < tokens.size(); ++i) {
                    std::string tok = upper(tokens[i]);
                    if (tok == "PRIMARY") {
                        if (i + 1 < tokens.size() && toupper(tokens[i+1][0]) == 'K') {
                            def.is_primary_key = true;
                            i++;
                        }
                    } else if (tok == "NOT") {
                        if (i + 1 < tokens.size() && toupper(tokens[i+1][0]) == 'N') {
                            def.is_not_null = true;
                            i++;
                        }
                    }
                }
                node.schema.push_back(def);
            }
            }
        }
        return node;
    }

    // INSERT INTO <table> VALUES (...),(...),... [EXPIRES <ts>]
    if (q_upper.find("INSERT INTO") == 0) {
        node.type = StatementType::INSERT;
        
        // Extract table name
        size_t after_into = 11;
        while (after_into < q.size() && isspace(q[after_into])) after_into++;
        size_t tbl_end = after_into;
        while (tbl_end < q.size() && !isspace(q[tbl_end])) tbl_end++;
        node.table_name = q.substr(after_into, tbl_end - after_into);

        // Find VALUES keyword
        size_t vals_pos = q_upper.find("VALUES", tbl_end);
        if (vals_pos == std::string::npos) return node;
        
        size_t after_values = vals_pos + 6;
        while (after_values < q.size() && isspace(q[after_values])) after_values++;

        // Check for EXPIRES at the end
        std::string remainder = q.substr(after_values);
        std::string remainder_upper = remainder;
        for (char& c : remainder_upper) c = toupper(c);
        
        size_t expires_pos = remainder_upper.rfind("EXPIRES");
        std::string tuples_str;
        if (expires_pos != std::string::npos) {
            // Check that it's not inside quotes
            std::string after_exp = remainder.substr(expires_pos + 7);
            while (!after_exp.empty() && isspace(after_exp.front())) after_exp.erase(0, 1);
            if (!after_exp.empty() && isdigit(after_exp[0])) {
                node.expires_at = std::stoll(after_exp);
                node.has_expires = true;
                tuples_str = remainder.substr(0, expires_pos);
            } else {
                tuples_str = remainder;
            }
        } else {
            tuples_str = remainder;
        }
        while (!tuples_str.empty() && isspace(tuples_str.back())) tuples_str.pop_back();

        // Parse tuples: (v1,v2),(v3,v4),... or (v1,v2)
        // Split by "),"  or just find each (...) group
        size_t pos = 0;
        int first_cols = 0;
        while (pos < tuples_str.size()) {
            size_t open = tuples_str.find('(', pos);
            if (open == std::string::npos) break;
            size_t close = std::string::npos;
            bool in_q = false;
            for (size_t i = open + 1; i < tuples_str.size(); ++i) {
                if (tuples_str[i] == '\'' || tuples_str[i] == '"') in_q = !in_q;
                if (tuples_str[i] == ')' && !in_q) { close = i; break; }
            }
            if (close == std::string::npos) break;
            
            int cols = 0;
            parseValueTuple(tuples_str, open + 1, close, node.batch_values, cols);
            if (first_cols == 0) first_cols = cols;
            pos = close + 1;
        }
        
        node.num_columns_per_row = first_cols;
        if (!node.batch_values.empty() && first_cols > 0) {
            // For backward compat: copy first tuple into insert_values
            for (int i = 0; i < first_cols; ++i) {
                if (static_cast<size_t>(i) < node.batch_values.size()) {
                    node.insert_values.push_back(node.batch_values[i]);
                }
            }
        }
        
        return node;
    }

    if (q_upper.find("SELECT") == 0) {
        node.type = StatementType::SELECT;

        size_t order_by_pos = q_upper.find(" ORDER BY ");
        if (order_by_pos != std::string::npos) {
            std::string order_part = trim(q.substr(order_by_pos + 10));
            size_t end_col = order_part.find_first_of(" \t\r\n");
            node.order_by_column = trim(end_col == std::string::npos ? order_part : order_part.substr(0, end_col));
            if (end_col != std::string::npos) {
                std::string dir = upper(trim(order_part.substr(end_col + 1)));
                node.order_by_desc = (dir == "DESC");
            }
            q = trim(q.substr(0, order_by_pos));
            q_upper = upper(q);
        }

        size_t from_pos = q_upper.find(" FROM ");
        if (from_pos == std::string::npos) return node;

        std::string cols = q.substr(6, from_pos - 6);
        while (!cols.empty() && isspace(cols.front())) cols.erase(0, 1);
        while (!cols.empty() && isspace(cols.back())) cols.pop_back();

        if (cols == "*") {
            node.select_columns.push_back("*");
        } else {
            size_t start = 0;
            size_t end = cols.find(',');
            while (start < cols.length()) {
                std::string c_name;
                if (end == std::string::npos) c_name = cols.substr(start);
                else c_name = cols.substr(start, end - start);
                while (!c_name.empty() && isspace(c_name.front())) c_name.erase(0, 1);
                while (!c_name.empty() && isspace(c_name.back())) c_name.pop_back();
                node.select_columns.push_back(c_name);
                if (end == std::string::npos) break;
                start = end + 1;
                end = cols.find(',', start);
            }
        }

        size_t after_from = from_pos + 6;
        size_t end_table = q_upper.find(" ", after_from);
        if (end_table == std::string::npos) {
            node.table_name = q.substr(after_from);
            return node;
        }

        node.table_name = q.substr(after_from, end_table - after_from);

        
        size_t join_pos = q_upper.find(" INNER JOIN ");
        int join_len = 12;
        if (join_pos == std::string::npos) {
            join_pos = q_upper.find(" JOIN ");
            join_len = 6;
        }
        size_t where_pos = q_upper.find(" WHERE ");

        if (join_pos != std::string::npos) {
            size_t on_pos = q_upper.find(" ON ", join_pos);
            if (on_pos != std::string::npos) {
                size_t tb_end = q_upper.find(" ", join_pos + join_len);
                if (tb_end > on_pos) tb_end = on_pos;
                node.join_table = q.substr(join_pos + join_len, tb_end - (join_pos + join_len));

                
                size_t on_end = where_pos != std::string::npos ? where_pos : q.length();
                std::string on_cond = trim(q.substr(on_pos + 4, on_end - (on_pos + 4)));
                std::vector<std::string> ops = {"<=", ">=", "!=", "=", "<", ">"};
                std::string matched_op = "=";
                size_t op_pos = std::string::npos;
                for (const auto& op : ops) {
                    op_pos = on_cond.find(op);
                    if (op_pos != std::string::npos) {
                        matched_op = op;
                        break;
                    }
                }
                if (op_pos != std::string::npos) {
                    node.join_on_col1 = trim(on_cond.substr(0, op_pos));
                    node.join_operator = matched_op;
                    node.join_on_col2 = trim(on_cond.substr(op_pos + matched_op.length()));
                }
            }
        }

        if (where_pos != std::string::npos) {
            std::string where_cond = trim(q.substr(where_pos + 7));

            // Assignment constraint: only one WHERE condition is allowed.
            // Reject logical combinations such as AND / OR when they appear outside quotes.
            auto has_logical_combination = [&](const std::string& cond) {
                std::string cond_upper = upper(cond);
                bool in_single_quote = false;
                bool in_double_quote = false;

                auto is_word_boundary = [](char ch) {
                    return !std::isalnum(static_cast<unsigned char>(ch)) && ch != '_';
                };

                for (size_t i = 0; i < cond_upper.size(); ++i) {
                    char c = cond_upper[i];
                    if (c == '\'' && !in_double_quote) {
                        in_single_quote = !in_single_quote;
                        continue;
                    }
                    if (c == '"' && !in_single_quote) {
                        in_double_quote = !in_double_quote;
                        continue;
                    }
                    if (in_single_quote || in_double_quote) continue;

                    if (i + 3 <= cond_upper.size() && cond_upper.compare(i, 3, "AND") == 0) {
                        bool left_ok = (i == 0) || is_word_boundary(cond_upper[i - 1]);
                        bool right_ok = (i + 3 == cond_upper.size()) || is_word_boundary(cond_upper[i + 3]);
                        if (left_ok && right_ok) return true;
                    }

                    if (i + 2 <= cond_upper.size() && cond_upper.compare(i, 2, "OR") == 0) {
                        bool left_ok = (i == 0) || is_word_boundary(cond_upper[i - 1]);
                        bool right_ok = (i + 2 == cond_upper.size()) || is_word_boundary(cond_upper[i + 2]);
                        if (left_ok && right_ok) return true;
                    }
                }
                return false;
            };

            if (has_logical_combination(where_cond)) {
                node.type = StatementType::UNKNOWN;
                return node;
            }

            const std::vector<std::string> ops = {">=", "<=", "=", ">", "<"};
            size_t op_pos = std::string::npos;
            std::string matched_op;
            for (const auto& op : ops) {
                op_pos = where_cond.find(op);
                if (op_pos != std::string::npos) {
                    matched_op = op;
                    break;
                }
            }
            if (!matched_op.empty()) {
                node.where_column = trim(where_cond.substr(0, op_pos));
                node.where_operator = matched_op;
                node.where_value = trim(where_cond.substr(op_pos + matched_op.size()));

                if (node.where_value.size() >= 2 &&
                    ((node.where_value.front() == '\'' && node.where_value.back() == '\'') ||
                     (node.where_value.front() == '"' && node.where_value.back() == '"'))) {
                    node.where_value = node.where_value.substr(1, node.where_value.size() - 2);
                }
            }
        }
        return node;
    }

    return node;
}

} // namespace flexql
