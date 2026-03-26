#include "flexql/parser.h"
#include <regex>
#include <iostream>

namespace flexql {

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

    std::string q_upper = q;
    for (char& c : q_upper) c = toupper(c);

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

    std::regex create_rgx(R"i(CREATE\s+TABLE\s+(\w+)\s*\((.*)\))i");
    if (std::regex_match(q, match, create_rgx)) {
        node.type = StatementType::CREATE_TABLE;
        node.table_name = match[1];
        
        std::string cols_str = match[2];
        size_t start = 0;
        size_t end = cols_str.find(',');
        while (start < cols_str.length()) {
            std::string col_def;
            if (end == std::string::npos) col_def = cols_str.substr(start);
            else col_def = cols_str.substr(start, end - start);

            std::vector<std::string> tokens;
            size_t token_start = 0;
            while (token_start < col_def.length()) {
                while (token_start < col_def.length() && isspace(col_def[token_start])) token_start++;
                if (token_start >= col_def.length()) break;
                size_t token_end = token_start;
                while (token_end < col_def.length() && !isspace(col_def[token_end])) token_end++;
                tokens.push_back(col_def.substr(token_start, token_end - token_start));
                token_start = token_end;
            }

            if (tokens.size() >= 2) {
                ColumnDef def;
                def.name = tokens[0];
                std::string type_str = tokens[1];
                for (char& c : type_str) c = toupper(c);

                if (type_str == "INT") def.type = ColumnType::INT;
                else if (type_str == "DECIMAL") def.type = ColumnType::DECIMAL;
                else if (type_str == "VARCHAR" || type_str == "TEXT") def.type = ColumnType::VARCHAR;
                else if (type_str == "DATETIME") def.type = ColumnType::DATETIME;
                else def.type = ColumnType::VARCHAR;

                for (size_t i = 2; i < tokens.size(); ++i) {
                    std::string tok = tokens[i];
                    for (char& c : tok) c = toupper(c);
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
            if (end == std::string::npos) break;
            start = end + 1;
            end = cols_str.find(',', start);
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
                if (i < node.batch_values.size()) {
                    node.insert_values.push_back(node.batch_values[i]);
                }
            }
        }
        
        return node;
    }

    if (q_upper.find("SELECT") == 0) {
        node.type = StatementType::SELECT;
        
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
        size_t where_pos = q_upper.find(" WHERE ");

        if (join_pos != std::string::npos) {
            size_t on_pos = q_upper.find(" ON ", join_pos);
            if (on_pos != std::string::npos) {
                size_t tb_end = q_upper.find(" ", join_pos + 12);
                if (tb_end > on_pos) tb_end = on_pos;
                node.join_table = q.substr(join_pos + 12, tb_end - (join_pos + 12));
                
                size_t on_end = where_pos != std::string::npos ? where_pos : q.length();
                std::string on_cond = q.substr(on_pos + 4, on_end - (on_pos + 4));
                size_t eq_pos = on_cond.find('=');
                if (eq_pos != std::string::npos) {
                    node.join_on_col1 = on_cond.substr(0, eq_pos);
                    node.join_on_col2 = on_cond.substr(eq_pos + 1);
                    while(!node.join_on_col1.empty() && isspace(node.join_on_col1.back())) node.join_on_col1.pop_back();
                    while(!node.join_on_col2.empty() && isspace(node.join_on_col2.front())) node.join_on_col2.erase(0,1);
                }
            }
        }

        if (where_pos != std::string::npos) {
            std::string where_cond = q.substr(where_pos + 7);
            size_t eq_pos = where_cond.find('=');
            if (eq_pos != std::string::npos) {
                node.where_column = where_cond.substr(0, eq_pos);
                node.where_value = where_cond.substr(eq_pos + 1);
                while(!node.where_column.empty() && isspace(node.where_column.back())) node.where_column.pop_back();
                while(!node.where_value.empty() && isspace(node.where_value.front())) node.where_value.erase(0,1);
                
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
