
    // Read remaining records seamlessly from background SST pages
    t->scanSSTables([&](const std::vector<std::string>& row, time_t expiration) {
        if (expiration != 0 && expiration < time(NULL)) {
            has_expiring_data = true;
            return;
        }

        if (where_col_idx != -1 && !where_is_t2) {
            if (!evaluate_condition(row[where_col_idx])) return;
        }

        if (t2) {
            std::string match_val = row[t1_jcol_idx];
            std::vector<std::string> t2_row;
            if (eval_join_op == "=" && t2->primary_key_col() == t2_jcol_idx && t2->getRowByPK(match_val, t2_row)) {
                if (where_col_idx != -1 && where_is_t2) {
                    if (!evaluate_condition(t2_row[where_col_idx])) return;
                }
                std::vector<std::string> row_out;
                for (size_t idx : col_indices) {
                    if (idx < schema.size()) row_out.push_back(row[idx]);
                    else row_out.push_back(t2_row[idx - schema.size()]);
                }
                res.rows.push_back(row_out);
            } else {
                size_t t2_max = t2->rowCount();
                for (size_t r2 = 0; r2 < t2_max; ++r2) {
                    if (evaluate_join(t2->getColumn(t2_jcol_idx)[r2], match_val)) {
                        if (where_col_idx != -1 && where_is_t2) {
                            if (!evaluate_condition(t2->getColumn(where_col_idx)[r2])) continue;
                        }
                        std::vector<std::string> row_out;
                        for (size_t idx : col_indices) {
                            if (idx < schema.size()) row_out.push_back(row[idx]);
                            else row_out.push_back(t2->getColumn(idx - schema.size())[r2]);
                        }
                        res.rows.push_back(row_out);
                    }
                }
            }
        } else {
            std::vector<std::string> row_out;
            for (size_t idx : col_indices) {
                row_out.push_back(row[idx]);
            }
            res.rows.push_back(row_out);
        }
    });

