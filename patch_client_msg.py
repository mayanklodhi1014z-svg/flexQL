with open("src/main_client.cpp", "r") as f:
    text = f.read()

import re
replacement = """
            int rc = flexql_exec(db, statement.c_str(), print_callback, nullptr, &errMsg);
            if (rc != FLEXQL_OK) {
                std::cerr << "SQL error: " << (errMsg ? errMsg : "Unknown error") << "\\n";
                flexql_free(errMsg);
            } else if (statement.substr(0, 6) == "INSERT" || statement.substr(0, 6) == "insert" || 
                       statement.substr(0, 6) == "CREATE" || statement.substr(0, 6) == "create" || 
                       statement.substr(0, 6) == "DELETE" || statement.substr(0, 6) == "delete" || 
                       statement.substr(0, 4) == "DROP" || statement.substr(0, 4) == "drop") {
                std::cout << "Query OK, operation successful.\\n";
            }
"""

text = re.sub(
    r'int rc = flexql_exec\(db, statement\.c_str\(\), print_callback, nullptr, &errMsg\);\s*if \(rc != FLEXQL_OK\) \{\s*std::cerr << "SQL error: " << \(errMsg \? errMsg : "Unknown error"\) << "\\n";\s*flexql_free\(errMsg\);\s*\} else if \(statement\.substr\(0, 6\) == "INSERT" \|\| statement\.substr\(0, 6\) == "insert" \|\| \s*statement\.substr\(0, 6\) == "CREATE" \|\| statement\.substr\(0, 6\) == "create" \|\| \s*statement\.substr\(0, 6\) == "DELETE" \|\| statement\.substr\(0, 6\) == "delete" \|\| \s*statement\.substr\(0, 4\) == "DROP" \|\| statement\.substr\(0, 4\) == "drop"\) \{\s*std::cout << "Query OK, operation successful." << "\\n";\s*\}',
    replacement, text
)

with open("src/main_client.cpp", "w") as f:
    f.write(text)
