#include "flexql.h"
#include <iostream>
#include <string>
#include <cctype>

int print_callback(void* /*arg*/, int columnCount, char** values, char** columnNames) {
    for (int i = 0; i < columnCount; ++i) {
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0; // continue
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc >= 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    }

    FlexQL *db = nullptr;
    int rc = flexql_open(host.c_str(), port, &db);

    if (rc != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }

    std::cout << "Connected to FlexQL server\n\n";

    std::string statement;  // Accumulate multi-line statements
    while (true) {
        if (statement.empty()) {
            std::cout << "flexql> ";
        } else {
            std::cout << "    -> ";  // Continuation prompt
        }
        
        std::string line;
        if (!std::getline(std::cin, line)) break;
        
        if (statement.empty() && (line.empty() || line[0] == '#' || (line.length() >= 2 && line[0] == '-' && line[1] == '-'))) {
            continue;
        }
        
        // Trim leading/trailing spaces from each line
        size_t start = 0;
        while (start < line.size() && isspace(static_cast<unsigned char>(line[start]))) start++;
        size_t end = line.size();
        while (end > start && isspace(static_cast<unsigned char>(line[end-1]))) end--;
        std::string trimmed = (start < end) ? line.substr(start, end - start) : "";

        if (statement.empty() && (trimmed == "quit" || trimmed == ".exit")) {
            std::cout << "Connection closed\n";
            flexql_exec(db, "QUIT", nullptr, nullptr, nullptr);
            break;
        }
        
        // Accumulate lines into statement with space separation
        if (!statement.empty() && !trimmed.empty()) statement += " ";
        if (!trimmed.empty()) statement += trimmed;
        
        // Check if statement is complete (ends with semicolon)
        if (statement.find(';') == std::string::npos) {
            continue;  // Keep reading more lines
        }
        
        char *errMsg = nullptr;
        int rc = flexql_exec(db, statement.c_str(), print_callback, nullptr, &errMsg);
        
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL error: " << (errMsg ? errMsg : "Unknown error") << "\n";
            flexql_free(errMsg);
        }
        
        // Force flush pipeline so interactive queries execute immediately
        flexql_flush_pipeline(db);

        statement.clear();  // Clear for next statement
    }

    flexql_close(db);
    return 0;
}
