#include "flexql.h"
#include <iostream>
#include <string>

int print_callback(void* /*arg*/, int columnCount, char** values, char** columnNames) {
    for (int i = 0; i < columnCount; ++i) {
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0; // continue
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 9001;

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

    std::string line;
    while (true) {
        std::cout << "flexql> ";
        if (!std::getline(std::cin, line)) break;
        if (line.empty() || line[0] == '#' || (line.length() >= 2 && line[0] == '-' && line[1] == '-')) continue;
        if (line == "quit" || line == ".exit") {
            std::cout << "Connection closed\n";
            flexql_exec(db, "QUIT", nullptr, nullptr, nullptr);
            break;
        }

        char *errMsg = nullptr;
        rc = flexql_exec(db, line.c_str(), print_callback, nullptr, &errMsg);
        
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL error: " << (errMsg ? errMsg : "Unknown error") << "\n";
            flexql_free(errMsg);
        }
    }

    flexql_close(db);
    return 0;
}
