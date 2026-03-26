#pragma once
#include <string>
#include <vector>

namespace flexql {

// Response status from the server logic
enum class Status {
    OK,
    ERROR
};

// Internal representation of query results that will be serialized to the client
struct QueryResult {
    Status status;
    std::string error_message;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

} // namespace flexql
