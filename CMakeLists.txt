cmake_minimum_required(VERSION 3.16)
project(FlexQL VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)

include_directories(include)

# We compile the server source files into an object library to reuse cleanly
file(GLOB SERVER_SRCS
    "src/server.cpp"
    "src/thread_pool.cpp"
    "src/table.cpp"
    "src/storage_engine.cpp"
    "src/parser.cpp"
    "src/executor.cpp"
    "src/lru_cache.cpp"
    "src/ast_cache.cpp"
)

add_library(flexql_core OBJECT ${SERVER_SRCS})
target_compile_options(flexql_core PRIVATE -Wall -Wextra -O3 -pthread)

# Executables
add_executable(flexql-server src/main_server.cpp)
target_link_libraries(flexql-server PRIVATE flexql_core Threads::Threads)
set_target_properties(flexql-server PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")

add_executable(flexql-client src/main_client.cpp src/client_api.cpp)
target_link_libraries(flexql-client PRIVATE Threads::Threads)
set_target_properties(flexql-client PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")


add_executable(benchmark_flexql scripts/benchmark_flexql.cpp src/client_api.cpp)
target_link_libraries(benchmark_flexql PRIVATE Threads::Threads)
set_target_properties(benchmark_flexql PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")
