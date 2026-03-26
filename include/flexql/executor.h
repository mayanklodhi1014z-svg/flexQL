#pragma once
#include "flexql/parser.h"
#include "flexql/ast_cache.h"
#include "flexql/storage_engine.h"
#include "flexql/lru_cache.h"
#include "flexql/types.h"
#include <mutex>
#include <fstream>
#include <thread>
#include <condition_variable>
#include <vector>

namespace flexql {

class Executor {
public:
    Executor(StorageEngine& storage);
    ~Executor();
    
    QueryResult execute(const std::string& query_string, bool is_replay = false);

    void replayWal();
    void sweepExpiredRows();

private:
    QueryResult executeCreate(const ASTNode& ast, bool is_replay, const std::string& raw_query);
    QueryResult executeDrop(const ASTNode& ast, bool is_replay, const std::string& raw_query);
    QueryResult executeInsert(const ASTNode& ast, bool is_replay, const std::string& raw_query);
    QueryResult executeSelect(const ASTNode& ast, const std::string& raw_query);

    StorageEngine& storage_;
    LRUCache cache_;
    ASTCache ast_cache_;
    
    void appendToWal(const std::string& raw_query);
    std::mutex wal_mutex_;
    std::ofstream wal_out_;

    // Async WAL
    std::thread wal_thread_;
    std::vector<std::string> wal_queue_;
    std::mutex wal_queue_mutex_;
    std::condition_variable wal_cv_;
    bool wal_running_ = true;
};

} // namespace flexql
