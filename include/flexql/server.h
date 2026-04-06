#pragma once
#include <string>
#include <atomic>
#include "flexql/thread_pool.h"
#include "flexql/storage_engine.h"
#include "flexql/executor.h"

namespace flexql {

class Server {
public:
    Server(const std::string& host, int port);
    ~Server();

    void run();
    void stop();

private:
    void acceptLoop();
    void handleClient(int client_fd);
    void handleBatch(int client_fd, uint32_t batch_count);
    std::string processQuery(const std::string& query);

    std::string host_;
    int port_;
    int server_fd_;
    std::atomic<bool> running_;
    ThreadPool thread_pool_;
    
    StorageEngine storage_;
    Executor executor_;
};

} // namespace flexql
