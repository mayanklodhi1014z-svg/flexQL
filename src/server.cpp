#include "flexql/server.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>

namespace flexql {

Server::Server(const std::string& host, int port) 
    : host_(host), port_(port), server_fd_(-1), running_(false), 
      thread_pool_(std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 4),
      storage_(), executor_(storage_) {
    
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error("Failed to set SO_REUSEADDR");
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(host.c_str());
    address.sin_port = htons(port);

    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        throw std::runtime_error("Bind failed");
    }

    if (listen(server_fd_, SOMAXCONN) < 0) {
        throw std::runtime_error("Listen failed");
    }
}

Server::~Server() {
    stop();
}

void Server::run() {
    running_ = true;
    
    std::cout << "Loading WAL on boot...\n";
    executor_.replayWal();
    std::cout << "WAL load complete.\n";

    std::thread sweeper([this]() {
        while (this->running_) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            this->executor_.sweepExpiredRows();
        }
    });
    sweeper.detach();

    std::cout << "FlexQL Server listening on " << host_ << ":" << port_ << " with " 
              << (std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 4) << " workers.\n";
    acceptLoop();
}

void Server::stop() {
    running_ = false;
    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }
}

void Server::acceptLoop() {
    while (running_) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &addrlen);
        
        if (client_fd < 0) {
            if (!running_) break;
            continue;
        }

        thread_pool_.enqueue([this, client_fd]() {
            this->handleClient(client_fd);
        });
    }
}

void Server::handleClient(int client_fd) {
    while (running_) {
        uint32_t len_net = 0;
        ssize_t bytes_read = read(client_fd, &len_net, 4);
        if (bytes_read <= 0) break;

        uint32_t len = ntohl(len_net);
        if (len > 1024 * 1024 * 256) break; // 256MB max

        std::vector<char> buffer(len);
        size_t total_read = 0;
        while (total_read < len) {
            ssize_t r = read(client_fd, buffer.data() + total_read, len - total_read);
            if (r <= 0) goto end_client;
            total_read += r;
        }

        std::string query(buffer.data(), len);
        std::string response = processQuery(query);

        uint32_t resp_len_net = htonl(response.size());
        std::vector<char> out_buf(4 + response.size());
        std::memcpy(out_buf.data(), &resp_len_net, 4);
        std::memcpy(out_buf.data() + 4, response.data(), response.size());
        
        size_t total_written = 0;
        while (total_written < out_buf.size()) {
            ssize_t w = write(client_fd, out_buf.data() + total_written, out_buf.size() - total_written);
            if (w <= 0) goto end_client;
            total_written += w;
        }
    }
end_client:
    close(client_fd);
}

std::string Server::processQuery(const std::string& query) {
    if (query == "QUIT" || query == "quit" || query == ".exit") {
        return "O\n\n---\n";
    }
    
    flexql::QueryResult res = executor_.execute(query);
    if (res.status == Status::ERROR) {
        return "E\n" + res.error_message + "\n";
    }

    std::string out = "O\n";
    for (size_t i = 0; i < res.columns.size(); ++i) {
        out += res.columns[i];
        if (i + 1 < res.columns.size()) out += "\t";
    }
    out += "\n---\n";
    
    // Prevent massive memory reallocation spikes and fragmentation during full scans
    out.reserve(out.size() + res.rows.size() * 32); 
    
    
    for (const auto& row : res.rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            out += row[i];
            if (i + 1 < row.size()) out += "\t";
        }
        out += "\n";
    }
    return out;
}

} // namespace flexql
