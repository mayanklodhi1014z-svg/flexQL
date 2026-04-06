#include "flexql.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <queue>
#include <functional>

// Pipelining batch size: send N queries at once to reduce RTT overhead
#define PIPELINING_BATCH_SIZE 64

struct PipelinedQuery {
    std::string sql;
    std::function<int(const std::string&)> callback;  // Process response
};

struct FlexQL {
    int fd;
    std::vector<PipelinedQuery> query_queue;  // Batched queries awaiting send
    size_t batch_size = PIPELINING_BATCH_SIZE;  // Configurable batch size
    
    FlexQL() : fd(-1), batch_size(PIPELINING_BATCH_SIZE) {}
};

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    struct addrinfo* result = nullptr;
    if (getaddrinfo(host, port_str.c_str(), &hints, &result) != 0) {
        return FLEXQL_ERROR;
    }

    int sock = -1;
    for (struct addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock < 0) {
            continue;
        }

        int opt = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        if (connect(sock, ai->ai_addr, ai->ai_addrlen) == 0) {
            break;
        }

        close(sock);
        sock = -1;
    }

    freeaddrinfo(result);

    if (sock < 0) {
        return FLEXQL_ERROR;
    }

    FlexQL *conn = new FlexQL();
    if (!conn) {
        close(sock);
        return FLEXQL_ERROR;
    }
    conn->fd = sock;
    *db = conn;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    // Flush any pending queries before closing
    if (!db->query_queue.empty()) {
        flexql_flush_pipeline(db);
    }
    close(db->fd);
    delete db;
    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    if (ptr) {
        free(ptr);
    }
}

// Internal helper: Process a single response string
static int process_response_internal(
    const std::string& response,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
) {
    if (response.empty()) return FLEXQL_ERROR;
    if (response[0] == 'E') {
        if (errmsg) {
            size_t newline_pos = response.find('\n');
            if (newline_pos != std::string::npos && newline_pos + 1 < response.size()) {
                *errmsg = strdup(response.substr(newline_pos + 1).c_str());
            }
        }
        return FLEXQL_ERROR;
    }
    
    if (response[0] != 'O') return FLEXQL_ERROR;
    
    if (!callback) return FLEXQL_OK;
    
    std::vector<std::string> lines;
    size_t start = 0;
    size_t end = response.find('\n');
    while (end != std::string::npos) {
        lines.push_back(response.substr(start, end - start));
        start = end + 1;
        end = response.find('\n', start);
    }
    if (start < response.size()) {
        lines.push_back(response.substr(start));
    }

    if (lines.size() > 3) {
        std::vector<std::string> cols;
        std::string col_line = lines[1];
        size_t c_start = 0;
        size_t c_end = col_line.find('\t');
        while (c_end != std::string::npos) {
            cols.push_back(col_line.substr(c_start, c_end - c_start));
            c_start = c_end + 1;
            c_end = col_line.find('\t', c_start);
        }
        cols.push_back(col_line.substr(c_start));

        std::vector<char*> col_ptrs;
        for (auto& c : cols) col_ptrs.push_back(const_cast<char*>(c.c_str()));

        if (lines.size() > 2 && lines[2] != "---") return FLEXQL_ERROR;

        for (size_t i = 3; i < lines.size(); ++i) {
            if (lines[i].empty()) continue;

            std::vector<std::string> vals;
            std::string val_line = lines[i];
            size_t v_start = 0;
            size_t v_end = val_line.find('\t');
            while (v_end != std::string::npos) {
                vals.push_back(val_line.substr(v_start, v_end - v_start));
                v_start = v_end + 1;
                v_end = val_line.find('\t', v_start);
            }
            vals.push_back(val_line.substr(v_start));

            std::vector<char*> val_ptrs;
            for (auto& v : vals) val_ptrs.push_back(const_cast<char*>(v.c_str()));

            int count = std::min(col_ptrs.size(), val_ptrs.size());
            if (callback(arg, count, val_ptrs.data(), col_ptrs.data()) != 0) {
                break;
            }
        }
    }
    
    return FLEXQL_OK;
}

// Flush all queued queries in a single batch (pipelining)
int flexql_flush_pipeline(FlexQL *db) {
    if (!db || db->query_queue.empty()) return FLEXQL_OK;
    
    // 1. Build batch: [batch_count][query1_len][query1]...[queryN_len][queryN]
    std::vector<uint8_t> batch;
    uint32_t batch_count = db->query_queue.size();
    
        // 1. Build batch: [0xFFFFFFFE (marker)][batch_count][query1_len][query1]...[queryN_len][queryN]
        // Write batch marker (magic value 0xFFFFFFFE indicates batch)
        uint32_t marker = 0xFFFFFFFE;
        uint32_t marker_net = htonl(marker);
        batch.insert(batch.end(), (uint8_t*)&marker_net, (uint8_t*)&marker_net + 4);
    
    // Write batch count
    uint32_t count_net = htonl(batch_count);
    batch.insert(batch.end(), (uint8_t*)&count_net, (uint8_t*)&count_net + 4);
    
    // Write all queries with their lengths
    for (const auto& q : db->query_queue) {
        uint32_t len = q.sql.length();
        uint32_t len_net = htonl(len);
        batch.insert(batch.end(), (uint8_t*)&len_net, (uint8_t*)&len_net + 4);
        batch.insert(batch.end(), (uint8_t*)q.sql.c_str(), (uint8_t*)q.sql.c_str() + len);
    }
    
    // 2. Send batch
    if (write(db->fd, batch.data(), batch.size()) != (ssize_t)batch.size()) {
        return FLEXQL_ERROR;
    }
    
    // 3. Read batch response: [batch_count][response1_len][response1]...[responseN_len][responseN]
    uint32_t resp_count_net = 0;
    if (read(db->fd, &resp_count_net, 4) != 4) return FLEXQL_ERROR;
    uint32_t resp_count = ntohl(resp_count_net);
    
    if (resp_count != batch_count) {
        return FLEXQL_ERROR;
    }
    
    // 4. Read and process each response
    for (size_t i = 0; i < db->query_queue.size(); ++i) {
        uint32_t resp_len_net = 0;
        if (read(db->fd, &resp_len_net, 4) != 4) return FLEXQL_ERROR;
        
        uint32_t resp_len = ntohl(resp_len_net);
        if (resp_len > 1024 * 1024 * 100) return FLEXQL_ERROR;  // 100MB sanity limit
        
        std::vector<char> buffer(resp_len + 1, '\0');
        size_t total_read = 0;
        while (total_read < resp_len) {
            ssize_t r = read(db->fd, buffer.data() + total_read, resp_len - total_read);
            if (r <= 0) return FLEXQL_ERROR;
            total_read += r;
        }
        
        std::string response(buffer.data(), resp_len);
        
        // Call the callback if provided
        const auto& q = db->query_queue[i];
        if (q.callback) {
            if (q.callback(response) != FLEXQL_OK) {
                return FLEXQL_ERROR;
            }
        }
    }
    
    db->query_queue.clear();
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
) {
    if (!db || !sql) return FLEXQL_ERROR;
    if (errmsg) *errmsg = nullptr;

    // Check if this is an INSERT query (suitable for pipelining)
    const char *trimmed = sql;
    while (*trimmed && (*trimmed == ' ' || *trimmed == '\t' || *trimmed == '\n')) trimmed++;
    bool is_insert = (strncasecmp(trimmed, "INSERT", 6) == 0);
    
    // For INSERT queries, queue them for batching
    if (is_insert && !callback && db->query_queue.size() < db->batch_size) {
        PipelinedQuery pq;
        pq.sql = sql;
        pq.callback = [](const std::string&) { return FLEXQL_OK; };  // No-op for INSERTs
        db->query_queue.push_back(std::move(pq));
        
        // Auto-flush when batch is full
        if (db->query_queue.size() >= db->batch_size) {
            return flexql_flush_pipeline(db);
        }
        return FLEXQL_OK;
    }
    
    // For non-INSERT or when callback is provided, flush any pending queries first
    if (!db->query_queue.empty()) {
        int flush_rc = flexql_flush_pipeline(db);
        if (flush_rc != FLEXQL_OK) return flush_rc;
    }

    // Now execute the current query normally (single query)
    uint32_t len = strlen(sql);
    uint32_t len_net = htonl(len);

    if (write(db->fd, &len_net, 4) != 4) return FLEXQL_ERROR;
    if (write(db->fd, sql, len) != len) return FLEXQL_ERROR;

    uint32_t resp_len_net = 0;
    if (read(db->fd, &resp_len_net, 4) != 4) return FLEXQL_ERROR;

    uint32_t resp_len = ntohl(resp_len_net);
    if (resp_len > 1024 * 1024 * 100) return FLEXQL_ERROR; // 100MB sanity limit

    std::vector<char> buffer(resp_len + 1, '\0');
    size_t total_read = 0;
    while (total_read < resp_len) {
        ssize_t r = read(db->fd, buffer.data() + total_read, resp_len - total_read);
        if (r <= 0) return FLEXQL_ERROR;
        total_read += r;
    }

    std::string response(buffer.data(), resp_len);

    if (resp_len == 0) return FLEXQL_ERROR;

    // Fast check directly on the buffer
    if (buffer[0] == 'E') {
        if (errmsg) {
            std::string err_str(buffer.data());
            size_t newline_pos = err_str.find('\n');
            if (newline_pos != std::string::npos && newline_pos + 1 < err_str.size()) {
                *errmsg = strdup(err_str.substr(newline_pos + 1).c_str());
            }
        }
        return FLEXQL_ERROR;
    }

    if (buffer[0] != 'O') return FLEXQL_ERROR;

    // Process callback if provided
    if (callback) {
        std::vector<std::string> lines;
        size_t start = 0;
        size_t end = response.find('\n');
        
        while (end != std::string::npos) {
            lines.push_back(response.substr(start, end - start));
            start = end + 1;
            end = response.find('\n', start);
        }
        if (start < response.size()) {
            lines.push_back(response.substr(start));
        }

        if (lines.size() > 3) {
            // Parse columns
            std::vector<std::string> cols;
            std::string col_line = lines[1];
            size_t c_start = 0;
            size_t c_end = col_line.find('\t');
            while (c_end != std::string::npos) {
                cols.push_back(col_line.substr(c_start, c_end - c_start));
                c_start = c_end + 1;
                c_end = col_line.find('\t', c_start);
            }
            cols.push_back(col_line.substr(c_start));

            std::vector<char*> col_ptrs;
            for (auto& c : cols) col_ptrs.push_back(const_cast<char*>(c.c_str()));

            if (lines[2] != "---") return FLEXQL_ERROR;

            for (size_t i = 3; i < lines.size(); ++i) {
                if (lines[i].empty()) continue;

                std::vector<std::string> vals;
                std::string val_line = lines[i];
                size_t v_start = 0;
                size_t v_end = val_line.find('\t');
                while (v_end != std::string::npos) {
                    vals.push_back(val_line.substr(v_start, v_end - v_start));
                    v_start = v_end + 1;
                    v_end = val_line.find('\t', v_start);
                }
                vals.push_back(val_line.substr(v_start));

                std::vector<char*> val_ptrs;
                for (auto& v : vals) val_ptrs.push_back(const_cast<char*>(v.c_str()));

                int count = std::min(col_ptrs.size(), val_ptrs.size());
                if (callback(arg, count, val_ptrs.data(), col_ptrs.data()) != 0) {
                    break; 
                }
            }
        }
    }

    return FLEXQL_OK;
}