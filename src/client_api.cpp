#include "flexql.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <algorithm>

struct FlexQL {
    int fd;
};

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return FLEXQL_ERROR;

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        close(sock);
        return FLEXQL_ERROR;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return FLEXQL_ERROR;
    }

    FlexQL *conn = (FlexQL*)malloc(sizeof(FlexQL));
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
    close(db->fd);
    free(db);
    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    if (ptr) {
        free(ptr);
    }
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

   std::string response(buffer.data(), resp_len); // Avoid strlen overhead

    // 1. Fast check for Error or Success without splitting the whole string
   // --- REPLACE EVERYTHING AFTER THE READ LOOP WITH THIS ---

    if (resp_len == 0) return FLEXQL_ERROR;

    // 1. Fast check directly on the raw vector buffer (ZERO memory copying)
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

    // 2. ONLY allocate strings and parse rows if the user provided a callback
    if (callback) {
        std::string response(buffer.data(), resp_len); // Only allocate when needed
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