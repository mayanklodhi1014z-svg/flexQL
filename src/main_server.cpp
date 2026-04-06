#include "flexql/server.h"
#include <iostream>
#include <csignal>

int main(int argc, char* argv[]) {
    // Prevent server crash when a client disconnects mid-response
    signal(SIGPIPE, SIG_IGN);
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc >= 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    }

    try {
        flexql::Server server(host, port);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
