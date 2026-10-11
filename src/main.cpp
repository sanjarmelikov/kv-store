// kv-store: a Redis-compatible in-memory key-value server.
//
//   kv-store --port 6380 --aof data/appendonly.aof --appendfsync everysec
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "server.h"

namespace {

kv::Server* g_server = nullptr;

void handle_signal(int) {
    if (g_server) g_server->stop();  // the loop exits within one tick and flushes the AOF
}

void usage() {
    std::cerr << "usage: kv-store [--bind ADDR] [--port N] [--aof PATH] [--appendfsync always|everysec|no]\n"
                 "                [--requirepass PASSWORD] [--max-output-buffer BYTES]\n"
                 "                [--auto-aof-rewrite-percentage N] [--auto-aof-rewrite-min-size BYTES]\n"
                 "  KV_REQUIREPASS in the environment also sets the password (keeps it out of `ps`).\n";
}

}  // namespace

int main(int argc, char** argv) {
    kv::ServerConfig config;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                usage();
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--bind") config.bind = next();
        else if (arg == "--port") config.port = std::atoi(next().c_str());
        else if (arg == "--aof") config.aof_path = next();
        else if (arg == "--appendfsync") {
            try {
                config.fsync = kv::parse_fsync_policy(next());
            } catch (const std::invalid_argument& e) {
                std::cerr << "kv-store: " << e.what() << "\n";
                return 2;
            }
        }
        else if (arg == "--requirepass") config.requirepass = next();
        else if (arg == "--auto-aof-rewrite-percentage") config.auto_aof_rewrite_percentage = std::atoi(next().c_str());
        else if (arg == "--auto-aof-rewrite-min-size") config.auto_aof_rewrite_min_size = std::strtoull(next().c_str(), nullptr, 10);
        else if (arg == "--max-output-buffer") config.max_output_buffer = std::strtoull(next().c_str(), nullptr, 10);
        else if (arg == "--help" || arg == "-h") return usage(), 0;
        else return usage(), 2;
    }

    if (const char* pass = std::getenv("KV_REQUIREPASS"); pass && config.requirepass.empty())
        config.requirepass = pass;
    if (config.bind != "127.0.0.1" && config.requirepass.empty())
        std::cerr << "warning: listening on " << config.bind << " with no password; set KV_REQUIREPASS\n";

    std::signal(SIGPIPE, SIG_IGN);  // a client vanishing mid-write must not kill the server
    try {
        kv::Server server(config);
        g_server = &server;
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "kv-store: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
