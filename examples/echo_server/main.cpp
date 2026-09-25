// Example: a plain cpp-httplib echo server with the Akto traffic SDK
// wired in as middleware. Every request/response is captured and sent
// to the configured sink (Kafka by default; falls back to console).
//
// This file demonstrates the ONLY integration work required:
//   1. Construct a sink (KafkaSink, FileSink, or ConsoleSink).
//   2. Construct an akto::TrafficLogger around it.
//   3. Construct an akto::HttplibMiddleware(server, logger).
// No changes to route handlers below are needed at all.

#include <akto/akto_sdk.hpp>
#include <akto/httplib_middleware.hpp>

#include <httplib.h>
#include <iostream>
#include <cstdlib>
#include <csignal>

namespace {
    akto::TrafficLogger* g_logger = nullptr;
    httplib::Server* g_server = nullptr;

    void handleShutdown(int) {
        std::cout << "\n[echo-server] shutting down..." << std::endl;
        if (g_server) g_server->stop();
    }
}

int main(int argc, char** argv) {
    // ---- 1. Choose a sink for captured traffic -------------------------
    // AKTO_SINK=kafka|console|file   (default: console, no broker needed)
    // AKTO_KAFKA_BROKERS=host:port   AKTO_KAFKA_TOPIC=topic-name
    std::string sinkType = std::getenv("AKTO_SINK") ? std::getenv("AKTO_SINK") : "console";

    std::shared_ptr<akto::ITrafficSink> sink;
    if (sinkType == "kafka") {
        akto::KafkaConfig kconf;
        if (auto v = std::getenv("AKTO_KAFKA_BROKERS")) kconf.brokers = v;
        if (auto v = std::getenv("AKTO_KAFKA_TOPIC")) kconf.topic = v;
        std::cout << "[echo-server] using KafkaSink -> brokers=" << kconf.brokers
                   << " topic=" << kconf.topic << std::endl;
        sink = std::make_shared<akto::KafkaSink>(kconf);
    } else if (sinkType == "file") {
        std::string path = std::getenv("AKTO_FILE_PATH") ? std::getenv("AKTO_FILE_PATH")
                                                            : "akto_traffic.ndjson";
        std::cout << "[echo-server] using FileSink -> " << path << std::endl;
        sink = std::make_shared<akto::FileSink>(path);
    } else {
        std::cout << "[echo-server] using ConsoleSink (set AKTO_SINK=kafka for real delivery)"
                   << std::endl;
        sink = std::make_shared<akto::ConsoleSink>();
    }

    // ---- 2. Wrap the sink in the async logger --------------------------
    akto::TrafficLogger logger(sink);
    g_logger = &logger;

    // ---- 3. Plain httplib server with normal route handlers -----------
    httplib::Server server;
    g_server = &server;

    server.Get("/echo", [](const httplib::Request& req, httplib::Response& res) {
        res.set_content(req.body.empty() ? "echo: (empty body)" : "echo: " + req.body,
                          "text/plain");
    });

    server.Post("/echo", [](const httplib::Request& req, httplib::Response& res) {
        res.set_content(req.body, "application/json");
        res.status = 200;
    });

    server.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(R"({"status":"ok"})", "application/json");
    });

    server.Get("/users/:id", [](const httplib::Request& req, httplib::Response& res) {
        std::string id = req.path_params.at("id");
        res.set_content(R"({"id":")" + id + R"(","name":"demo-user"})", "application/json");
    });

    server.Get("/stats", [&](const httplib::Request&, httplib::Response& res) {
        res.set_content(
            R"({"queued":)" + std::to_string(logger.queueDepth()) +
                R"(,"dropped":)" + std::to_string(logger.droppedCount()) + "}",
            "application/json");
    });

    // ---- 4. Wire the SDK in -- one line, no handler changes -----------
    akto::HttplibMiddleware middleware(server, logger);

    std::signal(SIGINT, handleShutdown);
    std::signal(SIGTERM, handleShutdown);

    int port = 8080;
    if (auto v = std::getenv("PORT")) port = std::atoi(v);

    std::cout << "[echo-server] listening on http://localhost:" << port << std::endl;
    std::cout << "[echo-server] try: curl -X POST localhost:" << port
               << "/echo -d '{\"hello\":\"world\"}'" << std::endl;

    server.listen("0.0.0.0", port);

    logger.shutdown();
    return 0;
}
