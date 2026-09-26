#pragma once
//
// akto_sdk_httplib.hpp -- cpp-httplib middleware adapter (single-header).
//
// GENERATED FILE -- do not edit directly. Edit
// include/akto/httplib_middleware.hpp, then run scripts/amalgamate.sh.
//
// Include order in your translation unit:
//   #include <httplib.h>       // https://github.com/yhirose/cpp-httplib
//   #include "akto_sdk.hpp"
//   #include "akto_sdk_httplib.hpp"
//

// ============================================================================
// httplib_middleware.hpp
// ============================================================================

// Adapter that plugs akto::TrafficLogger into a cpp-httplib server.
// This is the "middleware" piece: one call wires up capture of every
// request/response pair the server handles, with no changes needed to
// individual route handlers -- true plug-and-play.
//
// If you're using a different C++ web framework, this file is the only
// one you need to swap out: implement the same idea (capture req/res,
// build an akto::TrafficRecord, call logger.log(record)) against that
// framework's hook.

#include <httplib.h>
#include <chrono>
#include <sstream>
#include <unordered_map>

namespace akto {

inline std::string headersToJsonString(const httplib::Headers& headers) {
    std::ostringstream oss;
    oss << "{";
    bool first = true;
    for (const auto& [k, v] : headers) {
        if (!first) oss << ",";
        first = false;
        // Minimal escaping; header names/values are rarely adversarial,
        // but we don't want a stray quote to break the JSON string.
        auto esc = [](const std::string& s) {
            std::string out;
            for (char c : s) {
                if (c == '"' || c == '\\') out += '\\';
                out += c;
            }
            return out;
        };
        oss << "\"" << esc(k) << "\":\"" << esc(v) << "\"";
    }
    oss << "}";
    return oss.str();
}

class HttplibMiddleware {
public:
    HttplibMiddleware(httplib::Server& server, TrafficLogger& logger)
        : server_(server), logger_(logger) {
        attach();
    }

private:
    void attach() {
        // Stamp the start time as early as possible so latency reflects
        // real server-side processing time, not just the handler itself.
        server_.set_pre_routing_handler([this](const httplib::Request& req, httplib::Response&) {
            std::lock_guard<std::mutex> lock(startTimesMutex_);
            startTimes_[requestKey(req)] = std::chrono::steady_clock::now();
            return httplib::Server::HandlerResponse::Unhandled;
        });

        // Fires once the response has been fully generated -- this is
        // where we capture the complete request/response pair.
        server_.set_logger([this](const httplib::Request& req, const httplib::Response& res) {
            TrafficRecord record;
            // Matches the reference Go SDK: path only, no query string.
            // (Query params are still visible in requestHeaders/body when
            // relevant; this keeps the record schema-compatible with an
            // existing Akto collector built against that SDK's output.)
            record.path = req.path;
            record.method = req.method;
            record.requestHeaders = headersToJsonString(req.headers);
            record.requestPayload = req.body;
            record.responseHeaders = headersToJsonString(res.headers);
            record.responsePayload = res.body;
            record.statusCode = res.status;

            // Honor X-Forwarded-For when present, same precedence as the
            // reference Go SDK, so records are correct behind a proxy/LB.
            record.ip = req.has_header("X-Forwarded-For")
                            ? req.get_header_value("X-Forwarded-For")
                            : req.remote_addr;

            record.time = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
            record.type = req.version.empty() ? "HTTP/1.1" : req.version;
            record.contentType = res.get_header_value("Content-Type");

            std::string key = requestKey(req);
            {
                std::lock_guard<std::mutex> lock(startTimesMutex_);
                auto it = startTimes_.find(key);
                if (it != startTimes_.end()) {
                    record.latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now() - it->second)
                                            .count();
                    startTimes_.erase(it);
                }
            }

            logger_.log(std::move(record));
        });
    }

    // Cheap per-request correlation key. Good enough for a single
    // in-process server handling one request at a time per connection;
    // for very high concurrency swap this for a request-scoped opaque
    // pointer/UUID passed via req.
    static std::string requestKey(const httplib::Request& req) {
        return req.remote_addr + ":" + std::to_string(req.remote_port) + ":" +
               req.method + ":" + req.path;
    }

    httplib::Server& server_;
    TrafficLogger& logger_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> startTimes_;
    std::mutex startTimesMutex_;
};

} // namespace akto
