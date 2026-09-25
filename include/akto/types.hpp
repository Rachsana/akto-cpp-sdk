#pragma once

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace akto {

// A single captured request/response pair.
//
// Field names and semantics deliberately mirror akto-api-security's own
// Go SDK (github.com/akto-api-security/gomiddleware, see middleware.go's
// `process()` function) so this record is drop-in compatible with an
// existing Akto Kafka topic / collector -- not just "similar in spirit".
// That reference sends a flat map with these exact keys:
//   akto_account_id, path, requestHeaders, responseHeaders, method,
//   requestPayload, responsePayload, ip, time, statusCode, type,
//   status, contentType
// A couple of fields (source, latencyMs) are appended as additional,
// non-breaking metadata -- any consumer reading the known keys above
// is unaffected by their presence.
struct TrafficRecord {
    std::string path;              // request path (no query string, matching
                                    // the reference SDK's r.URL.Path capture)
    std::string method;            // GET, POST, ...
    std::string requestHeaders;    // JSON-encoded string of header map
    std::string requestPayload;    // raw request body
    std::string responseHeaders;   // JSON-encoded string of header map
    std::string responsePayload;   // raw response body
    int statusCode = 0;            // HTTP response status code
    std::string ip;                // client IP (honors X-Forwarded-For)
    int64_t time = 0;              // unix epoch seconds when request was received
    std::string type = "HTTP/1.1"; // protocol version, e.g. "HTTP/1.1" --
                                    // matches the reference SDK's r.Proto
    std::string status = "null";   // reference SDK always sends this literal;
                                    // kept for schema/collector compatibility
    std::string contentType;       // response Content-Type header, if any
    std::string aktoAccountId = "1000000";

    // extra fields beyond the reference schema, safe to ignore downstream
    std::string source = "AKTO_CPP_SDK";
    int64_t latencyMs = 0;         // time spent processing the request

    // Serialize to the flat JSON object Akto's collector expects.
    nlohmann::json toJson() const {
        return nlohmann::json{
            {"path", path},
            {"requestHeaders", requestHeaders},
            {"responseHeaders", responseHeaders},
            {"method", method},
            {"requestPayload", requestPayload},
            {"responsePayload", responsePayload},
            {"ip", ip},
            {"time", time},
            {"statusCode", statusCode},
            {"type", type},
            {"status", status},
            {"contentType", contentType},
            {"akto_account_id", aktoAccountId},
            {"source", source},
            {"latencyMs", latencyMs}
        };
    }
};

} // namespace akto
