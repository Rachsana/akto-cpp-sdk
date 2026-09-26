#pragma once
//
// akto_sdk.hpp -- single-header amalgamation of the Akto C++ traffic SDK.
//
// GENERATED FILE -- do not edit directly. Edit the sources under
// include/akto/ and src/kafka_sink.cpp, then run scripts/amalgamate.sh.
//
// Usage:
//   #include "akto_sdk.hpp"          // in every translation unit that uses the SDK
//   #define AKTO_SDK_IMPLEMENTATION  // in exactly ONE .cpp file, before the include
//   #include "akto_sdk.hpp"
//
// Requires (system packages, not vendored -- same as the multi-file build):
//   - librdkafka (dev headers + library), e.g. `apt install librdkafka-dev`
//   - nlohmann-json, e.g. `apt install nlohmann-json3-dev`
//   - a thread-capable C++17 toolchain (link with -lpthread / Threads::Threads)
//
// This single file is everything needed for capture + Kafka/File/Console
// delivery. For the cpp-httplib middleware adapter, also drop in
// akto_sdk_httplib.hpp (kept separate so this core header has no
// dependency on any particular web framework).
//

// ============================================================================
// types.hpp -- TrafficRecord + JSON schema
// ============================================================================

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

// ============================================================================
// sink.hpp -- ITrafficSink interface
// ============================================================================

#include <string>

namespace akto {

// A sink is anything that can accept a serialized traffic record.
// The logger doesn't know or care whether that's Kafka, a file, or
// stdout -- this is what lets the SDK be tested without a live broker,
// and lets a user swap transports without touching capture logic.
class ITrafficSink {
public:
    virtual ~ITrafficSink() = default;

    // Publish one already-serialized (JSON string) traffic record.
    // Must be safe to call from the logger's background worker thread.
    // Returns true if the record was accepted for delivery.
    virtual bool publish(const std::string& key, const std::string& jsonRecord) = 0;

    // Flush any buffered/in-flight records. Called on shutdown.
    virtual void flush() = 0;
};

} // namespace akto

// ============================================================================
// console_sink.hpp
// ============================================================================

#include <iostream>
#include <mutex>

namespace akto {

// Prints each record to stdout. Useful for local development and for
// verifying the capture pipeline works before wiring up a real broker.
class ConsoleSink : public ITrafficSink {
public:
    bool publish(const std::string& key, const std::string& jsonRecord) override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "[akto-sdk][kafka-topic=akto.api.logs][key=" << key << "] "
                   << jsonRecord << std::endl;
        return true;
    }

    void flush() override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout.flush();
    }

private:
    std::mutex mutex_;
};

} // namespace akto

// ============================================================================
// file_sink.hpp
// ============================================================================

#include <fstream>
#include <mutex>
#include <stdexcept>

namespace akto {

// Appends each record as one line of NDJSON to a file. Handy as a
// "replayable topic on disk" for testing without a Kafka cluster --
// each line is exactly what would have been the Kafka message value.
class FileSink : public ITrafficSink {
public:
    explicit FileSink(const std::string& path)
        : out_(path, std::ios::app) {
        if (!out_.is_open()) {
            throw std::runtime_error("akto::FileSink: failed to open " + path);
        }
    }

    bool publish(const std::string& key, const std::string& jsonRecord) override {
        std::lock_guard<std::mutex> lock(mutex_);
        out_ << jsonRecord << '\n';
        out_.flush(); // this sink exists for local testing/inspection, so
                       // favor durability/visibility over write throughput
        return true;
    }

    void flush() override {
        std::lock_guard<std::mutex> lock(mutex_);
        out_.flush();
    }

private:
    std::mutex mutex_;
    std::ofstream out_;
};

} // namespace akto

// ============================================================================
// kafka_sink.hpp -- declarations only; implementation is below,
// ============================================================================
// guarded by AKTO_SDK_IMPLEMENTATION

#include <memory>
#include <string>

// Forward-declare librdkafka C++ types so consumers of this header don't
// need <librdkafka/rdkafkacpp.h> unless they're building kafka_sink.cpp.
namespace RdKafka {
    class Producer;
    class Topic;
    class DeliveryReportCb;
}

namespace akto {

struct KafkaConfig {
    std::string brokers = "localhost:9092";   // comma-separated bootstrap servers
    std::string topic = "akto.api.logs";
    int queueBufferingMaxMs = 100;             // librdkafka linger.ms
    int messageMaxBytes = 10 * 1024 * 1024;    // 10MB, generous for large payloads
};

// Publishes traffic records to a Kafka topic via librdkafka's async
// producer. Delivery is fire-and-forget from the caller's perspective;
// failures are logged through the delivery-report callback rather than
// thrown, since a single dropped log line should never take down the
// web server the SDK is embedded in.
class KafkaSink : public ITrafficSink {
public:
    explicit KafkaSink(const KafkaConfig& config);
    ~KafkaSink() override;

    bool publish(const std::string& key, const std::string& jsonRecord) override;
    void flush() override;

private:
    KafkaConfig config_;
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::DeliveryReportCb> deliveryReportCb_;
};

} // namespace akto

// ============================================================================
// traffic_logger.hpp -- async capture pipeline (header-only)
// ============================================================================


#include <deque>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>
#include <cstdint>

namespace akto {

// TrafficLogger is the heart of the SDK. Request handlers call log()
// with a captured TrafficRecord and return immediately -- the actual
// serialization and publish to the sink (Kafka, file, ...) happens on
// a background worker thread, so a slow or unreachable broker can
// never add latency to the web server's response path.
//
// If the sink can't keep up, the bounded queue applies backpressure by
// dropping the oldest record rather than growing without bound or
// blocking the caller.
// Defined at namespace scope (rather than nested in TrafficLogger) so its
// default member initializers can be used as a default constructor
// argument value below -- nested-class default member initializers
// aren't usable until their enclosing class is complete.
struct TrafficLoggerOptions {
    size_t maxQueueSize = 10000;       // bounded queue, applies backpressure
    size_t workerThreads = 1;          // usually 1 is enough; sink does I/O
    size_t maxBodyBytes = 1 << 20;      // truncate captured bodies at 1MB
};

class TrafficLogger {
public:
    using Options = TrafficLoggerOptions;

    explicit TrafficLogger(std::shared_ptr<ITrafficSink> sink, Options opts = Options())
        : sink_(std::move(sink)), opts_(opts) {
        for (size_t i = 0; i < std::max<size_t>(1, opts_.workerThreads); ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    ~TrafficLogger() {
        shutdown();
    }

    TrafficLogger(const TrafficLogger&) = delete;
    TrafficLogger& operator=(const TrafficLogger&) = delete;

    // Non-blocking. Truncates oversized bodies, enqueues the record, and
    // returns. Safe to call concurrently from many request-handling threads.
    void log(TrafficRecord record) {
        if (record.requestPayload.size() > opts_.maxBodyBytes) {
            record.requestPayload.resize(opts_.maxBodyBytes);
        }
        if (record.responsePayload.size() > opts_.maxBodyBytes) {
            record.responsePayload.resize(opts_.maxBodyBytes);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.size() >= opts_.maxQueueSize) {
                queue_.pop_front();          // drop oldest, apply backpressure
                droppedCount_.fetch_add(1, std::memory_order_relaxed);
            }
            queue_.push_back(std::move(record));
        }
        cv_.notify_one();
    }

    // Flushes the sink and stops worker threads. Safe to call more than
    // once; the destructor calls this too.
    void shutdown() {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(expected, true)) {
            return; // already shutting down / shut down
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
        if (sink_) sink_->flush();
    }

    uint64_t droppedCount() const { return droppedCount_.load(std::memory_order_relaxed); }
    size_t queueDepth() {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    void workerLoop() {
        while (true) {
            TrafficRecord record;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stopping_.load() || !queue_.empty(); });
                if (queue_.empty()) {
                    if (stopping_.load()) return;
                    continue;
                }
                record = std::move(queue_.front());
                queue_.pop_front();
            }

            // Partition key: group by path so a given endpoint's traffic
            // lands on the same Kafka partition (useful for downstream
            // per-endpoint ordering/aggregation).
            sink_->publish(record.path, record.toJson().dump());
        }
    }

    std::shared_ptr<ITrafficSink> sink_;
    Options opts_;

    std::deque<TrafficRecord> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stopping_{false};
    std::atomic<uint64_t> droppedCount_{0};
};

} // namespace akto

// ============================================================================
// kafka_sink.cpp implementation -- compiled into exactly one TU
// ============================================================================
#ifdef AKTO_SDK_IMPLEMENTATION
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <stdexcept>

namespace akto {

namespace {

// Reports the outcome of each async produce() call. We only log failures
// -- a slow/erroring Kafka broker must never propagate back into the
// web server's request path.
class LoggingDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) override {
        if (message.err() != RdKafka::ERR_NO_ERROR) {
            std::cerr << "[akto-sdk] kafka delivery failed: "
                       << message.errstr() << std::endl;
        }
    }
};

} // namespace

KafkaSink::KafkaSink(const KafkaConfig& config) : config_(config) {
    std::string errstr;

    std::unique_ptr<RdKafka::Conf> conf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    if (conf->set("bootstrap.servers", config_.brokers, errstr) != RdKafka::Conf::CONF_OK)
        throw std::runtime_error("akto::KafkaSink: bad bootstrap.servers: " + errstr);

    if (conf->set("queue.buffering.max.ms", std::to_string(config_.queueBufferingMaxMs), errstr)
        != RdKafka::Conf::CONF_OK)
        throw std::runtime_error("akto::KafkaSink: bad queue.buffering.max.ms: " + errstr);

    if (conf->set("message.max.bytes", std::to_string(config_.messageMaxBytes), errstr)
        != RdKafka::Conf::CONF_OK)
        throw std::runtime_error("akto::KafkaSink: bad message.max.bytes: " + errstr);

    // Don't let a broker that's briefly unreachable block startup for long.
    conf->set("socket.timeout.ms", "10000", errstr);

    deliveryReportCb_ = std::make_unique<LoggingDeliveryReportCb>();
    if (conf->set("dr_cb", deliveryReportCb_.get(), errstr) != RdKafka::Conf::CONF_OK)
        throw std::runtime_error("akto::KafkaSink: failed to set dr_cb: " + errstr);

    producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_)
        throw std::runtime_error("akto::KafkaSink: failed to create producer: " + errstr);
}

KafkaSink::~KafkaSink() {
    if (producer_) {
        flush();
    }
}

bool KafkaSink::publish(const std::string& key, const std::string& jsonRecord) {
    RdKafka::ErrorCode err = producer_->produce(
        config_.topic,
        RdKafka::Topic::PARTITION_UA,       // let the partitioner choose
        RdKafka::Producer::RK_MSG_COPY,     // librdkafka copies the payload
        const_cast<char*>(jsonRecord.data()), jsonRecord.size(),
        key.empty() ? nullptr : key.data(), key.size(),
        0,                                   // timestamp: now
        nullptr);

    // Drive the delivery-report callback queue. Non-blocking.
    producer_->poll(0);

    if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "[akto-sdk] kafka produce failed: "
                   << RdKafka::err2str(err) << std::endl;
        return false;
    }
    return true;
}

void KafkaSink::flush() {
    // Give librdkafka up to 5s to drain the outbound queue.
    producer_->flush(5000);
}

} // namespace akto
#endif // AKTO_SDK_IMPLEMENTATION
