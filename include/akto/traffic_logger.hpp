#pragma once

#include "akto/types.hpp"
#include "akto/sink.hpp"

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
