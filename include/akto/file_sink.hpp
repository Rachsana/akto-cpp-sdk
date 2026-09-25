#pragma once

#include "akto/sink.hpp"
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
