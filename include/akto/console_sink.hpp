#pragma once

#include "akto/sink.hpp"
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
