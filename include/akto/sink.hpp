#pragma once

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
