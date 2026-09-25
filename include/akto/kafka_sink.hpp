#pragma once

#include "akto/sink.hpp"
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
