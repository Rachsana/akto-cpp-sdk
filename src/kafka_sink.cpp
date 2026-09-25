#include "akto/kafka_sink.hpp"
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
