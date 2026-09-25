# akto-cpp-sdk

A C++ traffic-processing SDK for the Akto internship assignment: a middleware
that logs every HTTP request/response pair handled by a web server and ships
them to Kafka, in a way that never adds latency to the request path.

Built in C++ (not Java / Express-Node / Go / Ruby / Flask, per the
assignment's constraint), and designed to be schema-compatible with Akto's
existing SDKs — see [Schema compatibility](#schema-compatibility) below.

> **Note on submission:** per the assignment's instructions ("In case of any
> doubts, assume the needful"), I assumed C++ was an acceptable "any other
> language" choice and built the SDK's transport against a real Kafka client
> (librdkafka) rather than a mock. I did not have a live Kafka broker
> available while developing, so I verified the async, non-blocking, and
> failure-handling behavior against an unreachable broker (see
> [Testing notes](#testing-notes)), and included a `docker-compose.yml` so
> you can verify actual message delivery end-to-end.

## Architecture

```
┌─────────────────────┐      log()       ┌──────────────────┐
│  Your web server /   │ ───────────────► │   TrafficLogger   │
│  cpp-httplib route   │   (non-blocking) │  (bounded queue + │
│  handler             │ ◄─────────────── │  worker thread)   │
└─────────────────────┘   returns at once └─────────┬─────────┘
                                                      │ background
                                                      │ thread
                                                      ▼
                                           ┌──────────────────┐
                                           │   ITrafficSink    │
                                           │  (interface)      │
                                           └─────────┬─────────┘
                                    ┌─────────────────┼─────────────────┐
                                    ▼                 ▼                 ▼
                              KafkaSink          FileSink          ConsoleSink
                            (librdkafka,      (NDJSON to disk,    (stdout, for
                             production)       for local testing)  quick debugging)
```

- **`TrafficLogger`** (`include/akto/traffic_logger.hpp`) is the only class
  request handlers talk to. `log()` copies the captured record onto a
  bounded, mutex-protected queue and returns immediately. A background
  worker thread drains the queue and calls the sink. If the sink can't keep
  up, the queue drops the *oldest* record rather than growing unbounded or
  blocking the request thread — this is what "won't have any impact on
  production performance" (per Akto's own SDK description) means in
  practice.
- **`ITrafficSink`** (`include/akto/sink.hpp`) decouples capture from
  transport. Three implementations are included:
  - `KafkaSink` — the real transport, built on `librdkafka`'s async producer.
  - `FileSink` — writes newline-delimited JSON to disk; exactly what would
    have been the Kafka message value, one line per record. Useful as a
    "topic on disk" when you don't have a broker handy.
  - `ConsoleSink` — prints each record to stdout.
- **`HttplibMiddleware`** (`include/akto/httplib_middleware.hpp`) is the
  plug-and-play adapter for [cpp-httplib](https://github.com/yhirose/cpp-httplib).
  It hooks `set_pre_routing_handler` (to stamp a start time) and
  `set_logger` (which cpp-httplib calls once a response is fully generated)
  to build a `TrafficRecord` and hand it to the logger — **zero changes to
  route handlers required**.
- If you're using a different C++ framework, `httplib_middleware.hpp` is the
  only file you need to replace: implement the same idea (capture
  req/res → build a `TrafficRecord` → call `logger.log(record)`) against
  that framework's request/response hook.

## Schema compatibility

The captured record's JSON fields deliberately mirror
[`akto-api-security/gomiddleware`](https://github.com/akto-api-security/gomiddleware)'s
`process()` function (the reference implementation named in the assignment),
field-for-field:

| Field             | Notes                                                            |
|-------------------|-------------------------------------------------------------------|
| `akto_account_id` | account identifier, configurable                                  |
| `path`            | request path, **no query string** (matches the Go SDK's `r.URL.Path`) |
| `requestHeaders`  | JSON-encoded header map                                          |
| `responseHeaders` | JSON-encoded header map                                          |
| `method`          | GET, POST, ...                                                    |
| `requestPayload`  | raw request body (truncated at `maxBodyBytes`, default 1MB)       |
| `responsePayload` | raw response body (same truncation)                                |
| `ip`              | honors `X-Forwarded-For` if present, else the socket peer address |
| `time`            | unix epoch seconds                                                 |
| `statusCode`      | HTTP response status                                              |
| `type`            | protocol version, e.g. `"HTTP/1.1"` (matches the Go SDK's `r.Proto`) |
| `status`          | always `"null"` — the reference SDK sends this literal too; kept purely for downstream schema compatibility |
| `contentType`     | response `Content-Type` header                                    |

Two extra fields (`source`, `latencyMs`) are appended beyond the reference
schema as additional metadata. They're additive — any consumer reading the
known keys above is unaffected by their presence.

## Repo layout

```
akto-cpp-sdk/
├── CMakeLists.txt
├── docker-compose.yml        # local Kafka+Zookeeper, for real end-to-end testing
├── include/akto/
│   ├── akto_sdk.hpp           # umbrella include for the core (sink-agnostic) SDK
│   ├── types.hpp              # TrafficRecord + JSON schema
│   ├── sink.hpp                # ITrafficSink interface
│   ├── console_sink.hpp
│   ├── file_sink.hpp
│   ├── kafka_sink.hpp
│   ├── traffic_logger.hpp     # async capture pipeline (header-only)
│   └── httplib_middleware.hpp # cpp-httplib integration
├── src/
│   └── kafka_sink.cpp         # librdkafka implementation
├── examples/echo_server/
│   └── main.cpp               # test server wired up with the SDK
└── third_party/httplib.h      # vendored single-header cpp-httplib
```

## Building

### Dependencies

- CMake ≥ 3.16, a C++17 compiler
- `librdkafka` (dev headers + library)
- `nlohmann-json` (dev headers)

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y g++ cmake make librdkafka-dev nlohmann-json3-dev libssl-dev pkg-config
```

### Build

```bash
git clone <this-repo-url> akto-cpp-sdk
cd akto-cpp-sdk
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

This produces:
- `libakto_sdk.a` — the SDK, linkable into your own project
- `akto_echo_server` — the example server, for a quick end-to-end demo

## Running the example

The example server picks its sink via environment variables:

```bash
# Console sink -- prints each captured record to stdout, no broker needed
AKTO_SINK=console PORT=8080 ./build/akto_echo_server

# File sink -- appends NDJSON records to disk
AKTO_SINK=file AKTO_FILE_PATH=traffic.ndjson PORT=8080 ./build/akto_echo_server

# Kafka sink -- the real transport
AKTO_SINK=kafka AKTO_KAFKA_BROKERS=localhost:9092 AKTO_KAFKA_TOPIC=akto.api.logs \
  PORT=8080 ./build/akto_echo_server
```

Then, in another terminal:

```bash
curl -X POST localhost:8080/echo -d '{"hello":"world"}' -H 'Content-Type: application/json'
curl localhost:8080/users/42
curl localhost:8080/stats     # queue depth / dropped-record counters
```

Every request above gets captured and published to whichever sink you chose
— no changes were made to the route handlers themselves to enable this; see
`examples/echo_server/main.cpp` for the ~3 lines of integration code.

### Testing the real Kafka path

Spin up a local broker:

```bash
docker compose up -d
AKTO_SINK=kafka AKTO_KAFKA_BROKERS=localhost:9092 PORT=8080 ./build/akto_echo_server
```

In another terminal, watch messages arrive:

```bash
docker exec -it akto-cpp-sdk-kafka-1 \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic akto.api.logs --from-beginning
```

Then hit any endpoint with `curl` as above and watch the consumer print each
captured record.

## Using the SDK in your own project

```cpp
#include <akto/akto_sdk.hpp>
#include <akto/httplib_middleware.hpp>

int main() {
    akto::KafkaConfig kconf;
    kconf.brokers = "localhost:9092";
    kconf.topic = "akto.api.logs";
    auto sink = std::make_shared<akto::KafkaSink>(kconf);

    akto::TrafficLogger logger(sink);   // starts its worker thread here

    httplib::Server server;
    // ... register your routes as normal ...

    akto::HttplibMiddleware middleware(server, logger); // one line, plug-and-play

    server.listen("0.0.0.0", 8080);
    logger.shutdown();                  // flushes remaining records on exit
}
```

Link against `akto_sdk` (and `Threads::Threads`, pulled in transitively) via
CMake:

```cmake
find_package(akto_sdk REQUIRED)   # after `make install`, or add_subdirectory()
target_link_libraries(your_app PRIVATE akto::akto_sdk)
```

or just `add_subdirectory(path/to/akto-cpp-sdk)` and link `akto_sdk` directly
if you'd rather not install it system-wide.

## Design notes / good-practice choices

- **Non-blocking by construction.** `TrafficLogger::log()` never does I/O; a
  dedicated worker thread owns all interaction with the sink. Verified
  empirically: requests complete in <1ms even when pointed at a completely
  unreachable Kafka broker (see [Testing notes](#testing-notes)).
- **Bounded queue with explicit backpressure.** A pathological sink (or a
  Kafka outage) can't turn into unbounded memory growth; the oldest record
  is dropped and a counter (`TrafficLogger::droppedCount()`) tracks it,
  exposed via the example's `/stats` endpoint.
- **Sink abstraction over Kafka specifics.** Lets the capture/middleware
  code be tested and demoed without a live broker, while still shipping a
  real `librdkafka`-based production path.
- **Body truncation.** Request/response bodies are capped (`maxBodyBytes`,
  default 1MB) before being queued, so a large upload/download can't blow
  up memory or message size.
- **Header-only core + one .cpp.** `traffic_logger.hpp` and the sink
  interfaces are header-only for easy vendoring; only the librdkafka
  wrapper needs a translation unit (to keep `<librdkafka/rdkafkacpp.h>` out
  of every consumer's include graph).

## Testing notes

Since I didn't have a live Kafka broker available while building this, I
verified correctness in layers:

1. **Capture correctness** — verified against `ConsoleSink` and `FileSink`
   with real `curl` requests (GET/POST, query strings, form-encoded and
   JSON bodies, custom headers, `X-Forwarded-For`) and confirmed each
   captured record matches what was actually sent/received, field-for-field
   against the reference schema above.
2. **Non-blocking behavior under Kafka failure** — pointed `KafkaSink` at an
   unreachable address (`127.0.0.1:19092`, nothing listening) and confirmed:
   requests still complete in under 1ms, librdkafka's connection failures
   are logged but never thrown/propagated, and the server shuts down
   cleanly (no crash, no hang) even with the broker still down.
3. **End-to-end Kafka delivery** — not verified in the sandbox this was
   built in (no Docker/Kafka available there), which is why
   `docker-compose.yml` is included: run it locally to confirm messages
   actually land on the topic.

Two real bugs were found and fixed during testing, worth calling out since
they're the kind of thing that's easy to miss without exercising the code:

- cpp-httplib parses `application/x-www-form-urlencoded` POST bodies into
  `req.params` — reconstructing the captured path from those params (an
  earlier version of this code did) silently folded form fields into the
  logged path for such requests. Fixed by capturing `req.path` directly.
- `FileSink` originally didn't flush after each write, so records sat in
  `ofstream`'s userspace buffer and were invisible until the process
  exited. Fixed by flushing per-write, since this sink exists for local
  inspection where visibility matters more than raw throughput.
