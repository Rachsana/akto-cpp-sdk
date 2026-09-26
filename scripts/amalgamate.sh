#!/usr/bin/env bash
# Generates single_header/akto_sdk.hpp and single_header/akto_sdk_httplib.hpp
# by concatenating the multi-file library in dependency order, stripping
# internal #include "akto/..." lines and redundant #pragma once directives.
#
# Run this after editing anything under include/akto/ or src/kafka_sink.cpp;
# the single-header files are generated artifacts, not hand-edited.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_CORE="single_header/akto_sdk.hpp"
OUT_HTTPLIB="single_header/akto_sdk_httplib.hpp"

strip_internal() {
    # Drop internal includes and #pragma once (we add one at the top of
    # the amalgamated file instead); keep everything else verbatim.
    grep -v -E '^#include "akto/' | grep -v -E '^#pragma once$'
}

banner() {
    echo ""
    echo "// ============================================================================"
    echo "// $1"
    echo "// ============================================================================"
}

{
    cat <<'HEADER'
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
HEADER

    banner "types.hpp -- TrafficRecord + JSON schema"
    strip_internal < include/akto/types.hpp

    banner "sink.hpp -- ITrafficSink interface"
    strip_internal < include/akto/sink.hpp

    banner "console_sink.hpp"
    strip_internal < include/akto/console_sink.hpp

    banner "file_sink.hpp"
    strip_internal < include/akto/file_sink.hpp

    banner "kafka_sink.hpp -- declarations only; implementation is below,"
    echo "// guarded by AKTO_SDK_IMPLEMENTATION"
    strip_internal < include/akto/kafka_sink.hpp

    banner "traffic_logger.hpp -- async capture pipeline (header-only)"
    strip_internal < include/akto/traffic_logger.hpp

    banner "kafka_sink.cpp implementation -- compiled into exactly one TU"
    echo "#ifdef AKTO_SDK_IMPLEMENTATION"
    grep -v -E '^#include "akto/' < src/kafka_sink.cpp
    echo "#endif // AKTO_SDK_IMPLEMENTATION"

} > "$OUT_CORE"

{
    cat <<'HEADER'
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
HEADER

    banner "httplib_middleware.hpp"
    strip_internal < include/akto/httplib_middleware.hpp

} > "$OUT_HTTPLIB"

echo "Wrote $OUT_CORE ($(wc -l < "$OUT_CORE") lines)"
echo "Wrote $OUT_HTTPLIB ($(wc -l < "$OUT_HTTPLIB") lines)"
