cmake_minimum_required(VERSION 3.16)
project(akto_traffic_sdk VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)
find_package(nlohmann_json 3.2.0 QUIET)
find_package(PkgConfig QUIET)

if(PkgConfig_FOUND)
  pkg_check_modules(RDKAFKA QUIET rdkafka++)
endif()

# ---------------------------------------------------------------------------
# Core SDK library (sink-agnostic capture + async delivery)
# ---------------------------------------------------------------------------
add_library(akto_sdk
  src/kafka_sink.cpp
)

target_include_directories(akto_sdk
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(akto_sdk PUBLIC Threads::Threads)

if(nlohmann_json_FOUND)
  target_link_libraries(akto_sdk PUBLIC nlohmann_json::nlohmann_json)
else()
  # Debian/Ubuntu package installs headers without exporting a CMake
  # target; the include path already covers it, nothing else to link.
endif()

if(RDKAFKA_FOUND)
  target_include_directories(akto_sdk PUBLIC ${RDKAFKA_INCLUDE_DIRS})
  target_link_libraries(akto_sdk PUBLIC ${RDKAFKA_LIBRARIES})
else()
  # Fall back to linking the conventional shared library names directly;
  # covers systems where the .pc file isn't installed but the dev
  # package is (e.g. plain `apt install librdkafka-dev`).
  target_link_libraries(akto_sdk PUBLIC rdkafka++ rdkafka)
endif()

target_compile_options(akto_sdk PRIVATE -Wall -Wextra)

# ---------------------------------------------------------------------------
# Example: echo server using the SDK as middleware
# ---------------------------------------------------------------------------
option(AKTO_BUILD_EXAMPLES "Build example echo server" ON)

if(AKTO_BUILD_EXAMPLES)
  add_executable(akto_echo_server examples/echo_server/main.cpp)
  target_include_directories(akto_echo_server PRIVATE third_party)
  target_link_libraries(akto_echo_server PRIVATE akto_sdk Threads::Threads)
endif()

# ---------------------------------------------------------------------------
# Install rules (for `make install` / packaging into other projects)
# ---------------------------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS akto_sdk
  EXPORT akto_sdk-targets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/akto DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT akto_sdk-targets
  FILE akto_sdk-targets.cmake
  NAMESPACE akto::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/akto_sdk
)
