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
include(CMakePackageConfigHelpers)

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

# Generates akto_sdkConfig.cmake + a version file, so that after
# `make install`, any downstream project can simply do:
#   find_package(akto_sdk REQUIRED)
#   target_link_libraries(your_app PRIVATE akto::akto_sdk)
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/akto_sdkConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/akto_sdkConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/akto_sdk
)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/akto_sdkConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/akto_sdkConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/akto_sdkConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/akto_sdk
)

# ---------------------------------------------------------------------------
# CPack: `cpack` in the build dir produces a distributable .tar.gz and, on
# Debian/Ubuntu, a .deb -- a second, ready-made packaging path alongside the
# single-header drop-in (see single_header/) for teams that prefer installing
# a versioned artifact over vendoring a header.
# ---------------------------------------------------------------------------
set(CPACK_PACKAGE_NAME "akto-cpp-sdk")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_CONTACT "shivansh@akto.io")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Akto traffic-processing SDK for C++")
set(CPACK_GENERATOR "TGZ;DEB")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "librdkafka++1, nlohmann-json3-dev")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "shivansh@akto.io")
include(CPack)
