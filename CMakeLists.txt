cmake_minimum_required(VERSION 3.24)

set(DFTRACER_AGGREGATE_VERSION_MAJOR 0)
set(DFTRACER_AGGREGATE_VERSION_MINOR 1)
set(DFTRACER_AGGREGATE_VERSION_PATCH 0)
set(DFTRACER_AGGREGATE_VERSION
    "${DFTRACER_AGGREGATE_VERSION_MAJOR}.${DFTRACER_AGGREGATE_VERSION_MINOR}.${DFTRACER_AGGREGATE_VERSION_PATCH}"
)

project(
  dftracer_aggregator
  VERSION ${DFTRACER_AGGREGATE_VERSION}
  LANGUAGES C CXX)

set(DFTRACER_AGGREGATE_PACKAGE ${PROJECT_NAME})
set(DFTRACER_AGGREGATE_PACKAGE_NAME ${PROJECT_NAME})
set(DFTRACER_AGGREGATE_PACKAGE_VERSION "${PROJECT_VERSION}")
set(DFTRACER_AGGREGATE_PACKAGE_VERSION_MAJOR
    "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
set(DFTRACER_AGGREGATE_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_PATCH}")
set(DFTRACER_AGGREGATE_PACKAGE_STRING
    "${DFTRACER_AGGREGATE_PACKAGE_NAME} ${DFTRACER_AGGREGATE_PACKAGE_VERSION}")
set(DFTRACER_AGGREGATE_PACKAGE_TARNAME "${DFTRACER_AGGREGATE_PACKAGE}")

# ##############################################################################
# Options
# ##############################################################################

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ##############################################################################
# Internal Paths for cmake libraries and Setup install and output Directories
# ##############################################################################

# This sets where to look for dependent libraries
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR}
                      ${CMAKE_INSTALL_PREFIX})
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/share/cmake)

include(CPM)
include(Dependencies)

need_dftracer_utils()
add_executable(dftracer_aggregator src/main.cpp)
target_link_directories(dftracer_aggregator PUBLIC dftracer_utils::shared)
