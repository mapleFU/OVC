cmake_minimum_required(VERSION 3.29)
project(ovc LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -fno-omit-frame-pointer")

# --- Google Benchmark --- #
# Add Google Benchmark from FetchContent
include(FetchContent)
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG    v1.14.0
)

FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG    v1.8.0
)

FetchContent_MakeAvailable(googletest)
FetchContent_MakeAvailable(benchmark)

# Add subdirectories
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(benchmarks)

# Enable testing
enable_testing()