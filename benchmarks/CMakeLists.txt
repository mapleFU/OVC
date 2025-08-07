# Benchmark executable
add_executable(
    ovc_benchmarks
    benchmark_ovc.cpp
)

# Link libraries
target_link_libraries(ovc_benchmarks
    PRIVATE
    ovc
    benchmark::benchmark
)
