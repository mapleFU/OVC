#include <benchmark/benchmark.h>

#include <vector>
#include <algorithm>
#include <random>
#include <string>
#include <iostream>
#include <ranges>
#include <chrono>

#include "ovc.h"

namespace ovc {

// StlSortMerge implementation for comparison
class StlSortMerge {
public:
  static void GenerateSortedRun(std::span<const std::string> inputs,
          std::span<std::string> outputs,
                         std::span<uint64_t>) {
    std::ranges::copy(inputs, outputs.begin());
    // Then sort the outputs
    std::ranges::sort(outputs);
  }
  
  static void MergeRunsWithOvc(std::span<const MergeStream> streams,
                        std::span<std::string> outputs) {
    // Simple merge implementation using priority queue
    size_t idx = 0;
    for (const auto& stream : streams) {
      for (const auto& str : stream.strings) {
        outputs[idx] = str;
        ++idx;
      }
    }
    std::ranges::sort(outputs);
  }
};

} // namespace ovc

// Helper function to generate random strings
std::vector<std::string> GenerateRandomStrings(int count, int min_length = 5, int max_length = 20) {
  std::vector<std::string> strings;
  strings.reserve(count);
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> length_dist(min_length, max_length);
  std::uniform_int_distribution<> char_dist('a', 'z');
  
  for (int i = 0; i < count; ++i) {
    int length = length_dist(gen);
    std::string str;
    str.reserve(length);
    
    for (int j = 0; j < length; ++j) {
      str += static_cast<char>(char_dist(gen));
    }
    strings.push_back(std::move(str));
  }
  
  return strings;
}

template <typename Merger>
void RunGenerateSortedRunBenchmark(benchmark::State& state) {
  const int string_count = state.range(0);
  auto input_strings = GenerateRandomStrings(string_count);

  std::vector<std::string> output_strings(string_count);
  std::vector<uint64_t> ovc_data(string_count);

  for (auto _ : state) {
    Merger::GenerateSortedRun(
      std::span<const std::string>(input_strings),
      std::span<std::string>(output_strings),
      std::span<uint64_t>(ovc_data)
    );
    benchmark::DoNotOptimize(output_strings);
    benchmark::DoNotOptimize(ovc_data);
  }

  state.SetComplexityN(string_count);
  state.SetItemsProcessed(state.iterations() * string_count);
}

static void BM_OVC_GenerateSortedRun(benchmark::State& state) {
  RunGenerateSortedRunBenchmark<ovc::OVC>(state);
}

// Benchmark for StlSortMerge GenerateSortedRun
static void BM_StlSortMerge_GenerateSortedRun(benchmark::State& state) {
  RunGenerateSortedRunBenchmark<ovc::StlSortMerge>(state);
}

template <typename Merger>
static void RunMergeRunsWithOvcBenchmark(benchmark::State& state) {
  const int stream_count = 4;
  const int strings_per_stream = state.range(0) / stream_count;

  // Generate sorted streams
  std::vector<std::vector<std::string>> stream_data(stream_count);
  std::vector<std::vector<uint64_t>> ovc_data(stream_count);
  std::vector<ovc::MergeStream> streams(stream_count);

  for (int i = 0; i < stream_count; ++i) {
    stream_data[i] = GenerateRandomStrings(strings_per_stream);
    std::ranges::sort(stream_data[i]);
    ovc_data[i].resize(strings_per_stream);

    streams[i] = {
      std::span<const std::string>(stream_data[i]),
      std::span<uint64_t>(ovc_data[i])
    };
  }

  std::vector<std::string> output_strings(state.range(0));

  for (auto _ : state) {
    Merger::MergeRunsWithOvc(
      std::span<const ovc::MergeStream>(streams),
      std::span<std::string>(output_strings)
    );
    benchmark::DoNotOptimize(output_strings);
  }

  state.SetComplexityN(state.range(0));
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

// Benchmark for OVC MergeRunsWithOvc
static void BM_OVC_MergeRunsWithOvc(benchmark::State& state) {
  RunMergeRunsWithOvcBenchmark<ovc::OVC>(state);
}

// Benchmark for StlSortMerge MergeRunsWithOvc
static void BM_StlSortMerge_MergeRunsWithOvc(benchmark::State& state) {
  RunMergeRunsWithOvcBenchmark<ovc::StlSortMerge>(state);
}


// Register benchmarks with different input sizes
BENCHMARK(BM_OVC_GenerateSortedRun)
    ->Range(8, 8<<12);
BENCHMARK(BM_StlSortMerge_GenerateSortedRun)
    ->Range(8, 8<<12);
BENCHMARK(BM_OVC_MergeRunsWithOvc)
    ->Range(32, 32<<10);
BENCHMARK(BM_StlSortMerge_MergeRunsWithOvc)
    ->Range(32, 32<<10);

BENCHMARK_MAIN();