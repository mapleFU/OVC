#include "ovc.h"
#include <algorithm>
#include <ranges>
#include <iostream>
#include <vector>

namespace ovc {

void OVC::GenerateSortedRun(
std::span<const std::string> inputs,
    std::span<std::string> outputs,
                           std::span<uint64_t> ovc) {
  std::cout << "Unimplemented: OVC::GenerateSortedRun" << std::endl;
}

void OVC::MergeRunsWithOvc(std::span<const MergeStream> streams,
                          std::span<std::string> outputs) {
  std::cout << "Unimplemented: OVC::MergeRunsWithOvc" << std::endl;
}

}
