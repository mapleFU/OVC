#pragma once

#include <cstdint>
#include <span>
#include <string>

namespace ovc {

struct MergeStream {
  std::span<const std::string> strings;
  std::span<uint64_t> ovc;
};

class OVC {
public:
  /// Sorts the input strings and generates output strings and ovc.
  static void GenerateSortedRun(std::span<const std::string> inputs,
                         std::span<std::string> outputs,
                         std::span<uint64_t> ovc);
  /// Merges multiple sorted runs into a single output string.
  ///
  /// FIXME(mwish): This can also generate OVCs for the merged output.
  static void MergeRunsWithOvc(std::span<const MergeStream> streams,
                        std::span<std::string> outputs);
};

} // namespace ovc
