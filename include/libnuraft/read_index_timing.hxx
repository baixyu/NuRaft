/************************************************************************
Modifications Copyright 2026 Percona LLC and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#ifndef _READ_INDEX_TIMING_HXX_
#define _READ_INDEX_TIMING_HXX_

#include <array>
#include <cstdint>

namespace nuraft {

/** Snapshot of the optional, ReadIndex-correlated RPC timing counters. */
struct read_index_rpc_timing_snapshot {
    std::uint64_t pending_generation{0};
    std::uint64_t attempted_generation{0};
    std::uint64_t callback_generation{0};
    std::uint64_t callback_term{0};
    std::uint64_t callback_outcome{0}; // 0=none, 1=accepted, 2=rejected, 3=RPC error
    std::uint64_t callbacks{0};
    std::uint64_t accepted{0};
    std::uint64_t rejected{0};
    std::uint64_t errors{0};
    std::uint64_t dispatch_failures{0};
    std::uint64_t rtt_samples{0};
    std::uint64_t rtt_sum_us{0};
    std::uint64_t rtt_last_us{0};
    std::uint64_t rtt_max_us{0};
    std::uint64_t accepted_rtt_samples{0};
    std::uint64_t accepted_rtt_sum_us{0};
    std::uint64_t accepted_rtt_last_us{0};
    std::uint64_t accepted_rtt_max_us{0};
    // Accepted RPC send-to-callback elapsed buckets: <=100, 250, 500, 1000,
    // 2000, 3000, 4000, 5000, 10000, and >10000 microseconds.
    std::array<std::uint64_t, 10> accepted_rtt_buckets{};
};

}  // namespace nuraft

#endif  // _READ_INDEX_TIMING_HXX_
