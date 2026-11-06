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
#include <atomic>
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

/** Cumulative timing snapshot used by peer-liveness diagnostics. */
struct timing_stats_snapshot {
    std::uint64_t samples{0};
    std::uint64_t total_us{0};
    std::uint64_t last_us{0};
    std::uint64_t max_us{0};
};

/** Lock-free accumulator for low-overhead diagnostics on Raft hot paths. */
class atomic_timing_stats {
public:
    void record(std::uint64_t elapsed_us) {
        samples_.fetch_add(1, std::memory_order_relaxed);
        total_us_.fetch_add(elapsed_us, std::memory_order_relaxed);
        last_us_.store(elapsed_us, std::memory_order_relaxed);
        std::uint64_t observed = max_us_.load(std::memory_order_relaxed);
        while (elapsed_us > observed &&
               !max_us_.compare_exchange_weak(
                   observed, elapsed_us, std::memory_order_relaxed,
                   std::memory_order_relaxed)) {}
    }

    timing_stats_snapshot snapshot() const {
        return {samples_.load(std::memory_order_relaxed),
                total_us_.load(std::memory_order_relaxed),
                last_us_.load(std::memory_order_relaxed),
                max_us_.load(std::memory_order_relaxed)};
    }

private:
    std::atomic<std::uint64_t> samples_{0};
    std::atomic<std::uint64_t> total_us_{0};
    std::atomic<std::uint64_t> last_us_{0};
    std::atomic<std::uint64_t> max_us_{0};
};

/** Per-peer counters that distinguish replication delay from liveness delay. */
struct peer_liveness_timing_snapshot {
    timing_stats_snapshot append_rpc_rtt;
    timing_stats_snapshot inbound_append_lock_wait;
    timing_stats_snapshot inbound_append_lock_hold;
    timing_stats_snapshot inbound_append_handler;
    timing_stats_snapshot inbound_append_log_work;
    timing_stats_snapshot inbound_append_durable_wait;
    timing_stats_snapshot inbound_append_commit;
    timing_stats_snapshot raft_response_lock_wait;
    timing_stats_snapshot heartbeat_timer_lock_wait;
    timing_stats_snapshot heartbeat_side_channel_rtt;
    timing_stats_snapshot heartbeat_side_channel_lock_wait;
    timing_stats_snapshot election_timer_lock_wait;
    timing_stats_snapshot liveness_lock_hold;
    std::uint64_t heartbeat_side_channel_sent{0};
    std::uint64_t heartbeat_side_channel_accepted{0};
    std::uint64_t heartbeat_side_channel_failed{0};
};

}  // namespace nuraft

#endif  // _READ_INDEX_TIMING_HXX_
