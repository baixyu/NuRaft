/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#ifndef _MATCHED_IDX_UPDATE_HXX_
#define _MATCHED_IDX_UPDATE_HXX_

#include "basic_types.hxx"

namespace nuraft {

/** One recent change to the leader's replication cursor for a peer. */
struct matched_idx_update {
    ulong matched_before_{0};
    ulong response_next_idx_{0};
    ulong response_matched_idx_{0};
    ulong matched_after_{0};
    ulong term_{0};
    bool regressed_{false};
    bool regression_ignored_{false};
    bool rpc_reconnect_reset_{false};
    uint64_t time_us_{0};
};

struct matched_idx_update_decision {
    ulong matched_after_{0};
    ulong next_log_idx_after_{0};
    bool regression_ignored_{false};
};

/**
 * A delayed accepted response may report an older prefix than another response
 * already processed in the same leader term. Preserve that term's known
 * high-water mark, while allowing a new term's first accepted response to
 * establish a fresh match position.
 */
inline matched_idx_update_decision decide_matched_idx_update(
    ulong matched_before,
    ulong next_log_idx_before,
    ulong last_ack_term,
    ulong response_next_idx,
    ulong current_term) {
    const ulong response_matched_idx =
        response_next_idx == 0 ? 0 : response_next_idx - 1;
    const bool same_term = last_ack_term == current_term;
    if (!same_term)
        return {response_matched_idx, response_next_idx, false};

    const bool regressed = response_matched_idx < matched_before;
    return {regressed ? matched_before : response_matched_idx,
            response_next_idx < next_log_idx_before ? next_log_idx_before
                                                    : response_next_idx,
            regressed};
}

} // namespace nuraft

#endif // _MATCHED_IDX_UPDATE_HXX_
