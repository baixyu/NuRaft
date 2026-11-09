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
    ulong matched_after_{0};
    ulong term_{0};
    bool regressed_{false};
    bool rpc_reconnect_reset_{false};
    uint64_t time_us_{0};
};

} // namespace nuraft

#endif // _MATCHED_IDX_UPDATE_HXX_
