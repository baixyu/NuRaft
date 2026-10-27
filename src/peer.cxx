/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

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

#include "peer.hxx"

#include "debugging_options.hxx"
#include "raft_server.hxx"
#include "tracer.hxx"

#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace nuraft {

bool peer::send_req( ptr<peer> myself,
                     ptr<req_msg>& req,
                     rpc_handler& handler,
                     bool streaming,
                     bool read_index_channel,
                     uint64_t send_timeout_ms,
                     bool read_index_timing )
{
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request",
             config_->get_id());
        return false;
    }

    if (req) {
        p_tr("send req %d -> %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string( req->get_type() ).c_str() );
    }

    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    size_t read_index_slot = 0;
    bool read_index_pool_all_slots_busy = false;
    {   std::lock_guard<std::mutex> l(rpc_protector_);
        if (read_index_channel) {
            read_index_pool_all_slots_busy =
                !rpc_read_index_pool_.empty() &&
                rpc_read_index_pool_.size() >= max_read_index_rpc_slots_;
            for (; read_index_slot < rpc_read_index_pool_.size();
                 ++read_index_slot) {
                auto &slot = rpc_read_index_pool_[read_index_slot];
                if (slot.busy) continue;
                read_index_pool_all_slots_busy = false;
                if ((!slot.rpc || slot.rpc->is_abandoned()) && rpc_factory_)
                    slot.rpc =
                        rpc_factory_->create_client(config_->get_endpoint());
                if (slot.rpc) {
                    slot.busy = true;
                    rpc_local = slot.rpc;
                    break;
                }
            }
            if (!rpc_local && rpc_read_index_pool_.size() <
                                  max_read_index_rpc_slots_ &&
                rpc_factory_) {
                read_index_rpc_slot slot;
                slot.rpc = rpc_factory_->create_client(config_->get_endpoint());
                slot.busy = static_cast<bool>(slot.rpc);
                if (slot.rpc) {
                    read_index_slot = rpc_read_index_pool_.size();
                    rpc_local = slot.rpc;
                    rpc_read_index_pool_.push_back(std::move(slot));
                }
            }
        } else {
            rpc_local = rpc_;
        }
        if (!rpc_local) {
            // Nothing will be sent, immediately free it to serve the next
            // operation. ReadIndex callers will fail this probe and can use
            // the existing bounded barrier fallback.
            if (read_index_pool_all_slots_busy)
                read_index_rpc_pool_saturations_.fetch_add(
                    1, std::memory_order_relaxed);
            p_tr("rpc local is null or ReadIndex RPC pool is full");
            if (!read_index_channel) set_free();
            return false;
        }
    }

    size_t req_size_bytes = 0;
    if (req->get_type() == append_entries_request) {
        for (auto& entry: req->log_entries()) {
            req_size_bytes += entry->get_buf_ptr()->size();
        }
    }

    // The flag is passed by raft_server and is normally false. Only requests
    // carrying a ReadIndex generation pay for the clock read.
    const bool capture_read_index_timing =
        read_index_timing && req &&
        req->get_read_index_probe_generation() != 0;
    const auto sent_at = capture_read_index_timing
                             ? std::chrono::steady_clock::now()
                             : std::chrono::steady_clock::time_point{};

    rpc_handler h = (rpc_handler)std::bind
                    ( &peer::handle_rpc_result,
                      this,
                      myself,
                      rpc_local,
                      read_index_slot,
                      req,
                      pending,
                      streaming,
                      read_index_channel,
                      req_size_bytes,
                      capture_read_index_timing,
                      sent_at,
                      std::placeholders::_1,
                      std::placeholders::_2 );
    if (rpc_local) {
        myself->bytes_in_flight_add(req_size_bytes);
        rpc_local->send(req, h, send_timeout_ms);
        return true;
    }
    return false;
}

// WARNING:
//   We should have the shared pointer of itself (`myself`)
//   and pointer to RPC client (`my_rpc_client`),
//   for the case when
//     1) this peer is removed before this callback function is invoked. OR
//     2) RPC client has been reset and re-connected.
void peer::handle_rpc_result( ptr<peer> myself,
                              ptr<rpc_client> my_rpc_client,
                              size_t read_index_slot,
                              ptr<req_msg>& req,
                              ptr<rpc_result>& pending_result,
                              bool streaming,
                              bool read_index_channel,
                              size_t req_size_bytes,
                              bool read_index_timing,
                              std::chrono::steady_clock::time_point sent_at,
                              ptr<resp_msg>& resp,
                              ptr<rpc_exception>& err )
{
    if (read_index_timing) {
        const uint64_t elapsed_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - sent_at)
                .count());
        const uint64_t generation =
            req ? req->get_read_index_probe_generation() : 0;
        const uint64_t term = req ? req->get_term() : 0;
        const bool accepted = resp && resp->get_accepted() &&
                              resp->get_term() == term;
        const uint64_t outcome = err != nilptr ? 3 : (accepted ? 1 : 2);
        record_read_index_rpc_result(generation, term, outcome, elapsed_us);
    }

    if (abandoned_) {
        p_in("peer %d has been shut down, ignore response.", config_->get_id());
        return;
    }

    if (req) {
        p_tr( "resp of req %d -> %d, type %s, %s",
              req->get_src(),
              req->get_dst(),
              msg_type_to_string( req->get_type() ).c_str(),
              (err) ? err->what() : "OK" );
    }

    if (err == nilptr) {
        // Succeeded.
        if (resp && req)
            resp->set_read_index_probe_generation(
                req->get_read_index_probe_generation());
        if (resp && req)
            resp->set_read_index_probe_only(req->is_read_index_probe_only());
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            // The same as below, freeing busy flag should be done
            // only if the RPC hasn't been changed.
            const ptr<rpc_client> current_rpc = read_index_channel
                ? (read_index_slot < rpc_read_index_pool_.size()
                       ? rpc_read_index_pool_[read_index_slot].rpc
                       : ptr<rpc_client>())
                : rpc_;
            uint64_t cur_rpc_id = current_rpc ? current_rpc->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id != given_rpc_id) {
                int32_t stale_resps = inc_stale_rpc_responses();
                int32_t limit = raft_server::get_raft_limits().response_limit_;
                if (stale_resps < limit) {
                    p_wn( "[EDGE CASE] got stale RPC response from %d: "
                          "current %p (%" PRIu64 "), from parameter %p (%" PRIu64 "). "
                          "will ignore this response",
                          config_->get_id(),
                          rpc_.get(),
                          cur_rpc_id,
                          my_rpc_client.get(),
                          given_rpc_id );
                } else if (stale_resps == limit) {
                    p_wn( "[EDGE CASE] too verbose stale RPC response from peer %d, "
                          "will suppress it from now", config_->get_id() );
                }

            } else {
                // WARNING:
                //   `set_free()` should be protected by `rpc_protector_`, otherwise
                //   it may free the peer even though new RPC client is already created.
                reset_stale_rpc_responses();
                bytes_in_flight_sub(req_size_bytes);
                if (read_index_channel &&
                    read_index_slot < rpc_read_index_pool_.size())
                    rpc_read_index_pool_[read_index_slot].busy = false;
                else
                    try_set_free(req->get_type(), streaming);
            }
        }

        if (!read_index_channel) {
            reset_active_timer();
            auto_lock(lock_);
            resume_hb_speed();
        }
        ptr<rpc_exception> no_except;
        resp->set_peer(myself);
        pending_result->set_result(resp, no_except);

        if (!read_index_channel) {
            reconn_backoff_.reset();
            reconn_backoff_.set_duration_ms(1);
        }

    } else {
        // Failed.

        // NOTE: Explicit failure is also treated as an activity
        //       of that connection.
        if (!read_index_channel) {
            reset_active_timer();
            auto_lock(lock_);
            slow_down_hb();
        }

        if (read_index_channel) {
            // Retire this socket before notifying the ReadIndex completion;
            // the callback may immediately submit another probe.
            {
                std::lock_guard<std::mutex> l(rpc_protector_);
                if (read_index_slot < rpc_read_index_pool_.size()) {
                    auto &slot = rpc_read_index_pool_[read_index_slot];
                    if (slot.rpc && my_rpc_client &&
                        slot.rpc->get_id() == my_rpc_client->get_id()) {
                        slot.rpc.reset();
                        slot.busy = false;
                    }
                }
            }
            ptr<resp_msg> no_resp;
            pending_result->set_result(no_resp, err);
            return;
        }
        ptr<resp_msg> no_resp;
        pending_result->set_result(no_resp, err);

        // Destroy this connection, we MUST NOT re-use existing socket.
        // Next append operation will create a new one.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            const ptr<rpc_client>& current_rpc = rpc_;
            uint64_t cur_rpc_id = current_rpc ? current_rpc->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id == given_rpc_id) {
                rpc_.reset();
                uint64_t last_streamed_log_idx = get_last_streamed_log_idx();
                reset_stream();
                if (last_streamed_log_idx) {
                    p_in("stop stream mode for peer %d at idx: %" PRIu64 "",
                         config_->get_id(), last_streamed_log_idx);
                }
                reset_bytes_in_flight();
                try_set_free(req->get_type(), streaming);
                reset_stale_rpc_responses();
            } else {
                // WARNING (MONSTOR-9378):
                //   RPC client has been reset before this request returns
                //   error. Those two are different instances and we
                //   SHOULD NOT reset the new one.

                // NOTE: In streaming mode, there can be lots of below errors
                //       at the same time. We should avoid verbose logs.

                int32_t stale_resps = inc_stale_rpc_responses();
                int32_t limit = raft_server::get_raft_limits().response_limit_;
                if (stale_resps < limit) {
                    p_wn( "[EDGE CASE] RPC for %d has been reset before "
                          "returning error: current %p (%" PRIu64
                          "), from parameter %p (%" PRIu64 ")",
                          config_->get_id(),
                          rpc_.get(),
                          cur_rpc_id,
                          my_rpc_client.get(),
                          given_rpc_id );
                } else if (stale_resps == limit) {
                    p_wn( "[EDGE CASE] too verbose stale RPC response from peer %d, "
                          "will suppress it from now", config_->get_id() );
                }
            }
        }
    }
}

void peer::record_read_index_rpc_result(uint64_t generation,
                                        uint64_t term,
                                        uint64_t outcome,
                                        uint64_t elapsed_us) {
    read_index_rpc_callbacks_.fetch_add(1, std::memory_order_relaxed);
    read_index_rpc_callback_generation_.store(generation,
                                               std::memory_order_relaxed);
    read_index_rpc_callback_term_.store(term, std::memory_order_relaxed);
    read_index_rpc_callback_outcome_.store(outcome,
                                           std::memory_order_relaxed);
    read_index_rpc_rtt_samples_.fetch_add(1, std::memory_order_relaxed);
    read_index_rpc_rtt_sum_us_.fetch_add(elapsed_us,
                                         std::memory_order_relaxed);
    read_index_rpc_rtt_last_us_.store(elapsed_us, std::memory_order_relaxed);
    auto update_max = [elapsed_us](std::atomic<uint64_t> &maximum) {
        uint64_t previous = maximum.load(std::memory_order_relaxed);
        while (elapsed_us > previous &&
               !maximum.compare_exchange_weak(previous, elapsed_us,
                                              std::memory_order_relaxed)) {
        }
    };
    update_max(read_index_rpc_rtt_max_us_);

    if (outcome == 1) {
        read_index_rpc_accepted_.fetch_add(1, std::memory_order_relaxed);
        read_index_rpc_accepted_rtt_samples_.fetch_add(
            1, std::memory_order_relaxed);
        read_index_rpc_accepted_rtt_sum_us_.fetch_add(
            elapsed_us, std::memory_order_relaxed);
        read_index_rpc_accepted_rtt_last_us_.store(
            elapsed_us, std::memory_order_relaxed);
        update_max(read_index_rpc_accepted_rtt_max_us_);
        static constexpr uint64_t limits[] = {
            100, 250, 500, 1000, 2000, 3000, 4000, 5000, 10000};
        size_t bucket = 0;
        while (bucket < 9 && elapsed_us > limits[bucket])
            ++bucket;
        read_index_rpc_accepted_rtt_buckets_[bucket].fetch_add(
            1, std::memory_order_relaxed);
    } else if (outcome == 2) {
        read_index_rpc_rejected_.fetch_add(1, std::memory_order_relaxed);
    } else {
        read_index_rpc_errors_.fetch_add(1, std::memory_order_relaxed);
    }
}

read_index_rpc_timing_snapshot peer::get_read_index_rpc_timing_snapshot()
    const {
    read_index_rpc_timing_snapshot snapshot;
    snapshot.pending_generation = pending_read_index_probe_generation_;
    snapshot.attempted_generation = last_read_index_probe_attempted_generation_;
    snapshot.callback_generation = read_index_rpc_callback_generation_.load(
        std::memory_order_relaxed);
    snapshot.callback_term = read_index_rpc_callback_term_.load(
        std::memory_order_relaxed);
    snapshot.callback_outcome = read_index_rpc_callback_outcome_.load(
        std::memory_order_relaxed);
    snapshot.callbacks = read_index_rpc_callbacks_.load(
        std::memory_order_relaxed);
    snapshot.accepted = read_index_rpc_accepted_.load(
        std::memory_order_relaxed);
    snapshot.rejected = read_index_rpc_rejected_.load(
        std::memory_order_relaxed);
    snapshot.errors = read_index_rpc_errors_.load(
        std::memory_order_relaxed);
    snapshot.dispatch_failures = read_index_dispatch_failures_.load(
        std::memory_order_relaxed);
    snapshot.rtt_samples = read_index_rpc_rtt_samples_.load(
        std::memory_order_relaxed);
    snapshot.rtt_sum_us = read_index_rpc_rtt_sum_us_.load(
        std::memory_order_relaxed);
    snapshot.rtt_last_us = read_index_rpc_rtt_last_us_.load(
        std::memory_order_relaxed);
    snapshot.rtt_max_us = read_index_rpc_rtt_max_us_.load(
        std::memory_order_relaxed);
    snapshot.accepted_rtt_samples = read_index_rpc_accepted_rtt_samples_.load(
        std::memory_order_relaxed);
    snapshot.accepted_rtt_sum_us = read_index_rpc_accepted_rtt_sum_us_.load(
        std::memory_order_relaxed);
    snapshot.accepted_rtt_last_us = read_index_rpc_accepted_rtt_last_us_.load(
        std::memory_order_relaxed);
    snapshot.accepted_rtt_max_us = read_index_rpc_accepted_rtt_max_us_.load(
        std::memory_order_relaxed);
    for (size_t i = 0; i < snapshot.accepted_rtt_buckets.size(); ++i)
        snapshot.accepted_rtt_buckets[i] =
            read_index_rpc_accepted_rtt_buckets_[i].load(
                std::memory_order_relaxed);
    return snapshot;
}

void peer::try_set_free(msg_type type, bool streaming) {
    const static std::unordered_set<int> msg_types_to_free( {
        // msg_type::append_entries_request,
        msg_type::install_snapshot_request,
        msg_type::request_vote_request,
        msg_type::pre_vote_request,
        msg_type::leave_cluster_request,
        msg_type::custom_notification_request,
        msg_type::reconnect_request,
        msg_type::priority_change_request
    } );

    if ( msg_types_to_free.find(type) !=
                msg_types_to_free.end() ) {
        set_free();
    }

    if (type == msg_type::append_entries_request && !streaming) {
        set_free();
    }
}

bool peer::recreate_rpc(ptr<srv_config>& config,
                        context& ctx)
{
    if (abandoned_) {
        p_tr("peer %d is abandoned", config->get_id());
        return false;
    }

    ptr<rpc_client_factory> factory = nullptr;
    {   std::lock_guard<std::mutex> l(ctx.ctx_lock_);
        factory = ctx.rpc_cli_factory_;
    }
    if (!factory) {
        p_tr("client factory is empty");
        return false;
    }

    std::lock_guard<std::mutex> l(rpc_protector_);

    bool backoff_timer_disabled =
        debugging_options::get_instance()
        .disable_reconn_backoff_.load(std::memory_order_relaxed);
    if (backoff_timer_disabled) {
        p_tr("reconnection back-off timer is disabled");
    }

    // To avoid too frequent reconnection attempt,
    // we use exponential backoff (x2) from 1 ms to heartbeat interval.
    if (backoff_timer_disabled || reconn_backoff_.timeout()) {
        reconn_backoff_.reset();
        size_t new_duration_ms = reconn_backoff_.get_duration_us() / 1000;
        new_duration_ms = std::min( hb_interval_, (int32)new_duration_ms * 2 );
        if (!new_duration_ms) new_duration_ms = 1;
        reconn_backoff_.set_duration_ms(new_duration_ms);

        rpc_ = factory->create_client(config->get_endpoint());
        rpc_read_index_pool_.clear();
        p_tr("%p reconnect peer %d", rpc_.get(), config_->get_id());

        // WARNING:
        //   A reconnection attempt should be treated as an activity,
        //   hence reset timer.
        reset_active_timer();

        reset_stream();
        reset_bytes_in_flight();
        set_free();
        set_manual_free();
        return true;

    } else {
        p_tr("skip reconnect this time");
    }
    return false;
}

void peer::shutdown() {
    // Should set the flag to block all incoming requests.
    abandoned_ = true;

    // Cut off all shared pointers related to ASIO and Raft server.
    scheduler_.reset();
    {   // To guarantee atomic reset
        // (race between send_req()).
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
        rpc_read_index_pool_.clear();
    }
    hb_task_.reset();
}

} // namespace nuraft;

