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

#include "raft_server.hxx"

#include "event_awaiter.hxx"
#include "peer.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <algorithm>
#include <cassert>
#include <sstream>

namespace nuraft {

void raft_server::enable_hb_for_peer(peer& p) {
    p.enable_hb(true);
    p.resume_hb_speed();
    p_tr("peer %d, interval: %d\n", p.get_id(), p.get_current_hb_interval());
    schedule_task(p.get_hb_task(), p.get_current_hb_interval());
}

void raft_server::check_srv_to_leave_timeout() {
    if (!srv_to_leave_) return;
    ulong last_resp_ms = srv_to_leave_->get_resp_timer_us() / 1000;
    if ( last_resp_ms >
             (ulong)raft_server::raft_limits_.leave_limit_ *
             ctx_->get_params()->heart_beat_interval_ ) {
        // Timeout: remove peer.
        p_wn("server to be removed %d, response timeout %" PRIu64 " ms. "
             "force remove now",
             srv_to_leave_->get_id(),
             last_resp_ms);
        remove_peer_from_peers(srv_to_leave_);
        reset_srv_to_leave();
    }
}

void raft_server::handle_hb_timeout(int32 srv_id) {
    const auto lock_wait_started = std::chrono::steady_clock::now();
    recur_lock(lock_);
    const auto lock_wait_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - lock_wait_started).count());
    ptr<peer> timing_peer;
    auto timing_peer_it = peers_.find(srv_id);
    if (timing_peer_it != peers_.end()) {
        timing_peer = timing_peer_it->second;
        timing_peer->record_heartbeat_timer_lock_wait(lock_wait_us);
    }
    peer_liveness_lock_hold_scope lock_hold_scope(timing_peer);

    check_srv_to_leave_timeout();

    if (write_paused_ && reelection_timer_.timeout()) {
        p_in("resign by timeout, %" PRIu64 " us elapsed, resign now",
             reelection_timer_.get_us());
        leader_ = -1;
        become_follower();

        // Clear this flag to avoid pre-vote rejection.
        hb_alive_ = false;
        return;
    }

    if ( srv_to_join_snp_retry_required_ &&
         srv_to_join_ &&
         srv_to_join_->get_id() == srv_id ) {
        p_in("retrying snapshot read for server %d", srv_id);
        if (srv_to_join_->need_to_reconnect()) {
            p_in("rpc client for %d needs reconnection", srv_id);

            ptr<raft_params> params = ctx_->get_params();
            uint64_t resp_timer_ms = srv_to_join_->get_resp_timer_us() / 1000;
            if ( resp_timer_ms >= (uint64_t)params->heart_beat_interval_ *
                                  raft_server::raft_limits_.response_limit_ ) {
                p_in("response timeout: %" PRIu64 " ms, will not retry", resp_timer_ms);
                clear_snapshot_sync_ctx(*srv_to_join_);
                return;
            }

            ptr<srv_config> s_config =
                srv_config::deserialize( *srv_to_join_->get_config().serialize() );
            bool succ = srv_to_join_->recreate_rpc(s_config, *ctx_);
            if (!succ) {
                // Reconnection failed.
                p_wn("reconnection failed, will not retry");
                clear_snapshot_sync_ctx(*srv_to_join_);
                return;
            }
        }
        sync_log_to_new_srv(0);
        return;
    }

    auto pit = peers_.find(srv_id);
    if (pit == peers_.end()) {
        p_er("heartbeat handler error: server %d not exist", srv_id);
        return;
    }

    // To avoid freeing this pointer in the middle of this function.
    ptr<peer> p = pit->second;

    if (p->is_leave_flag_set()) {
        // Leave request has been sent but not removed yet,
        // increase the counter.
        p->inc_hb_cnt_since_leave();
        int32 cur_cnt = p->get_hb_cnt_since_leave();
        p_in("peer %d is not responding for %d HBs since leave request",
             p->get_id(), cur_cnt);

        if (cur_cnt >= raft_server::raft_limits_.leave_limit_) {
            // Force remove the server.
            p_er("force remove peer %d", p->get_id());
            handle_join_leave_rpc_err(msg_type::leave_cluster_request, p);
            return;
        }
    }

    cb_func::Param param(id_, leader_, p->get_id());
    uint64_t last_log_idx = log_store_->next_slot() - 1;
    param.ctx = &last_log_idx;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::HeartBeat, &param);
    (void)rc;

    // Server is being shut down.
    if (stopping_) {
        p_wn("Triggered HB timer but server is shutting down");
        return;
    }

    if (!check_leadership_validity()) return;

    p_db("heartbeat timeout for %d", p->get_id());
    if (role_ == srv_role::leader) {
        update_target_priority();
        const bool replication_request_sent = request_append_entries(p);
        const ptr<raft_params> params = ctx_->get_params();
        const uint64_t heartbeat_interval_us = static_cast<uint64_t>(
            std::max(params->heart_beat_interval_, 1)) * 1000;
        if (!replication_request_sent && params->heartbeat_side_channel_ &&
            p->is_busy() && p->get_last_streamed_log_idx() == 0 &&
            p->get_ls_timer_us() >= heartbeat_interval_us) {
            // Streaming already allows a heartbeat to pass an earlier
            // AppendEntries. In non-streaming mode, keep liveness traffic on
            // its own socket when the replication request has occupied the
            // normal peer connection for at least one heartbeat interval.
            dispatch_heartbeat_side_channel(p);
        }
        {
            std::lock_guard<std::mutex> guard(p->get_lock());
            if (p->is_hb_enabled()) {
                // Schedule another heartbeat if heartbeat is still enabled
                schedule_task(p->get_hb_task(), p->get_current_hb_interval());
                p_tr("reschedule heartbeat for peer %d", p->get_id());
            } else {
                p_db("heartbeat is disabled for peer %d", p->get_id());
            }
        }
    } else {
        p_wn("Receive a heartbeat event for %d "
             "while no longer as a leader", p->get_id());
    }
}

void raft_server::dispatch_heartbeat_side_channel(ptr<peer> p) {
    if (!p || role_ != srv_role::leader || stopping_) return;

    const ptr<raft_params> params = ctx_->get_params();
    const ulong start_index = log_store_->start_index();
    const ulong min_prev_index = start_index == 0 ? 0 : start_index - 1;
    const ulong prev_index = std::max(p->get_matched_idx(), min_prev_index);
    const ulong prev_term = term_for_log(prev_index);
    ptr<req_msg> request = cs_new<req_msg>(
        state_->get_term(), msg_type::append_entries_request, id_, p->get_id(),
        prev_term, prev_index, quick_commit_index_.load());
    // This is process-local metadata: the follower receives an ordinary empty
    // AppendEntries request, while its response must not move replication
    // next/matched indexes on the leader.
    request->set_read_index_probe_only(true);

    if (p->mark_heartbeat_side_channel_used())
        p_in("Percona Raft heartbeat side channel first used for peer %d: "
             "replication RPC remained busy for at least %d ms",
             p->get_id(), params->heart_beat_interval_);

    rpc_handler handler = [this, p](ptr<resp_msg>& response,
                                    ptr<rpc_exception>& error) {
        handle_heartbeat_side_channel_resp(p, response, error);
    };
    const int32 response_limit = std::max(
        raft_server::get_raft_limits().response_limit_.load(), 1);
    const uint64_t timeout_ms = static_cast<uint64_t>(
        std::max(params->heart_beat_interval_, 1)) * response_limit;
    if (!p->send_req(p, request, handler, false, false, timeout_ms, false,
                     true)) {
        p->record_heartbeat_side_channel_failed();
        heartbeat_side_channel_failed_.fetch_add(1,
                                                std::memory_order_relaxed);
        if (p->mark_heartbeat_side_channel_failure_logged())
            p_wn("Percona Raft heartbeat side channel could not dispatch to "
                 "peer %d; the isolated heartbeat socket is busy or unavailable",
                 p->get_id());
        return;
    }
    cb_func::Param sent_param(id_, leader_, p->get_id(), request.get());
    ctx_->cb_func_.call(cb_func::SentAppendEntriesReq, &sent_param);
    heartbeat_side_channel_sent_.fetch_add(1, std::memory_order_relaxed);
}

void raft_server::handle_heartbeat_side_channel_resp(
    ptr<peer> p, ptr<resp_msg>& response, ptr<rpc_exception>& error) {
    if (error || !response) {
        heartbeat_side_channel_failed_.fetch_add(1,
                                                std::memory_order_relaxed);
        if (p && p->mark_heartbeat_side_channel_failure_logged())
            p_wn("Percona Raft heartbeat side channel to peer %d failed: %s",
                 p->get_id(), error ? error->what() : "empty response");
        // A failure on the isolated heartbeat socket must not mark the peer
        // lost or alter the main replication connection's busy state.
        return;
    }

    const auto lock_wait_started = std::chrono::steady_clock::now();
    recur_lock(lock_);
    const auto lock_wait_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - lock_wait_started).count());
    if (p) p->record_heartbeat_side_channel_lock_wait(lock_wait_us);
    peer_liveness_lock_hold_scope lock_hold_scope(p);
    const bool accepted_in_current_term =
        response->get_accepted() && response->get_term() == state_->get_term();
    if (!accepted_in_current_term) {
        if (p) p->record_heartbeat_side_channel_failed();
        heartbeat_side_channel_failed_.fetch_add(1,
                                                std::memory_order_relaxed);
        if (p && p->mark_heartbeat_side_channel_failure_logged())
            p_wn("Percona Raft heartbeat side channel response from peer %d "
                 "did not confirm current-term liveness: accepted=%d "
                 "response_term=%" PRIu64 " current_term=%" PRIu64
                 " next_idx=%" PRIu64,
                 p->get_id(), response->get_accepted() ? 1 : 0,
                 response->get_term(), state_->get_term(),
                 response->get_next_idx());
    } else if (p) {
        p->record_heartbeat_side_channel_accepted();
        heartbeat_side_channel_accepted_.fetch_add(
            1, std::memory_order_relaxed);
    }
    ptr<rpc_exception> no_error;
    handle_peer_resp_locked(response, no_error);
}

void raft_server::restart_election_timer(int32 source_peer_id,
                                         uint64_t* lock_wait_us_out) {
    // don't start the election timer while this server is still catching up the logs
    // or this server is the leader
    const auto lock_wait_started = std::chrono::steady_clock::now();
    recur_lock(lock_);
    const auto lock_wait_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - lock_wait_started).count());
    if (lock_wait_us_out != nullptr) *lock_wait_us_out = lock_wait_us;
    ptr<peer> source_peer;
    auto peer_it = peers_.find(source_peer_id);
    if (source_peer_id >= 0 && peer_it != peers_.end()) {
        source_peer = peer_it->second;
        source_peer->record_election_timer_lock_wait(lock_wait_us);
    }
    peer_liveness_lock_hold_scope lock_hold_scope(source_peer);
    if (state_->is_catching_up() || role_ == srv_role::leader) {
        return;
    }

    // If election timer was not allowed, clear the flag.
    if (!state_->is_election_timer_allowed()) {
        state_->allow_election_timer(true);
        ctx_->state_mgr_->save_state(*state_);
    }

    if (election_task_) {
        p_tr("cancel existing timer");
        cancel_task(election_task_);
    } else {
        election_task_ = cs_new< timer_task<void> >
                               ( election_exec_,
                                 timer_task_type::election_timer );
    }

    p_tr("re-schedule election timer");
    last_election_timer_reset_.reset();

    schedule_task(election_task_, rand_timeout_());
}

void raft_server::stop_election_timer() {
    if (!election_task_) {
        p_wn("Election Timer is never started but is "
             "requested to stop, protential a bug");
        return;
    }

    p_tr("stop election timer");
    cancel_task(election_task_);
}

void raft_server::handle_election_timeout() {
    p_tr("election timeout");
    recur_lock(lock_);
    if (stopping_) {
        p_wn("Triggered election timer but server is shutting down");
        return;
    }

    if (steps_to_down_ > 0) {
        if (--steps_to_down_ == 0) {
            p_in("no hearing further news from leader, "
                 "remove this server from cluster and step down");
            // Modified by Jung-Sang Ahn (Oct 25, 2017):
            // Should maintain the info of itself in the config,
            // for the next launch.
            /*
            for ( std::list<ptr<srv_config>>::iterator it =
                      config_->get_servers().begin();
                  it != config_->get_servers().end();
                  ++it ) {
                if ((*it)->get_id() == id_) {
                    config_->get_servers().erase(it);
                    ctx_->state_mgr_->save_config(*config_);
                    break;
                }
            }
            */
            state_->allow_election_timer(false);
            ctx_->state_mgr_->save_state(*state_);

            // Modified by Jung-Sang Ahn (Dec 24, 2019):
            // Same as in reconfigure().
            //reset_peer_info();
            cancel_schedulers();
            return;
        }

        p_in( "stepping down (cycles left: %d), "
              "skip this election timeout event",
              steps_to_down_ );
        restart_election_timer();
        return;
    }

    if (state_->is_catching_up()) {
        // this is a new server for the cluster, will not send out vote req
        // until conf that includes this srv is committed
        p_in("election timeout while joining the cluster, ignore it.");
        restart_election_timer();
        return;
    }

    if (out_of_log_range_) {
        p_wn("Triggered election timer but server is out of log range");
        return;
    }

    if (receiving_snapshot_ && et_cnt_receiving_snapshot_ < 20) {
        // If this node is receiving snapshot,
        // ignore election timeout 20 times.
        et_cnt_receiving_snapshot_.fetch_add(1);
        p_wn("election timeout while receiving snapshot, count %" PRIu64 ", "
             "ignore it.", et_cnt_receiving_snapshot_.load());
        restart_election_timer();
        return;
    }

    int time_ms = last_election_timer_reset_.get_us() / 1000;
    if ( serving_req_ ||
         time_ms < ctx_->get_params()->election_timeout_lower_bound_ ) {
        // Handling appending entries is now taking long time,
        // so that server keeps skipping sending heartbeat.
        // It doesn't mean server is gone. Just ignore.
        p_in("election timeout while serving append entries, ignore it.");
        restart_election_timer();
        return;
    }

    if (role_ == srv_role::leader) {
        p_er( "A leader should never encounter election timeout, "
              "illegal application state, ignore it.");
        return;
    }

    // Only voting member can suggest vote.
    if (!im_learner_) {
        p_wn("Election timeout, initiate leader election");
        if (!hb_alive_) {
            // Not the first election timeout, decay the target priority.
            decay_target_priority();
        }

        ulong last_log_term = 0;
        if (log_store_ && log_store_->last_entry()) {
            last_log_term = log_store_->last_entry()->get_term();
        }

        ulong state_term = state_->get_term();

        p_in( "[ELECTION TIMEOUT] current role: %s, log last term %" PRIu64 ", "
              "state term %" PRIu64 ", target p %d, my p %d, %s, %s",
              srv_role_to_string(role_).c_str(), last_log_term, state_term,
              target_priority_, my_priority_,
              (hb_alive_) ? "hb alive" : "hb dead",
              (pre_vote_.done_) ? "pre-vote done" : "pre-vote NOT done");

        // `term` changed, cannot use previous pre-vote result.
        if (pre_vote_.term_ != state_term) {
            p_in("pre-vote term (%" PRIu64 ") is different, reset it to %" PRIu64 "",
                 pre_vote_.term_, state_term);
            pre_vote_.reset(state_term);
        }

        if ( !peers_.size() ||
             pre_vote_.done_ ||
             get_quorum_for_election() == 0 ) {
            initiate_vote();
        } else {
            request_prevote();
        }

    }

    // restart the election timer if this is not yet a leader
    if (role_ != srv_role::leader) {
        restart_election_timer();
    }
}

void raft_server::cancel_schedulers() {
    if (!scheduler_) {
        // Already cancelled.
        return;
    }

    if (election_task_) {
        cancel_task(election_task_);
    }
    if (read_index_warmup_task_) {
        cancel_task(read_index_warmup_task_);
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        const ptr<peer>& p = it->second;
        if (p->get_hb_task()) {
            cancel_task(p->get_hb_task());
        }
        // Shutdown peer to cut off smart pointers.
        p->shutdown();

        // Free user context of snapshot if exists.
        clear_snapshot_sync_ctx(*p);
    }
    scheduler_.reset();
}

void raft_server::schedule_task(ptr<delayed_task>& task, int32 milliseconds) {
    if (stopping_) return;

    if (!scheduler_) {
        std::lock_guard<std::mutex> l(ctx_->ctx_lock_);
        scheduler_ = ctx_->scheduler_;
    }
    if (scheduler_) {
        scheduler_->schedule(task, milliseconds);
    }
}

void raft_server::cancel_task(ptr<delayed_task>& task) {
    if (!scheduler_) return;
    scheduler_->cancel(task);
}

}// namespace nuraft;

