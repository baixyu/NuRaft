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

#include "handle_client_request.hxx"

#include "cluster_config.hxx"
#include "context.hxx"
#include "debugging_options.hxx"
#include "error_code.hxx"
#include "global_mgr.hxx"
#include "peer.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "tracer.hxx"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <sstream>

namespace nuraft {

ptr<resp_msg> raft_server::handle_cli_req_prelock(req_msg& req,
                                                  const req_ext_params& ext_params)
{
    ptr<resp_msg> resp = nullptr;
    ptr<raft_params> params = ctx_->get_params();
    uint64_t timestamp_us = timer_helper::get_timeofday_us();

    switch (params->locking_method_type_) {
        case raft_params::single_mutex: {
            recur_lock(lock_);
            resp = handle_cli_req(req, ext_params, timestamp_us);
            break;
        }
        case raft_params::dual_mutex:
        default: {
            // TODO: Use RW lock here.
            recur_lock(cli_lock_);
            resp = handle_cli_req(req, ext_params, timestamp_us);
            break;
        }
    }

    // Urgent commit, so that the commit will not depend on hb.
    request_append_entries_for_all();

    return resp;
}

void raft_server::request_append_entries_for_all() {
    ptr<raft_params> params = ctx_->get_params();
    if (params->use_bg_thread_for_urgent_commit_) {
        // Let background generate request (some delay may happen).
        global_mgr* mgr = get_global_mgr();
        if (mgr) {
            // Global thread pool exists, request it.
            p_tr("found global thread pool");
            mgr->request_append( this->shared_from_this() );
        } else {
            bg_append_ea_->invoke();
        }
    } else {
        // Directly generate request in user thread.
        recur_lock(lock_);
        request_append_entries();
    }
}

raft_server::read_index_probe_result raft_server::request_read_index_probe(
    uint64_t timeout_ms) {
    read_index_probe_result result;
    recur_lock(lock_);
    if (role_ != srv_role::leader || !initialized_ || stopping_) return result;

    const auto config = get_config();
    if (config == nullptr) return result;
    const auto self = config->get_server(id_);
    if (self == nullptr || self->is_learner() || self->is_new_joiner()) {
        result.status_ = read_index_probe_result::status::local_not_voter;
        return result;
    }

    // Capture the read index, local voting eligibility, and configuration
    // version under the same lock that constructs the AppendEntries probes.
    // Each probe request carries this read index or a newer committed prefix.
    result.term_ = state_->get_term();
    result.safe_read_index_ = quick_commit_index_.load();
    result.config_log_index_ = config->get_log_idx();

    uint64_t generation = ++next_read_index_probe_generation_;
    if (generation == 0) generation = ++next_read_index_probe_generation_;

    for (auto &entry : peers_) {
        ptr<peer> p = entry.second;
        {
            auto_lock(p->get_lock());
            if (!is_regular_member(p)) continue;
            p->set_pending_read_index_probe_generation(generation);
        }
        dispatch_pending_read_index_probe(p, timeout_ms);
    }

    // Drive the regular voters immediately with a separate empty AppendEntries
    // probe. It can overlap the ordinary replication RPC without changing the
    // peer's replication cursor.
    result.status_ = read_index_probe_result::status::started;
    result.generation_ = generation;
    return result;
}

ptr<req_msg> raft_server::create_read_index_probe_req(
    ptr<peer>& p, uint64_t generation) {
    if (role_ != srv_role::leader || generation == 0) return nullptr;

    const ulong first_retained_idx = log_store_->start_index();
    const ulong min_prev_idx = first_retained_idx == 0
                                   ? 0
                                   : first_retained_idx - 1;
    ulong prev_idx = std::max(p->get_matched_idx(), min_prev_idx);
    const ulong prev_term = term_for_log(prev_idx);
    ptr<req_msg> request = cs_new<req_msg>(
        state_->get_term(), msg_type::append_entries_request, id_, p->get_id(),
        prev_term, prev_idx, quick_commit_index_.load());
    request->set_read_index_probe_generation(generation);
    request->set_read_index_probe_only(true);
    return request;
}

void raft_server::dispatch_pending_read_index_probe(ptr<peer>& p,
                                                   uint64_t timeout_ms) {
    if (role_ != srv_role::leader) return;
    const uint64_t generation = p->begin_read_index_probe();
    if (generation == 0) return;

    ptr<req_msg> request = create_read_index_probe_req(p, generation);
    if (request == nullptr) {
        p->complete_read_index_probe(generation);
        return;
    }

    cb_func::Param request_param(id_, leader_, p->get_id(), request.get());
    if (ctx_->cb_func_.call(cb_func::RequestAppendEntries, &request_param) ==
        CbReturnCode::ReturnNull) {
        p->complete_read_index_probe(generation);
        return;
    }

    rpc_handler handler = [this, p, generation](
                              ptr<resp_msg>& response,
                              ptr<rpc_exception>& error) {
        handle_read_index_probe_resp(p, generation, response, error);
    };
    if (!p->send_req(p, request, handler, true, true,
                     std::max<std::uint64_t>(timeout_ms, 1))) {
        p->complete_read_index_probe(generation);
        return;
    }

    cb_func::Param sent_param(id_, leader_, p->get_id(), request.get());
    ctx_->cb_func_.call(cb_func::SentAppendEntriesReq, &sent_param);
}

void raft_server::handle_read_index_probe_resp(
    ptr<peer> p, uint64_t generation, ptr<resp_msg>& response,
    ptr<rpc_exception>& error) {
    if (response) {
        handle_peer_resp(response, error);
    } else if (error) {
        // A probe-channel failure must not mark the replication channel's
        // peer as lost. The read will fail quorum confirmation and use the
        // normal barrier fallback; a later probe recreates this socket.
        p_wn("ReadIndex probe RPC to peer %d failed: %s",
             p->get_id(), error->what());
    }

    recur_lock(lock_);
    auto it = peers_.find(p->get_id());
    if (it == peers_.end() || it->second != p) return;
    p->complete_read_index_probe(generation);
}

ptr<resp_msg> raft_server::handle_cli_req(req_msg& req,
                                          const req_ext_params& ext_params,
                                          uint64_t timestamp_us)
{
    ptr<resp_msg> resp = nullptr;
    ulong last_idx = 0;
    ptr<buffer> ret_value = nullptr;
    ulong resp_idx = 1;
    ulong cur_term = state_->get_term();
    ptr<raft_params> params = ctx_->get_params();

    resp = cs_new<resp_msg>( cur_term,
                             msg_type::append_entries_response,
                             id_,
                             leader_ );
    if (role_ != srv_role::leader || write_paused_) {
        resp->set_result_code( cmd_result_code::NOT_LEADER );
        return resp;
    }

    if (ext_params.expected_term_) {
        // If expected term is given, check the current term.
        if (ext_params.expected_term_ != cur_term) {
            resp->set_result_code( cmd_result_code::TERM_MISMATCH );
            return resp;
        }
    }

    std::vector< ptr<log_entry> >& entries = req.log_entries();
    size_t num_entries = entries.size();

    for (size_t i = 0; i < num_entries; ++i) {
        // force the log's term to current term
        entries.at(i)->set_term(cur_term);
        entries.at(i)->set_timestamp(timestamp_us);

        ulong next_slot = store_log_entry(entries.at(i));
        p_db("append at log_idx %" PRIu64 ", timestamp %" PRIu64,
             next_slot, timestamp_us);
        last_idx = next_slot;

        ptr<buffer> buf = entries.at(i)->get_buf_ptr();
        buf->pos(0);
        ret_value = state_machine_->pre_commit_ext
                    ( state_machine::ext_op_params( last_idx, buf ) );

        if (ext_params.after_precommit_) {
            req_ext_cb_params cb_params;
            cb_params.log_idx = last_idx;
            cb_params.log_term = cur_term;
            cb_params.context = ext_params.context_;
            ext_params.after_precommit_(cb_params);
        }
    }
    if (num_entries) {
        const auto append_batch_started = std::chrono::steady_clock::now();
        log_store_->end_of_append_batch(last_idx - num_entries + 1, num_entries);
        const auto append_batch_ended = std::chrono::steady_clock::now();
        cb_func::AppendBatchDurabilityArgs durability_args{
            last_idx - num_entries + 1, last_idx,
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                append_batch_ended - append_batch_started).count()),
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                append_batch_started.time_since_epoch()).count()),
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                append_batch_ended.time_since_epoch()).count())};
        cb_func::Param durability_param(id_, leader_);
        durability_param.ctx = &durability_args;
        ctx_->cb_func_.call(cb_func::LocalAppendBatchDurable, &durability_param);
    }
    try_update_precommit_index(last_idx);
    resp_idx = log_store_->next_slot();

    // Finished appending logs and pre_commit of itself.
    cb_func::Param param(id_, leader_);
    param.ctx = &last_idx;
    CbReturnCode rc = ctx_->cb_func_.call(cb_func::AppendLogs, &param);
    if (rc == CbReturnCode::ReturnNull) return nullptr;

    size_t sleep_us = debugging_options::get_instance()
                      .handle_cli_req_sleep_us_.load(std::memory_order_relaxed);
    if (sleep_us) {
        // Sleep if the debugging option is given.
        timer_helper::sleep_us(sleep_us);
    }

    if (!get_config()->is_async_replication()) {
        // Sync replication:
        //   Set callback function for `last_idx`.
        ptr<commit_ret_elem> elem = cs_new<commit_ret_elem>();
        elem->idx_ = last_idx;
        elem->result_code_ = cmd_result_code::TIMEOUT;

        {   auto_lock(commit_ret_elems_lock_);
            auto entry = commit_ret_elems_.find(last_idx);
            if (entry != commit_ret_elems_.end()) {
                // Commit thread was faster than this.
                elem = entry->second;
                p_tr("commit thread was faster than this thread: %p", elem.get());
            } else {
                commit_ret_elems_.insert( std::make_pair(last_idx, elem) );
            }

            switch (ctx_->get_params()->return_method_) {
            case raft_params::blocking:
            default:
                // Blocking call: set callback function waiting for the result.
                resp->set_cb( std::bind( &raft_server::handle_cli_req_callback,
                                         this,
                                         elem,
                                         std::placeholders::_1 ) );
                break;

            case raft_params::async_handler:
                // Async handler: create & set async result object.
                if (!elem->async_result_) {
                    elem->async_result_ = cs_new< cmd_result< ptr<buffer> > >();
                }
                resp->set_async_cb
                      ( std::bind( &raft_server::handle_cli_req_callback_async,
                                   this,
                                   elem->async_result_ ) );
                break;
            }
        }

    } else {
        // Async replication:
        //   Immediately return with the result of pre-commit.
        p_dv( "asynchronously replicated %" PRIu64 ", return value %p",
              last_idx, ret_value.get() );
        resp->set_ctx(ret_value);
    }

    resp->accept(resp_idx);
    return resp;
}

ptr<resp_msg> raft_server::handle_cli_req_callback(ptr<commit_ret_elem> elem,
                                                   ptr<resp_msg> resp) {
    p_dv("commit_ret_cv %" PRIu64 " %p sleep", elem->idx_, &elem->awaiter_);

    // Will wake up after timeout.
    elem->awaiter_.wait_ms(ctx_->get_params()->client_req_timeout_);

    uint64_t idx = 0;
    uint64_t elapsed_us = 0;
    ptr<buffer> ret_value = nullptr;
    {   auto_lock(commit_ret_elems_lock_);
        idx = elem->idx_;
        elapsed_us = elem->timer_.get_us();
        ret_value = elem->ret_value_;
        elem->callback_invoked_ = true;
        if (elem->result_code_ != cmd_result_code::TIMEOUT) {
            commit_ret_elems_.erase(elem->idx_);
        } else {
            p_dv("Client timeout leave commit thread to remove commit_ret_elem %" PRIu64,
                 idx);
        }
        p_dv("remaining elems in waiting queue: %zu", commit_ret_elems_.size());
    }

    if (elem->result_code_ == cmd_result_code::OK) {
        p_dv( "[OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), return value %p",
              idx, elapsed_us, ret_value.get() );
    } else {
        // Null `ret_value`, most likely timeout.
        p_wn( "[NOT OK] commit_ret_cv %" PRIu64 " wake up (%" PRIu64 " us), "
              "return value %p, result code %d",
              idx, elapsed_us, ret_value.get(), elem->result_code_ );
        bool valid_leader = check_leadership_validity();
        if (valid_leader) {
            p_in("leadership is still valid");
        } else {
            p_er("leadership is invalid");
        }
    }
    resp->set_ctx(ret_value);
    resp->set_result_code(elem->result_code_);

    return resp;
}

ptr< cmd_result< ptr<buffer> > >
    raft_server::handle_cli_req_callback_async(ptr< cmd_result< ptr<buffer> > > async_res)
{
    async_res->accept();
    return async_res;
}

void raft_server::drop_all_pending_commit_elems() {
    // Blocking mode:
    //   Invoke all awaiting requests to return `CANCELLED`.
    if (ctx_->get_params()->return_method_ == raft_params::blocking) {
        auto_lock(commit_ret_elems_lock_);
        ulong min_idx = std::numeric_limits<ulong>::max();
        ulong max_idx = 0;
        for (auto& entry: commit_ret_elems_) {
            ptr<commit_ret_elem>& elem = entry.second;
            elem->ret_value_ = nullptr;
            elem->result_code_ = cmd_result_code::CANCELLED;
            elem->awaiter_.invoke();
            if (min_idx > elem->idx_) {
                min_idx = elem->idx_;
            }
            if (max_idx < elem->idx_) {
                max_idx = elem->idx_;
            }
            p_db("cancelled blocking client request %" PRIu64 ", waited %" PRIu64 " us",
                 elem->idx_, elem->timer_.get_us());
        }
        if (!commit_ret_elems_.empty()) {
            p_wn("cancelled %zu blocking client requests from %" PRIu64
                 " to %" PRIu64 ".",
                 commit_ret_elems_.size(), min_idx, max_idx);
        }
        commit_ret_elems_.clear();
        return;
    }

    // Non-blocking mode:
    //   Set `CANCELLED` and set result & error.
    std::list< ptr<commit_ret_elem> > elems;

    {   auto_lock(commit_ret_elems_lock_);
        for (auto& entry: commit_ret_elems_) {
            ptr<commit_ret_elem>& ee = entry.second;
            elems.push_back(ee);
        }
        commit_ret_elems_.clear();
    }

    // Calling handler should be done outside the mutex.
    for (auto& entry: elems) {
        ptr<commit_ret_elem>&ee = entry;
        p_wn("cancelled non-blocking client request %" PRIu64, ee->idx_);

        ptr<buffer> result = nullptr;
        ptr<std::exception> err =
            cs_new<std::runtime_error>("Request cancelled.");
        ee->async_result_->set_result(result, err, cmd_result_code::CANCELLED);
    }
}

} // namespace nuraft;

