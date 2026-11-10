/************************************************************************
Modifications Copyright 2026 Percona LLC and/or its affiliates.

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

#ifndef LIB_NURAFT_LOCK_TIMING_HXX
#define LIB_NURAFT_LOCK_TIMING_HXX

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace nuraft {

/**
 * Low-overhead cumulative contention snapshot for an instrumented mutex.
 * Thread IDs are OS TIDs on Linux/macOS and process-local hashes of
 * std::thread::id on other platforms. They are not stable across restarts.
 */
struct lock_timing_snapshot {
    uint64_t wait_calls{0};
    uint64_t wait_us{0};
    uint64_t wait_max_us{0};
    uint64_t last_waiter_thread{0};
    uint64_t max_waiter_thread{0};
    uint64_t hold_calls{0};
    uint64_t hold_us{0};
    uint64_t hold_max_us{0};
    uint64_t last_holder_thread{0};
    uint64_t max_holder_thread{0};
    uint64_t owner_thread{0};
    uint64_t owner_for_us{0};
};

namespace detail {

inline uint64_t lock_timing_now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

inline uint64_t lock_timing_elapsed_us(uint64_t start_ns, uint64_t end_ns) {
    return end_ns >= start_ns ? (end_ns - start_ns) / 1000 : 0;
}

inline uint64_t lock_timing_thread_id() {
    static thread_local const uint64_t value = [] {
        uint64_t thread_id = 0;
#if defined(__linux__)
        thread_id = static_cast<uint64_t>(::syscall(SYS_gettid));
#elif defined(__APPLE__)
        (void)pthread_threadid_np(nullptr, &thread_id);
#endif
        if (thread_id == 0) {
            thread_id = static_cast<uint64_t>(
                std::hash<std::thread::id>{}(std::this_thread::get_id()));
        }
        return thread_id == 0 ? uint64_t{1} : thread_id;
    }();
    return value;
}

class lock_timing_counters {
public:
    void record_wait(uint64_t elapsed_us, uint64_t thread_id) {
        wait_calls_.fetch_add(1, std::memory_order_relaxed);
        wait_us_.fetch_add(elapsed_us, std::memory_order_relaxed);
        last_waiter_thread_.store(thread_id, std::memory_order_relaxed);
        update_max(wait_max_us_, max_waiter_thread_, wait_max_update_lock_,
                   elapsed_us, thread_id);
    }

    void begin_hold(uint64_t thread_id, uint64_t started_ns) {
        owner_started_ns_.store(started_ns, std::memory_order_relaxed);
        owner_thread_.store(thread_id, std::memory_order_release);
    }

    uint64_t end_hold() {
        const uint64_t started_ns =
            owner_started_ns_.load(std::memory_order_relaxed);
        owner_thread_.store(0, std::memory_order_release);
        owner_started_ns_.store(0, std::memory_order_relaxed);
        return started_ns;
    }

    void record_hold(uint64_t elapsed_us, uint64_t thread_id) {
        hold_calls_.fetch_add(1, std::memory_order_relaxed);
        hold_us_.fetch_add(elapsed_us, std::memory_order_relaxed);
        last_holder_thread_.store(thread_id, std::memory_order_relaxed);
        update_max(hold_max_us_, max_holder_thread_, hold_max_update_lock_,
                   elapsed_us, thread_id);
    }

    lock_timing_snapshot snapshot() const {
        lock_timing_snapshot result;
        result.wait_calls = wait_calls_.load(std::memory_order_relaxed);
        result.wait_us = wait_us_.load(std::memory_order_relaxed);
        result.last_waiter_thread =
            last_waiter_thread_.load(std::memory_order_relaxed);
        read_max_pair(wait_max_us_, max_waiter_thread_, wait_max_update_lock_,
                      &result.wait_max_us, &result.max_waiter_thread);
        result.hold_calls = hold_calls_.load(std::memory_order_relaxed);
        result.hold_us = hold_us_.load(std::memory_order_relaxed);
        result.last_holder_thread =
            last_holder_thread_.load(std::memory_order_relaxed);
        read_max_pair(hold_max_us_, max_holder_thread_, hold_max_update_lock_,
                      &result.hold_max_us, &result.max_holder_thread);
        result.owner_thread = owner_thread_.load(std::memory_order_acquire);
        const uint64_t started_ns =
            owner_started_ns_.load(std::memory_order_relaxed);
        if (result.owner_thread != 0 && started_ns != 0)
            result.owner_for_us = lock_timing_elapsed_us(
                started_ns, lock_timing_now_ns());
        return result;
    }

private:
    static void update_max(std::atomic<uint64_t> &maximum,
                           std::atomic<uint64_t> &thread,
                           std::atomic_flag &writer, uint64_t value,
                           uint64_t thread_id) {
        if (value <= maximum.load(std::memory_order_relaxed)) return;
        while (writer.test_and_set(std::memory_order_acquire)) {
        }
        const uint64_t previous = maximum.load(std::memory_order_relaxed);
        if (value > previous) {
            thread.store(thread_id, std::memory_order_relaxed);
            maximum.store(value, std::memory_order_release);
        }
        writer.clear(std::memory_order_release);
    }

    static void read_max_pair(const std::atomic<uint64_t> &maximum,
                              const std::atomic<uint64_t> &thread,
                              std::atomic_flag &writer,
                              uint64_t *value, uint64_t *thread_id) {
        while (writer.test_and_set(std::memory_order_acquire)) {
        }
        *value = maximum.load(std::memory_order_relaxed);
        *thread_id = thread.load(std::memory_order_relaxed);
        writer.clear(std::memory_order_release);
    }

    std::atomic<uint64_t> wait_calls_{0};
    std::atomic<uint64_t> wait_us_{0};
    std::atomic<uint64_t> wait_max_us_{0};
    std::atomic<uint64_t> last_waiter_thread_{0};
    std::atomic<uint64_t> max_waiter_thread_{0};
    mutable std::atomic_flag wait_max_update_lock_ = ATOMIC_FLAG_INIT;
    std::atomic<uint64_t> hold_calls_{0};
    std::atomic<uint64_t> hold_us_{0};
    std::atomic<uint64_t> hold_max_us_{0};
    std::atomic<uint64_t> last_holder_thread_{0};
    std::atomic<uint64_t> max_holder_thread_{0};
    mutable std::atomic_flag hold_max_update_lock_ = ATOMIC_FLAG_INIT;
    std::atomic<uint64_t> owner_thread_{0};
    std::atomic<uint64_t> owner_started_ns_{0};
};

}  // namespace detail

/** std::mutex-compatible lock that records per-acquisition wait and hold data. */
class timed_mutex {
public:
    timed_mutex() = default;
    timed_mutex(const timed_mutex &) = delete;
    timed_mutex &operator=(const timed_mutex &) = delete;

    void lock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const uint64_t wait_started_ns = detail::lock_timing_now_ns();
        mutex_.lock();
        const uint64_t acquired_ns = detail::lock_timing_now_ns();
        counters_.record_wait(
            detail::lock_timing_elapsed_us(wait_started_ns, acquired_ns),
            thread_id);
        counters_.begin_hold(thread_id, acquired_ns);
    }

    bool try_lock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const uint64_t wait_started_ns = detail::lock_timing_now_ns();
        const bool acquired = mutex_.try_lock();
        const uint64_t now_ns = detail::lock_timing_now_ns();
        if (acquired) {
            counters_.record_wait(
                detail::lock_timing_elapsed_us(wait_started_ns, now_ns),
                thread_id);
            counters_.begin_hold(thread_id, now_ns);
        }
        return acquired;
    }

    void unlock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const uint64_t started_ns = counters_.end_hold();
        mutex_.unlock();
        counters_.record_hold(
            detail::lock_timing_elapsed_us(started_ns,
                                           detail::lock_timing_now_ns()),
            thread_id);
    }

    lock_timing_snapshot contention_snapshot() const {
        return counters_.snapshot();
    }

private:
    std::mutex mutex_;
    detail::lock_timing_counters counters_;
};

/** std::recursive_mutex-compatible lock; only outermost hold intervals count. */
class timed_recursive_mutex {
public:
    timed_recursive_mutex() = default;
    timed_recursive_mutex(const timed_recursive_mutex &) = delete;
    timed_recursive_mutex &operator=(const timed_recursive_mutex &) = delete;

    void lock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const uint64_t wait_started_ns = detail::lock_timing_now_ns();
        mutex_.lock();
        const uint64_t acquired_ns = detail::lock_timing_now_ns();
        counters_.record_wait(
            detail::lock_timing_elapsed_us(wait_started_ns, acquired_ns),
            thread_id);
        if (owner_thread_ == thread_id) {
            ++depth_;
        } else {
            owner_thread_ = thread_id;
            depth_ = 1;
            counters_.begin_hold(thread_id, acquired_ns);
        }
    }

    bool try_lock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const uint64_t wait_started_ns = detail::lock_timing_now_ns();
        const bool acquired = mutex_.try_lock();
        const uint64_t now_ns = detail::lock_timing_now_ns();
        if (acquired) {
            counters_.record_wait(
                detail::lock_timing_elapsed_us(wait_started_ns, now_ns),
                thread_id);
            if (owner_thread_ == thread_id) {
                ++depth_;
            } else {
                owner_thread_ = thread_id;
                depth_ = 1;
                counters_.begin_hold(thread_id, now_ns);
            }
        }
        return acquired;
    }

    void unlock() {
        const uint64_t thread_id = detail::lock_timing_thread_id();
        const bool outermost = --depth_ == 0;
        const uint64_t started_ns =
            outermost ? counters_.end_hold() : 0;
        if (outermost) owner_thread_ = 0;
        mutex_.unlock();
        if (outermost) {
            counters_.record_hold(
                detail::lock_timing_elapsed_us(started_ns,
                                               detail::lock_timing_now_ns()),
                thread_id);
        }
    }

    lock_timing_snapshot contention_snapshot() const {
        return counters_.snapshot();
    }

private:
    std::recursive_mutex mutex_;
    detail::lock_timing_counters counters_;
    uint64_t owner_thread_{0};
    uint64_t depth_{0};
};

}  // namespace nuraft

#endif  // LIB_NURAFT_LOCK_TIMING_HXX
