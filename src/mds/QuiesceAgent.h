/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License db_version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once
#include "mds/QuiesceDb.h"
#include <functional>
#include <optional>
#include <map>
#include <mutex>
#include <thread>

class QuiesceAgent {
  public:
    struct ControlInterface {
      QuiesceInterface::RequestSubmit submit_request;
      QuiesceInterface::RequestCancel cancel_request;
      QuiesceInterface::AgentAck agent_ack;
      // TODO: do we need a "cancel all"?
    };

    QuiesceAgent(const ControlInterface& quiesce_control)
        : quiesce_control(quiesce_control)
        , stop_agent_thread(false)
        , agent_thread(this) {
      agent_thread.create("quiesce.agt");
    };

    ~QuiesceAgent() {
      agent_thread.kill(SIGTERM);
    }

    void reset() {
      std::unique_lock l(agent_mutex);
      TrackedRoots current_roots;

      pending.clear();
      await_idle_locked(l);

      current.roots.swap(current.roots);
      current.db_version = 0;

      l.unlock();

      // do this outside of the lock
      current_roots.clear();
    }

    void shutdown()
    {
      std::unique_lock l(agent_mutex);
      stop_agent_thread = true;
      agent_cond.notify_all();
      l.unlock();
      agent_thread.join();

      current.clear();
      pending.clear();
    }

    bool db_update(QuiesceMap& map);

    struct TrackedRoot {
        std::optional<QuiesceInterface::RequestHandle> quiesce_request;
        // we could have hidden the request handle
        // inside the cancel functor, but then we'd lose
        // the ability to identify individual requests
        // when looking at the tracked root.
        QuiesceInterface::RequestCancel cancel; 
        std::optional<int> quiesce_result;
        std::optional<int> cancel_result;

        QuiesceDbVersion committed_version;
        QuiesceState committed_state;
        QuiesceTimePoint expires_at;

        TrackedRoot(QuiesceDbVersion db_version, QuiesceState state, QuiesceTimeInterval ttl)
            : committed_version(db_version)
            , committed_state(state)
            , expires_at(interval_saturate_add_now(ttl))
            , busy_lock(false)
        {
        }

        TrackedRoot() : TrackedRoot(0, QS__INVALID, QuiesceTimeInterval::zero()) {}

        bool should_quiesce() const
        {
          return committed_state == QS_QUIESCING || committed_state == QS_QUIESCED;
        }

        bool should_release() const {
          return committed_state == QS_RELEASING || committed_state == QS_RELEASED;
        }

        ~TrackedRoot();

        void update_committed(QuiesceDbVersion db_version, QuiesceMap::RootInfo const & info) {
          if (committed_version < db_version) {
            committed_version = db_version;
            committed_state = info.state;
            expires_at = interval_saturate_add_now(info.ttl);
          }
        }

        QuiesceTimeInterval get_ttl() const
        {
          auto now = QuiesceClock::now();
          if (expires_at.time_since_epoch() == QuiesceTimeInterval::max()) {
            return QuiesceTimeInterval::max();
          }
          if (expires_at > now) {
            return expires_at - now;
          } else {
            return QuiesceTimeInterval::zero();
          }
        }

        QuiesceState get_actual_state() const {
          QuiesceState result = QS_QUIESCING;
          bool did_quiesce = quiesce_result == 0;
          bool did_cancel = cancel_result == 0;
          if (did_quiesce) {
            if (cancel_result.has_value()) {
              result = did_cancel ? QS_RELEASED : QS_EXPIRED;
            } else {
              result = QS_QUIESCED;
            }
          } else {
            if (quiesce_result.has_value()) {
              result = QS_FAILED;
            } else if (should_release()) {
              // we must have lost track of this root,
              // probably, due to expiration. But even if due to an error,
              // this is our best guess for the situation
              result = QS_EXPIRED;
            }
          }
          return result;
        }

        void lock() const {
          while (busy_lock.test_and_set(std::memory_order_acquire))
            ; // spin
        }

        void unlock() const {
          busy_lock.clear(std::memory_order_release);
        }
      private:
        mutable std::atomic_flag busy_lock;
    };

    using TrackedRootRef = std::shared_ptr<TrackedRoot>;

    using TrackedRoots = std::unordered_map<QuiesceRoot, TrackedRootRef>;

    TrackedRoots tracked_roots() {
      std::lock_guard l(agent_mutex);
      return current.roots;
    }

    TrackedRootRef get_tracked_root(QuiesceRoot root) {
      std::lock_guard l(agent_mutex);
      if (auto it = current.roots.find(root); it != current.roots.end()) {
        return it->second;
      } else {
        return nullptr;
      }
    }

    QuiesceDbVersion get_current_version() {
      std::lock_guard l(agent_mutex);
      return current.db_version;
    }

  protected:
    ControlInterface quiesce_control;

    struct TrackedRootsVersion {
      TrackedRoots roots;
      QuiesceDbVersion db_version = 0;
      void clear() {
        db_version = 0;
        roots.clear();
      }
    };

    TrackedRootsVersion current;
    TrackedRootsVersion working;
    TrackedRootsVersion pending;

    std::mutex agent_mutex;
    std::condition_variable agent_cond;
    bool stop_agent_thread;
  
    template<class L>
    QuiesceDbVersion await_idle_locked(L &lock) {
      auto latest_v = std::max({ current.db_version, working.db_version, pending.db_version });

      agent_cond.wait(lock, [this, latest_v] {
        return current.db_version >= latest_v && working.db_version == 0 && pending.db_version == 0;
      });

      return latest_v;
    }

    bool set_pending_roots(QuiesceDbVersion db_version, TrackedRoots&& new_roots);

    bool set_upkeep_needed();

    class AgentThread : public Thread {
      public:
          explicit AgentThread(QuiesceAgent* qa)
              : qa(qa)
          {
          }
          void* entry() override
          {
            return qa->agent_thread_main();
          }

      private:
          QuiesceAgent* qa;
    } agent_thread;

    void* agent_thread_main();
};
