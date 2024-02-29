/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once
#include "mds/QuiesceDb.h"
#include "include/Context.h"
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <set>
#include <queue>

struct QuiesceClusterMembership {
  static const QuiesceInterface::PeerId INVALID_MEMBER;

  epoch_t epoch = 0;
  fs_cluster_id_t fs_id = FS_CLUSTER_ID_NONE;
  std::string fs_name;

  QuiesceInterface::PeerId me = INVALID_MEMBER;
  QuiesceInterface::PeerId leader = INVALID_MEMBER;
  std::unordered_set<QuiesceInterface::PeerId> members;

  // A courier interface to decouple from the messaging layer
  // Failures can be ignored, manager will call this repeatedly if needed
  QuiesceInterface::DbPeerUpdate send_listing_to;
  QuiesceInterface::AgentAck send_ack;

  bool is_leader() const { return leader == me && me != INVALID_MEMBER; }
};

class QuiesceDbManager {
  public:

    struct RequestContext : public Context {
      QuiesceDbRequest request;
      QuiesceDbListing response;
    };

    QuiesceDbManager() : quiesce_db_thread(this) {};
    virtual ~QuiesceDbManager()
    {
      update_membership({});
    }

    // This will reset the manager state
    // according to the new cluster config
    void update_membership(const QuiesceClusterMembership& new_membership) {
      update_membership(new_membership, nullptr);
    }
    void update_membership(const QuiesceClusterMembership& new_membership, RequestContext* inject_request);

    // ============================
    // quiesce db leader interface: 
    //    -> ENOTTY unless this is the leader
    
    // client interface to the DB
    int submit_request(RequestContext* ctx) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership || !cluster_membership->is_leader()) {
        return -ENOTTY;
      }

      pending_requests.push_back(ctx);
      submit_condition.notify_all();
      return 0;
    }
  
    // acks the messaging system
    int submit_peer_ack(QuiesceDbPeerAck&& ack) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership || !cluster_membership->is_leader()) {
        return -EPERM;
      }

      if (!cluster_membership->members.contains(ack.origin)) {
        return -ESTALE;
      }

      pending_acks.push(std::move(ack));
      submit_condition.notify_all();
      return 0;
    }
    
    // =============================
    // quiesce db replica interface:
    //    -> EPERM if this is the leader

    // process an incoming listing from a leader
    int submit_peer_listing(QuiesceDbPeerListing&& listing) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership) {
        return -EPERM;
      }

      if (cluster_membership->epoch != listing.db.db_version.epoch) {
        return -ESTALE;
      }

      pending_db_updates.push(std::move(listing));
      submit_condition.notify_all();
      return 0;
    }

    // =============================
    // Quiesce Agent interface:

    int submit_agent_ack(QuiesceMap&& diff_map)
    {
      std::unique_lock l(submit_mutex);
      if (!cluster_membership) {
        return -EPERM;
      }

      if (cluster_membership->leader == cluster_membership->me) {
        // local delivery
        pending_acks.push({ cluster_membership->me, std::move(diff_map) });
        submit_condition.notify_all();
      } else {
        // send to the leader outside of the lock
        auto send_ack = cluster_membership->send_ack;
        l.unlock();
        send_ack(std::move(diff_map));
      }
      return 0;
    }

    struct AgentCallback {
      using Notify = std::function<bool(QuiesceMap&)>;
      Notify notify;
      QuiesceDbVersion if_newer = {0, 0};

      AgentCallback(const Notify &notify, QuiesceDbVersion if_newer = {0, 0})
          : notify(notify)
          , if_newer(if_newer)
      {
      }
    };

    std::optional<AgentCallback> reset_agent_callback(AgentCallback::Notify notify, QuiesceDbVersion if_newer = {0, 0}) {
      return reset_agent_callback(AgentCallback(notify, if_newer));
    }

    std::optional<AgentCallback> reset_agent_callback(std::optional<AgentCallback> callback_if_newer = std::nullopt)
    {
      std::lock_guard ls(submit_mutex);
      std::lock_guard lc(agent_mutex);
      agent_callback.swap(callback_if_newer);
      if (agent_callback) {
        submit_condition.notify_all();
      }
      return callback_if_newer;
    }

    std::optional<AgentCallback> reset_agent_callback(QuiesceDbVersion if_newer)
    {
      std::lock_guard ls(submit_mutex);
      std::lock_guard lc(agent_mutex);
      if (agent_callback) {
        agent_callback->if_newer = if_newer;
        submit_condition.notify_all();
      }
      return agent_callback;
    }

    std::optional<AgentCallback> get_agent_callback() const
    {
      std::lock_guard lc(agent_mutex);
      return agent_callback;
    }

  protected:
    mutable std::mutex submit_mutex;
    mutable std::mutex agent_mutex;
    std::condition_variable submit_condition;

    std::optional<AgentCallback> agent_callback;
    std::optional<QuiesceClusterMembership> cluster_membership;
    std::queue<QuiesceDbPeerListing> pending_db_updates;
    std::queue<QuiesceDbPeerAck> pending_acks;
    std::deque<RequestContext*> pending_requests;

    class QuiesceDbThread : public Thread {
      public:
        explicit QuiesceDbThread(QuiesceDbManager* qm)
            : qm(qm)
        {
        }
        void* entry() override
        {
          return qm->quiesce_db_thread_main();
        }

      private:
        QuiesceDbManager* qm;
    } quiesce_db_thread;

    // =============================================
    // The below is managed by the quiesce db thread

    // the database.
    struct Db {
      QuiesceTimePoint time_zero;
      QuiesceSetVersion set_version = 0;
      using Sets = std::unordered_map<QuiesceSetId, QuiesceSet>;
      Sets sets;

      QuiesceTimeInterval get_age() const {
        return QuiesceClock::now() - time_zero;
      }
      void reset() { 
        set_version = 0; 
        sets.clear();
        time_zero = QuiesceClock::now();
      }
    } db;

    QuiesceDbVersion db_version() const { return {membership.epoch, db.set_version}; }

    QuiesceClusterMembership membership;

    struct PeerInfo {
        QuiesceMap diff_map;
        QuiesceTimePoint last_seen;
        PeerInfo(QuiesceMap&& diff_map, QuiesceTimePoint last_seen)
            : diff_map(diff_map)
            , last_seen(last_seen)
        {
        }
        PeerInfo() { }
    };
    std::unordered_map<QuiesceInterface::PeerId, PeerInfo> peers;

    struct AwaitContext {
      QuiesceTimeInterval expire_at_age = QuiesceTimeInterval::zero();
      RequestContext* req_ctx = nullptr;
      AwaitContext(QuiesceTimeInterval exp, RequestContext* r)
          : expire_at_age(exp)
          , req_ctx(r)
      {
      }
    };
    // multiple awaits may be active per set
    std::unordered_multimap<QuiesceSetId, AwaitContext> awaits;
    std::unordered_map<RequestContext*, int> done_requests;

    void* quiesce_db_thread_main();

    void db_thread_enter() {
      // this will invalidate the membership, see membership_upkeep()
      membership.epoch = 0;
      peers.clear();
      awaits.clear();
      done_requests.clear();
      db.reset();
    }

    void db_thread_exit() {
      complete_requests();
    }

    bool db_thread_has_work() const;

    bool membership_upkeep();

    QuiesceTimeInterval replica_upkeep(decltype(pending_db_updates)&& db_updates);
    bool leader_bootstrap(decltype(pending_db_updates)&& db_updates, QuiesceTimeInterval &next_event_at_age);
    QuiesceTimeInterval leader_upkeep(decltype(pending_acks)&& acks, decltype(pending_requests)&& requests);
    

    void leader_record_ack(QuiesceInterface::PeerId from, QuiesceMap&& diff_map);
    int leader_process_request(RequestContext* req_ctx);
    bool sanitize_roots(QuiesceDbRequest::Roots &roots);
    int leader_update_set(Db::Sets::value_type& set_it, const QuiesceDbRequest& req);
    QuiesceTimeInterval leader_upkeep_set(Db::Sets::value_type& set_it);
    QuiesceTimeInterval leader_upkeep_db();
    QuiesceTimeInterval leader_upkeep_awaits();

    size_t check_peer_reports(const QuiesceSetId& set_id, const QuiesceSet& set, const QuiesceRoot& root, const QuiesceSet::MemberInfo& member, QuiesceState& min_reported_state, QuiesceState& max_reported_state);

    void calculate_quiesce_map(QuiesceMap &map);

    void complete_requests();
};