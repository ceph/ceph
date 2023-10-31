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
  using MemberId = mds_rank_t;
  static const MemberId INVALID_MEMBER = MDS_RANK_NONE;

  epoch_t epoch = 0;
  fs_cluster_id_t fs_id;
  std::string fs_name;

  MemberId me = INVALID_MEMBER;
  MemberId leader = INVALID_MEMBER;
  std::set<MemberId> members;

  // A courier interface to decouple from the messaging layer
  // Failures can be ignored, manager will call this repeatedly if needed
  QuiesceInterface::DbPeerUpdate send_listing_to;
  QuiesceInterface::AgentAck send_ack;
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
    int update_membership(const QuiesceClusterMembership& new_membership) {

      std::unique_lock lock(submit_mutex);

      bool will_participate = new_membership.members.contains(new_membership.me);

      if (cluster_membership && !will_participate) {
        // stop the thread
        cluster_membership.reset();
        submit_condition.notify_all();
        lock.unlock();
        ceph_assert(quiesce_db_thread.is_started());
        quiesce_db_thread.join();
      } else if (!cluster_membership && will_participate) {
        // start the thread
        cluster_membership = new_membership;
        quiesce_db_thread.create("quiesce_db_mgr");
      }

      return 0;
    }

    // ============================
    // quiesce db leader interface: 
    //    -> EPERM unless this is the leader
    
    // client interface to the DB
    int submit_request(RequestContext* ctx) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership || cluster_membership->leader != cluster_membership->me) {
        return -EPERM;
      }

      pending_requests.push(ctx);
      submit_condition.notify_all();
      return 0;
    }
    // acks the messaging system
    int submit_ack_from(mds_rank_t sender, const QuiesceMap& diff_map) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership || cluster_membership->leader != cluster_membership->me) {
        return -EPERM;
      }

      if (!cluster_membership->members.contains(sender)) {
        return -ESTALE;
      }

      pending_acks.push({ sender, diff_map });
      submit_condition.notify_all();
      return 0;
    }
    
    // =============================
    // quiesce db replica interface:
    //    -> EPERM if this is the leader

    // process an incoming listing from a leader
    int submit_listing(const QuiesceDbListing& listing) {
      std::lock_guard l(submit_mutex);

      if (!cluster_membership || cluster_membership->leader == cluster_membership->me) {
        return -EPERM;
      }

      if (cluster_membership->epoch != listing.epoch) {
        return -ESTALE;
      }

      pending_db_update = listing;
      submit_condition.notify_all();
      return 0;
    }

    // =============================
    // Quiesce Agent interface:

    int submit_agent_ack(const QuiesceMap& diff_map)
    {
      std::unique_lock l(submit_mutex);
      if (!cluster_membership) {
        return -EPERM;
      }

      if (cluster_membership->leader == cluster_membership->me) {
        // local delivery
        pending_acks.push({ cluster_membership->me, diff_map });
        submit_condition.notify_all();
      } else {
        // send to the leader outside of the lock
        auto send_ack = cluster_membership->send_ack;
        l.unlock();
        send_ack(diff_map);
      }
      return 0;
    }

    struct AgentCallback {
      using Notify = std::function<bool(QuiesceMap&)>;
      Notify notify;
      QuiesceDbVersion if_newer = 0;

      AgentCallback(const Notify &notify, QuiesceDbVersion if_newer = 0)
          : notify(notify)
          , if_newer(if_newer)
      {
      }
    };

    std::optional<AgentCallback> reset_agent_callback(AgentCallback::Notify notify, QuiesceDbVersion if_newer = 0) {
      return reset_agent_callback(AgentCallback(notify, if_newer));
    }

    std::optional<AgentCallback> reset_agent_callback(std::optional<AgentCallback> callback_if_newer = std::nullopt)
    {
      std::lock_guard lc(agent_mutex);
      std::lock_guard ls(submit_mutex);
      agent_callback.swap(callback_if_newer);
      if (agent_callback) {
        submit_condition.notify_all();
      }
      return callback_if_newer;
    }

    std::optional<AgentCallback> reset_agent_callback(QuiesceDbVersion if_newer)
    {
      std::lock_guard lc(agent_mutex);
      std::lock_guard ls(submit_mutex);
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
    std::optional<QuiesceDbListing> pending_db_update;
    std::queue<std::pair<mds_rank_t, QuiesceMap>> pending_acks;
    std::queue<RequestContext*> pending_requests;

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
      QuiesceDbVersion version = 0;
      using Sets = std::unordered_map<QuiesceSetId, QuiesceSet>;
      Sets sets;

      QuiesceTimeInterval get_age() const { return QuiesceClock::now() - time_zero; }
      void reset() { version = 0; sets.clear(); }
    } db;

    QuiesceClusterMembership membership;

    struct PeerInfo {
        QuiesceMap diff_map;
        QuiesceTimeInterval at_age = QuiesceTimeInterval::zero();
        PeerInfo(QuiesceMap&& diff_map, QuiesceTimeInterval at_age)
            : diff_map(diff_map)
            , at_age(at_age)
        {
        }
        PeerInfo() { }
    };
    std::unordered_map<mds_rank_t, PeerInfo> peers;

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
    }

    void db_thread_exit() {
      complete_requests();
    }

    bool db_thread_has_work() const {
      return false
        || pending_acks.size() > 0
        || pending_requests.size() > 0
        || pending_db_update.has_value()
        || (agent_callback.has_value() && agent_callback->if_newer < db.version)
        || (!cluster_membership.has_value() || cluster_membership->epoch != membership.epoch)
      ;
    }

    bool membership_upkeep();
    QuiesceTimeInterval replica_upkeep(QuiesceDbListing& db_update);
    QuiesceTimeInterval leader_upkeep(decltype(pending_acks)& acks, decltype(pending_requests)& requests);

    void leader_record_ack(mds_rank_t from, QuiesceMap& diff_map);
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