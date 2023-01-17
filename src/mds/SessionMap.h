// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MDS_SESSIONMAP_H
#define CEPH_MDS_SESSIONMAP_H

#include <set>

#include "include/unordered_map.h"

#include "include/Context.h"
#include "include/xlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "mdstypes.h"
#include "mds/MDSAuthCaps.h"
#include "common/perf_counters.h"
#include "common/DecayCounter.h"

#include "CInode.h"
#include "Capability.h"
#include "MDSContext.h"
#include "msg/Message.h"

struct MDRequestImpl;

enum {
  l_mdssm_first = 5500,
  l_mdssm_session_count,
  l_mdssm_session_add,
  l_mdssm_session_remove,
  l_mdssm_session_open,
  l_mdssm_session_stale,
  l_mdssm_total_load,
  l_mdssm_avg_load,
  l_mdssm_avg_session_uptime,
  l_mdssm_metadata_threshold_sessions_evicted,
  l_mdssm_last,
};

class CInode;

/* 
 * session
 */

class Session : public RefCountedObject {
  // -- state etc --
public:
  /*
                    
        <deleted> <-- closed <------------+
             ^         |                  |
             |         v                  |
          killing <-- opening <----+      |
             ^         |           |      |
             |         v           |      |
           stale <--> open --> closing ---+

    + additional dimension of 'importing' (with counter)

  */

  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  enum {
    STATE_CLOSED = 0,
    STATE_OPENING = 1,   // journaling open
    STATE_OPEN = 2,
    STATE_CLOSING = 3,   // journaling close
    STATE_STALE = 4,
    STATE_KILLING = 5
  };

  Session() = delete;
  Session(ConnectionRef con) :
    item_session_list(this),
    requests(member_offset(MDRequestImpl, item_session_request)),
    recall_caps(g_conf().get_val<double>("mds_recall_warning_decay_rate")),
    release_caps(g_conf().get_val<double>("mds_recall_warning_decay_rate")),
    recall_caps_throttle(g_conf().get_val<double>("mds_recall_max_decay_rate")),
    recall_caps_throttle2o(0.5),
    session_cache_liveness(g_conf().get_val<double>("mds_session_cache_liveness_decay_rate")),
    cap_acquisition(g_conf().get_val<double>("mds_session_cap_acquisition_decay_rate")),
    birth_time(clock::now())
  {
    set_connection(std::move(con));
  }
  ~Session() override {
    ceph_assert(!item_session_list.is_on_list());
    preopen_out_queue.clear();
  }

  static std::string_view get_state_name(int s) {
    switch (s) {
    case STATE_CLOSED: return "closed";
    case STATE_OPENING: return "opening";
    case STATE_OPEN: return "open";
    case STATE_CLOSING: return "closing";
    case STATE_STALE: return "stale";
    case STATE_KILLING: return "killing";
    default: return "???";
    }
  }

  void dump(ceph::Formatter *f, bool cap_dump=false) const;
  void push_pv(version_t pv)
  {
    ceph_assert(projected.empty() || projected.back() != pv);
    projected.push_back(pv);
  }

  void pop_pv(version_t v)
  {
    ceph_assert(!projected.empty());
    ceph_assert(projected.front() == v);
    projected.pop_front();
  }

  int get_state() const { return state; }
  void set_state(int new_state)
  {
    if (state != new_state) {
      state = new_state;
      state_seq++;
    }
  }

  void set_reconnecting(bool s) { reconnecting = s; }

  void decode(ceph::buffer::list::const_iterator &p);
  template<typename T>
  void set_client_metadata(T&& meta)
  {
    info.client_metadata = std::forward<T>(meta);
    _update_human_name();
  }

  const std::string& get_human_name() const {return human_name;}

  size_t get_request_count() const;

  void notify_cap_release(size_t n_caps);
  uint64_t notify_recall_sent(size_t new_limit);
  auto get_recall_caps_throttle() const {
    return recall_caps_throttle.get();
  }
  auto get_recall_caps_throttle2o() const {
    return recall_caps_throttle2o.get();
  }
  auto get_recall_caps() const {
    return recall_caps.get();
  }
  auto get_release_caps() const {
    return release_caps.get();
  }
  auto get_session_cache_liveness() const {
    return session_cache_liveness.get();
  }
  auto get_cap_acquisition() const {
    return cap_acquisition.get();
  }

  inodeno_t take_ino(inodeno_t ino = 0) {
    if (ino) {
      if (!info.prealloc_inos.contains(ino))
        return 0;
      if (delegated_inos.contains(ino)) {
	delegated_inos.erase(ino);
      } else if (free_prealloc_inos.contains(ino)) {
	free_prealloc_inos.erase(ino);
      } else {
	ceph_assert(0);
      }
    } else if (!free_prealloc_inos.empty()) {
      ino = free_prealloc_inos.range_start();
      free_prealloc_inos.erase(ino);
    }
    return ino;
  }

  void delegate_inos(int want, interval_set<inodeno_t>& inos) {
    want -= (int)delegated_inos.size();
    if (want <= 0)
      return;

    for (auto it = free_prealloc_inos.begin(); it != free_prealloc_inos.end(); ) {
      if (want < (int)it.get_len()) {
	inos.insert(it.get_start(), (inodeno_t)want);
	delegated_inos.insert(it.get_start(), (inodeno_t)want);
	free_prealloc_inos.erase(it.get_start(), (inodeno_t)want);
	break;
      }
      want -= (int)it.get_len();
      inos.insert(it.get_start(), it.get_len());
      delegated_inos.insert(it.get_start(), it.get_len());
      free_prealloc_inos.erase(it++);
      if (want <= 0)
	break;
    }
  }

  // sans any delegated ones
  int get_num_prealloc_inos() const {
    return free_prealloc_inos.size();
  }

  int get_num_projected_prealloc_inos() const {
    return get_num_prealloc_inos() + pending_prealloc_inos.size();
  }

  client_t get_client() const {
    return info.get_client();
  }

  std::string_view get_state_name() const { return get_state_name(state); }
  uint64_t get_state_seq() const { return state_seq; }
  bool is_closed() const { return state == STATE_CLOSED; }
  bool is_opening() const { return state == STATE_OPENING; }
  bool is_open() const { return state == STATE_OPEN; }
  bool is_closing() const { return state == STATE_CLOSING; }
  bool is_stale() const { return state == STATE_STALE; }
  bool is_killing() const { return state == STATE_KILLING; }

  void inc_importing() {
    ++importing_count;
  }
  void dec_importing() {
    ceph_assert(importing_count > 0);
    --importing_count;
  }
  bool is_importing() const { return importing_count > 0; }

  void set_load_avg_decay_rate(double rate) {
    ceph_assert(is_open() || is_stale());
    load_avg = DecayCounter(rate);
  }
  uint64_t get_load_avg() const {
    return (uint64_t)load_avg.get();
  }
  void hit_session() {
    load_avg.adjust();
  }

  double get_session_uptime() const {
    std::chrono::duration<double> uptime = clock::now() - birth_time;
    return uptime.count();
  }

  time get_birth_time() const {
    return birth_time;
  }

  void inc_cap_gen() { ++cap_gen; }
  uint32_t get_cap_gen() const { return cap_gen; }

  version_t inc_push_seq() { return ++cap_push_seq; }
  version_t get_push_seq() const { return cap_push_seq; }

  version_t wait_for_flush(MDSContext* c) {
    waitfor_flush[get_push_seq()].push_back(c);
    return get_push_seq();
  }
  void finish_flush(version_t seq, MDSContext::vec& ls) {
    while (!waitfor_flush.empty()) {
      auto it = waitfor_flush.begin();
      if (it->first > seq)
	break;
      auto& v = it->second;
      ls.insert(ls.end(), v.begin(), v.end());
      waitfor_flush.erase(it);
    }
  }

  void touch_readdir_cap(uint32_t count) {
    cap_acquisition.hit(count);
  }

  void touch_cap(Capability *cap) {
    session_cache_liveness.hit(1.0);
    caps.push_front(&cap->item_session_caps);
  }

  void touch_cap_bottom(Capability *cap) {
    session_cache_liveness.hit(1.0);
    caps.push_back(&cap->item_session_caps);
  }

  void touch_lease(ClientLease *r) {
    session_cache_liveness.hit(1.0);
    leases.push_back(&r->item_session_lease);
  }

  bool is_any_flush_waiter() {
    return !waitfor_flush.empty();
  }

  void add_completed_request(ceph_tid_t t, inodeno_t created) {
    info.completed_requests[t] = created;
    completed_requests_dirty = true;
  }
  bool trim_completed_requests(ceph_tid_t mintid) {
    // trim
    bool erased_any = false;
    last_trim_completed_requests_tid = mintid;
    while (!info.completed_requests.empty() && 
	   (mintid == 0 || info.completed_requests.begin()->first < mintid)) {
      info.completed_requests.erase(info.completed_requests.begin());
      erased_any = true;
    }

    if (erased_any) {
      completed_requests_dirty = true;
    }
    return erased_any;
  }
  bool have_completed_request(ceph_tid_t tid, inodeno_t *pcreated) const {
    auto p = info.completed_requests.find(tid);
    if (p == info.completed_requests.end())
      return false;
    if (pcreated)
      *pcreated = p->second;
    return true;
  }

  void add_completed_flush(ceph_tid_t tid) {
    info.completed_flushes.insert(tid);
  }
  bool trim_completed_flushes(ceph_tid_t mintid) {
    bool erased_any = false;
    last_trim_completed_flushes_tid = mintid;
    while (!info.completed_flushes.empty() &&
	(mintid == 0 || *info.completed_flushes.begin() < mintid)) {
      info.completed_flushes.erase(info.completed_flushes.begin());
      erased_any = true;
    }
    if (erased_any) {
      completed_requests_dirty = true;
    }
    return erased_any;
  }
  bool have_completed_flush(ceph_tid_t tid) const {
    return info.completed_flushes.count(tid);
  }

  uint64_t get_num_caps() const {
    return caps.size();
  }

  unsigned get_num_completed_flushes() const { return info.completed_flushes.size(); }
  unsigned get_num_trim_flushes_warnings() const {
    return num_trim_flushes_warnings;
  }
  void inc_num_trim_flushes_warnings() { ++num_trim_flushes_warnings; }
  void reset_num_trim_flushes_warnings() { num_trim_flushes_warnings = 0; }

  unsigned get_num_completed_requests() const { return info.completed_requests.size(); }
  unsigned get_num_trim_requests_warnings() const {
    return num_trim_requests_warnings;
  }
  void inc_num_trim_requests_warnings() { ++num_trim_requests_warnings; }
  void reset_num_trim_requests_warnings() { num_trim_requests_warnings = 0; }

  bool has_dirty_completed_requests() const
  {
    return completed_requests_dirty;
  }

  void clear_dirty_completed_requests()
  {
    completed_requests_dirty = false;
  }

  int check_access(CInode *in, unsigned mask, int caller_uid, int caller_gid,
		   const std::vector<uint64_t> *gid_list, int new_uid, int new_gid);

  bool fs_name_capable(std::string_view fs_name, unsigned mask) const {
    return auth_caps.fs_name_capable(fs_name, mask);
  }

  void set_connection(ConnectionRef con) {
    connection = std::move(con);
    auto& c = connection;
    if (c) {
      info.auth_name = c->get_peer_entity_name();
      info.inst.addr = c->get_peer_socket_addr();
      info.inst.name = entity_name_t(c->get_peer_type(), c->get_peer_global_id());
    }
  }
  const ConnectionRef& get_connection() const {
    return connection;
  }

  void clear() {
    pending_prealloc_inos.clear();
    free_prealloc_inos.clear();
    delegated_inos.clear();
    info.clear_meta();

    cap_push_seq = 0;
    last_cap_renew = clock::zero();
  }

  Session *reclaiming_from = nullptr;
  session_info_t info;                         ///< durable bits
  MDSAuthCaps auth_caps;

  xlist<Session*>::item item_session_list;

  std::list<ceph::ref_t<Message>> preopen_out_queue;  ///< messages for client, queued before they connect

  /* This is mutable to allow get_request_count to be const. elist does not
   * support const iterators yet.
   */
  mutable elist<MDRequestImpl*> requests;

  interval_set<inodeno_t> pending_prealloc_inos; // journaling prealloc, will be added to prealloc_inos
  interval_set<inodeno_t> free_prealloc_inos; //
  interval_set<inodeno_t> delegated_inos; // hand these out to client

  xlist<Capability*> caps;     // inodes with caps; front=most recently used
  xlist<ClientLease*> leases;  // metadata leases to clients
  time last_cap_renew = clock::zero();
  time last_seen = clock::zero();

  // -- leases --
  uint32_t lease_seq = 0;

protected:
  ConnectionRef connection;

private:
  friend class SessionMap;

  // Human (friendly) name is soft state generated from client metadata
  void _update_human_name();

  int state = STATE_CLOSED;
  bool reconnecting = false;
  uint64_t state_seq = 0;
  int importing_count = 0;

  std::string human_name;

  // Versions in this session was projected: used to verify
  // that appropriate mark_dirty calls follow.
  std::deque<version_t> projected;

  // request load average for this session
  DecayCounter load_avg;

  // Ephemeral state for tracking progress of capability recalls
  // caps being recalled recently by this session; used for Beacon warnings
  DecayCounter recall_caps;  // caps that have been released
  DecayCounter release_caps;
  // throttle on caps recalled
  DecayCounter recall_caps_throttle;
  // second order throttle that prevents recalling too quickly
  DecayCounter recall_caps_throttle2o;
  // New limit in SESSION_RECALL
  uint32_t recall_limit = 0;

  // session caps liveness
  DecayCounter session_cache_liveness;

  // cap acquisition via readdir
  DecayCounter cap_acquisition;

  // session start time -- used to track average session time
  // note that this is initialized in the constructor rather
  // than at the time of adding a session to the sessionmap
  // as journal replay of sessionmap will not call add_session().
  time birth_time;

  // -- caps --
  uint32_t cap_gen = 0;
  version_t cap_push_seq = 0;        // cap push seq #
  std::map<version_t, MDSContext::vec > waitfor_flush; // flush session messages

  // Has completed_requests been modified since the last time we
  // wrote this session out?
  bool completed_requests_dirty = false;

  unsigned num_trim_flushes_warnings = 0;
  unsigned num_trim_requests_warnings = 0;

  ceph_tid_t last_trim_completed_requests_tid = 0;
  ceph_tid_t last_trim_completed_flushes_tid = 0;
};

class SessionFilter
{
public:
  SessionFilter() : reconnecting(false, false) {}

  bool match(
      const Session &session,
      std::function<bool(client_t)> is_reconnecting) const;
  int parse(const std::vector<std::string> &args, std::ostream *ss);
  void set_reconnecting(bool v)
  {
    reconnecting.first = true;
    reconnecting.second = v;
  }

  std::map<std::string, std::string> metadata;
  std::string auth_name;
  std::string state;
  int64_t id = 0;
protected:
  // First is whether to filter, second is filter value
  std::pair<bool, bool> reconnecting;
};

/*
 * session map
 */

class MDSRank;

/**
 * Encapsulate the serialized state associated with SessionMap.  Allows
 * encode/decode outside of live MDS instance.
 */
class SessionMapStore {
public:
  using clock = Session::clock;
  using time = Session::time;

  SessionMapStore(): total_load_avg(decay_rate) {}
  virtual ~SessionMapStore() {};

  version_t get_version() const {return version;}

  virtual void encode_header(ceph::buffer::list *header_bl);
  virtual void decode_header(ceph::buffer::list &header_bl);
  virtual void decode_values(std::map<std::string, ceph::buffer::list> &session_vals);
  virtual void decode_legacy(ceph::buffer::list::const_iterator& blp);
  void dump(ceph::Formatter *f) const;

  void set_rank(mds_rank_t r)
  {
    rank = r;
  }

  Session* get_or_add_session(const entity_inst_t& i) {
    Session *s;
    auto session_map_entry = session_map.find(i.name);
    if (session_map_entry != session_map.end()) {
      s = session_map_entry->second;
    } else {
      s = session_map[i.name] = new Session(ConnectionRef());
      s->info.inst = i;
      s->last_cap_renew = Session::clock::now();
      if (logger) {
        logger->set(l_mdssm_session_count, session_map.size());
        logger->inc(l_mdssm_session_add);
      }
    }

    return s;
  }

  static void generate_test_instances(std::list<SessionMapStore*>& ls);

  void reset_state()
  {
    session_map.clear();
  }

  mds_rank_t rank = MDS_RANK_NONE;

protected:
  version_t version = 0;
  ceph::unordered_map<entity_name_t, Session*> session_map;
  PerfCounters *logger =nullptr;

  // total request load avg
  double decay_rate = g_conf().get_val<double>("mds_request_load_average_decay_rate");
  DecayCounter total_load_avg;
};

class SessionMap : public SessionMapStore {
public:
  SessionMap() = delete;
  explicit SessionMap(MDSRank *m);

  ~SessionMap() override
  {
    for (auto p : by_state)
      delete p.second;

    if (logger) {
      g_ceph_context->get_perfcounters_collection()->remove(logger);
    }

    delete logger;
  }

  uint64_t set_state(Session *session, int state);
  void update_average_session_age();

  void register_perfcounters();

  void set_version(const version_t v)
  {
    version = projected = v;
  }

  void set_projected(const version_t v)
  {
    projected = v;
  }

  version_t get_projected() const
  {
    return projected;
  }

  version_t get_committed() const
  {
    return committed;
  }

  version_t get_committing() const
  {
    return committing;
  }

  // sessions
  void decode_legacy(ceph::buffer::list::const_iterator& blp) override;
  bool empty() const { return session_map.empty(); }
  const auto& get_sessions() const {
    return session_map;
  }

  bool is_any_state(int state) const {
    auto it = by_state.find(state);
    if (it == by_state.end() || it->second->empty())
      return false;
    return true;
  }

  bool have_unclosed_sessions() const {
    return
      is_any_state(Session::STATE_OPENING) ||
      is_any_state(Session::STATE_OPEN) ||
      is_any_state(Session::STATE_CLOSING) ||
      is_any_state(Session::STATE_STALE) ||
      is_any_state(Session::STATE_KILLING);
  }
  bool have_session(entity_name_t w) const {
    return session_map.count(w);
  }
  Session* get_session(entity_name_t w) {
    auto session_map_entry = session_map.find(w);
    return (session_map_entry != session_map.end() ?
	    session_map_entry-> second : nullptr);
  }
  const Session* get_session(entity_name_t w) const {
    ceph::unordered_map<entity_name_t, Session*>::const_iterator p = session_map.find(w);
    if (p == session_map.end()) {
      return NULL;
    } else {
      return p->second;
    }
  }

  void add_session(Session *s);
  void remove_session(Session *s);
  void touch_session(Session *session);

  Session *get_oldest_session(int state) {
    auto by_state_entry = by_state.find(state);
    if (by_state_entry == by_state.end() || by_state_entry->second->empty())
      return 0;
    return by_state_entry->second->front();
  }

  void dump();

  template<typename F>
  void get_client_sessions(F&& f) const {
    for (const auto& p : session_map) {
      auto& session = p.second;
      if (session->info.inst.name.is_client())
	f(session);
    }
  }
  template<typename C>
  void get_client_session_set(C& c) const {
    auto f = [&c](auto& s) {
      c.insert(s);
    };
    get_client_sessions(f);
  }

  // helpers
  entity_inst_t& get_inst(entity_name_t w) {
    ceph_assert(session_map.count(w));
    return session_map[w]->info.inst;
  }
  version_t get_push_seq(client_t client) {
    return get_session(entity_name_t::CLIENT(client.v))->get_push_seq();
  }
  bool have_completed_request(metareqid_t rid) {
    Session *session = get_session(rid.name);
    return session && session->have_completed_request(rid.tid, NULL);
  }
  void trim_completed_requests(entity_name_t c, ceph_tid_t tid) {
    Session *session = get_session(c);
    ceph_assert(session);
    session->trim_completed_requests(tid);
  }

  void wipe();
  void wipe_ino_prealloc();

  object_t get_object_name() const;

  void load(MDSContext *onload);
  void _load_finish(
      int operation_r,
      int header_r,
      int values_r,
      bool first,
      ceph::buffer::list &header_bl,
      std::map<std::string, ceph::buffer::list> &session_vals,
      bool more_session_vals);

  void load_legacy();
  void _load_legacy_finish(int r, ceph::buffer::list &bl);

  void save(MDSContext *onsave, version_t needv=0);
  void _save_finish(version_t v);

  /**
   * Advance the version, and mark this session
   * as dirty within the new version.
   *
   * Dirty means journalled but needing writeback
   * to the backing store.  Must have called
   * mark_projected previously for this session.
   */
  void mark_dirty(Session *session, bool may_save=true);

  /**
   * Advance the projected version, and mark this
   * session as projected within the new version
   *
   * Projected means the session is updated in memory
   * but we're waiting for the journal write of the update
   * to finish.  Must subsequently call mark_dirty
   * for sessions in the same global order as calls
   * to mark_projected.
   */
  version_t mark_projected(Session *session);

  /**
   * During replay, advance versions to account
   * for a session modification, and mark the
   * session dirty.
   */
  void replay_dirty_session(Session *session);

  /**
   * During replay, if a session no longer present
   * would have consumed a version, advance `version`
   * and `projected` to account for that.
   */
  void replay_advance_version();

  /**
   * During replay, open sessions, advance versions and
   * mark these sessions as dirty.
   */
  void replay_open_sessions(version_t event_cmapv,
			    std::map<client_t,entity_inst_t>& client_map,
			    std::map<client_t,client_metadata_t>& client_metadata_map);

  /**
   * For these session IDs, if a session exists with this ID, and it has
   * dirty completed_requests, then persist it immediately
   * (ahead of usual project/dirty versioned writes
   *  of the map).
   */
  void save_if_dirty(const std::set<entity_name_t> &tgt_sessions,
                     MDSGatherBuilder *gather_bld);

  void hit_session(Session *session);
  void handle_conf_change(const std::set <std::string> &changed);

  MDSRank *mds;
  std::map<int,xlist<Session*>*> by_state;
  std::map<version_t, MDSContext::vec> commit_waiters;

  // -- loading, saving --
  inodeno_t ino;
  MDSContext::vec waiting_for_load;

protected:
  void _mark_dirty(Session *session, bool may_save);

  version_t projected = 0, committing = 0, committed = 0;
  std::set<entity_name_t> dirty_sessions;
  std::set<entity_name_t> null_sessions;
  bool loaded_legacy = false;

private:
  uint64_t get_session_count_in_state(int state) {
    return !is_any_state(state) ? 0 : by_state[state]->size();
  }

  void update_average_birth_time(const Session &s, bool added=true) {
    uint32_t sessions = session_map.size();
    time birth_time = s.get_birth_time();

    if (sessions == 1) {
      avg_birth_time = added ? birth_time : clock::zero();
      return;
    }

    if (added) {
      avg_birth_time = clock::time_point(
        ((avg_birth_time - clock::zero()) / sessions) * (sessions - 1) +
        (birth_time - clock::zero()) / sessions);
    } else {
      avg_birth_time = clock::time_point(
        ((avg_birth_time - clock::zero()) / (sessions - 1)) * sessions -
        (birth_time - clock::zero()) / (sessions - 1));
    }
  }

  time avg_birth_time = clock::zero();

  size_t mds_session_metadata_threshold;

  bool validate_and_encode_session(MDSRank *mds, Session *session, bufferlist& bl);
  void apply_blocklist(const std::set<entity_name_t>& victims);
};

std::ostream& operator<<(std::ostream &out, const Session &s);
#endif
