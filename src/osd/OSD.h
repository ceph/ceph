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

#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/LogClient.h"

#include "mon/MonMap.h"

#include "os/ObjectStore.h"
#include "PG.h"

#include "common/DecayCounter.h"

#include "include/LogEntry.h"

#include <map>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


class Messenger;
class Message;
class Logger;
class ObjectStore;
class OSDMap;
class MLog;

class OSD : public Dispatcher {

  /** OSD **/
protected:
  Mutex osd_lock;     // global lock
  SafeTimer timer;    // safe timer (osd_lock)

  Messenger   *messenger; 
  Logger      *logger;
  ObjectStore *store;
  MonMap      *monmap;

  LogClient   logclient;

  int whoami;
  const char *dev_name, *journal_name;

  class C_Tick : public Context {
    OSD *osd;
  public:
    C_Tick(OSD *o) : osd(o) {}
    void finish(int r) {
      osd->tick();
    }
  };

  void tick();

  void _dispatch(Message *m);

public:
  int get_nodeid() { return whoami; }
  
  static pobject_t get_osdmap_pobject_name(epoch_t epoch) { 
    return pobject_t(OSD_METADATA_PG_POOL, 0, object_t(0, epoch << 1)); 
  }
  static pobject_t get_inc_osdmap_pobject_name(epoch_t epoch) { 
    return pobject_t(OSD_METADATA_PG_POOL, 0, object_t(0, (epoch << 1) + 1)); 
  }
  

private:
  // -- superblock --
  OSDSuperblock superblock;
  epoch_t boot_epoch;      
  epoch_t last_active_epoch;

  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();


  // -- state --
public:
  static const int STATE_BOOTING = 1;
  static const int STATE_ACTIVE = 2;
  static const int STATE_STOPPING = 3;

private:
  int state;

public:
  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }

private:

  ThreadPool op_tp;
  ThreadPool recovery_tp;
  ThreadPool disk_tp;



  // -- heartbeat --
  Mutex heartbeat_lock;
  Cond heartbeat_cond;
  bool heartbeat_stop;
  epoch_t heartbeat_epoch;
  map<int, epoch_t> heartbeat_to, heartbeat_from;
  map<int, utime_t> heartbeat_from_stamp;
  map<int, entity_inst_t> heartbeat_inst;
  utime_t last_mon_heartbeat;
  Messenger *heartbeat_messenger;
  
  void update_heartbeat_peers();
  void heartbeat();
  void heartbeat_entry();

  struct T_Heartbeat : public Thread {
    OSD *osd;
    T_Heartbeat(OSD *o) : osd(o) {}
    void *entry() {
      osd->heartbeat_entry();
      return 0;
    }
  } heartbeat_thread;

public:
  bool heartbeat_dispatch(Message *m);

  struct HeartbeatDispatcher : public Dispatcher {
  private:
    bool dispatch_impl(Message *m) {
      return osd->heartbeat_dispatch(m);
    };
  public:
    OSD *osd;
    HeartbeatDispatcher(OSD *o) : osd(o) {}
  } heartbeat_dispatcher;


private:
  // -- stats --
  DecayCounter stat_oprate;
  int stat_ops;  // ops since last heartbeat
  int stat_rd_ops;
  int stat_rd_ops_shed_in;
  int stat_rd_ops_shed_out;
  int stat_qlen; // cumulative queue length since last refresh
  int stat_rd_ops_in_queue;  // in queue

  Mutex peer_stat_lock;
  osd_peer_stat_t my_stat;
  hash_map<int, osd_peer_stat_t, rjhash<uint32_t> > peer_stat;
  hash_map<int, osd_peer_stat_t, rjhash<uint32_t> > my_stat_on_peer;  // what the peer thinks of me

  void _refresh_my_stat(utime_t now);
  osd_peer_stat_t get_my_stat_for(utime_t now, int peer);
  void take_peer_stat(int peer, const osd_peer_stat_t& stat);
  
  // load calculation
  //current implementation is moving averges.
  class MovingAverager {
  private:
    Mutex lock;
    deque<double> m_Data;
    unsigned m_Size;
    double m_Total;
    
  public:
    MovingAverager(unsigned size) : lock("OSD::MovingAverager::lock"), m_Size(size), m_Total(0) { }

    void set_size(unsigned size) {
      m_Size = size;
    }

    void add(double value) {
      Mutex::Locker locker(lock);

      // add item
      m_Data.push_back(value);
      m_Total += value;

      // trim
      while (m_Data.size() > m_Size) {
	m_Total -= m_Data.front();
	m_Data.pop_front();
      }
    }
    
    double get_average() {
      Mutex::Locker locker(lock);
      if (m_Data.empty()) return -1;
      return m_Total / (double)m_Data.size();
    }
  } read_latency_calc, qlen_calc;

  class IATAverager {
  public:
    struct iat_data {
      double last_req_stamp;
      double average_iat;
      iat_data() : last_req_stamp(0), average_iat(0) {}
    };
  private:
    mutable Mutex lock;
    double alpha;
    hash_map<object_t, iat_data> iat_map;

  public:
    IATAverager(double a) : lock("IATAverager::lock"),alpha(a) {}
    
    void add_sample(object_t oid, double now) {
      Mutex::Locker locker(lock);
      iat_data &r = iat_map[oid];
      double iat = now - r.last_req_stamp;
      r.last_req_stamp = now;
      r.average_iat = r.average_iat*(1.0-alpha) + iat*alpha;
    }
    
    bool have(object_t oid) const {
      Mutex::Locker locker(lock);
      return iat_map.count(oid);
    }

    double get_average_iat(object_t oid) const {
      Mutex::Locker locker(lock);
      hash_map<object_t, iat_data>::const_iterator p = iat_map.find(oid);
      assert(p != iat_map.end());
      return p->second.average_iat;
    }

    bool is_flash_crowd_candidate(object_t oid) const {
      Mutex::Locker locker(lock);
      return get_average_iat(oid) <= g_conf.osd_flash_crowd_iat_threshold;
    }
  };

  IATAverager    iat_averager;
 

  // -- waiters --
  list<class Message*> finished;
  Mutex finished_lock;
  
  void take_waiters(list<class Message*>& ls) {
    finished_lock.Lock();
    finished.splice(finished.end(), ls);
    finished_lock.Unlock();
  }
  void push_waiters(list<class Message*>& ls) {
    finished_lock.Lock();
    finished.splice(finished.begin(), ls);
    finished_lock.Unlock();
  }
  void do_waiters();
  
  // -- op queue --
  deque<PG*> op_queue;
  
  struct OpWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    OpWQ(OSD *o, ThreadPool *tp) : ThreadPool::WorkQueue<PG>("OSD::OpWQ", tp), osd(o) {}

    bool _enqueue(PG *pg) {
      pg->get();
      osd->op_queue.push_back(pg);
      return true;
    }
    void _dequeue(PG *pg) {
      assert(0);
    }
    PG *_dequeue() {
      if (osd->op_queue.empty())
	return NULL;
      PG *pg = osd->op_queue.front();
      osd->op_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      osd->dequeue_op(pg);
    }
    void _clear() {
      assert(osd->op_queue.empty());
    }
  } op_wq;

  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;
  Cond  op_queue_cond;
  
  void wait_for_no_ops();
  void throttle_op_queue();
  void enqueue_op(PG *pg, Message *op);
  void dequeue_op(PG *pg);
  static void static_dequeueop(OSD *o, PG *pg) {
    o->dequeue_op(pg);
  };


  friend class PG;
  friend class ReplicatedPG;
  //friend class RAID4PG;


 protected:

  // -- osd map --
  OSDMap         *osdmap;
  utime_t         had_map_since;
  RWLock          map_lock;
  list<Message*>  waiting_for_osdmap;

  hash_map<entity_name_t, epoch_t>  peer_map_epoch;  // FIXME types

  bool _share_map_incoming(const entity_inst_t& inst, epoch_t epoch);
  void _share_map_outgoing(const entity_inst_t& inst);

  void wait_for_new_map(Message *m);
  void handle_osd_map(class MOSDMap *m);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  
  void advance_map(ObjectStore::Transaction& t, interval_set<snapid_t>& removed_snaps);
  void activate_map(ObjectStore::Transaction& t);

  // osd map cache (past osd maps)
  map<epoch_t,OSDMap*> map_cache;
  Mutex map_cache_lock;

  OSDMap* get_map(epoch_t e);
  void clear_map_cache();

  bool get_map_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map(epoch_t e, OSDMap::Incremental &inc);
  
  void send_incremental_map(epoch_t since, const entity_inst_t& inst, bool full, bool lazy=false);



  // -- placement groups --
  hash_map<pg_t, PG*> pg_map;
  hash_map<pg_t, list<Message*> > waiting_for_pg;

  bool  _have_pg(pg_t pgid);
  PG   *_lookup_lock_pg(pg_t pgid);
  PG   *_open_lock_pg(pg_t pg);  // create new PG (in memory)
  PG   *_create_lock_pg(pg_t pg, ObjectStore::Transaction& t); // create new PG
  PG   *_create_lock_new_pg(pg_t pgid, vector<int>& acting, ObjectStore::Transaction& t);
  void  _remove_unlock_pg(PG *pg);         // remove from store and memory

  void load_pgs();
  void calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset);
  void project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from,
			  vector<int>& last);

  void wake_pg_waiters(pg_t pgid) {
    if (waiting_for_pg.count(pgid)) {
      take_waiters(waiting_for_pg[pgid]);
      waiting_for_pg.erase(pgid);
    }
  }
  void wake_all_pg_waiters() {
    for (hash_map<pg_t, list<Message*> >::iterator p = waiting_for_pg.begin();
	 p != waiting_for_pg.end();
	 p++)
      take_waiters(p->second);
    waiting_for_pg.clear();
  }


  // -- pg creation --
  struct create_pg_info {
    epoch_t created;
    vector<int> acting;
    set<int> prior;
    pg_t parent;
    int split_bits;
  };
  hash_map<pg_t, create_pg_info> creating_pgs;
  map<pg_t, set<pg_t> > pg_split_ready;  // children ready to be split to, by parent

  PG *try_create_pg(pg_t pgid, ObjectStore::Transaction& t);
  void handle_pg_create(class MOSDPGCreate *m);

  void kick_pg_split_queue();
  void split_pg(PG *parent, map<pg_t,PG*>& children, ObjectStore::Transaction &t);


  // == monitor interaction ==
  utime_t last_mon_report;

  void do_mon_report();

  // -- boot --
  void send_boot();

  // -- alive --
  epoch_t up_thru_wanted;
  epoch_t up_thru_pending;

  void queue_want_up_thru(epoch_t want);
  void send_alive();

  // -- failures --
  set<int> failure_queue;
  set<int> failure_pending;

  void queue_failure(int n) {
    failure_queue.insert(n);
  }
  void send_failures();

  // -- pg stats --
  Mutex pg_stat_queue_lock;
  xlist<PG*> pg_stat_queue;
  bool osd_stat_updated;
  bool osd_stat_pending;

  void send_pg_stats();
  void handle_pg_stats_ack(class MPGStatsAck *ack);

  void pg_stat_queue_enqueue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->is_primary() && !pg->stat_queue_item.is_on_xlist()) {
      pg->get();
      pg_stat_queue.push_back(&pg->stat_queue_item);
    }
    osd_stat_updated = true;
    pg_stat_queue_lock.Unlock();
  }
  void pg_stat_queue_dequeue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->stat_queue_item.remove_myself())
      pg->put();
    pg_stat_queue_lock.Unlock();
  }
  void clear_pg_stat_queue() {
    pg_stat_queue_lock.Lock();
    while (!pg_stat_queue.empty()) {
      PG *pg = pg_stat_queue.front();
      pg_stat_queue.pop_front();
      pg->put();
    }
    pg_stat_queue_lock.Unlock();
  }


  // -- tids --
  // for ops i issue
  tid_t               last_tid;

  Mutex tid_lock;
  tid_t get_tid() {
    tid_t t;
    tid_lock.Lock();
    t = ++last_tid;
    tid_lock.Unlock();
    return t;
  }



  // -- generic pg peering --
  int num_pulling;

  void do_notifies(map< int, vector<PG::Info> >& notify_list);
  void do_queries(map< int, map<pg_t,PG::Query> >& query_map);
  void do_infos(map<int, MOSDPGInfo*>& info_map);
  void repeer(PG *pg, map< int, map<pg_t,PG::Query> >& query_map);

  bool require_current_map(Message *m, epoch_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_log(class MOSDPGLog *m);
  void handle_pg_info(class MOSDPGInfo *m);
  void handle_pg_remove(class MOSDPGRemove *m);

  // helper for handle_pg_log and handle_pg_info
  void _process_pg_info(epoch_t epoch, int from,
			PG::Info &info, 
			PG::Log &log, 
			PG::Missing &missing,
			map<int, MOSDPGInfo*>* info_map,
			int& created);

  // backlogs
  xlist<PG*> backlog_queue;

  struct BacklogWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    BacklogWQ(OSD *o, ThreadPool *tp) : ThreadPool::WorkQueue<PG>("OSD::BacklogWQ", tp), osd(o) {}

    bool _enqueue(PG *pg) {
      if (!pg->backlog_item.get_xlist()) {
	pg->get();
	osd->backlog_queue.push_back(&pg->backlog_item);
	return true;
      }
      return false;
    }
    void _dequeue(PG *pg) {
      if (pg->backlog_item.remove_myself())
	pg->put();
    }
    PG *_dequeue() {
      if (osd->backlog_queue.empty())
	return NULL;
      PG *pg = osd->backlog_queue.front();
      osd->backlog_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      osd->generate_backlog(pg);
    }
    void _clear() {
      while (!osd->backlog_queue.empty()) {
	PG *pg = osd->backlog_queue.front();
	osd->backlog_queue.pop_front();
	pg->put();
      }
    }
  } backlog_wq;

  void queue_generate_backlog(PG *pg);
  void cancel_generate_backlog(PG *pg);
  void generate_backlog(PG *pg);


  // -- pg recovery --
  xlist<PG*> recovery_queue;
  utime_t defer_recovery_until;
  int recovery_ops_active;

  struct RecoveryWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    RecoveryWQ(OSD *o, ThreadPool *tp) : ThreadPool::WorkQueue<PG>("OSD::RecoveryWQ", tp), osd(o) {}

    bool _enqueue(PG *pg) {
      if (!pg->recovery_item.get_xlist()) {
	pg->get();
	osd->recovery_queue.push_back(&pg->recovery_item);

	if (g_conf.osd_recovery_delay_start > 0) {
	  osd->defer_recovery_until = g_clock.now();
	  osd->defer_recovery_until += g_conf.osd_recovery_delay_start;
	}
	return true;
      }
      return false;
    }
    void _dequeue(PG *pg) {
      if (pg->recovery_item.remove_myself())
	pg->put();
    }
    PG *_dequeue() {
      if (osd->recovery_queue.empty())
	return NULL;
      
      if (!osd->_recover_now())
	return NULL;

      PG *pg = osd->recovery_queue.front();
      osd->recovery_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      osd->do_recovery(pg);
    }
    void _clear() {
      while (!osd->recovery_queue.empty()) {
	PG *pg = osd->recovery_queue.front();
	osd->recovery_queue.pop_front();
	pg->put();
      }
    }
  } recovery_wq;

  bool queue_for_recovery(PG *pg);
  void finish_recovery_op(PG *pg, int count, bool more);
  void defer_recovery(PG *pg);
  void do_recovery(PG *pg);
  bool _recover_now();

  Mutex remove_list_lock;
  map<epoch_t, map<int, vector<pg_t> > > remove_list;

  void queue_for_removal(int osd, pg_t pgid) {
    remove_list_lock.Lock();
    remove_list[osdmap->get_epoch()][osd].push_back(pgid);
    remove_list_lock.Unlock();
  }

  // replay / delayed pg activation
  Mutex replay_queue_lock;
  list< pair<pg_t, utime_t > > replay_queue;
  
  void check_replay_queue();
  void activate_pg(pg_t pgid, utime_t activate_at);


  // -- snap trimming --
  xlist<PG*> snap_trim_queue;
  
  struct SnapTrimWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    SnapTrimWQ(OSD *o, ThreadPool *tp) : ThreadPool::WorkQueue<PG>("OSD::SnapTrimWQ", tp), osd(o) {}

    bool _enqueue(PG *pg) {
      if (pg->snap_trim_item.is_on_xlist())
	return false;
      osd->snap_trim_queue.push_back(&pg->snap_trim_item);
      return true;
    }
    void _dequeue(PG *pg) {
      pg->snap_trim_item.remove_myself();
    }
    PG *_dequeue() {
      if (osd->snap_trim_queue.empty())
	return NULL;
      PG *pg = osd->snap_trim_queue.front();
      osd->snap_trim_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->snap_trimmer();
    }
    void _clear() {
      osd->snap_trim_queue.clear();
    }
  } snap_trim_wq;


  // -- scrubbing --
  xlist<PG*> scrub_queue;

  struct ScrubWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    ScrubWQ(OSD *o, ThreadPool *tp) : ThreadPool::WorkQueue<PG>("OSD::ScrubWQ", tp), osd(o) {}

    bool _enqueue(PG *pg) {
      if (pg->scrub_item.is_on_xlist())
	return false;
      pg->get();
      osd->scrub_queue.push_back(&pg->scrub_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_item.remove_myself())
	pg->put();
    }
    PG *_dequeue() {
      if (osd->scrub_queue.empty())
	return NULL;
      PG *pg = osd->scrub_queue.front();
      osd->scrub_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->scrub();
      pg->get();
    }
    void _clear() {
      while (!osd->scrub_queue.empty()) {
	PG *pg = osd->scrub_queue.front();
	osd->scrub_queue.pop_front();
	pg->put();
      }
    }
  } scrub_wq;

 private:
  virtual bool dispatch_impl(Message *m);
 public:
  OSD(int id, Messenger *m, Messenger *hbm, MonMap *mm, const char *dev = 0, const char *jdev = 0);
  ~OSD();

  // static bits
  static int find_osd_dev(char *result, int whoami);
  static ObjectStore *create_object_store(const char *dev, const char *jdev);
  static int mkfs(const char *dev, const char *jdev, ceph_fsid_t fsid, int whoami);
  static int peek_super(const char *dev, nstring& magic, ceph_fsid_t& fsid, int& whoami);

  // startup/shutdown
  int init();
  int shutdown();

  // messages
  virtual void ms_handle_failure(Message *m, const entity_inst_t& inst);

  void reply_op_error(MOSDOp *op, int r);

  void handle_scrub(class MOSDScrub *m);
  void handle_osd_ping(class MOSDPing *m);
  void handle_op(class MOSDOp *m);
  void handle_sub_op(class MOSDSubOp *m);
  void handle_sub_op_reply(class MOSDSubOpReply *m);

  void force_remount();

  LogClient *get_logclient() { return &logclient; }
};

#endif
