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
#include "common/ThreadPool.h"
#include "common/Timer.h"

#include "mon/MonMap.h"

#include "ObjectStore.h"
#include "PG.h"

#include "common/DecayCounter.h"


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

class OSD : public Dispatcher {
public:
  // -- states --
  static const int STATE_BOOTING = 1;
  static const int STATE_ACTIVE = 2;
  static const int STATE_STOPPING = 3;



  /** OSD **/
protected:
  Mutex osd_lock;     // global lock
  SafeTimer timer;    // safe timer

  Messenger   *messenger; 
  Logger      *logger;
  ObjectStore *store;
  MonMap      *monmap;

  int whoami;
  char dev_path[100];

public:
  int get_nodeid() { return whoami; }
  
  static object_t get_osdmap_object_name(epoch_t epoch) { return object_t(0,epoch << 1); }
  static object_t get_inc_osdmap_object_name(epoch_t epoch) { return object_t(0, (epoch << 1) + 1); }
  

private:
  /** superblock **/
  OSDSuperblock superblock;
  epoch_t  boot_epoch;      

  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();


  // -- state --
  int state;

public:
  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }

private:

  // -- heartbeat --
  set<int> heartbeat_to, heartbeat_from;
  map<int, utime_t> heartbeat_from_stamp;

  void update_heartbeat_sets();
  void heartbeat();

  class C_Heartbeat : public Context {
    OSD *osd;
  public:
    C_Heartbeat(OSD *o) : osd(o) {}
    void finish(int r) {
      osd->heartbeat();
    }
  };


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
    MovingAverager(unsigned size) : m_Size(size), m_Total(0) { }

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
    IATAverager(double a) : alpha(a) {}
    
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
  
  // -- op queue --
  class ThreadPool<class OSD*, PG*>   *threadpool;

  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;
  Cond  op_queue_cond;
  
  void wait_for_no_ops();

  void enqueue_op(PG *pg, Message *op);
  void dequeue_op(PG *pg);
  static void static_dequeueop(OSD *o, PG *pg) {
    o->dequeue_op(pg);
  };


  friend class PG;
  friend class ReplicatedPG;
  friend class RAID4PG;


 protected:

  // -- osd map --
  OSDMap         *osdmap;
  list<Message*>  waiting_for_osdmap;

  hash_map<entity_name_t, epoch_t>  peer_map_epoch;  // FIXME types
  bool _share_map_incoming(const entity_inst_t& inst, epoch_t epoch);
  void _share_map_outgoing(const entity_inst_t& inst);

  void wait_for_new_map(Message *m);
  void handle_osd_map(class MOSDMap *m);
  
  void advance_map(ObjectStore::Transaction& t);
  void activate_map(ObjectStore::Transaction& t);

  void get_map(epoch_t e, OSDMap &m);
  bool get_map_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map(epoch_t e, OSDMap::Incremental &inc);
  
  void send_incremental_map(epoch_t since, const entity_inst_t& inst, bool full);



  // -- placement groups --
  hash_map<pg_t, PG*> pg_map;
  hash_map<pg_t, list<Message*> > waiting_for_pg;

  bool  _have_pg(pg_t pgid);
  PG   *_lookup_lock_pg(pg_t pgid);
  PG   *_new_lock_pg(pg_t pg);  // create new PG (in memory)
  PG   *_create_lock_pg(pg_t pg, ObjectStore::Transaction& t); // create new PG
  void  _remove_unlock_pg(PG *pg);         // remove from store and memory

  void try_create_pg(pg_t pgid, ObjectStore::Transaction& t);

  void load_pgs();
  void project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from,
			  vector<int>& last);
  void activate_pg(pg_t pgid, epoch_t epoch);

  class C_Activate : public Context {
    OSD *osd;
    pg_t pgid;
    epoch_t epoch;
  public:
    C_Activate(OSD *o, pg_t p, epoch_t e) : osd(o), pgid(p), epoch(e) {}
    void finish(int r) {
      osd->activate_pg(pgid, epoch);
    }
  };


  // -- pg stats --
  Mutex pg_stat_queue_lock;
  set<pg_t> pg_stat_queue;
  bool osd_stat_updated;

  class C_Stats : public Context {
    OSD *osd;
  public:
    C_Stats(OSD *o) : osd(o) {}
    void finish(int r) { 
      osd->send_pg_stats(); 
    }
  };
  void send_pg_stats(); 


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


  // -- generic pg recovery --
  int num_pulling;

  void do_notifies(map< int, list<PG::Info> >& notify_list);
  void do_queries(map< int, map<pg_t,PG::Query> >& query_map);
  void do_activators(map<int, MOSDPGActivateSet*>& activator_map);
  void repeer(PG *pg, map< int, map<pg_t,PG::Query> >& query_map);

  bool require_current_map(Message *m, epoch_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_log(class MOSDPGLog *m);
  void handle_pg_activate_set(class MOSDPGActivateSet *m);
  void handle_pg_remove(class MOSDPGRemove *m);

  // helper for handle_pg_log and handle_pg_activate_set
  void _process_pg_info(epoch_t epoch, int from,
			PG::Info &info, 
			PG::Log &log, 
			PG::Missing &missing,
			map<int, MOSDPGActivateSet*>* activator_map);


 public:
  OSD(int id, Messenger *m, MonMap *mm, const char *dev = 0);
  ~OSD();

  // startup/shutdown
  int init();
  int shutdown();

  // messages
  virtual void dispatch(Message *m);
  virtual void ms_handle_failure(Message *m, const entity_inst_t& inst);

  void handle_osd_ping(class MOSDPing *m);
  void handle_op(class MOSDOp *m);
  void handle_sub_op(class MOSDSubOp *m);
  void handle_sub_op_reply(class MOSDSubOpReply *m);

  void force_remount();
};

#endif
