// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  
private:
  /** superblock **/
  OSDSuperblock superblock;
  epoch_t  boot_epoch;      

  object_t get_osdmap_object_name(epoch_t epoch) { return object_t(0,epoch << 1); }
  object_t get_inc_osdmap_object_name(epoch_t epoch) { return object_t(0, (epoch << 1) + 1); }
  
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

  // heartbeat
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
  int hb_stat_ops;  // ops since last heartbeat
  int hb_stat_qlen; // cumulative queue length since last hb

  hash_map<int, float> peer_qlen;
  

  // -- waiters --
  list<class Message*> finished;

  void take_waiters(list<class Message*>& ls) {
    finished.splice(finished.end(), ls);
  }
  
  // -- object locking --
  hash_map<object_t, list<Message*> > waiting_for_wr_unlock; 
  
  bool block_if_wrlocked(class MOSDOp* op);


  // -- op queue --
  class ThreadPool<class OSD*, pg_t>   *threadpool;
  hash_map<pg_t, list<Message*> >       op_queue;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;
  Cond  op_queue_cond;
  
  void wait_for_no_ops();

  void enqueue_op(pg_t pgid, Message *op);
  void dequeue_op(pg_t pgid);
  static void static_dequeueop(OSD *o, pg_t pgid) {
    o->dequeue_op(pgid);
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

  // per-pg locking (serializes AND acquired pg lock)
  hash_set<pg_t>               pg_lock;
  hash_map<pg_t, list<Cond*> > pg_lock_waiters;  
  
  PG   *_lock_pg(pg_t pgid);
  void  _unlock_pg(pg_t pgid);

  PG   *_create_lock_pg(pg_t pg, ObjectStore::Transaction& t);          // create new PG
  bool  _have_pg(pg_t pgid);
  void  _remove_unlock_pg(PG *pg);         // remove from store and memory

  void load_pgs();
  void project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from);
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
  void repeer(PG *pg, map< int, map<pg_t,PG::Query> >& query_map);

  bool require_current_map(Message *m, epoch_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_log(class MOSDPGLog *m);
  void handle_pg_remove(class MOSDPGRemove *m);


 public:
  OSD(int id, Messenger *m, MonMap *mm, char *dev = 0);
  ~OSD();

  // startup/shutdown
  int init();
  int shutdown();

  // messages
  virtual void dispatch(Message *m);
  virtual void ms_handle_failure(Message *m, const entity_inst_t& inst);

  void handle_osd_ping(class MOSDPing *m);
  void handle_op(class MOSDOp *m);
  void handle_op_reply(class MOSDOpReply *m);

  void force_remount();
};

#endif
