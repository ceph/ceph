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

#include "messages/MOSDOp.h"
#include "messages/MOSDUpdate.h"
#include "messages/MOSDUpdateReply.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;
#include "crypto/CapCache.h"
#include "crypto/CapGroup.h"
#include "crypto/MerkleTree.h"

class Messenger;
class Message;
  

class OSD : public Dispatcher {
public:

  /** superblock
   */
  OSDSuperblock superblock;
  epoch_t  boot_epoch;

  object_t get_osdmap_object_name(epoch_t epoch) { return object_t(0,epoch << 1); }
  object_t get_inc_osdmap_object_name(epoch_t epoch) { return object_t(0, (epoch << 1) + 1); }
  
  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();


  /** OSD **/
 protected:
  Messenger *messenger;
  int whoami;

  // public/private key
  esignPriv myPrivKey;
  esignPub myPubKey;

  // capability cache
  CapCache *cap_cache;  

  static const int STATE_BOOTING = 1;
  static const int STATE_ACTIVE = 2;
  static const int STATE_STOPPING = 3;

  int state;

  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }


  MonMap *monmap;

  class Logger      *logger;

  // local store
  char dev_path[100];
  class ObjectStore *store;

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

  // global lock
  Mutex osd_lock;
  SafeTimer timer;

  // -- stats --
  int hb_stat_ops;  // ops since last heartbeat
  int hb_stat_qlen; // cumulative queue length since last hb

  hash_map<int, float> peer_qlen;

  // user group cache
  map<hash_t, CapGroup> user_groups;
  map<hash_t, list<Message*> >update_waiter_op;
  void handle_osd_update_reply(MOSDUpdateReply *m);
  // requests that are had their hashs updated
  map<reqid_t, utime_t> outstanding_updates;
  
  // per-pg locking (serializing)
  hash_set<pg_t>               pg_lock;
  hash_map<pg_t, list<Cond*> > pg_lock_waiters;  
  PG *lock_pg(pg_t pgid);
  PG *_lock_pg(pg_t pgid);
  void unlock_pg(pg_t pgid);
  void _unlock_pg(pg_t pgid);

  // finished waiting messages, that will go at tail of dispatch()
  list<class Message*> finished;
  void take_waiters(list<class Message*>& ls) {
    finished.splice(finished.end(), ls);
  }
  
  // object locking
  hash_map<object_t, list<Message*> > waiting_for_wr_unlock; /** list of operations for each object waiting for 'wrunlock' */

  bool block_if_wrlocked(MOSDOp* op);

  // -- ops --
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

  void do_op(Message *m, PG *pg);  // actually do it

  void prepare_log_transaction(ObjectStore::Transaction& t, MOSDOp* op, eversion_t& version, 
			       objectrev_t crev, objectrev_t rev, PG *pg, eversion_t trim_to);
  void prepare_op_transaction(ObjectStore::Transaction& t, MOSDOp* op, eversion_t& version, 
			      objectrev_t crev, objectrev_t rev, PG *pg);
  
  bool waitfor_missing_object(MOSDOp *op, PG *pg);
  bool pick_missing_object_rev(object_t& oid, PG *pg);
  bool pick_object_rev(object_t& oid);


  
 friend class PG;

 protected:

  // -- osd map --
  class OSDMap  *osdmap;
  list<class Message*> waiting_for_osdmap;

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



  // -- replication --

  // PG
  hash_map<pg_t, PG*>      pg_map;
  void  load_pgs();
  bool  pg_exists(pg_t pg);
  PG   *create_pg(pg_t pg, ObjectStore::Transaction& t);          // create new PG
  PG   *get_pg(pg_t pg);             // return existing PG, or null
  void  _remove_pg(pg_t pg);         // remove from store and memory

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


  tid_t               last_tid;
  int                 num_pulling;

  hash_map<pg_t, list<Message*> >        waiting_for_pg;

  // replica ops
  void get_repop_gather(PG::RepOpGather*);
  void apply_repop(PG *pg, PG::RepOpGather *repop);
  void put_repop_gather(PG *pg, PG::RepOpGather*);
  void issue_repop(PG *pg, MOSDOp *op, int osd);
  PG::RepOpGather *new_repop_gather(PG *pg, MOSDOp *op);
  void repop_ack(PG *pg, PG::RepOpGather *repop,
                 int result, bool commit,
                 int fromosd, eversion_t pg_complete_thru=0);
  
  void handle_rep_op_ack(MOSDOpReply *m);

  // recovery
  void do_notifies(map< int, list<PG::Info> >& notify_list);
  void do_queries(map< int, map<pg_t,PG::Query> >& query_map);
  void repeer(PG *pg, map< int, map<pg_t,PG::Query> >& query_map);

  void pull(PG *pg, object_t oid);
  void push(PG *pg, object_t oid, int dest);

  bool require_current_map(Message *m, epoch_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_log(class MOSDPGLog *m);
  void handle_pg_remove(class MOSDPGRemove *m);

  void op_pull(class MOSDOp *op, PG *pg);
  void op_push(class MOSDOp *op, PG *pg);
  
  void op_rep_modify(class MOSDOp *op, PG *pg);   // write, trucnate, delete
  void op_rep_modify_commit(class MOSDOp *op, int ackerosd, 
                            eversion_t last_complete);
  friend class C_OSD_RepModifyCommit;


 public:
  OSD(int id, Messenger *m, MonMap *mm, char *dev = 0);
  ~OSD();
  
  // startup/shutdown
  int init();
  int shutdown();

  // security ops
  bool check_request(class MOSDOp *op, ExtCap *op_capability);
  void update_group(entity_inst_t client, hash_t group, MOSDOp *op);
  bool verify_cap(ExtCap *cap);

  // messages
  virtual void dispatch(Message *m);
  virtual void ms_handle_failure(Message *m, const entity_inst_t& inst);

  void handle_osd_ping(class MOSDPing *m);
  void handle_op(class MOSDOp *m);

  void op_read(class MOSDOp *m);//, PG *pg);
  void op_stat(class MOSDOp *m);//, PG *pg);
  void op_modify(class MOSDOp *m, PG *pg);
  void op_modify_commit(pg_t pgid, tid_t rep_tid, eversion_t pg_complete_thru);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);

  void force_remount();
};

#endif
