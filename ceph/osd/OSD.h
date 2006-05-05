// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "Objecter.h"
#include "ObjectStore.h"
#include "PG.h"

#include <map>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "messages/MOSDOp.h"

class Messenger;
class Message;





/**
 *
 */

class OSD : public Dispatcher {
public:

  /** OSDReplicaOp
   * state associated with an in-progress replicated update.
   */
  class OSDReplicaOp {
  public:
	class MOSDOp        *op;
	Mutex                lock;
	map<__uint64_t,int>  waitfor_ack;
	map<__uint64_t,int>  waitfor_commit;
	
	utime_t   start;
	
	bool cancel;
	bool sent_ack, sent_commit;
	
	set<int>         osds;
	version_t        new_version, old_version;
	
	OSDReplicaOp(class MOSDOp *o, version_t nv, version_t ov) : 
	  op(o), 
	  //local_ack(false), local_commit(false), 
	  cancel(false),
	  sent_ack(false), sent_commit(false),
	  new_version(nv), old_version(ov)
	{ }
	bool can_send_ack() { return !sent_ack && !sent_commit &&   //!cancel && 
							waitfor_ack.empty(); }
	bool can_send_commit() { return !sent_commit &&    //!cancel && 
							   waitfor_ack.empty() && waitfor_commit.empty(); }
	bool can_delete() { return waitfor_ack.empty() && waitfor_commit.empty(); }
  };
  
  /** OSD **/
 protected:
  Messenger *messenger;
  int whoami;

  class Logger      *logger;

  int max_recovery_ops;

  // local store
  char dev_path[100];
  class ObjectStore *store;

  // failure monitoring
  class HostMonitor *monitor;

  // global lock
  Mutex osd_lock;                          

  // per-object locking (serializing)
  hash_set<object_t>               object_lock;
  hash_map<object_t, list<Cond*> > object_lock_waiters;  
  void lock_object(object_t oid);
  void _lock_object(object_t oid);
  void unlock_object(object_t oid);

  // finished waiting messages, that will go at tail of dispatch()
  list<class Message*> finished;
  void take_waiters(list<class Message*>& ls) {
	finished.splice(finished.end(), ls);
  }
  
  // -- ops --
  class ThreadPool<class OSD*, object_t> *threadpool;
  hash_map<object_t, list<MOSDOp*> >      op_queue;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;
  Cond  op_queue_cond;
  
  void wait_for_no_ops();

  void enqueue_op(object_t oid, MOSDOp *op);
  void dequeue_op(object_t oid);
  static void static_dequeueop(OSD *o, object_t oid) {
	o->dequeue_op(oid);
  };

  void do_op(class MOSDOp *m);  // actually do it

  int apply_write(MOSDOp *op, version_t v,
				  Context *oncommit = 0); 
  
  
  
  friend class PG;

 protected:

  // -- osd map --
  class OSDMap  *osdmap;
  list<class Message*> waiting_for_osdmap;
  map<version_t, OSDMap*> osdmaps;
  
  void update_map(bufferlist& state);
  void wait_for_new_map(Message *m);
  void handle_osd_map(class MOSDMap *m);
  OSDMap *get_osd_map(version_t v);
  
  void advance_map(list<pg_t>& ls);
  void activate_map(list<pg_t>& ls);



  // -- replication --

  // PG
  hash_map<pg_t, PG*>      pg_map;
  void  get_pg_list(list<pg_t>& ls);
  bool  pg_exists(pg_t pg);
  PG   *create_pg(pg_t pg);          // create new PG
  PG   *get_pg(pg_t pg);             // return existing PG, load state from store (if needed)
  void  close_pg(pg_t pg);           // close in-memory state
  void  remove_pg(pg_t pg);          // remove state from store

  __uint64_t               last_tid;

  hash_map<pg_t, list<Message*> >        waiting_for_pg;

  // replica ops
  map<__uint64_t, OSDReplicaOp*>         replica_ops;
  map<pg_t, map<int, set<__uint64_t> > > replica_pg_osd_tids; // pg -> osd -> tid
  
  void get_repop(OSDReplicaOp*);
  void put_repop(OSDReplicaOp*);   // will send ack/commit msgs, and delete as necessary.
  void issue_replica_op(PG *pg, OSDReplicaOp *repop, int osd);
  void handle_rep_op_ack(__uint64_t tid, int result, bool commit, int fromosd);

  // recovery
  map<tid_t,PG::ObjectInfo>  pull_ops;   // tid -> PGPeer*

  void do_notifies(map< int, list<PG::PGInfo> >& notify_list);
  void do_queries(map< int, map<pg_t,version_t> >& query_map);
  void repeer(PG *pg, map< int, map<pg_t,version_t> >& query_map);

  void pg_pull(PG *pg, int maxops);
  void pull_replica(PG *pg, PG::ObjectInfo& oi);

  bool require_current_map(Message *m, version_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_summary(class MOSDPGSummary *m);

  void op_rep_pull(class MOSDOp *op);
  void op_rep_pull_reply(class MOSDOpReply *op);
  
  void op_rep_modify(class MOSDOp *op);   // write, trucnate, delete
  void op_rep_modify_commit(class MOSDOp *op);
  friend class C_OSD_RepModifyCommit;


 public:
  OSD(int id, Messenger *m);
  ~OSD();
  
  // startup/shutdown
  int init();
  int shutdown();

  // messages
  virtual void dispatch(Message *m);

  void handle_ping(class MPing *m);
  void handle_op(class MOSDOp *m);

  void op_read(class MOSDOp *m);
  void op_stat(class MOSDOp *m);
  void op_modify(class MOSDOp *m);
  void op_modify_commit(class OSDReplicaOp *repop);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);

  void force_remount();
};

inline ostream& operator<<(ostream& out, OSD::OSDReplicaOp& repop)
{
  out << "repop(wfack=" << repop.waitfor_ack << " wfcommit=" << repop.waitfor_commit;
  if (repop.cancel) out << " cancel";
  out << " op=" << *(repop.op);
  out << " repop=" << &repop;
  out << ")";
  return out;
}


#endif
