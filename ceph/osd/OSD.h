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
	
	//bool cancel;
	bool sent_ack, sent_commit;
	
	set<int>         osds;
	version_t        new_version, old_version;

	map<int,version_t> pg_complete_thru;
	
	OSDReplicaOp(class MOSDOp *o, version_t nv, version_t ov) :
	  op(o), 
	  //local_ack(false), local_commit(false), 
	  //cancel(false),
	  sent_ack(false), sent_commit(false),
	  new_version(nv), old_version(ov)
	{ }
	bool can_send_ack() { return !sent_ack && !sent_commit &&   //!cancel && 
							waitfor_ack.empty(); }
	bool can_send_commit() { return !sent_commit &&    //!cancel && 
							   waitfor_ack.empty() && waitfor_commit.empty(); }
	bool can_delete() { return waitfor_ack.empty() && waitfor_commit.empty(); }
  };
  

class OSD : public Dispatcher {
public:

  /** superblock
   */
  const static object_t SUPERBLOCK_OBJECT = 0;
  OSDSuperblock superblock;

  object_t get_osdmap_object_name(epoch_t epoch) { return (object_t)epoch; }

  void write_superblock();
  void read_superblock();


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
  class ThreadPool<class OSD*, object_t> *threadpool;
  hash_map<pg_t, list<MOSDOp*> >         op_queue;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;
  Cond  op_queue_cond;
  
  void wait_for_no_ops();

  void enqueue_op(pg_t pgid, MOSDOp *op);
  void dequeue_op(pg_t pgid);
  static void static_dequeueop(OSD *o, pg_t pgid) {
	o->dequeue_op(pgid);
  };

  void do_op(class MOSDOp *m, PG *pg);  // actually do it

  void prepare_log_transaction(ObjectStore::Transaction& t, MOSDOp* op, version_t& version, PG *pg);
  void prepare_op_transaction(ObjectStore::Transaction& t, MOSDOp* op, version_t& version, PG *pg);
  
  bool waitfor_missing_object(MOSDOp *op, PG *pg);

  
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
  void  _remove_pg(pg_t pg);         // remove from store and memory

  epoch_t calc_pg_primary_since(int primary, pg_t pgid, epoch_t start);

  tid_t               last_tid;

  hash_map<pg_t, list<Message*> >        waiting_for_pg;

  // replica ops
  void get_repop(OSDReplicaOp*);
  void put_repop(OSDReplicaOp*);   // will send ack/commit msgs, and delete as necessary.
  void issue_replica_op(PG *pg, OSDReplicaOp *repop, int osd);
  void handle_rep_op_ack(PG *pg, __uint64_t tid, int result, bool commit, int fromosd, 
						 version_t pg_complete_thru=0);

  // recovery
  void do_notifies(map< int, list<PG::PGInfo> >& notify_list);
  void do_queries(map< int, map<pg_t,version_t> >& query_map);
  void repeer(PG *pg, map< int, map<pg_t,version_t> >& query_map);

  void pull(PG *pg, object_t, version_t);

  bool require_current_map(Message *m, version_t v);
  bool require_same_or_newer_map(Message *m, epoch_t e);

  void handle_pg_query(class MOSDPGQuery *m);
  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_summary(class MOSDPGSummary *m);
  void handle_pg_log(class MOSDPGLog *m);
  void handle_pg_remove(class MOSDPGRemove *m);

  void op_rep_pull(class MOSDOp *op, PG *pg);
  void op_rep_pull_reply(class MOSDOpReply *op);
  
  void op_rep_modify(class MOSDOp *op, PG *pg);   // write, trucnate, delete
  void op_rep_modify_commit(class MOSDOp *op, version_t last_complete);
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

  void op_read(class MOSDOp *m, PG *pg);
  void op_stat(class MOSDOp *m, PG *pg);
  void op_modify(class MOSDOp *m, PG *pg);
  void op_modify_commit(class OSDReplicaOp *repop, version_t last_complete);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);

  void force_remount();
};

inline ostream& operator<<(ostream& out, OSDReplicaOp& repop)
{
  out << "repop(wfack=" << repop.waitfor_ack << " wfcommit=" << repop.waitfor_commit;
  //if (repop.cancel) out << " cancel";
  out << " op=" << *(repop.op);
  out << " repop=" << &repop;
  out << " lc=" << repop.pg_complete_thru;
  out << ")";
  return out;
}


#endif
