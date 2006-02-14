// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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

class OSDReplicaOp {
 public:
  class MOSDOp        *op;
  Mutex                lock;
  map<__uint64_t,int>  waitfor_ack;
  map<__uint64_t,int>  waitfor_safe;

  bool cancel;
  bool sent_ack, sent_safe;

  set<int>         osds;
  version_t        new_version, old_version;
  
  OSDReplicaOp(class MOSDOp *o, version_t nv, version_t ov) : 
	op(o), 
	//local_ack(false), local_safe(false), 
	cancel(false),
	sent_ack(false), sent_safe(false),
	new_version(nv), old_version(ov)
	{ }
  bool can_send_ack() { return !sent_ack && !sent_safe &&   //!cancel && 
						  waitfor_ack.empty(); }
  bool can_send_safe() { return !sent_safe &&    //!cancel && 
						   waitfor_ack.empty() && waitfor_safe.empty(); }
  bool can_delete() { return waitfor_ack.empty() && waitfor_safe.empty(); }
};

inline ostream& operator<<(ostream& out, OSDReplicaOp& repop)
{
  out << "repop(wfack=" << repop.waitfor_ack << " wfsafe=" << repop.waitfor_safe;
  //if (repop.local_ack) out << " local_ack";
  //if (repop.local_safe) out << " local_safe";
  if (repop.cancel) out << " cancel";
  out << " op=" << *(repop.op);
  out << " repop=" << &repop;
  out << ")";
  return out;
}

class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  char dev_path[100];

  class ObjectStore *store;
  class HostMonitor *monitor;
  class Logger      *logger;

  int max_recovery_ops;

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
  
  // -- objects --
  //int read_onode(onode_t& onode);
  //int write_onode(onode_t& onode);


  // -- ops --
  class ThreadPool<class OSD*, object_t> *threadpool;
  hash_map<object_t, list<MOSDOp*> >      op_queue;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;

  void wait_for_no_ops();

  int apply_write(MOSDOp *op, version_t v,
				  Context *onsafe = 0); 


  void get_repop(OSDReplicaOp*);
  void put_repop(OSDReplicaOp*);   // will send ack/safe msgs, and delete as necessary.
  
  void do_op(class MOSDOp *m);

 public:
  void enqueue_op(object_t oid, MOSDOp *op);
  void dequeue_op(object_t oid);
  static void static_dequeueop(OSD *o, object_t oid) {
	o->dequeue_op(oid);
  };

 protected:

  // -- osd map --
  class OSDMap  *osdmap;
  list<class Message*> waiting_for_osdmap;
  map<version_t, OSDMap*> osdmaps;

  void update_map(bufferlist& state, bool mkfs=false);
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
  void  close_pg(pg_t pg);          // close in-memory state
  void  remove_pg(pg_t pg);         // remove state from store

  __uint64_t               last_tid;

  hash_map<pg_t, list<Message*> >        waiting_for_pg;

  // replica ops
  map<__uint64_t, OSDReplicaOp*>         replica_ops;
  map<pg_t, map<int, set<__uint64_t> > > replica_pg_osd_tids; // pg -> osd -> tid
  
  void issue_replica_op(PG *pg, OSDReplicaOp *repop, int osd);
  void ack_replica_op(__uint64_t tid, int result, bool safe, int fromosd);

  // recovery
  map<__uint64_t,PGPeer*>  pull_ops;   // tid -> PGPeer*
  map<__uint64_t,PGPeer*>  push_ops;   // tid -> PGPeer*
  map<__uint64_t,PGPeer*>  remove_ops; // tid -> PGPeer*

  void start_peers(PG *pg, map< int, map<PG*,int> >& start_map);

  void peer_notify(int primary, map<pg_t,version_t>& pg_list);
  void peer_start(int replica, map<PG*,int>& pg_map);

  void plan_recovery(PG *pg);
  void do_recovery(PG *pg);
  void pg_pull(PG *pg, int maxops);
  void pg_push(PG *pg, int maxops);
  void pg_clean(PG *pg, int maxops);

  void pull_replica(PG *pg, object_t oid);
  void push_replica(PG *pg, object_t oid);
  void remove_replica(PG *pg, object_t oid);

  bool require_current_map(Message *m, version_t v);
  bool require_current_pg_primary(Message *m, version_t v, PG *pg);

  void handle_pg_notify(class MOSDPGNotify *m);
  void handle_pg_peer(class MOSDPGPeer *m);
  void handle_pg_peer_ack(class MOSDPGPeerAck *m);
  void handle_pg_update(class MOSDPGUpdate *m);

  void op_rep_pull(class MOSDOp *op);
  void op_rep_pull_reply(class MOSDOpReply *op);
  void op_rep_push(class MOSDOp *op);
  void op_rep_push_reply(class MOSDOpReply *op);
  void op_rep_remove(class MOSDOp *op);
  void op_rep_remove_reply(class MOSDOpReply *op);
  
  void op_rep_modify(class MOSDOp *op);   // write, trucnate, delete
  void op_rep_modify_safe(class MOSDOp *op);
  friend class C_OSD_RepModifySafe;

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
  void op_modify_safe(class OSDReplicaOp *repop);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);
};

#endif
