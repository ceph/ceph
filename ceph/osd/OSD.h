
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


class Messenger;
class Message;

class OSDReplicaOp {
 public:
  class MOSDOp        *op;
  map<__uint64_t,int>  waitfor_ack;
  map<__uint64_t,int>  waitfor_sync;
  bool                 local_ack;
  bool                 local_sync;
  bool                 cancel;

  set<int>         osds;
  version_t        new_version, old_version;
  
  OSDReplicaOp(class MOSDOp *o, version_t nv, version_t ov) : 
	op(o), 
	local_ack(false), local_sync(false), cancel(false),
	new_version(nv), old_version(ov)
	{ }
  bool can_send_ack() { return !cancel && local_ack && waitfor_ack.empty(); }
  bool can_send_sync() { return !cancel && local_sync && waitfor_sync.empty(); }
  bool can_delete() { return cancel || can_send_sync(); }
};


class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  class ObjectStore *store;
#ifdef USE_EBOFS
  class BlockDevice *storedev;      // for ebofs
#endif
  class HostMonitor *monitor;
  class Logger      *logger;

  int max_recovery_ops;

  // global lock
  Mutex osd_lock;                          

  // per-object locking (serializing)
  hash_set<object_t>               object_lock;
  hash_map<object_t, list<Cond*> > object_lock_waiters;  
  void lock_object(object_t oid);
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
  class ThreadPool<class OSD, class MOSDOp>  *threadpool;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;

  void queue_op(class MOSDOp *m);
  void wait_for_no_ops();

  int apply_write(MOSDOp *op, version_t v,
				  Context *onsync = 0); 


  
 public:
  void do_op(class MOSDOp *m);
  static void doop(OSD *o, MOSDOp *op) {
	o->do_op(op);
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
  void op_rep_modify_sync(class MOSDOp *op);
  friend class C_OSD_RepModifySync;

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
  void op_modify_sync(class OSDReplicaOp *repop);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);
};

#endif
