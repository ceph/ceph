
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




class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

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
  void unlock_object(object_t oid);

  // finished waiting messages, that will go at tail of dispatch()
  list<class Message*> finished;
  void take_waiters(list<class Message*>& ls) {
	finished.splice(finished.end(), ls);
  }
  
  // -- objects --
  int read_onode(onode_t& onode);
  int write_onode(onode_t& onode);


  // -- ops --
  class ThreadPool<class OSD, class MOSDOp>  *threadpool;
  int   pending_ops;
  bool  waiting_for_no_ops;
  Cond  no_pending_ops;

  void queue_op(class MOSDOp *m);
  void wait_for_no_ops();

  void apply_write(MOSDOp *op, bool write_sync, version_t v); // for op_write and op_rep_write

  
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

  void update_map(bufferlist& state);
  void wait_for_new_map(Message *m);
  void handle_osd_map(class MOSDMap *m);
  OSDMap *get_osd_map(version_t v);
  
  // <old replica hack>
  Mutex                          replica_write_lock;
  map<__uint64_t, MOSDOp*>       replica_writes;
  map<MOSDOp*, set<__uint64_t> > replica_write_tids;
  set<MOSDOp*>                   replica_write_local;
  map<pg_t, map<int, set<__uint64_t> > > replica_pg_osd_tids; // pg -> osd -> tid
  // </hack>


  // -- replication --

  // PS
  /*
  hash_map<ps_t, PS*>      ps_map;

  void get_ps_list(list<ps_t>& ls);
  bool ps_exists(ps_t ps);
  PS *create_ps(ps_t ps);          // create new PS
  PS *open_ps(ps_t ps);            // return existing PS, load state from store (if needed)
  void close_ps(ps_t ps);          // close in-memory state
  void remove_ps(ps_t ps);         // remove state from store
  */
  // PG
  hash_map<pg_t, PG*>      pg_map;
  void get_pg_list(list<pg_t>& ls);
  bool pg_exists(pg_t pg);
  PG *create_pg(pg_t pg);             // create new PG
  PG *open_pg(pg_t pg);            // return existing PG, load state from store (if needed)
  void close_pg(pg_t pg);          // close in-memory state
  void remove_pg(pg_t pg);         // remove state from store

  set<PG*>                 pg_unstable;
  __uint64_t               last_tid;
  map<__uint64_t,PGPeer*>  pull_ops;   // tid -> PGPeer*
  map<__uint64_t,PGPeer*>  push_ops;   // tid -> PGPeer*
  map<__uint64_t,PGPeer*>  remove_ops; // tid -> PGPeer*

  hash_map<object_t, list<Message*> >  waiting_for_object;
  hash_map<object_t, list<Message*> >  waiting_for_clean_object;
  hash_map<pg_t, list<Message*> >      waiting_for_pg;
  hash_map<pg_t, list<Message*> >      waiting_for_pg_peered;


  void advance_map(list<pg_t>& ls);
  void activate_map(list<pg_t>& ls);

  void start_peers(PG *pg, map< int, map<PG*,int> >& start_map);

  void peer_notify(int primary, map<pg_t,version_t>& pg_list);
  void peer_start(int replica, map<PG*,int>& pg_map);

  void do_recovery(PG *pg);
  void pg_pull(PG *pg, int maxops);
  void pg_push(PG *pg, int maxops);
  void pg_clean(PG *pg, int maxops);

  void pull_replica(object_t oid, version_t v, PGPeer *p);
  void push_replica(object_t oid, version_t v, PGPeer *p);
  void remove_replica(object_t oid, version_t v, PGPeer *p);

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
  
  void op_rep_write(class MOSDOp *op);

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
  void op_write(class MOSDOp *m);
  void op_mkfs(class MOSDOp *m);
  void op_delete(class MOSDOp *m);
  void op_truncate(class MOSDOp *m);
  void op_stat(class MOSDOp *m);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);
};

#endif
