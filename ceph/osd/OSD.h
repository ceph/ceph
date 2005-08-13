
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/ThreadPool.h"

#include "ObjectStore.h"

#include "RG.h"

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
  
 public:
  void do_op(class MOSDOp *m);
  static void doop(OSD *o, MOSDOp *op) {
	o->do_op(op);
  };

 protected:

  // -- osd map --
  class OSDMap  *osdmap;
  list<class Message*> waiting_for_osdmap;

  void update_map(bufferlist& state);
  void wait_for_new_map(Message *m);
  void handle_osd_map(class MOSDMap *m);

  
  // <old replica hack>
  Mutex                          replica_write_lock;
  map<MOSDOp*, Cond*>            replica_write_cond;
  map<MOSDOp*, set<__uint64_t> > replica_write_tids;
  map<__uint64_t, MOSDOp*>       replica_writes;
  // </hack>


  // -- replication --
  hash_map<repgroup_t, RG*>      rg_map;
  set<RG*>                       rg_unstable;
  __uint64_t                     last_tid;
  map<__uint64_t,RGPeer*>        pull_ops;   // tid -> RGPeer*
  map<__uint64_t,RGPeer*>        push_ops;   // tid -> RGPeer*
  map<__uint64_t,RGPeer*>        remove_ops;   // tid -> RGPeer*

  hash_map<object_t, list<Message*> >    waiting_for_object;
  hash_map<repgroup_t, list<Message*> >  waiting_for_rg;

  void get_rg_list(list<repgroup_t>& ls);
  bool rg_exists(repgroup_t rg);
  RG *new_rg(repgroup_t rg);             // create new RG
  RG *open_rg(repgroup_t rg);            // return existing RG, load state from store (if needed)
  void close_rg(repgroup_t rg);          // close in-memory state
  void remove_rg(repgroup_t rg);         // remove state from store

  void scan_rg();
  void peer_notify(int primary, list<repgroup_t>& rg_list);
  void peer_start(int replica, map<RG*,int>& rg_map);

  void do_recovery(RG *rg);
  void rg_pull(RG *rg, int maxops);
  void rg_push(RG *rg, int maxops);
  void rg_clean(RG *rg, int maxops);

  void pull_replica(object_t oid, version_t v, RGPeer *p);
  void push_replica(object_t oid, version_t v, RGPeer *p);
  void remove_replica(object_t oid, version_t v, RGPeer *p);

  void handle_rg_notify(class MOSDRGNotify *m);
  void handle_rg_peer(class MOSDRGPeer *m);
  void handle_rg_peer_ack(class MOSDRGPeerAck *m);

  void op_rep_pull(class MOSDOp *op);
  void op_rep_pull_reply(class MOSDOpReply *op);
  void op_rep_push(class MOSDOp *op);
  void op_rep_push_reply(class MOSDOpReply *op);
  void op_rep_remove(class MOSDOp *op);
  void op_rep_remove_reply(class MOSDOpReply *op);


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
