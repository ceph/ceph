
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/ThreadPool.h"

#include "ObjectStore.h"

#include <map>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;


class Messenger;
class Message;

typedef __uint64_t version_t;


/** RGImport
 * state associated with import of RG contents from another 
 * OSD.
 */
#define RG_IMPORT_STATE_STARTING    1   // fetching object, delete lists.
#define RG_IMPORT_STATE_IMPORTING   2   // fetching replicas.
#define RG_IMPORT_STATE_FINISHING   3   // got everything; telling old guy to hose residual state

struct RGImport {
  int                      peer;               // the peer
  int                      peer_role;          // peer's role.  if <0, we should delete strays.
  int                      import_state;

  map<object_t,version_t>  remaining_objects;  // remote object list
  map<object_t,version_t>  stray_objects;      // imported but not deleted. 

  // FIXME: add destructive vs non-destructive.  maybe peer is a replica!
};


/** RGPeer
 * state associated with (possibly old) RG peers.
 * only used by primary?
 *
 */

// by primary
#define RG_PEER_STATE_ACTIVE    1   // active peer
#define RG_PEER_STATE_COMPLETE  2   // peer has everything replicated

struct RGPeer {
  int       peer;
  int       role;    // 0 primary, 1+ replica, -1 residual
  int       state;

  // used by primary for syncing (old) replicas
  map<object_t,version_t>  objects;        // remote object list
  map<object_t,version_t>  deleted;        // remote delete list
  map<object_t,version_t>  fetching;       // objects i'm reading from replica
  map<object_t,version_t>  stray;          // objects that need to be deleted
  
  // used by primary for normal replication stuff
  map<object_t,version_t>  writing;        // objects i've written to replica
  map<object_t,version_t>  flushing;       // objects i've written to remote buffer cache only
};



/** RG - Replica Group
 *
 */

// bits used on any
#define RG_STATE_COMPLETE    1  // i have full RG contents locally.
#define RG_STATE_PEERED      2  // i have contacted prior primary and all
                                // replica osds and/or fetched their 
                                // content lists, and thus know what's up.
                                // or, i have check in w/ new primary (on replica)

// on primary or old-primary only
#define RG_STATE_CLEAN       4  // i am fully replicated

class RG {
 protected:
  repgroup_t rg;
  int        role;    // 0 = primary, 1 = secondary, etc.  -1=undef/none.
  int        state;   // see bit defns above

  int        primary;     // replica: who the primary is (if not me)
  set<int>   old_replica_set; // old primary: where replicas used to be
  
  map<int, RGPeer*>         peers;  // primary: (soft state) active peers

  // for unstable states,
  map<object_t, version_t>  deleted_objects;  // locally deleted objects

 public:  
  RG(repgroup_t r) : rg(r),
	role(0),
	state(0) { }
  
  repgroup_t get_rg() { return rg; }
  int        get_role() { return role; }
  int        get_primary() { return primary; }

  void       set_role(int r) { role = r; }
  void       set_primary(int p) { primary = p; }

  map<int, RGPeer*>& get_peers() { return peers; }
  RGPeer* get_peer(int p) {
	if (peers.count(p)) return peers[p];
	return 0;
  }
  set<int>   get_old_replica_set() { return old_replica_set; }

  int  get_state() { return state; }
  bool state_test(int m) { return (state & m) != 0; }
  void set_state(int s) { state = s; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }
  
  void store(ObjectStore *store) {
	if (!store->collection_exists(rg))
	  store->collection_create(rg);
	store->collection_setattr(rg, "role", &role, sizeof(role));
	store->collection_setattr(rg, "primary", &primary, sizeof(primary));
	store->collection_setattr(rg, "state", &state, sizeof(state));	
  }
  void fetch(ObjectStore *store) {
	store->collection_getattr(rg, "role", &role, sizeof(role));
	store->collection_getattr(rg, "primary", &primary, sizeof(primary));
	store->collection_getattr(rg, "state", &state, sizeof(state));	
  }

  void add_object(ObjectStore *store, object_t oid) {
	store->collection_add(rg, oid);
  }
  void remove_object(ObjectStore *store, object_t oid) {
	store->collection_remove(rg, oid);
  }
  void list_objects(ObjectStore *store, list<object_t>& ls) {
	store->collection_list(rg, ls);
  }
};


/** Onode
 * per-object OSD metadata
 */
class Onode {
  object_t            oid;
  version_t           version;

  map<int, version_t> stray_replicas;   // osds w/ stray replicas.

 public:
  Onode(object_t o) : oid(o), version(0) { }

  void store(ObjectStore *store) {
	
  }
  void fetch(ObjectStore *store) {

  }

};


class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  class ObjectStore *store;
  class HostMonitor *monitor;
  class Logger      *logger;

  // global lock
  Mutex osd_lock;


  // -- ops --
  class ThreadPool<class OSD, class MOSDOp>  *threadpool;

  void queue_op(class MOSDOp *m);
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
  __uint64_t                     last_tid;
  Mutex                          replica_write_lock;
  map<MOSDOp*, Cond*>            replica_write_cond;
  map<MOSDOp*, set<__uint64_t> > replica_write_tids;
  map<__uint64_t, MOSDOp*>       replica_writes;
  // </hack>


  // -- replication --
  hash_map<repgroup_t, RG*>      rg_map;

  void get_rg_list(list<repgroup_t>& ls);
  bool rg_exists(repgroup_t rg);
  RG *open_rg(repgroup_t rg);            // return RG, load state from store (if needed)
  void close_rg(repgroup_t rg);          // close in-memory state
  void remove_rg(repgroup_t rg);         // remove state from store

  void scan_rg();
  void peer_notify(int primary, list<repgroup_t>& rg_list);
  void peer_start(int replica, set<RG*>& rg_list);

  void handle_rg_notify(class MOSDRGNotify *m);
  void handle_rg_peer(class MOSDRGPeer *m);
  void handle_rg_peer_ack(class MOSDRGPeerAck *m);

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
