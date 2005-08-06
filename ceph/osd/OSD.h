
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


struct RGReplicaInfo {
  int state;
  map<object_t,version_t>  objects;        // remote object list
  map<object_t,version_t>  deleted;        // remote delete list

  void _encode(bufferlist& blist) {
	blist.append((char*)&state, sizeof(state));
	::_encode(objects, blist);
	::_encode(deleted, blist);
  }
  void _decode(bufferlist& blist, int& off) {
	blist.copy(off, sizeof(state), (char*)&state);
	off += sizeof(state);
	::_decode(objects, blist, off);
	::_decode(deleted, blist, off);
  }

  RGReplicaInfo() : state(0) { }
};


/** RGPeer
 * state associated with non-primary OSDS with RG content.
 * only used by primary.
 */

// by primary
#define RG_PEER_STATE_ACTIVE    1   // peer has acked our request, sent back RG state.
#define RG_PEER_STATE_COMPLETE  2   // peer has everything replicated

class RGPeer {
 private:
  int       peer;
  int       role;    // 0 primary, 1+ replica, -1 residual
  int       state;

  // peer state
 public:
  RGReplicaInfo peer_state;

 protected:
  // active|residual: used by primary for syncing (old) replicas
  map<object_t,version_t>  fetching;       // objects i'm reading from replica
  map<object_t,version_t>  stray;          // objects that need to be deleted
  
  // active: used by primary for normal replication stuff
  map<object_t,version_t>  writing;        // objects i've written to replica
  map<object_t,version_t>  flushing;       // objects i've written to remote buffer cache only

 public:
  RGPeer(int p, int r) : peer(p), role(r), state(0) { }

  int get_role() { return role; }
  int get_peer() { return peer; }
  bool state_test(int m) { return state & m != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_active() { return state_test(RG_PEER_STATE_ACTIVE); }
  bool is_complete() { return state_test(RG_PEER_STATE_COMPLETE); }

  bool is_residual() { return role < 0; }
  bool is_empty() { return is_active() && peer_state.objects.empty(); }  // *** && peer_state & COMPLETE
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
  repgroup_t rgid;
  int        role;    // 0 = primary, 1 = secondary, etc.  -1=undef/none.
  int        state;   // see bit defns above

  int        primary;         // replica: who the primary is (if not me)
  set<int>   old_replica_set; // old primary: where replicas used to be
  
  map<int, RGPeer*>         peers;  // primary: (soft state) active peers

  // for unstable states,
  map<object_t, version_t>  deleted_objects;  // locally deleted objects

 public:  
  RG(repgroup_t r) : rgid(r),
	role(0),
	state(0),
	primary(-1) { }
  
  repgroup_t get_rgid() { return rgid; }
  int        get_role() { return role; }
  int        get_primary() { return primary; }

  void       set_role(int r) { role = r; }
  void       set_primary(int p) { primary = p; }

  map<int, RGPeer*>& get_peers() { return peers; }
  RGPeer* get_peer(int p) {
	if (peers.count(p)) return peers[p];
	return 0;
  }
  RGPeer* new_peer(int p, int r) {
	return peers[p] = new RGPeer(p, r);
  }
  void remove_peer(int p) {
	assert(peers.count(p));
	delete peers[p];
	peers.erase(p);
  }

  set<int>&                 get_old_replica_set() { return old_replica_set; }
  map<object_t, version_t>& get_deleted_objects() { return deleted_objects; }


  int  get_state() { return state; }
  bool state_test(int m) { return (state & m) != 0; }
  void set_state(int s) { state = s; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }
  
  void store(ObjectStore *store) {
	if (!store->collection_exists(rgid))
	  store->collection_create(rgid);
	store->collection_setattr(rgid, "role", &role, sizeof(role));
	store->collection_setattr(rgid, "primary", &primary, sizeof(primary));
	store->collection_setattr(rgid, "state", &state, sizeof(state));	
  }
  void fetch(ObjectStore *store) {
	store->collection_getattr(rgid, "role", &role, sizeof(role));
	store->collection_getattr(rgid, "primary", &primary, sizeof(primary));
	store->collection_getattr(rgid, "state", &state, sizeof(state));	
  }

  void add_object(ObjectStore *store, object_t oid) {
	store->collection_add(rgid, oid);
  }
  void remove_object(ObjectStore *store, object_t oid) {
	store->collection_remove(rgid, oid);
  }
  void list_objects(ObjectStore *store, list<object_t>& ls) {
	store->collection_list(rgid, ls);
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
  void peer_start(int replica, map<RG*,int>& rg_map);

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
