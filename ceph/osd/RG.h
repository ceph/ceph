
#include "include/types.h"
#include "include/bufferlist.h"
#include "ObjectStore.h"

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
 public:
  class RG *rg;
 private:
  int       peer;
  int       role;    // 0 primary, 1+ replica, -1 residual
  int       state;

  // peer state
 public:
  RGReplicaInfo peer_state;

 protected:
  // recovery: for pulling content from (old) replicas
  map<object_t,version_t>  pulling;
  map<object_t,version_t>  pushing;
  map<object_t,version_t>  removing;
  
  // replication: for pushing replicas (new or old)
  map<object_t,version_t>  writing;        // objects i've written to replica
  map<object_t,version_t>  flushing;       // objects i've written to remote buffer cache only

 public:
  RGPeer(class RG *rg, int p, int ro) : rg(rg), peer(p), role(ro), state(0) { }

  int get_role() { return role; }
  int get_peer() { return peer; }
  bool state_test(int m) { return state & m != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_active() { return state_test(RG_PEER_STATE_ACTIVE); }
  bool is_complete() { return state_test(RG_PEER_STATE_COMPLETE); }

  bool is_residual() { return role < 0; }
  bool is_empty() { return is_active() && peer_state.objects.empty(); }  // *** && peer_state & COMPLETE

  void pull(object_t o, version_t v) { pulling[o] = v; }
  bool is_pulling(object_t o) { return pulling.count(o); }
  version_t pulling_version(object_t o) { return pulling[o]; }
  void pulled(object_t o) { pulling.erase(o); }

  void push(object_t o, version_t v) { pushing[o] = v; }
  bool is_pushing(object_t o) { return pushing.count(o); }
  version_t pushing_version(object_t o) { return pushing[o]; }
  void pushed(object_t o) { pushing.erase(o); }

  void remove(object_t o, version_t v) { removing[o] = v; }
  bool is_removing(object_t o) { return removing.count(o); }
  version_t removing_version(object_t o) { return removing[o]; }
  void removed(object_t o) { 
	removing.erase(o); 
	peer_state.objects.erase(o);
  }

  int num_active_ops() {
	return pulling.size() + pushing.size() + removing.size();
  }
};




// a task list for moving objects around
class RGQueue {
  list<object_t>  objects;
  list<version_t> versions;
  list<int>       peers;
 public:
  void push_back(object_t o, version_t v, int p) {
	objects.push_back(o); versions.push_back(v); peers.push_back(p);
  }
  void push_front(object_t o, version_t v, int p) {
	objects.push_front(o); versions.push_front(v); peers.push_front(p);
  }
  bool get_next(object_t& o, version_t& v, int& p) {
	if (objects.empty()) return false;
	o = objects.front(); v = versions.front(); p = peers.front();
	objects.pop_front(); versions.pop_front(); peers.pop_front();
	return true;
  }
  void clear() {
	objects.clear(); versions.clear(); peers.clear();
  }
  bool empty() { return objects.empty(); }
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
  int whoami;  // osd, purely for debug output, yucka

  repgroup_t rgid;
  int        role;    // 0 = primary, 1 = secondary, etc.  -1=undef/none.
  int        state;   // see bit defns above

  int        primary;         // replica: who the primary is (if not me)
  set<int>   old_replica_set; // old primary: where replicas used to be
  
  map<int, RGPeer*>         peers;  // primary: (soft state) active peers

 public:
  RGQueue                   pull_plan; 
  RGQueue                   push_plan;
  RGQueue                   clean_plan;

  list<class Message*>                 waiting_for_peered;   // any op will hang until peered
  map<object_t, list<class Message*> > waiting_for_object;   // ops waiting for specific objects.

  // recovery
  map<object_t, version_t>  local_objects;
  map<object_t, version_t>  objects;          // what the current object set is
  map<object_t, int>        objects_loc;      // where latest live
  
  // for unstable states,
  map<object_t, version_t>  deleted_objects;  // locally deleted objects

 public:  
  RG(int osd, repgroup_t r) : whoami(osd), rgid(r),
	role(0),
	state(0),
	primary(-1) { }
  
  repgroup_t get_rgid() { return rgid; }
  int        get_role() { return role; }
  int        get_primary() { return primary; }

  void       set_role(int r) { role = r; }
  void       set_primary(int p) { primary = p; }

  bool       is_primary() { return role == 0; }
  bool       is_residual() { return role < 0; }

  bool       is_pulling() { return !pull_plan.empty(); }
  void       pulled(object_t oid, version_t v, RGPeer *p);
  bool       is_pushing() { return !push_plan.empty(); }
  void       pushed(object_t oid, version_t v, RGPeer *p);
  bool       is_removing() { return !push_plan.empty(); }
  void       removed(object_t oid, version_t v, RGPeer *p);

  RGPeer*    get_proxy_peer(object_t o) { 
	if (objects_loc.count(o))
	  return get_peer(objects_loc[o]);
	return 0;
  }
  version_t  get_proxy_version(object_t o) { return objects[o]; }
  
  int  get_state() { return state; }
  bool state_test(int m) { return (state & m) != 0; }
  void set_state(int s) { state = s; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool       is_peered() { return state_test(RG_STATE_PEERED); }
  void       mark_peered();
  bool       is_complete() { return state_test(RG_STATE_COMPLETE); }
  void       mark_complete();
  bool       is_clean() { return state_test(RG_STATE_CLEAN); }
  void       mark_clean();

  int num_active_ops() {
	int o = 0;
	for (map<int, RGPeer*>::iterator it = peers.begin();
         it != peers.end();
		 it++) 
	  o += it->second->num_active_ops();
	return o;
  }

  map<int, RGPeer*>& get_peers() { return peers; }
  RGPeer* get_peer(int p) {
	if (peers.count(p)) return peers[p];
	return 0;
  }
  RGPeer* new_peer(int p, int r) {
	return peers[p] = new RGPeer(this, p, r);
  }
  void remove_peer(int p) {
	assert(peers.count(p));
	delete peers[p];
	peers.erase(p);
  }

  set<int>&                 get_old_replica_set() { return old_replica_set; }
  map<object_t, version_t>& get_deleted_objects() { return deleted_objects; }


  
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

  void scan_local_objects(ObjectStore *store) {
	list<object_t> olist;
	local_objects.clear();
	list_objects(store,olist);
	for (list<object_t>::iterator it = olist.begin();
		 it != olist.end();
		 it++) {
	  version_t v = 0;
	  store->getattr(*it, 
					 "version",
					 &v, sizeof(v));
	  local_objects[*it] = v;
	}
  }


  void assim_deleted_objects(map<object_t,version_t>& dl) {
	for (map<object_t, version_t>::iterator oit = dl.begin();
		 oit != dl.end();
		 oit++) {
	  if (objects.count(oit->first) == 0) continue;  // dne
	  if (objects[oit->first] < oit->second) {       // deleted.
		objects.erase(oit->first);
		objects_loc.erase(oit->first);
	  }
	}
  }

  void analyze_peers(ObjectStore *store);
  void plan_push();
  void plan_cleanup();

};

