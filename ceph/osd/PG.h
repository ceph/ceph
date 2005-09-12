
#include "include/types.h"
#include "include/bufferlist.h"
#include "ObjectStore.h"

struct PGReplicaInfo {
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

  PGReplicaInfo() : state(0) { }
};


/** PGPeer
 * state associated with non-primary OSDS with PG content.
 * only used by primary.
 */

// by primary
#define PG_PEER_STATE_ACTIVE    1   // peer has acked our request, sent back PG state.
#define PG_PEER_STATE_COMPLETE  2   // peer has everything replicated

class PGPeer {
 public:
  class PG *pg;
 private:
  int       peer;
  //int       role;    // 0 primary, 1+ replica, -1 residual
  int       state;

  // peer state
 public:
  PGReplicaInfo peer_state;

 protected:
  // recovery: for pulling content from (old) replicas
  map<object_t,version_t>  pulling;
  map<object_t,version_t>  pushing;
  map<object_t,version_t>  removing;
  
  // replication: for pushing replicas (new or old)
  map<object_t,version_t>  writing;        // objects i've written to replica
  map<object_t,version_t>  flushing;       // objects i've written to remote buffer cache only

 public:
  PGPeer(class PG *pg, int p/*, int ro*/) : 
	pg(pg), 
	peer(p), 
	//role(ro), 
	state(0) { }

  //int get_role() { return role; }
  int get_peer() { return peer; }
  bool state_test(int m) { return state & m != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_active() { return state_test(PG_PEER_STATE_ACTIVE); }
  bool is_complete() { return state_test(PG_PEER_STATE_COMPLETE); }

  //bool is_residual() { return role < 0; }
  bool is_empty() { return is_active() && peer_state.objects.empty(); }  // *** && peer_state & COMPLETE

  bool has_latest(object_t o, version_t v) {
	if (is_complete()) return true;
	if (peer_state.objects.count(o) == 0) return false;
	return peer_state.objects[o] == v;
  }

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
class PGQueue {
  list<object_t>  objects;
  list<version_t> versions;
  list<int>       peers;
  int _size;
 public:
  PGQueue() : _size(0) { }

  int size() { return _size; }

  void push_back(object_t o, version_t v, int p) {
	objects.push_back(o); versions.push_back(v); peers.push_back(p);
	_size++;
  }
  void push_front(object_t o, version_t v, int p) {
	objects.push_front(o); versions.push_front(v); peers.push_front(p);
	_size++;
  }
  bool get_next(object_t& o, version_t& v, int& p) {
	if (objects.empty()) return false;
	o = objects.front(); v = versions.front(); p = peers.front();
	objects.pop_front(); versions.pop_front(); peers.pop_front();
	_size--;
	return true;
  }
  void clear() {
	objects.clear(); versions.clear(); peers.clear();
	_size = 0;
  }
  bool empty() { return objects.empty(); }
};



/** PG - Replica Group
 *
 */

// bits used on any
#define PG_STATE_COMPLETE    1  // i have full PG contents locally.
#define PG_STATE_PEERED      2  // i have contacted prior primary and all
                                // replica osds and/or fetched their 
                                // content lists, and thus know what's up.
                                // or, i have check in w/ new primary (on replica)

// on primary or old-primary only
#define PG_STATE_CLEAN       4  // i am fully replicated

class PG {
 protected:
  int whoami;  // osd#, purely for debug output, yucka

  pg_t        pgid;
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above
  version_t   primary_since;  // (only defined if role==0)

 public:
  vector<int> acting;
  //pginfo_t    info;

 protected:
  version_t   last_complete;
  //set<int>    old_replica_set; // old primary: where replicas used to be
  
  map<int, PGPeer*>         peers;  // primary: (soft state) active peers

 public:
  pginfo_t   info;



  // -- recovery state (useful on primary only) --
 public:
  PGQueue                   pull_plan; 
  PGQueue                   push_plan;
  PGQueue                   clean_plan;
  
  list<class Message*>                 waiting_for_peered;   // any op will hang until peered
  map<object_t, list<class Message*> > waiting_for_object;   // ops waiting for specific objects.

  // recovery
  map<object_t, version_t>  objects;          // what the current object set is
  map<object_t, int>        objects_loc;      // proxy map: where current non-local live
  map<object_t, set<int> >  objects_unrep;    // insufficiently replicated
  map<object_t, set<int> >  objects_stray;    // stray objects
  map<object_t, int>        objects_nrep;     // [temp] not quite accurate.  for pull.

  // for unstable states,
  //map<object_t, version_t>  deleted_objects;  // locally deleted objects

 public:
  void plan_recovery(ObjectStore *store);
  void plan_pull();
  void plan_push_cleanup();

  void discard_recovery_plan() {
	pull_plan.clear(); 
	push_plan.clear(); 
	clean_plan.clear();

	assert(waiting_for_peered.empty());
	assert(waiting_for_object.empty());

	objects.clear();
	objects_loc.clear();
	objects_unrep.clear();
	objects_stray.clear();
	objects_nrep.clear();
  }


 public:  
  PG(int osd, pg_t p) : whoami(osd), pgid(p),
	role(0),
	state(0),
	last_complete(0) { }
  
  pg_t       get_pgid() { return pgid; }
  int        get_primary() { return acting[0]; }
  int        get_nrep() { return acting.size(); }

  version_t  get_last_complete() { return last_complete; }
  void       set_last_complete(version_t v) { last_complete = v; }

  version_t  get_primary_since() { return primary_since; }
  void       set_primary_since(version_t v) { primary_since = v; }

  int        get_role() { return role; }
  void       set_role(int r) { role = r; }
  void       calc_role(int whoami) {
	role = -1;
	for (unsigned i=0; i<acting.size(); i++)
	  if (acting[i] == whoami) role = i>0 ? 1:0;
  }
  bool       is_primary() { return role == 0; }
  bool       is_residual() { return role < 0; }

  bool       is_pulling() { return !pull_plan.empty(); }
  void       pulled(object_t oid, version_t v, PGPeer *p);
  bool       is_pushing() { return !push_plan.empty(); }
  void       pushed(object_t oid, version_t v, PGPeer *p);
  bool       is_removing() { return !push_plan.empty(); }
  void       removed(object_t oid, version_t v, PGPeer *p);


  bool existant_object_is_clean(object_t o, version_t v);
  bool nonexistant_object_is_clean(object_t o);

  PGPeer*    get_proxy_peer(object_t o) { 
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

  bool       is_peered() { return state_test(PG_STATE_PEERED); }
  void       mark_peered();
  bool       is_complete() { return state_test(PG_STATE_COMPLETE); }
  void       mark_complete();
  bool       is_clean() { return state_test(PG_STATE_CLEAN); }
  void       mark_clean();

  int num_active_ops() {
	int o = 0;
	for (map<int, PGPeer*>::iterator it = peers.begin();
         it != peers.end();
		 it++) 
	  o += it->second->num_active_ops();
	return o;
  }

  map<int, PGPeer*>& get_peers() { return peers; }
  PGPeer* get_peer(int p) {
	if (peers.count(p)) return peers[p];
	return 0;
  }
  PGPeer* new_peer(int p/*, int r*/) {
	return peers[p] = new PGPeer(this, p/*, r*/);
  }
  void remove_peer(int p) {
	assert(peers.count(p));
	delete peers[p];
	peers.erase(p);
  }
  void drop_peers() {
	for (map<int,PGPeer*>::iterator it = peers.begin();
		 it != peers.end();
		 it++)
	  delete it->second;
	peers.clear();
  }

  //set<int>&                 get_old_replica_set() { return old_replica_set; }
  //map<object_t, version_t>& get_deleted_objects() { return deleted_objects; }

  

  void assim_info(pginfo_t& i) {
	if (i.created > info.created) info.created = i.created;
	if (i.last_clean > info.last_clean) info.last_clean = i.last_clean;
	if (i.last_complete > info.last_complete) info.last_complete = i.last_complete;
  }
  
  void store(ObjectStore *store) {
	if (!store->collection_exists(pgid))
	  store->collection_create(pgid);
	store->collection_setattr(pgid, "role", &role, sizeof(role));
	store->collection_setattr(pgid, "primary_since", &primary_since, sizeof(primary_since));
	store->collection_setattr(pgid, "state", &state, sizeof(state));	
  }
  void fetch(ObjectStore *store) {
	store->collection_getattr(pgid, "role", &role, sizeof(role));
	store->collection_getattr(pgid, "primary_since", &primary_since, sizeof(primary_since));
	store->collection_getattr(pgid, "state", &state, sizeof(state));	
  }

  void add_object(ObjectStore *store, object_t oid) {
	store->collection_add(pgid, oid);
  }
  void remove_object(ObjectStore *store, object_t oid) {
	store->collection_remove(pgid, oid);
  }
  void list_objects(ObjectStore *store, list<object_t>& ls) {
	store->collection_list(pgid, ls);
  }

  void scan_local_objects(map<object_t, version_t>& local_objects, ObjectStore *store) {
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


};


inline ostream& operator<<(ostream& out, PG& pg)
{
  out << "pg[" << hex << pg.get_pgid() << dec << " " << pg.get_role();
  if (pg.is_complete()) out << " complete";
  if (pg.is_peered()) out << " peered";
  if (pg.is_clean()) out << " clean";
  out << "]";
  return out;
}


/*
class PS {
  ps_t psid;
  int  rank;
  
 public:
  map<int,PG*>  pg_map;

 public:
  PS(ps_t p) : psid(p), rank(0) { }
  
  int        get_rank() { return rank; }
  void       set_rank(int r) { rank = r; }

  // PG
  bool pg_exists(pg_t pg);
  PG* get_pg(int r) {
	if (pg_map.count(r)) return pg_map[r];
	return 0;
  }
  PG* new_pg(int r) {
	return pg_map[r] = new PG;
  }

  // store
  void store(ObjectStore *store) {
	if (!store->collection_exists(psid))
	  store->collection_create(psid);
	store->collection_setattr(psid, "rank", &rank, sizeof(rank));
  }
  void fetch(ObjectStore *store) {
	store->collection_getattr(pgid, "rank", &rank, sizeof(rank));
  }

};
*/
