
#include "include/types.h"
#include "include/bufferlist.h"
#include "ObjectStore.h"
#include "msg/Messenger.h"

#include <ext/hash_map>
using namespace __gnu_cxx;


struct PGReplicaInfo {
  int state;
  version_t last_complete;
  version_t last_any_complete;
  map<object_t,version_t>  objects;        // remote object list

  void _encode(bufferlist& blist) {
	blist.append((char*)&state, sizeof(state));
	blist.append((char*)&last_complete, sizeof(last_complete));
	blist.append((char*)&last_any_complete, sizeof(last_any_complete));
	::_encode(objects, blist);
	//::_encode(deleted, blist);
  }
  void _decode(bufferlist& blist, int& off) {
	blist.copy(off, sizeof(state), (char*)&state);
	off += sizeof(state);
	blist.copy(off, sizeof(last_complete), (char*)&last_complete);
	off += sizeof(last_complete);
	blist.copy(off, sizeof(last_any_complete), (char*)&last_any_complete);
	off += sizeof(last_any_complete);
	::_decode(objects, blist, off);
	//::_decode(deleted, blist, off);
  }

  PGReplicaInfo() : state(0) { }
};


/** PGPeer
 * state associated with non-primary OSDS with PG content.
 * only used by primary.
 */

// by primary
#define PG_PEER_STATE_ACTIVE    1   // peer has acked our request, sent back PG state.
#define PG_PEER_STATE_COMPLETE  2   // peer has everything replicated+clean

class PGPeer {
 public:
  class PG *pg;
 private:
  int       peer;
  int       role;
  int       state;

 public:
  // peer state
  version_t     last_complete;
  map<object_t,version_t>  objects;    // cleared after pg->is_peered()

 private:
  // recovery todo
  set<object_t>            missing;    // missing or old objects to push
  set<object_t>            stray;      // extra objects to delete

  // recovery in-flight
  map<object_t,version_t>  pulling;
  map<object_t,version_t>  pushing;
  map<object_t,version_t>  removing;
  
  // replication: for pushing replicas (new or old)
  //map<object_t,version_t>  writing;        // objects i've written to replica

  friend class PG;

 public:
  PGPeer(class PG *pg, int p, int ro) : 
	pg(pg), 
	peer(p),
	role(ro),
	state(0) { }

  int get_peer() { return peer; }
  int get_role() { return role; }

  int get_state() { return state; } 
  bool state_test(int m) { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_active() { return state_test(PG_PEER_STATE_ACTIVE); }
  bool is_complete() { return state_test(PG_PEER_STATE_COMPLETE); }
  bool is_recovering() { return is_active() && !is_complete(); }

  bool is_missing(object_t o) {
	if (is_complete()) return false;
	return missing.count(o);
  }
  bool is_stray(object_t o) {
	if (is_complete()) return false;
	return stray.count(o);
  }

  // actors
  void pull(object_t o, version_t v) { pulling[o] = v; }
  bool is_pulling(object_t o) { return pulling.count(o); }
  version_t pulling_version(object_t o) { return pulling[o]; }
  void pulled(object_t o) { pulling.erase(o); }

  void push(object_t o, version_t v) { pushing[o] = v; }
  bool is_pushing(object_t o) { return pushing.count(o); }
  version_t pushing_version(object_t o) { return pushing[o]; }
  void pushed(object_t o) { 
	pushing.erase(o); 
	missing.erase(o);
	if (missing.empty() && stray.empty()) 
	  state_set(PG_PEER_STATE_COMPLETE);
  }

  void remove(object_t o, version_t v) { removing[o] = v; }
  bool is_removing(object_t o) { return removing.count(o); }
  version_t removing_version(object_t o) { return removing[o]; }
  void removed(object_t o) { 
	removing.erase(o); 
	stray.erase(o);
	if (missing.empty() && stray.empty()) 
	  state_set(PG_PEER_STATE_COMPLETE);
  }

  int num_active_ops() {
	return pulling.size() + pushing.size() + removing.size();
  }
};



/*
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
*/



/** PG - Replica Placement Group
 *
 */

// any
//#define PG_STATE_COMPLETE    1  // i have full PG contents locally
#define PG_STATE_PEERED      2  // primary: peered with everybody
                                // replica: peered with auth

// primary
#define PG_STATE_CLEAN       8  // peers are fully replicated and clean of stray objects

// replica
#define PG_STATE_STRAY      32  // i need to announce myself to new auth



class PG {
 protected:
  int whoami;  // osd#, purely for debug output, yucka

  pg_t        pgid;
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above
  version_t   primary_since;  // (only defined if role==0)

  version_t   last_complete;      // me
  version_t   last_any_complete;  // anybody in the set
  
 public:
  map<int, PGPeer*>         peers;  // primary: (soft state) active peers

 public:
  vector<int> acting;
  //pginfo_t    info;


  /*
  lamport_t   last_complete_stamp;     // lamport timestamp of last complete op
  lamport_t   last_modify_stamp;       // most recent modification
  lamport_t   last_clean_stamp;
  */

  // pg waiters
  list<class Message*>                      waiting_for_peered;   // any op will hang until peered
  hash_map<object_t, list<class Message*> > waiting_for_missing_object;   
  hash_map<object_t, list<class Message*> > waiting_for_clean_object;


  // recovery
  map<object_t, set<int> >  objects_missing;  // pull: missing locally
  map<object_t, version_t > objects_missing_v; // stupid
  map<object_t, set<int> >  objects_unrep;    // push: missing remotely
  map<object_t, map<int, version_t> > objects_stray;    // clean: stray (remote) objects

  map<object_t, PGPeer*>       objects_pulling;
  map<object_t, set<PGPeer*> > objects_pushing;
  map<object_t, map<PGPeer*, version_t> > objects_removing;
  
 private:
  map<object_t, set<int> >::iterator pull_pos;
  map<object_t, set<int> >::iterator push_pos;
  map<object_t, map<int, version_t> >::iterator remove_pos;

 public:
  bool get_next_pull(object_t& oid) {
	if (objects_missing.empty()) return false;
	if (objects_missing.size() == objects_pulling.size()) return false;

	if (objects_pulling.empty() || pull_pos == objects_missing.end()) 
	  pull_pos = objects_missing.begin();
	while (objects_pulling.count(pull_pos->first)) {
	  pull_pos++;
	  if (pull_pos == objects_missing.end()) 
		pull_pos = objects_missing.begin();
	}
	
	oid = pull_pos->first;
	pull_pos++;
	return true;
  }
  bool get_next_push(object_t& oid) {
	if (objects_unrep.empty()) return false;
	if (objects_unrep.size() == objects_pushing.size()) return false;

	if (objects_pushing.empty() || push_pos == objects_unrep.end()) 
	  push_pos = objects_unrep.begin();
	while (objects_pushing.count(push_pos->first)) {
	  push_pos++;
	  if (push_pos == objects_unrep.end()) 
		push_pos = objects_unrep.begin();
	}
	
	oid = push_pos->first;
	push_pos++;
	return true;
  }
  bool get_next_remove(object_t& oid) {
	if (objects_stray.empty()) return false;
	if (objects_stray.size() == objects_removing.size()) return false;

	if (objects_removing.empty() || remove_pos == objects_stray.end()) 
	  remove_pos = objects_stray.begin();
	while (objects_removing.count(remove_pos->first)) {
	  remove_pos++;
	  if (remove_pos == objects_stray.end()) 
		remove_pos = objects_stray.begin();
	}
	
	oid = remove_pos->first;
	remove_pos++;
	return true;
  }

  void pulling(object_t oid, version_t v, PGPeer *p) {
	p->pull(oid, v);
	objects_pulling[oid] = p;
  }
  void pulled(object_t oid, version_t v, PGPeer *p);

  void pushing(object_t oid, version_t v, PGPeer *p) {
	p->push(oid, v);
	objects_pushing[oid].insert(p);
  }
  void pushed(object_t oid, version_t v, PGPeer *p);

  void removing(object_t oid, version_t v, PGPeer *p) {
	p->remove(oid, v);
	objects_removing[oid][p] = v;
  }
  void removed(object_t oid, version_t v, PGPeer *p);


  // log
  map< version_t, set<object_t> > log_write_version_objects;
  map< object_t, set<version_t> > log_write_object_versions;
  map< version_t, set<object_t> > log_delete_version_objects;
  map< object_t, set<version_t> > log_delete_object_versions;

  void log_write(object_t o, version_t v) {
	log_write_object_versions[o].insert(v);
	log_write_version_objects[v].insert(o);
  }
  void unlog_write(object_t o, version_t v) {
	log_write_object_versions[o].erase(v);
	log_write_version_objects[v].erase(o);
  }
  void log_delete(object_t o, version_t v) {
	log_delete_object_versions[o].insert(v);
	log_delete_version_objects[v].insert(o);
  }
  void unlog_delete(object_t o, version_t v) {
	log_write_object_versions[o].erase(v);
	log_write_version_objects[v].erase(o);
  }


 public:
  void plan_recovery(ObjectStore *store, version_t current_version,
					 list<PGPeer*>& complete_peers);

  void discard_recovery_plan() {
	assert(waiting_for_peered.empty());
	assert(waiting_for_missing_object.empty());

	objects_missing.clear();
	objects_missing_v.clear();
	objects_unrep.clear();
	objects_stray.clear();
  }


 public:  
  PG(int osd, pg_t p) : whoami(osd), pgid(p),
	role(0),
	state(0),
	primary_since(0),
	last_complete(0), last_any_complete(0)
	//last_complete_stamp(0), last_modify_stamp(0), last_clean_stamp(0) 
	{ }
  
  pg_t       get_pgid() { return pgid; }
  int        get_primary() { return acting[0]; }
  int        get_nrep() { return acting.size(); }

  version_t  get_last_complete() { return last_complete; }
  //void       set_last_complete(version_t v) { last_complete = v; }
  version_t  get_last_any_complete() { return last_any_complete; }
  //void       set_last_any_complete(version_t v) { last_any_complete = v; }

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

  int  get_state() { return state; }
  bool state_test(int m) { return (state & m) != 0; }
  void set_state(int s) { state = s; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool       is_complete(version_t v)  { 
	//return state_test(PG_STATE_COMPLETE); 
	return v == last_complete;
  }
  bool       is_peered()    { return state_test(PG_STATE_PEERED); }
  //bool       is_crowned()   { return state_test(PG_STATE_CROWNED); }
  bool       is_clean()     { return state_test(PG_STATE_CLEAN); }
  //bool       is_flushing() { return state_test(PG_STATE_FLUSHING); }
  bool       is_stray() { return state_test(PG_STATE_STRAY); }

  void       mark_peered() { 
	state_set(PG_STATE_PEERED);
  }
  void       mark_complete(version_t v) {
	last_complete = v;
	if (v > last_any_complete) last_any_complete = v;
  }
  void       mark_any_complete(version_t v) {
	if (v > last_any_complete) last_any_complete = v;
  }
  void       mark_clean() {
	state_set(PG_STATE_CLEAN);
  }

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
  PGPeer* new_peer(int p, int r) {
	return peers[p] = new PGPeer(this, p, r);
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

  void add_object(ObjectStore *store, const object_t oid) {
	store->collection_add(pgid, oid);
  }
  void remove_object(ObjectStore *store, const object_t oid) {
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
	  cout << " o " << hex << *it << dec << " v " << v << endl;
	}
  }



};


inline ostream& operator<<(ostream& out, PG& pg)
{
  out << "pg[" << hex << pg.get_pgid() << dec << " " << pg.get_role();
  //if (pg.is_complete()) out << " complete";
  if (pg.is_peered()) out << " peered";
  if (pg.is_clean()) out << " clean";
  out << " lc=" << pg.get_last_complete();
  out << "]";
  return out;
}

