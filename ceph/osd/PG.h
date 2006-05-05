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

#ifndef __PG_H
#define __PG_H


#include "include/types.h"
#include "include/bufferlist.h"

#include "OSDMap.h"
#include "ObjectStore.h"
#include "msg/Messenger.h"

#include <list>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


class OSD;


/** PG - Replica Placement Group
 *
 */

class PG {
public:
  
  /** ObjectInfo
   * summary info about an object (replica)
   */
  struct ObjectInfo {
	object_t oid;
	version_t version;
	int osd;   // -1 = unknown.  if local, osd == whoami.
	ObjectInfo(object_t o=0, version_t v=0, int os=-1) : oid(o), version(v), osd(os) {}
  };
  
  struct PGInfo {
	pg_t pgid;
	version_t last_update;    // last object version applied.
	version_t last_complete;  // last pg version pg was complete.
	epoch_t last_epoch_started;  // last epoch started.
	epoch_t last_epoch_finished; // last epoch finished.
	epoch_t same_primary_since;  // 
	PGInfo(pg_t p=0) : pgid(p), 
					   last_update(0), last_complete(0),
					   last_epoch_started(0), last_epoch_finished(0),
					   same_primary_since(0) {}
  };
  
  struct PGContentSummary {
	//version_t since;
	int remote, missing;
	list<ObjectInfo> ls;

	void _encode(bufferlist& blist) {
	  //blist.append((char*)&since, sizeof(since));
	  blist.append((char*)&remote, sizeof(remote));
	  blist.append((char*)&missing, sizeof(missing));
	  ::_encode(ls, blist);
	}
	void _decode(bufferlist& blist, int& off) {
	  //blist.copy(off, sizeof(since), (char*)&since);
	  //off += sizeof(since);
	  blist.copy(off, sizeof(remote), (char*)&remote);
	  off += sizeof(remote);
	  blist.copy(off, sizeof(missing), (char*)&missing);
	  off += sizeof(missing);
	  ::_decode(ls, blist, off);
	}

	PGContentSummary() : remote(0), missing(0) {}
  };

  
  /** PGPeer
   * state associated with non-primary OSDS with PG content.
   * only used by primary.
   */
  
  class PGPeer {
  public:
	// bits
	static const int STATE_INFO      = 1;  // we have info
	static const int STATE_SUMMARY  = 2;  // we have summary
	static const int STATE_QINFO     = 4;  // we are querying info|summary.
	static const int STATE_QSUMMARY = 8;  // we are querying info|summary.
	static const int STATE_WAITING   = 16; // peer is waiting for go.
	static const int STATE_ACTIVE    = 32; // peer is active.
	//static const int STATE_COMPLETE  = 64; // peer is complete.

	class PG *pg;
  private:
	int       peer;
	int       role;
	int       state;
	
  public:
	// peer state
	PGInfo            info;
	PGContentSummary *content_summary;
	
	friend class PG;
	
  public:
	PGPeer(class PG *pg, int p, int ro) : 
	  pg(pg), 
	  peer(p),
	  role(ro),
	  state(0),
	  content_summary(NULL) { }
	~PGPeer() {
	  if (content_summary) delete content_summary;
	}
	
	int get_peer() { return peer; }
	int get_role() { return role; }
	
	int get_state() { return state; } 
	bool state_test(int m) { return (state & m) != 0; }
	void state_set(int m) { state |= m; }
	void state_clear(int m) { state &= ~m; }
	
	bool have_info() { return state_test(STATE_INFO); }
	bool have_summary() { return state_test(STATE_SUMMARY); }

	bool is_waiting() { return state_test(STATE_WAITING); }
	bool is_active() { return state_test(STATE_ACTIVE); }
	bool is_complete() { return have_info() &&
						   info.last_update == info.last_complete; }
  };
  

  /*** PG ****/
public:
  // any
  //static const int STATE_SUMMARY = 1;  // i have a content summary.
  static const int STATE_ACTIVE = 2;   // i am active.  (primary: replicas too)
  //static const int STATE_COMPLETE = 4; // i am complete.

  // primary
  static const int STATE_CLEAN = 8;  // peers are complete, clean of stray replicas.
 
  // non-primary
  static const int STATE_STRAY = 16;  // i haven't sent notify yet.  primary may not know i exist.

 protected:
  OSD *osd;

  // generic state
public:
  PGInfo      info;
  PGContentSummary *content_summary;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above

  // primary state
public:
  epoch_t           last_epoch_started_any;
  map<int, PGPeer*> peers;  // primary: (soft state) active peers

 public:
  vector<int> acting;

  // pg waiters
  list<class Message*>            waiting_for_active;
  hash_map<object_t, 
		   list<class Message*> > waiting_for_missing_object;   

  // recovery
  map<object_t, version_t>   objects_missing;  // objects (versions) i need
  map<version_t, ObjectInfo> recovery_queue;   // objects i need to pull (in order)
  version_t requested_through;
  map<object_t, ObjectInfo>  objects_pulling;  // which objects are currently being pulled
  
  void plan_recovery();
  void generate_content_summary();
  void do_recovery();


 public:  
  PG(OSD *o, pg_t p) : 
	osd(o), 
	info(p), content_summary(0),
	role(0),
	state(0)
  { }
  
  pg_t       get_pgid() { return info.pgid; }
  int        get_primary() { return acting[0]; }
  int        get_nrep() { return acting.size(); }

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

  bool is_complete() { return info.last_complete == info.last_update; }

  bool       is_active()    { return state_test(STATE_ACTIVE); }
  //bool       is_complete()    { return state_test(STATE_COMPLETE); }
  bool       is_clean()     { return state_test(STATE_CLEAN); }
  bool       is_stray() { return state_test(STATE_STRAY); }

  void mark_complete() {
	info.last_complete = info.last_update;
  }
  void mark_active() {
	state_set(STATE_ACTIVE);
  }

  int num_active_ops() {
	return objects_pulling.size();
  }

  // peers
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


  // pg state storage
  /*
  void store() {
	if (!osd->store->collection_exists(pgid))
	  osd->store->create_collection(pgid);
	// ***
  }
  void fetch() {
	//osd->store->collection_getattr(pgid, "role", &role, sizeof(role));
	//osd->store->collection_getattr(pgid, "primary_since", &primary_since, sizeof(primary_since));
	//osd->store->collection_getattr(pgid, "state", &state, sizeof(state));	
  }

  void list_objects(list<object_t>& ls) {
	osd->store->collection_list(pgid, ls);
	}*/

};


inline ostream& operator<<(ostream& out, PG::ObjectInfo& oi) 
{
  return out << "object[" << hex << oi.oid << dec 
			 << " v " << oi.version 
			 << " osd" << oi.osd
			 << "]";
}

inline ostream& operator<<(ostream& out, PG::PGInfo& pgi) 
{
  return out << "pgi(" << hex << pgi.pgid << dec 
			 << " v " << pgi.last_update << "/" << pgi.last_complete
			 << " e " << pgi.last_epoch_started << "/" << pgi.last_epoch_finished
			 << ")";
}

inline ostream& operator<<(ostream& out, PG& pg)
{
  out << "pg[" << pg.info 
	  << " " << pg.get_role();
  if (pg.is_active()) out << " active";
  if (pg.is_clean()) out << " clean";
  if (pg.is_stray()) out << " stray";
  out << "]";
  return out;
}


#endif
