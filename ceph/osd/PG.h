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

#include "include/types.h"

#include <list>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


class OSD;


#define PG_QUERY_INFO      ((version_t)0xffffffffffffffffULL)
#define PG_QUERY_SUMMARY   ((version_t)0xfffffffffffffffeULL)


/** PG - Replica Placement Group
 *
 */

class PG {
public:
  
  /*
   * PGInfo - summary of PG statistics.
   */
  struct PGInfo {
	pg_t pgid;
	version_t last_update;    // last object version logged/updated.
	version_t last_complete;  // last version pg was complete through.
	version_t log_floor;      // oldest log entry.
	epoch_t last_epoch_started;  // last epoch started.
	epoch_t last_epoch_finished; // last epoch finished.
	epoch_t same_primary_since;  // upper bound: same primary at least back through this epoch.
	PGInfo(pg_t p=0) : pgid(p), 
					   last_update(0), last_complete(0),
					   last_epoch_started(0), last_epoch_finished(0),
					   same_primary_since(0) {}
	bool is_clean() { return last_update == last_complete; }
  };
  
  /*
   * PGSummary - snapshot of full pg contents
   */
  class PGSummary {
  public:
	map<object_t, version_t> objects;  // objects i currently store.
	
	void _encode(bufferlist& blist) {
	  ::_encode(objects, blist);
	}
	void _decode(bufferlist& blist, int& off) {
	  ::_decode(objects, blist, off);
	}
  };

  /*
   * PGMissing - summary of missing objects.
   *  kept in memory, as a supplement to PGLog.
   *  also used to pass missing info in messages.
   */
  class PGMissing {
  public:
	map<object_t, version_t> missing;   // oid -> v
	map<version_t, object_t> rmissing;  // v -> oid

	map<object_t, int>       loc;       // where i think i can get them.

	int num_lost() const { return missing.size() - loc.size(); }
	int num_missing() const { return missing.size(); }

	bool is_missing(object_t oid, version_t v) {
	  return missing.count(oid) && missing[oid] == v;
	}
	void add(object_t oid, version_t v) {
	  if (missing.count(oid)) rmissing.erase(missing[oid]);
	  missing[oid] = v;
	  rmissing[v] = oid;
	}
	void rm(object_t oid, version_t when) {
	  if (missing.count(oid) && missing[oid] < when) {
		rmissing.erase(missing[oid]);
		missing.erase(oid);
		loc.erase(oid);
	  }		
	}
	void got(object_t oid, version_t v) {
	  assert(missing.count(oid));
	  assert(missing[oid] <= v);
	  loc.erase(oid);
	  rmissing.erase(missing[oid]);
	  missing.erase(oid);
	}

	void merge_loc(PGMissing &other) {
	  if (num_lost() > 0) {
		// see if we can find anything new!
		cout << "merge_loc " << endl;//*this << " from " << other << endl;
		for (map<object_t,version_t>::iterator p = missing.begin();
			 p != missing.end();
			 p++) {
		  assert(other.missing[p->first] >= missing[p->first]);
		  if (other.loc.count(p->first)) 
			loc[p->first] = other.loc[p->first];
		}
	  }
	}

	void _encode(bufferlist& blist) {
	  ::_encode(missing, blist);
	  ::_encode(loc, blist);
	}
	void _decode(bufferlist& blist, int& off) {
	  ::_decode(missing, blist, off);
	  ::_decode(loc, blist, off);

	  for (map<object_t,version_t>::iterator it = missing.begin();
		   it != missing.end();
		   it++) 
		rmissing[it->second] = it->first;
	}
  };

  /*
   * PGLog - incremental log of recent pg changes.
   *  summary of persistent on-disk copy:
   *   multiply-modified objects are implicitly trimmed from in-memory log.
   *  also, serves as a recovery queue.
   */
  class PGLog {
  public:
	// single entry in the written on-disk log.
	class Entry {   
	public:
	  object_t oid;
	  version_t version;
	  bool deleted;
	  Entry(object_t o, version_t v, bool d=false) :
		oid(o), version(v), deleted(d) {}
	};

	version_t top;           // corresponds to newest entry.
	version_t bottom;        // corresponds to entry prev to oldest entry (t=bottom is trimmed).
	map<object_t, version_t> updated;  // oid -> v. items > bottom, + version.
	map<version_t, object_t> rupdated; // v -> oid.
	map<object_t, version_t> deleted;  // oid -> when.  items <= bottom that no longer exist
	map<version_t, object_t> rdeleted; // when -> oid.
	
	PGLog() : top(0), bottom(0) {}

	bool empty() const {
	  return top == 0;
	}

	void _reverse(map<object_t, version_t> &fw, map<version_t, object_t> &bw) {
	  for (map<object_t,version_t>::iterator it = fw.begin();
		   it != fw.end();
		   it++) 
		bw[it->second] = it->first;
	}
	void _encode(bufferlist& blist) const {
	  blist.append((char*)&top, sizeof(top));
	  blist.append((char*)&bottom, sizeof(bottom));
	  ::_encode(updated, blist);
	  ::_encode(deleted, blist);
	}
	void _decode(bufferlist& blist, int& off) {
	  blist.copy(off, sizeof(top), (char*)&top);
	  off += sizeof(top);
	  blist.copy(off, sizeof(bottom), (char*)&bottom);
	  off += sizeof(bottom);
	  ::_decode(updated, blist, off);
	  ::_decode(deleted, blist, off);

	  _reverse(updated, rupdated);
	  _reverse(deleted, rdeleted);
	}



	// accessors
	version_t is_updated(object_t oid) {
	  if (updated.count(oid)) return updated[oid];
	  return 0;
	}
	version_t is_deleted(object_t oid) {
	  if (deleted.count(oid)) return deleted[oid];
	  return 0;
	}

	// actors
	void add_update(object_t oid, version_t v) {
	  // superceding older update?
	  if (updated.count(oid)) {
		assert(v > updated[oid]);
		rupdated.erase(updated[oid]);
	  }

	  // superceding older delete?
	  if (deleted.count(oid)) {
		assert(v > deleted[oid]);      // future deletions or past mods impossible.
		rdeleted.erase(deleted[oid]);
		deleted.erase(oid);
	  }

	  // add this item
	  updated[oid] = v;
	  rupdated[v] = oid;

	  assert(v > top);
	  top = v;
	}
	void add_delete(object_t oid, version_t when) {
	  deleted[oid] = when;
	  rdeleted[when] = oid;
	  assert(when > top);
	  top = when;
	}

	void trim(version_t s);
	void copy_after(const PGLog &other, version_t v);
	void merge(const PGLog &other, PGMissing &missing);
	void assimilate_summary(const PGSummary &sum, version_t last_complete);
	ostream& print(ostream& out) const;
  };
  

  class PGOndiskLog {
	off_t pos;
  public:
	PGOndiskLog() : pos(0) {}

	off_t get_write_pos() {
	  return pos;
	}
	void inc_write_pos(int i) {
	  pos += i;
	}
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

public:
  // pg state
  PGInfo      info;
  PGLog       log;
  PGOndiskLog ondisklog;
  PGMissing   missing;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above

  // primary state
 public:
  vector<int> acting;
  epoch_t     last_epoch_started_any;

 protected:
  // [primary only] content recovery state
  set<int>    prior_set;   // current+prior OSDs, as defined by last_epoch_started_any.
  set<int>    stray_set;   // non-acting osds that have PG data.
  set<int>    clean_set;   // current OSDs that are clean
  map<int, PGInfo>      peer_info;  // info from peers (stray or prior)
  set<int>              peer_info_requested;
  //map<int, PGLog*>      peer_log;   // logs from peers (for recovering pg content)
  map<int, PGMissing>   peer_missing;
  map<int, version_t>   peer_log_requested;  // logs i've requested (and start stamps)
  //map<int, PGSummary*>  peer_summary;   // full contents of peers
  set<int>              peer_summary_requested;
  friend class OSD;

public:
  void clear_primary_recovery_state() {
	peer_info.clear();
	peer_missing.clear();
  }
  void clear_primary_state() {
	prior_set.clear();
	stray_set.clear();
	clean_set.clear();
	peer_info_requested.clear();
	peer_log_requested.clear();
	clear_primary_recovery_state();
  }

 public:
  bool is_acting(int osd) const { 
	for (unsigned i=0; i<acting.size(); i++)
	  if (acting[i] == osd) return true;
	return false;
  }
  bool is_prior(int osd) const { return prior_set.count(osd); }
  bool is_stray(int osd) const { return stray_set.count(osd); }
  
  bool is_all_clean() const { return clean_set.size() == acting.size(); }

  void build_prior();
  void adjust_prior();  // based on new peer_info.last_epoch_started_any

  // pg waiters
  list<class Message*>            waiting_for_active;
  hash_map<object_t, 
		   list<class Message*> > waiting_for_missing_object;   

  // recovery
  version_t                requested_thru;
  map<object_t, version_t> objects_pulling;  // which objects are currently being pulled
  
  void peer(map< int, map<pg_t,version_t> >& query_map);
  void generate_summary(PGSummary &summary);

  bool do_recovery();

  void clean_up_local();
  void clean_replicas();

  off_t get_log_write_pos() {
	return 0;
  }

 public:  
  PG(OSD *o, pg_t p) : 
	osd(o), 
	info(p),
	role(0),
	state(0)
  { }
  
  pg_t       get_pgid() const { return info.pgid; }
  int        get_primary() { return acting[0]; }
  int        get_nrep() const { return acting.size(); }

  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }
  void       calc_role(int whoami) {
	role = -1;
	for (unsigned i=0; i<acting.size(); i++)
	  if (acting[i] == whoami) role = i>0 ? 1:0;
  }
  bool       is_primary() const { return role == 0; }
  bool       is_residual() const { return role < 0; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }

  bool       is_active() const { return state_test(STATE_ACTIVE); }
  //bool       is_complete()    { return state_test(STATE_COMPLETE); }
  bool       is_clean() const { return state_test(STATE_CLEAN); }
  bool       is_stray() const { return state_test(STATE_STRAY); }

  int num_active_ops() const {
	return objects_pulling.size();
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


inline ostream& operator<<(ostream& out, const PG::PGInfo& pgi) 
{
  return out << "pginfo(" << hex << pgi.pgid << dec 
			 << " v " << pgi.last_update << "/" << pgi.last_complete
			 << " e " << pgi.last_epoch_started << "/" << pgi.last_epoch_finished
			 << ")";
}

inline ostream& operator<<(ostream& out, const PG::PGLog& log) 
{
  return out << "log(" << log.bottom << "," << log.top << "]";
}

inline ostream& operator<<(ostream& out, const PG::PGMissing& missing) 
{
  out << "missing(" << missing.num_missing();
  if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info 
	  << " r=" << pg.get_role();
  if (pg.is_active()) out << " active";
  if (pg.is_clean()) out << " clean";
  if (pg.is_stray()) out << " stray";
  out << " (" << pg.log.bottom << "," << pg.log.top << "]";
  if (pg.missing.num_missing()) out << " m=" << pg.missing.num_missing();
  if (pg.missing.num_lost()) out << " l=" << pg.missing.num_lost();
  out << "]";
  return out;
}


#endif
