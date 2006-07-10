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
	epoch_t same_role_since;     // upper bound: i have held same role since
	PGInfo(pg_t p=0) : pgid(p), 
					   last_update(0), last_complete(0), 
					   last_epoch_started(0), last_epoch_finished(0),
					   same_primary_since(0), same_role_since(0) {}
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
	  Entry() {}
	  Entry(object_t o, version_t v, bool d=false) :
		oid(o), version(v), deleted(d) {}
	};

	/** top, bottom
	 *    top - newest entry (update|delete)
	 * bottom - entry previous to oldest (update|delete) for which we have
	 *          complete negative information.  
	 * i.e. we can infer pg contents for any store whose last_update >= bottom.
	 */
	version_t top;       // newest entry (update|delete)
	version_t bottom;    // version prior to oldest (update|delete) 

	/** backlog - true if log is a complete summary of pg contents.  
	 * updated will include all items in pg, but deleted will not include
	 * negative entries for items deleted prior to 'bottom'.
	 */
	bool      backlog;

	/** update, deleted maps **/
	map<object_t, version_t> updated;  //  oid -> v. 
	map<version_t, object_t> rupdated; //    v -> oid.
	map<object_t, version_t> deleted;  //  oid -> when.  
	map<version_t, object_t> rdeleted; // when -> oid.
	
	/****/
	PGLog() : top(0), bottom(0), backlog(false) {}

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
	  blist.append((char*)&backlog, sizeof(backlog));
	  ::_encode(updated, blist);
	  ::_encode(deleted, blist);
	}
	void _decode(bufferlist& blist, int& off) {
	  blist.copy(off, sizeof(top), (char*)&top);
	  off += sizeof(top);
	  blist.copy(off, sizeof(bottom), (char*)&bottom);
	  off += sizeof(bottom);
	  blist.copy(off, sizeof(backlog), (char*)&backlog);
	  off += sizeof(backlog);

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

	void trim(ObjectStore::Transaction &t, version_t s);
	void copy_after(const PGLog &other, version_t v);
	ostream& print(ostream& out) const;
  };
  

  class PGOndiskLog {
  public:
	off_t bottom, top;
	map<off_t,version_t> block_map;  // block -> first stamp logged there

	PGOndiskLog() : bottom(0), top(0) {}

	bool trim_to(version_t v) {
	  map<off_t,version_t>::iterator p = block_map.upper_bound(v);
	  if (p == block_map.begin()) return false;
	  p--;
	  if (p == block_map.begin()) return false;

	  while (1) {
		map<off_t,version_t>::iterator t = block_map.begin();
		if (t == p) break;
		block_map.erase(t);
	  }
	  bottom = p->first;
	  return true;	  
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
  version_t   last_complete_commit;

 protected:
  // [primary only] content recovery state
  version_t   peers_complete_thru;
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

  map<__uint64_t, class OSDReplicaOp*> replica_ops;
  map<int, set<__uint64_t> > replica_tids_by_osd; // osd -> (tid,...)


  // [primary|replica]
  // pg waiters
  list<class Message*>            waiting_for_active;
  hash_map<object_t, 
		   list<class Message*> > waiting_for_missing_object;   
  
  // recovery
  version_t                requested_thru;
  map<object_t, version_t> objects_pulling;  // which objects are currently being pulled
  
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
	peers_complete_thru = 0;
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

  bool adjust_peers_complete_thru() {
	version_t t = info.last_complete;
	for (unsigned i=1; i<acting.size(); i++) 
	  if (peer_info[i].last_complete < t)
		t = peer_info[i].last_complete;
	if (t > peers_complete_thru) {
	  peers_complete_thru = t;
	  return true;
	}
	return false;
  }
 
  void generate_backlog(PGLog& log);      // generate summary backlog by scanning store.
  void merge_log(const PGLog &olog);

  void peer(map< int, map<pg_t,version_t> >& query_map);

  bool do_recovery();
  void start_recovery() {
	requested_thru = 0;
	do_recovery();
  }

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
	state(0),
	last_epoch_started_any(0),
	last_complete_commit(0),
	peers_complete_thru(0),
	requested_thru(0)
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

  bool  is_empty() const { return info.last_complete == 0; }

  int num_active_ops() const {
	return objects_pulling.size();
  }


  // pg on-disk state
  void append_log(ObjectStore::Transaction& t, PG::PGLog::Entry& logentry, version_t trim_to);
  void read_log(ObjectStore *store);

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
  out << "log(" << log.bottom << "," << log.top << "]";
  if (log.backlog) out << "+backlog";
  return out;
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
