// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"

#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"

#include "common/DecayCounter.h"

#include <list>
#include <string>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs


class OSD;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGInfo;


struct PGPool {
  int id;
  atomic_t nref;
  int num_pg;

  pg_pool_t info;      
  SnapContext snapc;   // the default pool snapc, ready to go.

  interval_set<snapid_t> removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(int i) : id(i), num_pg(0) {}

  void get() { nref.inc(); }
  void put() {
    if (nref.dec() == 0)
      delete this;
  }
};


/** PG - Replica Placement Group
 *
 */

class PG {
public:
  
  /*
   * PG::Info - summary of PG statistics.
   *
   * some notes: 
   *  - last_complete implies we have all objects that existed as of that
   *    stamp, OR a newer object, OR have already applied a later delete.
   *  - if last_complete >= log.bottom, then we know pg contents thru log.top.
   *    otherwise, we have no idea what the pg is supposed to contain.
   */
  struct Info {
    pg_t pgid;
    eversion_t last_update;    // last object version applied to store.
    eversion_t last_complete;  // last version pg was complete through.

    eversion_t log_bottom;     // oldest log entry.
    bool       log_backlog;    // do we store a complete log?

    set<snapid_t> snap_trimq; // snaps we need to trim

    pg_stat_t stats;

    struct History {
      epoch_t epoch_created;       // epoch in which PG was created
      epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)

      epoch_t same_since;          // same acting set since
      epoch_t same_primary_since;  // same primary at least back through this epoch.
      History() : 	      
	epoch_created(0),
	last_epoch_started(0),
	same_since(0), same_primary_since(0) {}

      void merge(const History &other) {
	if (epoch_created < other.epoch_created)
	  epoch_created = other.epoch_created;
	if (last_epoch_started < other.last_epoch_started)
	  last_epoch_started = other.last_epoch_started;
      }

      void encode(bufferlist &bl) const {
	::encode(epoch_created, bl);
	::encode(last_epoch_started, bl);
	::encode(same_since, bl);
	::encode(same_primary_since, bl);
      }
      void decode(bufferlist::iterator &bl) {
	::decode(epoch_created, bl);
	::decode(last_epoch_started, bl);
	::decode(same_since, bl);
	::decode(same_primary_since, bl);
      }
    } history;
    
    Info(pg_t p=0) : pgid(p), 
                     log_backlog(false)
    { }
    bool is_uptodate() const { return last_update == last_complete; }
    bool is_empty() const { return last_update.version == 0; }
    bool dne() const { return history.epoch_created == 0; }

    void encode(bufferlist &bl) const {
      __u8 v = CEPH_OSD_ONDISK_VERSION;
      ::encode(v, bl);

      ::encode(pgid, bl);
      ::encode(last_update, bl);
      ::encode(last_complete, bl);
      ::encode(log_bottom, bl);
      ::encode(log_backlog, bl);
      ::encode(stats, bl);
      history.encode(bl);
      ::encode(snap_trimq, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 v = CEPH_OSD_ONDISK_VERSION;
      ::decode(v, bl);

      ::decode(pgid, bl);
      ::decode(last_update, bl);
      ::decode(last_complete, bl);
      ::decode(log_bottom, bl);
      ::decode(log_backlog, bl);
      ::decode(stats, bl);
      history.decode(bl);
      ::decode(snap_trimq, bl);
    }
  };
  WRITE_CLASS_ENCODER(Info::History)
  WRITE_CLASS_ENCODER(Info)

  
  /** 
   * Query - used to ask a peer for information about a pg.
   *
   * note: if version=0, type=LOG, then we just provide our full log.
   *   only if type=BACKLOG do we generate a backlog and provide that too.
   */
  struct Query {
    const static int INFO = 0;
    const static int LOG = 1;
    const static int BACKLOG = 3;
    const static int MISSING = 4;
    const static int FULLLOG = 5;

    __s32 type;
    eversion_t since;
    Info::History history;

    Query() : type(-1) {}
    Query(int t, Info::History& h) : 
      type(t), history(h) { assert(t != LOG); }
    Query(int t, eversion_t s, Info::History& h) : 
      type(t), since(s), history(h) { assert(t == LOG); }

    void encode(bufferlist &bl) const {
      ::encode(type, bl);
      ::encode(since, bl);
      history.encode(bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(type, bl);
      ::decode(since, bl);
      history.decode(bl);
    }
  };
  WRITE_CLASS_ENCODER(Query)

  
  /*
   * Log - incremental log of recent pg changes.
   *  also, serves as a recovery queue.
   *
   * when backlog is true, 
   *  objects with versions <= bottom are in log.
   *  we do not have any deletion info before that time, however.
   *  log is a "summary" in that it contains all objects in the PG.
   */
  struct Log {
    /** Entry
     */
    struct Entry {
      const static int LOST = 0;
      const static int MODIFY = 1;
      const static int CLONE = 2;  
      const static int DELETE = 3;
      const static int BACKLOG = 4;  // event invented by generate_backlog

      __s32      op;   // write, zero, trunc, remove
      sobject_t  soid;
      snapid_t   snap;
      eversion_t version, prior_version;
      osd_reqid_t reqid;  // caller+tid to uniquely identify request
      utime_t     mtime;  // this is the _user_ mtime, mind you
      bufferlist snaps;   // only for clone entries
      
      Entry() : op(0) {}
      Entry(int _op, const sobject_t& _soid,
	    const eversion_t& v, const eversion_t& pv,
	    const osd_reqid_t& rid, const utime_t& mt) :
        op(_op), soid(_soid), version(v),
	prior_version(pv), 
	reqid(rid), mtime(mt) {}
      
      bool is_delete() const { return op == DELETE; }
      bool is_clone() const { return op == CLONE; }
      bool is_modify() const { return op == MODIFY; }
      bool is_backlog() const { return op == BACKLOG; }
      bool is_update() const { return is_clone() || is_modify() || is_backlog(); }

      void encode(bufferlist &bl) const {
	::encode(op, bl);
	::encode(soid, bl);
	::encode(version, bl);
	::encode(prior_version, bl);
	::encode(reqid, bl);
	::encode(mtime, bl);
	if (op == CLONE)
	  ::encode(snaps, bl);
      }
      void decode(bufferlist::iterator &bl) {
	::decode(op, bl);
	::decode(soid, bl);
	::decode(version, bl);
	::decode(prior_version, bl);
	::decode(reqid, bl);
	::decode(mtime, bl);
	if (op == CLONE)
	  ::decode(snaps, bl);
      }
    };
    WRITE_CLASS_ENCODER(Entry)

    /*
     *    top - newest entry (update|delete)
     * bottom - entry previous to oldest (update|delete) for which we have
     *          complete negative information.  
     * i.e. we can infer pg contents for any store whose last_update >= bottom.
     */
    eversion_t top;       // newest entry (update|delete)
    eversion_t bottom;    // version prior to oldest (update|delete) 

    /*
     * backlog - true if log is a complete summary of pg contents.
     * updated will include all items in pg, but deleted will not
     * include negative entries for items deleted prior to 'bottom'.
     */
    bool backlog;
    
    list<Entry> log;  // the actual log.

    Log() : backlog(false) {}

    void clear() {
      eversion_t z;
      top = bottom = z;
      backlog = false;
      log.clear();
    }
    bool empty() const {
      return top.version == 0 && top.epoch == 0;
    }

    void encode(bufferlist& bl) const {
      ::encode(top, bl);
      ::encode(bottom, bl);
      ::encode(backlog, bl);
      ::encode(log, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(top, bl);
      ::decode(bottom, bl);
      ::decode(backlog, bl);
      ::decode(log, bl);
    }

    void copy_after(const Log &other, eversion_t v);
    bool copy_after_unless_divergent(const Log &other, eversion_t split, eversion_t floor);
    void copy_non_backlog(const Log &other);
    ostream& print(ostream& out) const;
  };
  WRITE_CLASS_ENCODER(Log)

  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public Log {
    hash_map<sobject_t,Entry*> objects;  // ptrs into log.  be careful!
    hash_set<osd_reqid_t>      caller_ops;

    // recovery pointers
    list<Entry>::iterator complete_to;  // not inclusive of referenced item
    sobject_t last_requested;           // last object requested by primary

    /****/
    IndexedLog() {}

    void clear() {
      assert(0);
      unindex();
      Log::clear();
      reset_recovery_pointers();
    }
    void reset_recovery_pointers() {
      complete_to = log.end();
      last_requested = sobject_t();
    }

    bool logged_object(const sobject_t& oid) const {
      return objects.count(oid);
    }
    bool logged_req(const osd_reqid_t &r) const {
      return caller_ops.count(r);
    }

    void index() {
      objects.clear();
      caller_ops.clear();
      for (list<Entry>::iterator i = log.begin();
           i != log.end();
           i++) {
        objects[i->soid] = &(*i);
        caller_ops.insert(i->reqid);
      }
    }

    void index(Entry& e) {
      if (objects.count(e.soid) == 0 || 
          objects[e.soid]->version < e.version)
        objects[e.soid] = &e;
      if (e.op != Entry::BACKLOG)
	caller_ops.insert(e.reqid);
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
    }
    void unindex(Entry& e) {
      // NOTE: this only works if we remove from the _bottom_ of the log!
      assert(e.op == Entry::BACKLOG || caller_ops.count(e.reqid));
      if (objects.count(e.soid) && objects[e.soid]->version == e.version)
        objects.erase(e.soid);
      caller_ops.erase(e.reqid);
    }


    // accessors
    Entry *is_updated(const sobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_update()) return objects[oid];
      return 0;
    }
    Entry *is_deleted(const sobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_delete()) return objects[oid];
      return 0;
    }
    
    // actors
    void add(Entry& e) {
      // add to log
      log.push_back(e);
      assert(e.version > top);
      assert(top.version == 0 || e.version.version > top.version);
      top = e.version;

      // to our index
      objects[e.soid] = &(log.back());
      caller_ops.insert(e.reqid);
    }

    void trim(ObjectStore::Transaction &t, eversion_t s);
    void trim_write_ahead(eversion_t last_update);


    ostream& print(ostream& out) const;
  };
  

  /**
   * OndiskLog - some info about how we store the log on disk.
   */
  class OndiskLog {
  public:
    // ok
    __u64 bottom;                     // first byte of log. 
    __u64 top;                        // byte following end of log.
    map<__u64,eversion_t> block_map;  // offset->version of _last_ entry with _any_ bytes in each block

    OndiskLog() : bottom(0), top(0) {}

    __u64 length() { return top - bottom; }
    bool trim_to(eversion_t v, ObjectStore::Transaction& t);

    void encode(bufferlist& bl) const {
      ::encode(bottom, bl);
      ::encode(top, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(bottom, bl);
      ::decode(top, bl);
    }
  };
  WRITE_CLASS_ENCODER(OndiskLog)


  /*
   * Missing - summary of missing objects.
   *  kept in memory, as a supplement to Log.
   *  also used to pass missing info in messages.
   */
  struct Missing {
    struct item {
      eversion_t need, have;
      item() {}
      item(eversion_t n) : need(n) {}  // have no old version
      item(eversion_t n, eversion_t h) : need(n), have(h) {}
      void encode(bufferlist& bl) const {
	::encode(need, bl);
	::encode(have, bl);
      }
      void decode(bufferlist::iterator& bl) {
	::decode(need, bl);
	::decode(have, bl);
      }
    }; 
    WRITE_CLASS_ENCODER(item)

    map<sobject_t, item> missing;         // oid -> (need v, have v)
    map<eversion_t, sobject_t> rmissing;  // v -> oid

    unsigned num_missing() const { return missing.size(); }

    void swap(Missing& o) {
      missing.swap(o.missing);
      rmissing.swap(o.rmissing);
    }

    bool is_missing(const sobject_t& oid) {
      return missing.count(oid);
    }
    bool is_missing(const sobject_t& oid, eversion_t v) {
      return missing.count(oid) && missing[oid].need <= v;
    }
    eversion_t have_old(const sobject_t& oid) {
      return missing.count(oid) ? missing[oid].have : eversion_t();
    }
    
    /*
     * this needs to be called in log order as we extend the log.  it
     * assumes missing is accurate up through the previous log entry.
     */
    void add_next_event(Log::Entry& e) {
      if (e.is_update()) {
	if (e.prior_version == eversion_t()) {
	  // new object.
	  //assert(missing.count(e.soid) == 0);  // might already be missing divergent item.
	  if (missing.count(e.soid))  // already missing divergent item
	    rmissing.erase(missing[e.soid].need);
	  missing[e.soid] = item(e.version, eversion_t());  // .have = nil
	} else if (missing.count(e.soid)) {
	  // already missing (prior).
	  //assert(missing[e.soid].need == e.prior_version);
	  rmissing.erase(missing[e.soid].need);
	  missing[e.soid].need = e.version;  // leave .have unchanged.
	} else {
	  // not missing, we must have prior_version (if any)
	  missing[e.soid] = item(e.version, e.prior_version);
	}
	rmissing[e.version] = e.soid;
      } else
	rm(e.soid, e.version);
    }

    void add_event(Log::Entry& e) {
      if (e.is_update()) {
	if (missing.count(e.soid)) {
	  if (missing[e.soid].need >= e.version)
	    return;   // already missing same or newer.
	  // missing older, revise need
	  rmissing.erase(missing[e.soid].need);
	  missing[e.soid].need = e.version;
	} else 
	  // not missing => have prior_version (if any)
	  missing[e.soid] = item(e.version, e.prior_version);
	rmissing[e.version] = e.soid;
      } else
	rm(e.soid, e.version);
    }

    void revise_need(sobject_t oid, eversion_t need) {
      if (missing.count(oid)) {
	rmissing.erase(missing[oid].need);
	missing[oid].need = need;            // no not adjust .have
      } else {
	missing[oid] = item(need, eversion_t());
      }
      rmissing[need] = oid;
    }

    void add(const sobject_t& oid, eversion_t need, eversion_t have) {
      missing[oid] = item(need, have);
      rmissing[need] = oid;
    }
    
    void rm(const sobject_t& oid, eversion_t when) {
      if (missing.count(oid) && missing[oid].need < when) {
        rmissing.erase(missing[oid].need);
        missing.erase(oid);
      }
    }
    void got(const sobject_t& oid, eversion_t v) {
      assert(missing.count(oid));
      assert(missing[oid].need <= v);
      rmissing.erase(missing[oid].need);
      missing.erase(oid);
    }

    void encode(bufferlist &bl) const {
      ::encode(missing, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(missing, bl);

      for (map<sobject_t,item>::iterator it = missing.begin();
           it != missing.end();
           it++) 
        rmissing[it->second.need] = it->first;
    }
  };
  WRITE_CLASS_ENCODER(Missing)


  /*** PG ****/
protected:
  OSD *osd;
  PGPool *pool;

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  Cond _cond;
  atomic_t ref;
  bool deleted;

public:
  void lock(bool no_lockdep=false) {
    //generic_dout(0) << this << " " << info.pgid << " lock" << dendl;
    _lock.Lock(no_lockdep);
  }
  void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    _lock.Unlock();
  }
  void wait() {
    assert(_lock.is_locked());
    _cond.Wait(_lock);
  }
  void kick() {
    assert(_lock.is_locked());
    _cond.Signal();
  }

  void get() {
    //generic_dout(0) << this << " " << info.pgid << " get " << ref.test() << dendl;
    //assert(_lock.is_locked());
    ref.inc();
  }
  void put() { 
    //generic_dout(0) << this << " " << info.pgid << " put " << ref.test() << dendl;
    if (ref.dec() == 0)
      delete this;
  }


  list<Message*> op_queue;  // op queue


  void mark_deleted() { deleted = true; }
  bool is_deleted() { return deleted; }

  bool dirty_info, dirty_log;

public:
  struct Interval {
    vector<int> acting;
    epoch_t first, last;
    bool maybe_went_rw;

    void encode(bufferlist& bl) const {
      ::encode(first, bl);
      ::encode(last, bl);
      ::encode(acting, bl);
      ::encode(maybe_went_rw, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(first, bl);
      ::decode(last, bl);
      ::decode(acting, bl);
      ::decode(maybe_went_rw, bl);
    }
  };
  WRITE_CLASS_ENCODER(Interval)

  // pg state
  Info        info;
  IndexedLog  log;
  sobject_t    log_oid;
  OndiskLog   ondisklog;
  Missing     missing;
  map<sobject_t, set<int> > missing_loc;
  
  set<snapid_t> snap_collections;
  map<epoch_t,Interval> past_intervals;

  xlist<PG*>::item recovery_item, backlog_item, scrub_item, snap_trim_item, stat_queue_item;
  int recovery_ops_active;
#ifdef DEBUG_RECOVERY_OIDS
  set<sobject_t> recovering_oids;
#endif

  epoch_t generate_backlog_epoch;  // epoch we decided to build a backlog.
  utime_t replay_until;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above

public:
  eversion_t  last_complete_ondisk;  // last_complete that has committed.

  // primary state
 public:
  vector<int> acting;
  map<int,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  // [primary only] content recovery state
  bool        have_master_log;
 protected:
  set<int>    prior_set;   // current+prior OSDs, as defined by info.history.last_epoch_started.
  set<int>    prior_set_down;          // down osds exluded from prior_set
  map<int,epoch_t> prior_set_up_thru;  // osds whose up_thru we care about
  bool        must_notify_mon;
  bool        need_up_thru;
  set<int>    stray_set;   // non-acting osds that have PG data.
  set<int>    uptodate_set;  // current OSDs that are uptodate
  eversion_t  oldest_update; // lowest (valid) last_update in active set
  map<int,Info>        peer_info;   // info from peers (stray or prior)
  set<int>             peer_info_requested;
  map<int, Missing>    peer_missing;
  set<int>             peer_log_requested;  // logs i've requested (and start stamps)
  set<int>             peer_summary_requested;
  friend class OSD;


  // pg waiters
  list<class Message*>            waiting_for_active;
  hash_map<sobject_t, 
           list<class Message*> > waiting_for_missing_object;   
  map<eversion_t,class MOSDOp*>   replay_queue;
  
  hash_map<sobject_t, list<Message*> > waiting_for_wr_unlock; 

  bool block_if_wrlocked(MOSDOp* op, object_info_t& oi);


  // stats
  hash_map<sobject_t, DecayCounter> stat_object_temp_rd;

  Mutex pg_stats_lock;
  bool pg_stats_valid;
  pg_stat_t pg_stats_stable;

  void update_stats();
  void clear_stats();

public:
  void clear_primary_state();

 public:
  bool is_acting(int osd) const { 
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] == osd) return true;
    return false;
  }
  bool is_prior(int osd) const { return prior_set.count(osd); }
  bool is_stray(int osd) const { return stray_set.count(osd); }
  
  bool is_all_uptodate() const { return uptodate_set.size() == acting.size(); }

  void generate_past_intervals();
  void trim_past_intervals();
  void build_prior();
  void clear_prior();
  bool prior_set_affected(OSDMap *map);

  bool calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    for (unsigned i=1; i<acting.size(); i++) {
      if (peer_last_complete_ondisk.count(acting[i]) == 0)
	return false;   // we don't have complete info
      eversion_t a =peer_last_complete_ondisk[acting[i]];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return false;
    min_last_complete_ondisk = min;
    return true;
  }

  virtual void calc_trim_to() = 0;

  void proc_replica_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, Missing& omissing, int from);
  bool merge_old_entry(ObjectStore::Transaction& t, Log::Entry& oe);
  void merge_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, Missing& omissing, int from);
  void search_for_missing(Log &olog, Missing &omissing, int fromosd);
  
  bool build_backlog_map(map<eversion_t,Log::Entry>& omap);
  void assemble_backlog(map<eversion_t,Log::Entry>& omap);
  void drop_backlog();
  
  void trim_write_ahead();

  bool recover_master_log(map< int, map<pg_t,Query> >& query_map);
  void peer(ObjectStore::Transaction& t, 
	    map< int, map<pg_t,Query> >& query_map,
	    map<int, MOSDPGInfo*> *activator_map=0);
  void activate(ObjectStore::Transaction& t, 
		map<int, MOSDPGInfo*> *activator_map=0);

  virtual void clean_up_local(ObjectStore::Transaction& t) = 0;

  virtual int start_recovery_ops(int max) = 0;

  void purge_strays();

  Context *finish_sync_event;

  void finish_recovery();
  void _finish_recovery(Context *c);
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  void defer_recovery();
  void start_recovery_op(const sobject_t& soid);
  void finish_recovery_op(const sobject_t& soid, bool dequeue=false);

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;


  // -- scrub --
  map<int,ScrubMap> peer_scrub_map;

  void repair_object(ScrubMap::object *po, int bad_peer, int ok_peer);
  void scrub();
  void build_scrub_map(ScrubMap &map);
  virtual int _scrub(ScrubMap &map, int& errors, int& fixed) { return 0; }

  void sub_op_scrub(class MOSDSubOp *op);
  void sub_op_scrub_reply(class MOSDSubOpReply *op);


 public:  
  PG(OSD *o, PGPool *_pool, pg_t p, const sobject_t& oid) : 
    osd(o), pool(_pool),
    _lock("PG::_lock"),
    ref(0), deleted(false), dirty_info(false), dirty_log(false),
    info(p), log_oid(oid),
    recovery_item(this), backlog_item(this), scrub_item(this), snap_trim_item(this), stat_queue_item(this),
    recovery_ops_active(0),
    generate_backlog_epoch(0),
    role(0),
    state(0),
    have_master_log(true),
    must_notify_mon(false), need_up_thru(false),
    pg_stats_lock("PG::pg_stats_lock"),
    pg_stats_valid(false),
    finish_sync_event(NULL)
  {
    pool->get();
  }
  virtual ~PG() {
    pool->put();
  }
  
  pg_t       get_pgid() const { return info.pgid; }
  int        get_nrep() const { return acting.size(); }

  int        get_primary() { return acting.empty() ? -1:acting[0]; }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  bool       is_primary() const { return role == PG_ROLE_HEAD; }
  bool       is_replica() const { return role > 0; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_peering() const { return state_test(PG_STATE_PEERING); }
  bool       is_crashed() const { return state_test(PG_STATE_CRASHED); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  //bool       is_complete()    { return state_test(PG_STATE_COMPLETE); }
  bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool       is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool       is_stray() const { return state_test(PG_STATE_STRAY); }

  bool       is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  //bool is_complete_pg() { return acting.size() == info.pgid.size(); }

  void add_log_entry(Log::Entry& e, bufferlist& log_bl);

  // pg on-disk state
  void write_info(ObjectStore::Transaction& t);
  void write_log(ObjectStore::Transaction& t);
  void append_log(ObjectStore::Transaction &t, bufferlist& bl,
		  eversion_t log_version);
  void read_log(ObjectStore *store);
  void trim(ObjectStore::Transaction& t, eversion_t v);
  void trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v);
  void trim_peers();

  void read_state(ObjectStore *store);
  coll_t make_snap_collection(ObjectStore::Transaction& t, snapid_t sn);

  void queue_snap_trim();

  bool is_dup(osd_reqid_t rid) {
    return log.logged_req(rid);
  }



  // abstract bits
  virtual bool preprocess_op(MOSDOp *op, utime_t now) { return false; } 
  virtual void do_op(MOSDOp *op) = 0;
  virtual void do_sub_op(MOSDSubOp *op) = 0;
  virtual void do_sub_op_reply(MOSDSubOpReply *op) = 0;
  virtual bool snap_trimmer() = 0;

  virtual bool same_for_read_since(epoch_t e) = 0;
  virtual bool same_for_modify_since(epoch_t e) = 0;
  virtual bool same_for_rep_modify_since(epoch_t e) = 0;

  virtual bool is_write_in_progress() = 0;
  virtual bool is_missing_object(const sobject_t& oid) = 0;
  virtual void wait_for_missing_object(const sobject_t& oid, Message *op) = 0;

  virtual void on_osd_failure(int osd) = 0;
  virtual void on_role_change() = 0;
  virtual void on_change() = 0;
  virtual void on_shutdown() = 0;
};

WRITE_CLASS_ENCODER(PG::Info::History)
WRITE_CLASS_ENCODER(PG::Info)
WRITE_CLASS_ENCODER(PG::Query)
WRITE_CLASS_ENCODER(PG::Missing::item)
WRITE_CLASS_ENCODER(PG::Missing)
WRITE_CLASS_ENCODER(PG::Log::Entry)
WRITE_CLASS_ENCODER(PG::Log)
WRITE_CLASS_ENCODER(PG::Interval)
WRITE_CLASS_ENCODER(PG::OndiskLog)

inline ostream& operator<<(ostream& out, const PG::Info::History& h) 
{
  return out << "ec=" << h.epoch_created
	     << " les=" << h.last_epoch_started
	     << " " << h.same_since << "/" << h.same_primary_since;
}

inline ostream& operator<<(ostream& out, const PG::Info& pgi) 
{
  out << pgi.pgid << "(";
  if (pgi.dne())
    out << " DNE";
  if (pgi.is_empty())
    out << " empty";
  else
    out << " v " << pgi.last_update << "/" << pgi.last_complete
        << " (" << pgi.log_bottom << "," << pgi.last_update << "]"
        << (pgi.log_backlog ? "+backlog":"");
  //out << " c " << pgi.epoch_created;
  out << " n=" << pgi.stats.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Log::Entry& e)
{
  return out << e.version << " (" << e.prior_version << ")"
             << (e.is_delete() ? " - ":
		 (e.is_clone() ? " c ":
		  (e.is_modify() ? " m ":
		   (e.is_backlog() ? " b ":
		    " ? "))))
             << e.soid << " by " << e.reqid << " " << e.mtime;
}

inline ostream& operator<<(ostream& out, const PG::Log& log) 
{
  out << "log(" << log.bottom << "," << log.top << "]";
  if (log.backlog) out << "+backlog";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Missing::item& i) 
{
  out << i.need;
  if (i.have != eversion_t())
    out << "(" << i.have << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Missing& missing) 
{
  out << "missing(" << missing.num_missing();
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Interval& i)
{
  out << "interval(" << i.first << "-" << i.last << " " << i.acting;
  if (i.maybe_went_rw)
    out << " maybe_went_rw";
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info 
      << " r=" << pg.get_role();
  
  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  if (pg.log.bottom != pg.info.log_bottom ||
      pg.log.top != pg.info.last_update)
    out << " (info mismatch, " << pg.log << ")";

  if (pg.log.log.empty()) {
    // shoudl it be?
    if (pg.log.top.version - pg.log.bottom.version != 0) {
      out << " (log bound mismatch, empty)";
    }
  } else {
    if (((pg.log.log.begin()->version.version - 1 != pg.log.bottom.version) &&
         !pg.log.backlog) ||
        (pg.log.log.rbegin()->version.version != pg.log.top.version)) {
      out << " (log bound mismatch, actual=["
	  << pg.log.log.begin()->version << ","
	  << pg.log.log.rbegin()->version << "] len=" << pg.log.log.size() << ")";
    }
  }

  if (pg.last_complete_ondisk != pg.info.last_complete)
    out << " lcod " << pg.last_complete_ondisk;

  if (pg.get_role() == 0) {
    out << " mlcod " << pg.min_last_complete_ondisk;
    if (!pg.have_master_log)
      out << " !hml";
  }

  out << " " << pg_state_string(pg.get_state());

  //out << " (" << pg.log.bottom << "," << pg.log.top << "]";
  if (pg.missing.num_missing())
    out << " m=" << pg.missing.num_missing();
  if (pg.is_primary()) {
    int lost = pg.missing.num_missing() - pg.missing_loc.size();
    if (lost)
      out << " l=" << lost;
  }
  if (pg.info.snap_trimq.size())
    out << " snaptrimq=" << pg.info.snap_trimq;
  out << "]";


  return out;
}



#endif
