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


class OSD;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGInfo;

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

    set<snapid_t> dead_snaps; // snaps we need to trim

    pg_stat_t stats;

    struct History {
      epoch_t epoch_created;       // epoch in which PG was created
      epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)

      epoch_t same_since;          // same acting set since
      epoch_t same_primary_since;  // same primary at least back through this epoch.
      epoch_t same_acker_since;    // same acker at least back through this epoch.
      History() : 	      
	epoch_created(0),
	last_epoch_started(0),
	same_since(0), same_primary_since(0), same_acker_since(0) {}

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
	::encode(same_acker_since, bl);
      }
      void decode(bufferlist::iterator &bl) {
	::decode(epoch_created, bl);
	::decode(last_epoch_started, bl);
	::decode(same_since, bl);
	::decode(same_primary_since, bl);
	::decode(same_acker_since, bl);
      }
    } history;
    
    Info(pg_t p=0) : pgid(p), 
                     log_backlog(false)
    { }
    bool is_uptodate() const { return last_update == last_complete; }
    bool is_empty() const { return last_update.version == 0; }
    bool dne() const { return history.epoch_created == 0; }

    void encode(bufferlist &bl) const {
      ::encode(pgid, bl);
      ::encode(last_update, bl);
      ::encode(last_complete, bl);
      ::encode(log_bottom, bl);
      ::encode(log_backlog, bl);
      ::encode(stats, bl);
      history.encode(bl);
      ::encode(dead_snaps, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(pgid, bl);
      ::decode(last_update, bl);
      ::decode(last_complete, bl);
      ::decode(log_bottom, bl);
      ::decode(log_backlog, bl);
      ::decode(stats, bl);
      history.decode(bl);
      ::decode(dead_snaps, bl);
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
    const static int BACKLOG = 2;
    const static int FULLLOG = 3;

    __s32 type;
    eversion_t split, floor;
    Info::History history;

    Query() : type(-1) {}
    Query(int t, Info::History& h) : 
      type(t), history(h) { assert(t != LOG); }
    Query(int t, eversion_t s, eversion_t f, Info::History& h) : 
      type(t), split(s), floor(f), history(h) { assert(t == LOG); }

    void encode(bufferlist &bl) const {
      ::encode(type, bl);
      ::encode(split, bl);
      ::encode(floor, bl);
      history.encode(bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(type, bl);
      ::decode(split, bl);
      ::decode(floor, bl);
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

      __s32      op;   // write, zero, trunc, remove
      object_t   oid;
      eversion_t version, prior_version;
      osd_reqid_t reqid;  // caller+tid to uniquely identify request
      bufferlist snaps;   // only for clone entries
      
      Entry() : op(0) {}
      Entry(int _op, object_t _oid,
	    const eversion_t& v, const eversion_t& pv,
	    const osd_reqid_t& rid) :
        op(_op), oid(_oid), version(v),
	prior_version(pv), 
	reqid(rid) {}
      
      bool is_delete() const { return op == DELETE; }
      bool is_clone() const { return op == CLONE; }
      bool is_modify() const { return op == MODIFY; }
      bool is_update() const { return is_clone() || is_modify(); }

      void encode(bufferlist &bl) const {
	::encode(op, bl);
	::encode(oid, bl);
	::encode(version, bl);
	::encode(prior_version, bl);
	::encode(reqid, bl);
	if (op == CLONE)
	  ::encode(snaps, bl);
      }
      void decode(bufferlist::iterator &bl) {
	::decode(op, bl);
	::decode(oid, bl);
	::decode(version, bl);
	::decode(prior_version, bl);
	::decode(reqid, bl);
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
    hash_map<object_t,Entry*> objects;  // ptrs into log.  be careful!
    hash_set<osd_reqid_t>     caller_ops;

    // recovery pointers
    list<Entry>::iterator requested_to; // not inclusive of referenced item
    list<Entry>::iterator complete_to;  // not inclusive of referenced item

    /****/
    IndexedLog() {}

    void clear() {
      assert(0);
      unindex();
      Log::clear();
      reset_recovery_pointers();
    }
    void reset_recovery_pointers() {
      requested_to = log.end();
      complete_to = log.end();
    }

    bool logged_object(object_t oid) const {
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
        objects[i->oid] = &(*i);
        caller_ops.insert(i->reqid);
      }
    }

    void index(Entry& e) {
      if (objects.count(e.oid) == 0 || 
          objects[e.oid]->version < e.version)
        objects[e.oid] = &e;
      caller_ops.insert(e.reqid);
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
    }
    void unindex(Entry& e) {
      // NOTE: this only works if we remove from the _bottom_ of the log!
      assert(objects.count(e.oid));
      if (objects[e.oid]->version == e.version)
        objects.erase(e.oid);
      caller_ops.erase(e.reqid);
    }


    // accessors
    Entry *is_updated(object_t oid) {
      if (objects.count(oid) && objects[oid]->is_update()) return objects[oid];
      return 0;
    }
    Entry *is_deleted(object_t oid) {
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
      objects[e.oid] = &(log.back());
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
    loff_t bottom;                     // first byte of log. 
    loff_t top;                        // byte following end of log.
    map<loff_t,eversion_t> block_map;  // block -> first stamp logged there

    OndiskLog() : bottom(0), top(0) {}

    bool trim_to(eversion_t v, ObjectStore::Transaction& t);
  };


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

    map<object_t, item> missing;         // oid -> (need v, have v)
    map<eversion_t, object_t> rmissing;  // v -> oid

    unsigned num_missing() const { return missing.size(); }

    bool is_missing(object_t oid) {
      return missing.count(oid);
    }
    bool is_missing(object_t oid, eversion_t v) {
      return missing.count(oid) && missing[oid].need <= v;
    }
    eversion_t have_old(object_t oid) {
      return missing.count(oid) ? missing[oid].have : eversion_t();
    }
    
    void add_event(Log::Entry& e) {
      if (e.is_update())
	add(e.oid, e.version, e.prior_version);
      else
	rm(e.oid, e.version);
    }

    void add(object_t oid, eversion_t need) {
      eversion_t have;
      add(oid, need, have);
    }
    void add(object_t oid, eversion_t need, eversion_t have) {
      if (missing.count(oid)) {
        if (missing[oid].need > need) return;   // already missing newer.
        rmissing.erase(missing[oid].need);
	missing[oid].need = need;  // don't have .have
      } else 
	missing[oid] = item(need, have);
      rmissing[need] = oid;
    }
    void rm(object_t oid, eversion_t when) {
      if (missing.count(oid) && missing[oid].need < when) {
        rmissing.erase(missing[oid].need);
        missing.erase(oid);
      }
    }
    void got(object_t oid, eversion_t v) {
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

      for (map<object_t,item>::iterator it = missing.begin();
           it != missing.end();
           it++) 
        rmissing[it->second.need] = it->first;
    }
  };
  WRITE_CLASS_ENCODER(Missing)


  /*** PG ****/
protected:
  OSD *osd;

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  atomic_t ref;
  bool deleted;

public:
  void lock() {
    //generic_dout(0) << this << " " << info.pgid << " lock" << dendl;
    _lock.Lock();
  }
  void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    _lock.Unlock();
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
  OndiskLog   ondisklog;
  Missing     missing;
  map<object_t, set<int> > missing_loc;
  
  set<snapid_t> snap_collections;
  map<epoch_t,Interval> past_intervals;

  xlist<PG*>::item recovery_item, scrub_item, snap_trim_item;
  int recovery_ops_active;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above

  // primary state
 public:
  vector<int> acting;
  eversion_t  last_complete_commit;

  // [primary only] content recovery state
  eversion_t  peers_complete_thru;
  bool        have_master_log;
 protected:
  set<int>    prior_set;   // current+prior OSDs, as defined by info.history.last_epoch_started.
  set<int>    prior_set_down;          // down osds exluded from prior_set
  map<int,epoch_t> prior_set_up_thru;  // osds whose up_thru we care about
  bool        must_notify_mon;
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
  hash_map<object_t, 
           list<class Message*> > waiting_for_missing_object;   
  map<eversion_t,class MOSDOp*>   replay_queue;
  
  hash_map<object_t, list<Message*> > waiting_for_wr_unlock; 

  bool block_if_wrlocked(MOSDOp* op);


  // stats
  hash_map<object_t, DecayCounter> stat_object_temp_rd;

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
  bool prior_set_affected(OSDMap *map);

  bool adjust_peers_complete_thru() {
    eversion_t t = info.last_complete;
    for (unsigned i=1; i<acting.size(); i++) 
      if (peer_info[i].last_complete < t)
        t = peer_info[i].last_complete;
    if (t > peers_complete_thru) {
      peers_complete_thru = t;
      return true;
    }
    return false;
  }

  void proc_replica_log(ObjectStore::Transaction& t, Log &olog, Missing& omissing, int from);
  void merge_old_entry(ObjectStore::Transaction& t, Log::Entry& oe);
  void merge_log(ObjectStore::Transaction& t, Log &olog, Missing& omissing, int from);
  void proc_replica_missing(Log &olog, Missing &omissing, int fromosd);
  
  void generate_backlog();
  void drop_backlog();
  
  void trim_write_ahead();

  void peer(ObjectStore::Transaction& t, 
	    map< int, map<pg_t,Query> >& query_map,
	    map<int, MOSDPGInfo*> *activator_map=0);
  void activate(ObjectStore::Transaction& t, 
		map<int, MOSDPGInfo*> *activator_map=0);

  virtual void clean_up_local(ObjectStore::Transaction& t) = 0;

  virtual void cancel_recovery() = 0;
  virtual int start_recovery_ops(int max) = 0;

  void purge_strays();


  Context *finish_sync_event;

  void finish_recovery();
  void _finish_recovery(Context *c);
  void defer_recovery();

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;

 public:  
  PG(OSD *o, pg_t p) : 
    osd(o), 
    _lock("PG::_lock"),
    ref(0), deleted(false), dirty_info(false), dirty_log(false),
    info(p),
    recovery_item(this), scrub_item(this), snap_trim_item(this),
    recovery_ops_active(0),
    role(0),
    state(0),
    have_master_log(true),
    must_notify_mon(false),
    pg_stats_lock("PG::pg_stats_lock"),
    pg_stats_valid(false),
    finish_sync_event(NULL)
  { }
  virtual ~PG() { }
  
  pg_t       get_pgid() const { return info.pgid; }
  int        get_nrep() const { return acting.size(); }

  int        get_primary() { return acting.empty() ? -1:acting[0]; }
  //int        get_tail() { return acting.empty() ? -1:acting[ acting.size()-1 ]; }
  //int        get_acker() { return g_conf.osd_rep == OSD_REP_PRIMARY ? get_primary():get_tail(); }
  int        get_acker() { 
    if (g_conf.osd_rep == OSD_REP_PRIMARY ||
	acting.size() <= 1) 
      return get_primary();
    return acting[1];
  }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  bool       is_primary() const { return role == PG_ROLE_HEAD; }
  bool       is_replica() const { return role > 0; }
  bool       is_acker() const { 
    if (g_conf.osd_rep == OSD_REP_PRIMARY)
      return is_primary();
    else
      return role == PG_ROLE_ACKER; 
  }
  bool       is_head() const { return role == PG_ROLE_HEAD; }
  bool       is_middle() const { return role == PG_ROLE_MIDDLE; }
  bool       is_residual() const { return role == PG_ROLE_STRAY; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_crashed() const { return state_test(PG_STATE_CRASHED); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  //bool       is_complete()    { return state_test(PG_STATE_COMPLETE); }
  bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool       is_stray() const { return state_test(PG_STATE_STRAY); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  bool is_complete_pg() { return acting.size() == info.pgid.size(); }

  void add_log_entry(Log::Entry& e, bufferlist& log_bl);

  // pg on-disk state
  void write_info(ObjectStore::Transaction& t);
  void write_log(ObjectStore::Transaction& t);
  void append_log(ObjectStore::Transaction &t, bufferlist& bl,
		  eversion_t log_version, eversion_t trim_to);
  void read_log(ObjectStore *store);
  void trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v);

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
  virtual void scrub() { };

  virtual bool same_for_read_since(epoch_t e) = 0;
  virtual bool same_for_modify_since(epoch_t e) = 0;
  virtual bool same_for_rep_modify_since(epoch_t e) = 0;

  virtual bool is_missing_object(object_t oid) = 0;
  virtual void wait_for_missing_object(object_t oid, Message *op) = 0;

  virtual void on_osd_failure(int osd) = 0;
  virtual void on_acker_change() = 0;
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


inline ostream& operator<<(ostream& out, const PG::Info::History& h) 
{
  return out << " ec=" << h.epoch_created
	     << " les=" << h.last_epoch_started
	     << " " << h.same_since << "/" << h.same_primary_since << "/" << h.same_acker_since;
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
  out << " " << pgi.history
      << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Log::Entry& e)
{
  return out << " " << e.version << " (" << e.prior_version << ")"
             << (e.is_delete() ? " - ":
		 (e.is_clone() ? " c ":
		  (e.is_modify() ? " m ":
		   " ? ")))
             << e.oid << " by " << e.reqid;
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

  if (pg.get_role() == 0) {
    out << " pct " << pg.peers_complete_thru;
    if (!pg.have_master_log) out << " !hml";
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
  if (pg.info.dead_snaps.size())
    out << " dead=" << pg.info.dead_snaps;
  out << "]";


  return out;
}



#endif
