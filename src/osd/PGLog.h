// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_PG_LOG_H
#define CEPH_PG_LOG_H

// re-include our assert to clobber boost's
#include "include/assert.h" 
#include "osd_types.h"
#include "os/ObjectStore.h"
#include <list>
using namespace std;

#define PGLOG_INDEXED_OBJECTS          (1 << 0)
#define PGLOG_INDEXED_CALLER_OPS       (1 << 1)
#define PGLOG_INDEXED_EXTRA_CALLER_OPS (1 << 2)
#define PGLOG_INDEXED_ALL              (PGLOG_INDEXED_OBJECTS | PGLOG_INDEXED_CALLER_OPS | PGLOG_INDEXED_EXTRA_CALLER_OPS)

class CephContext;

struct PGLog : DoutPrefixProvider {
  DoutPrefixProvider *prefix_provider;
  string gen_prefix() const {
    return prefix_provider ? prefix_provider->gen_prefix() : "";
  }
  unsigned get_subsys() const {
    return prefix_provider ? prefix_provider->get_subsys() :
      (unsigned)ceph_subsys_osd;
  }
  CephContext *get_cct() const {
    return cct;
  }

  ////////////////////////////// sub classes //////////////////////////////
  struct LogEntryHandler {
    virtual void rollback(
      const pg_log_entry_t &entry) = 0;
    virtual void rollforward(
      const pg_log_entry_t &entry) = 0;
    virtual void trim(
      const pg_log_entry_t &entry) = 0;
    virtual void remove(
      const hobject_t &hoid) = 0;
    virtual void try_stash(
      const hobject_t &hoid,
      version_t v) = 0;
    virtual ~LogEntryHandler() {}
  };

  /* Exceptions */
  class read_log_and_missing_error : public buffer::error {
  public:
    explicit read_log_and_missing_error(const char *what) {
      snprintf(buf, sizeof(buf), "read_log_and_missing_error: %s", what);
    }
    const char *what() const throw () {
      return buf;
    }
  private:
    char buf[512];
  };

public:
  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public pg_log_t {
    mutable ceph::unordered_map<hobject_t,pg_log_entry_t*> objects;  // ptrs into log.  be careful!
    mutable ceph::unordered_map<osd_reqid_t,pg_log_entry_t*> caller_ops;
    mutable ceph::unordered_multimap<osd_reqid_t,pg_log_entry_t*> extra_caller_ops;

    // recovery pointers
    list<pg_log_entry_t>::iterator complete_to; // not inclusive of referenced item
    version_t last_requested = 0;               // last object requested by primary

    //
  private:
    mutable __u16 indexed_data = 0;
    /**
     * rollback_info_trimmed_to_riter points to the first log entry <=
     * rollback_info_trimmed_to
     *
     * It's a reverse_iterator because rend() is a natural representation for
     * tail, and rbegin() works nicely for head.
     */
    list<pg_log_entry_t>::reverse_iterator rollback_info_trimmed_to_riter;

    template <typename F>
    void advance_can_rollback_to(eversion_t to, F &&f) {
      if (to > can_rollback_to)
	can_rollback_to = to;

      if (to > rollback_info_trimmed_to)
	rollback_info_trimmed_to = to;

      while (rollback_info_trimmed_to_riter != log.rbegin()) {
	--rollback_info_trimmed_to_riter;
	if (rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to) {
	  ++rollback_info_trimmed_to_riter;
	  break;
	}
	f(*rollback_info_trimmed_to_riter);
      }
    }

    void reset_rollback_info_trimmed_to_riter() {
      rollback_info_trimmed_to_riter = log.rbegin();
      while (rollback_info_trimmed_to_riter != log.rend() &&
	     rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to)
	++rollback_info_trimmed_to_riter;
    }

    // indexes objects, caller ops and extra caller ops
  public:
    IndexedLog() :
      complete_to(log.end()),
      last_requested(0),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin())
      {}

    template <typename... Args>
    IndexedLog(Args&&... args) :
      pg_log_t(std::forward<Args>(args)...),
      complete_to(log.end()),
      last_requested(0),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin()) {
      reset_rollback_info_trimmed_to_riter();
      index();
    }

    IndexedLog(const IndexedLog &rhs) :
      pg_log_t(rhs),
      complete_to(log.end()),
      last_requested(rhs.last_requested),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin()) {
      reset_rollback_info_trimmed_to_riter();
      index(rhs.indexed_data);
    }
    IndexedLog &operator=(const IndexedLog &rhs) {
      this->~IndexedLog();
      new (this) IndexedLog(rhs);
      return *this;
    }

    void trim_rollback_info_to(eversion_t to, LogEntryHandler *h) {
      advance_can_rollback_to(
	to,
	[&](pg_log_entry_t &entry) {
	  h->trim(entry);
	});
    }
    void roll_forward_to(eversion_t to, LogEntryHandler *h) {
      advance_can_rollback_to(
	to,
	[&](pg_log_entry_t &entry) {
	  h->rollforward(entry);
	});
    }

    void skip_can_rollback_to_to_head() {
      advance_can_rollback_to(head, [&](const pg_log_entry_t &entry) {});
    }

    list<pg_log_entry_t> rewind_from_head(eversion_t newhead) {
      list<pg_log_entry_t> divergent = pg_log_t::rewind_from_head(newhead);
      index();
      reset_rollback_info_trimmed_to_riter();
      return divergent;
    }

    /****/
    void claim_log_and_clear_rollback_info(const pg_log_t& o) {
      // we must have already trimmed the old entries
      assert(rollback_info_trimmed_to == head);
      assert(rollback_info_trimmed_to_riter == log.rbegin());

      *this = IndexedLog(o);

      skip_can_rollback_to_to_head();
      index();
    }

    IndexedLog split_out_child(
      pg_t child_pgid,
      unsigned split_bits);

    void zero() {
      // we must have already trimmed the old entries
      assert(rollback_info_trimmed_to == head);
      assert(rollback_info_trimmed_to_riter == log.rbegin());

      unindex();
      pg_log_t::clear();
      rollback_info_trimmed_to_riter = log.rbegin();
      reset_recovery_pointers();
    }
    void clear() {
      skip_can_rollback_to_to_head();
      zero();
    }
    void reset_recovery_pointers() {
      complete_to = log.end();
      last_requested = 0;
    }

    bool logged_object(const hobject_t& oid) const {
      if (!(indexed_data & PGLOG_INDEXED_OBJECTS)) {
         index_objects();
      }
      return objects.count(oid);
    }

    bool logged_req(const osd_reqid_t &r) const {
      if (!(indexed_data & PGLOG_INDEXED_CALLER_OPS)) {
        index_caller_ops();
      }
      if (!caller_ops.count(r)) {
        if (!(indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS)) {
          index_extra_caller_ops();
        }
        return extra_caller_ops.count(r);
      }
      return true;
    }

    bool get_request(
      const osd_reqid_t &r,
      eversion_t *replay_version,
      version_t *user_version,
      int *return_code) const {
      assert(replay_version);
      assert(user_version);
      assert(return_code);
      ceph::unordered_map<osd_reqid_t,pg_log_entry_t*>::const_iterator p;
      if (!(indexed_data & PGLOG_INDEXED_CALLER_OPS)) {
        index_caller_ops();
      }
      p = caller_ops.find(r);
      if (p != caller_ops.end()) {
	*replay_version = p->second->version;
	*user_version = p->second->user_version;
	*return_code = p->second->return_code;
	return true;
      }

      // warning: we will return *a* request for this reqid, but not
      // necessarily the most recent.
      if (!(indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS)) {
        index_extra_caller_ops();
      }
      p = extra_caller_ops.find(r);
      if (p != extra_caller_ops.end()) {
	for (vector<pair<osd_reqid_t, version_t> >::const_iterator i =
	       p->second->extra_reqids.begin();
	     i != p->second->extra_reqids.end();
	     ++i) {
	  if (i->first == r) {
	    *replay_version = p->second->version;
	    *user_version = i->second;
	    *return_code = p->second->return_code;
	    return true;
	  }
	}
	assert(0 == "in extra_caller_ops but not extra_reqids");
      }
      return false;
    }

    /// get a (bounded) list of recent reqids for the given object
    void get_object_reqids(const hobject_t& oid, unsigned max,
			   vector<pair<osd_reqid_t, version_t> > *pls) const {
       // make sure object is present at least once before we do an
       // O(n) search.
      if (!(indexed_data & PGLOG_INDEXED_OBJECTS)) {
        index_objects();
      }
      if (objects.count(oid) == 0)
	return;
      for (list<pg_log_entry_t>::const_reverse_iterator i = log.rbegin();
           i != log.rend();
           ++i) {
	if (i->soid == oid) {
	  if (i->reqid_is_indexed())
	    pls->push_back(make_pair(i->reqid, i->user_version));
	  pls->insert(pls->end(), i->extra_reqids.begin(), i->extra_reqids.end());
	  if (pls->size() >= max) {
	    if (pls->size() > max) {
	      pls->resize(max);
	    }
	    return;
	  }
	}
      }
    }
    
    void index(__u16 to_index = PGLOG_INDEXED_ALL) const {
      if (to_index & PGLOG_INDEXED_OBJECTS)
	objects.clear();
      if (to_index & PGLOG_INDEXED_CALLER_OPS)
	caller_ops.clear();
      if (to_index & PGLOG_INDEXED_EXTRA_CALLER_OPS)
	extra_caller_ops.clear();

      for (list<pg_log_entry_t>::const_iterator i = log.begin();
             i != log.end();
             ++i) {

	if (to_index & PGLOG_INDEXED_OBJECTS) {
	  if (i->object_is_indexed()) {
	    objects[i->soid] = const_cast<pg_log_entry_t*>(&(*i));
	  }
	}

	if (PGLOG_INDEXED_CALLER_OPS) {
	  if (i->reqid_is_indexed()) {
	    //assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
	    caller_ops[i->reqid] = const_cast<pg_log_entry_t*>(&(*i));
	  }
	}
        
	if (PGLOG_INDEXED_EXTRA_CALLER_OPS) {
	  for (vector<pair<osd_reqid_t, version_t> >::const_iterator j =
		 i->extra_reqids.begin();
	       j != i->extra_reqids.end();
	       ++j) {
            extra_caller_ops.insert(
	      make_pair(j->first, const_cast<pg_log_entry_t*>(&(*i))));
	  }
	}
      }
        
      indexed_data |= to_index;
    }

    void index_objects() const {
      index(PGLOG_INDEXED_OBJECTS);
    }

    void index_caller_ops() const {
      index(PGLOG_INDEXED_CALLER_OPS);
    }

    void index_extra_caller_ops() const {
      index(PGLOG_INDEXED_EXTRA_CALLER_OPS);
    }

    void index(pg_log_entry_t& e) {
      if ((indexed_data & PGLOG_INDEXED_OBJECTS) && e.object_is_indexed()) {
        if (objects.count(e.soid) == 0 ||
            objects[e.soid]->version < e.version)
          objects[e.soid] = &e;
      }
      if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
        if (e.reqid_is_indexed()) {
    //assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
    caller_ops[e.reqid] = &e;
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (vector<pair<osd_reqid_t, version_t> >::const_iterator j =
         e.extra_reqids.begin();
       j != e.extra_reqids.end();
       ++j) {
    extra_caller_ops.insert(make_pair(j->first, &e));
        }
      }
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
      extra_caller_ops.clear();
      indexed_data = 0;
    }
    void unindex(pg_log_entry_t& e) {
      // NOTE: this only works if we remove from the _tail_ of the log!
      if (indexed_data & PGLOG_INDEXED_OBJECTS) {
        if (objects.count(e.soid) && objects[e.soid]->version == e.version)
          objects.erase(e.soid);
      }
      if (e.reqid_is_indexed()) {
        if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
          if (caller_ops.count(e.reqid) &&  // divergent merge_log indexes new before unindexing old
              caller_ops[e.reqid] == &e)
            caller_ops.erase(e.reqid);    
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (vector<pair<osd_reqid_t, version_t> >::const_iterator j =
             e.extra_reqids.begin();
             j != e.extra_reqids.end();
             ++j) {
          for (ceph::unordered_multimap<osd_reqid_t,pg_log_entry_t*>::iterator k =
               extra_caller_ops.find(j->first);
               k != extra_caller_ops.end() && k->first == j->first;
               ++k) {
            if (k->second == &e) {
              extra_caller_ops.erase(k);
              break;
            }
          }
        }
      }
    }

    // actors
    void add(const pg_log_entry_t& e, bool applied = true) {
      if (!applied) {
	get_can_rollback_to() == head;
      }

      // add to log
      log.push_back(e);

      // riter previously pointed to the previous entry
      if (rollback_info_trimmed_to_riter == log.rbegin())
	++rollback_info_trimmed_to_riter;

      assert(e.version > head);
      assert(head.version == 0 || e.version.version > head.version);
      head = e.version;

      // to our index
      if ((indexed_data & PGLOG_INDEXED_OBJECTS) && e.object_is_indexed()) {
        objects[e.soid] = &(log.back());
      }
      if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
        if (e.reqid_is_indexed()) {
    caller_ops[e.reqid] = &(log.back());
        }
      }
      
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (vector<pair<osd_reqid_t, version_t> >::const_iterator j =
         e.extra_reqids.begin();
       j != e.extra_reqids.end();
       ++j) {
    extra_caller_ops.insert(make_pair(j->first, &(log.back())));
        }
      }

      if (!applied) {
	skip_can_rollback_to_to_head();
      }
    }

    void trim(
      eversion_t s,
      set<eversion_t> *trimmed);

    ostream& print(ostream& out) const;
  };


protected:
  //////////////////// data members ////////////////////

  pg_missing_tracker_t missing;
  IndexedLog  log;

  eversion_t dirty_to;         ///< must clear/writeout all keys <= dirty_to
  eversion_t dirty_from;       ///< must clear/writeout all keys >= dirty_from
  eversion_t writeout_from;    ///< must writout keys >= writeout_from
  set<eversion_t> trimmed;     ///< must clear keys in trimmed
  CephContext *cct;
  bool pg_log_debug;
  /// Log is clean on [dirty_to, dirty_from)
  bool touched_log;
  bool clear_divergent_priors;

  void mark_dirty_to(eversion_t to) {
    if (to > dirty_to)
      dirty_to = to;
  }
  void mark_dirty_from(eversion_t from) {
    if (from < dirty_from)
      dirty_from = from;
  }
  void mark_writeout_from(eversion_t from) {
    if (from < writeout_from)
      writeout_from = from;
  }
public:
  bool is_dirty() const {
    return !touched_log ||
      (dirty_to != eversion_t()) ||
      (dirty_from != eversion_t::max()) ||
      (writeout_from != eversion_t::max()) ||
      !(trimmed.empty()) ||
      !missing.is_clean();
  }
  void mark_log_for_rewrite() {
    mark_dirty_to(eversion_t::max());
    mark_dirty_from(eversion_t());
    touched_log = false;
  }
protected:

  /// DEBUG
  set<string> log_keys_debug;
  static void clear_after(set<string> *log_keys_debug, const string &lb) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->lower_bound(lb);
	 i != log_keys_debug->end();
	 log_keys_debug->erase(i++));
  }
  static void clear_up_to(set<string> *log_keys_debug, const string &ub) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->begin();
	 i != log_keys_debug->end() && *i < ub;
	 log_keys_debug->erase(i++));
  }

  void check();
  void undirty() {
    dirty_to = eversion_t();
    dirty_from = eversion_t::max();
    touched_log = true;
    trimmed.clear();
    writeout_from = eversion_t::max();
    check();
    missing.flush();
  }
public:
  // cppcheck-suppress noExplicitConstructor
  PGLog(CephContext *cct, DoutPrefixProvider *dpp = 0) :
    prefix_provider(dpp),
    dirty_from(eversion_t::max()),
    writeout_from(eversion_t::max()), 
    cct(cct), 
    pg_log_debug(!(cct && !(cct->_conf->osd_debug_pg_log_writeout))),
    touched_log(false),
    clear_divergent_priors(false) {}


  void reset_backfill();

  void clear();

  //////////////////// get or set missing ////////////////////

  const pg_missing_tracker_t& get_missing() const { return missing; }
  void resort_missing(bool sort_bitwise) {
    missing.resort(sort_bitwise);
  }

  void revise_have(hobject_t oid, eversion_t have) {
    missing.revise_have(oid, have);
  }

  void revise_need(hobject_t oid, eversion_t need) {
    missing.revise_need(oid, need);
  }

  void missing_add(const hobject_t& oid, eversion_t need, eversion_t have) {
    missing.add(oid, need, have);
  }

  void missing_add_event(const pg_log_entry_t &e) {
    missing.add_next_event(e);
  }

  //////////////////// get or set log ////////////////////

  const IndexedLog &get_log() const { return log; }

  const eversion_t &get_tail() const { return log.tail; }

  void set_tail(eversion_t tail) { log.tail = tail; }

  const eversion_t &get_head() const { return log.head; }

  void set_head(eversion_t head) { log.head = head; }

  void set_last_requested(version_t last_requested) {
    log.last_requested = last_requested;
  }

  void index() { log.index(); }

  void unindex() { log.unindex(); }

  void add(const pg_log_entry_t& e, bool applied = true) {
    mark_writeout_from(e.version);
    log.add(e, applied);
  }

  void reset_recovery_pointers() { log.reset_recovery_pointers(); }

  static void clear_info_log(
    spg_t pgid,
    ObjectStore::Transaction *t);

  void trim(
    eversion_t trim_to,
    pg_info_t &info);

  void roll_forward_to(
    eversion_t roll_forward_to,
    LogEntryHandler *h) {
    log.roll_forward_to(
      roll_forward_to,
      h);
  }

  eversion_t get_can_rollback_to() const {
    return log.get_can_rollback_to();
  }

  void roll_forward(LogEntryHandler *h) {
    roll_forward_to(
      log.head,
      h);
  }

  //////////////////// get or set log & missing ////////////////////

  void reset_backfill_claim_log(const pg_log_t &o, LogEntryHandler *h) {
    log.trim_rollback_info_to(log.head, h);
    log.claim_log_and_clear_rollback_info(o);
    missing.clear();
    mark_dirty_to(eversion_t::max());
  }

  void split_into(
      pg_t child_pgid,
      unsigned split_bits,
      PGLog *opg_log) { 
    opg_log->log = log.split_out_child(child_pgid, split_bits);
    missing.split_into(child_pgid, split_bits, &(opg_log->missing));
    opg_log->mark_dirty_to(eversion_t::max());
    mark_dirty_to(eversion_t::max());
  }

  void recover_got(hobject_t oid, eversion_t v, pg_info_t &info) {
    if (missing.is_missing(oid, v)) {
      missing.got(oid, v);
      
      // raise last_complete?
      if (missing.get_items().empty()) {
	log.complete_to = log.log.end();
	info.last_complete = info.last_update;
      }
      while (log.complete_to != log.log.end()) {
	if (missing.get_items().at(
	      missing.get_rmissing().begin()->second
	      ).need <= log.complete_to->version)
	  break;
	if (info.last_complete < log.complete_to->version)
	  info.last_complete = log.complete_to->version;
	++log.complete_to;
      }
    }

    assert(log.get_can_rollback_to() >= v);
  }

  void activate_not_complete(pg_info_t &info) {
    log.complete_to = log.log.begin();
    while (log.complete_to->version <
	   missing.get_items().at(
	     missing.get_rmissing().begin()->second
	     ).need)
      ++log.complete_to;
    assert(log.complete_to != log.log.end());
    if (log.complete_to == log.log.begin()) {
      info.last_complete = eversion_t();
    } else {
      --log.complete_to;
      info.last_complete = log.complete_to->version;
      ++log.complete_to;
    }
    log.last_requested = 0;
  }

  void proc_replica_log(ObjectStore::Transaction& t, pg_info_t &oinfo, const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from) const;

protected:
  static void split_by_object(
    list<pg_log_entry_t> &entries,
    map<hobject_t, list<pg_log_entry_t>, hobject_t::BitwiseComparator> *out_entries) {
    while (!entries.empty()) {
      list<pg_log_entry_t> &out_list = (*out_entries)[entries.front().soid];
      out_list.splice(out_list.end(), entries, entries.begin());
    }
  }

  /**
   * _merge_object_divergent_entries
   *
   * There are 5 distinct cases:
   * 1) There is a more recent update: in this case we assume we adjusted the
   *    store and missing during merge_log
   * 2) The first entry in the divergent sequence is a create.  This might
   *    either be because the object is a clone or because prior_version is
   *    eversion_t().  In this case the object does not exist and we must
   *    adjust missing and the store to match.
   * 3) We are currently missing the object.  In this case, we adjust the
   *    missing to our prior_version taking care to add a divergent_prior
   *    if necessary
   * 4) We can rollback all of the entries.  In this case, we do so using
   *    the rollbacker and return -- the object does not go into missing.
   * 5) We cannot rollback at least 1 of the entries.  In this case, we
   *    clear the object out of the store and add a missing entry at
   *    prior_version taking care to add a divergent_prior if
   *    necessary.
   */
  template <typename missing_type>
  static void _merge_object_divergent_entries(
    const IndexedLog &log,               ///< [in] log to merge against
    const hobject_t &hoid,               ///< [in] object we are merging
    const list<pg_log_entry_t> &entries, ///< [in] entries for hoid to merge
    const pg_info_t &info,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &missing,              ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    ldpp_dout(dpp, 20) << __func__ << ": merging hoid " << hoid
		       << " entries: " << entries << dendl;

    if (cmp(hoid, info.last_backfill, info.last_backfill_bitwise) > 0) {
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " after last_backfill"
			 << dendl;
      return;
    }

    // entries is non-empty
    assert(!entries.empty());
    eversion_t last;
    for (list<pg_log_entry_t>::const_iterator i = entries.begin();
	 i != entries.end();
	 ++i) {
      // all entries are on hoid
      assert(i->soid == hoid);
      if (i != entries.begin() && i->prior_version != eversion_t()) {
	// in increasing order of version
	assert(i->version > last);
	// prior_version correct
	assert(i->prior_version == last);
      }
      last = i->version;
    }

    const eversion_t prior_version = entries.begin()->prior_version;
    const eversion_t first_divergent_update = entries.begin()->version;
    const eversion_t last_divergent_update = entries.rbegin()->version;
    const bool object_not_in_store =
      !missing.is_missing(hoid) &&
      entries.rbegin()->is_delete();
    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       << " prior_version: " << prior_version
		       << " first_divergent_update: " << first_divergent_update
		       << " last_divergent_update: " << last_divergent_update
		       << dendl;

    ceph::unordered_map<hobject_t, pg_log_entry_t*>::const_iterator objiter =
      log.objects.find(hoid);
    if (objiter != log.objects.end() &&
	objiter->second->version >= first_divergent_update) {
      /// Case 1)
      ldpp_dout(dpp, 10) << __func__ << ": more recent entry found: "
			 << *objiter->second << ", already merged" << dendl;

      assert(objiter->second->version > last_divergent_update);

      // ensure missing has been updated appropriately
      if (objiter->second->is_update()) {
	assert(missing.is_missing(hoid) &&
	       missing.get_items().at(hoid).need == objiter->second->version);
      } else {
	assert(!missing.is_missing(hoid));
      }
      missing.revise_have(hoid, eversion_t());
      if (rollbacker) {
	if (!object_not_in_store) {
	  rollbacker->remove(hoid);
	}
	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      return;
    }

    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       <<" has no more recent entries in log" << dendl;
    if (prior_version == eversion_t() || entries.front().is_clone()) {
      /// Case 2)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " prior_version or op type indicates creation,"
			 << " deleting"
			 << dendl;
      if (missing.is_missing(hoid))
	missing.rm(missing.get_items().find(hoid));
      if (rollbacker) {
	if (!object_not_in_store) {
	  rollbacker->remove(hoid);
	}
	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      return;
    }

    if (missing.is_missing(hoid)) {
      /// Case 3)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " missing, " << missing.get_items().at(hoid)
			 << " adjusting" << dendl;

      if (missing.get_items().at(hoid).have == prior_version) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " missing.have is prior_version " << prior_version
			   << " removing from missing" << dendl;
	missing.rm(missing.get_items().find(hoid));
      } else {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " missing.have is " << missing.get_items().at(hoid).have
			   << ", adjusting" << dendl;
	missing.revise_need(hoid, prior_version);
	if (prior_version <= info.log_tail) {
	  ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			     << " prior_version " << prior_version
			     << " <= info.log_tail "
			     << info.log_tail << dendl;
	}
      }
      if (rollbacker) {
	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      return;
    }

    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       << " must be rolled back or recovered,"
		       << " attempting to rollback"
		       << dendl;
    bool can_rollback = true;
    /// Distinguish between 4) and 5)
    for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
	 i != entries.rend();
	 ++i) {
      if (!i->can_rollback() || i->version <= olog_can_rollback_to) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " cannot rollback "
			   << *i << dendl;
	can_rollback = false;
	break;
      }
    }

    if (can_rollback) {
      /// Case 4)
      for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
	   i != entries.rend();
	   ++i) {
	assert(i->can_rollback() && i->version > olog_can_rollback_to);
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " rolling back " << *i << dendl;
	if (rollbacker)
	  rollbacker->rollback(*i);
      }
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " rolled back" << dendl;
      return;
    } else {
      /// Case 5)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " cannot roll back, "
			 << "removing and adding to missing" << dendl;
      if (rollbacker) {
	if (!object_not_in_store)
	  rollbacker->remove(hoid);
	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      missing.add(hoid, prior_version, eversion_t());
      if (prior_version <= info.log_tail) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " prior_version " << prior_version
			   << " <= info.log_tail "
			   << info.log_tail << dendl;
      }
    }
  }

  /// Merge all entries using above
  template <typename missing_type>
  static void _merge_divergent_entries(
    const IndexedLog &log,               ///< [in] log to merge against
    list<pg_log_entry_t> &entries,       ///< [in] entries to merge
    const pg_info_t &oinfo,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &omissing,              ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    map<hobject_t, list<pg_log_entry_t>, hobject_t::BitwiseComparator > split;
    split_by_object(entries, &split);
    for (map<hobject_t, list<pg_log_entry_t>, hobject_t::BitwiseComparator>::iterator i = split.begin();
	 i != split.end();
	 ++i) {
      _merge_object_divergent_entries(
	log,
	i->first,
	i->second,
	oinfo,
	olog_can_rollback_to,
	omissing,
	rollbacker,
	dpp);
    }
  }

  /**
   * Exists for use in TestPGLog for simply testing single divergent log
   * cases
   */
  void merge_old_entry(
    ObjectStore::Transaction& t,
    const pg_log_entry_t& oe,
    const pg_info_t& info,
    LogEntryHandler *rollbacker) {
    list<pg_log_entry_t> entries;
    entries.push_back(oe);
    _merge_object_divergent_entries(
      log,
      oe.soid,
      entries,
      info,
      log.get_can_rollback_to(),
      missing,
      rollbacker,
      this);
  }
public:
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
                            pg_info_t &info, LogEntryHandler *rollbacker,
                            bool &dirty_info, bool &dirty_big_info);

  void merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		 pg_shard_t from,
		 pg_info_t &info, LogEntryHandler *rollbacker,
		 bool &dirty_info, bool &dirty_big_info);

  template <typename missing_type>
  static bool append_log_entries_update_missing(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const list<pg_log_entry_t> &entries,
    bool maintain_rollback,
    IndexedLog *log,
    missing_type &missing,
    LogEntryHandler *rollbacker,
    const DoutPrefixProvider *dpp) {
    bool invalidate_stats = false;
    if (log && !entries.empty()) {
      assert(log->head < entries.begin()->version);
    }
    for (list<pg_log_entry_t>::const_iterator p = entries.begin();
	 p != entries.end();
	 ++p) {
      invalidate_stats = invalidate_stats || !p->is_error();
      if (log) {
	ldpp_dout(dpp, 20) << "update missing, append " << *p << dendl;
	log->add(*p);
      }
      if (cmp(p->soid, last_backfill, last_backfill_bitwise) <= 0 &&
	  !p->is_error()) {
	missing.add_next_event(*p);
	if (rollbacker) {
	  // hack to match PG::mark_all_unfound_lost
	  if (maintain_rollback && p->is_lost_delete() && p->can_rollback()) {
	    rollbacker->try_stash(p->soid, p->version.version);
	  } else if (p->is_delete()) {
	    rollbacker->remove(p->soid);
	  }
	}
      }
    }
    return invalidate_stats;
  }
  bool append_new_log_entries(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const list<pg_log_entry_t> &entries,
    LogEntryHandler *rollbacker) {
    bool invalidate_stats = append_log_entries_update_missing(
      last_backfill,
      last_backfill_bitwise,
      entries,
      true,
      &log,
      missing,
      rollbacker,
      this);
    if (!entries.empty()) {
      mark_writeout_from(entries.begin()->version);
    }
    return invalidate_stats;
  }

  void write_log_and_missing(ObjectStore::Transaction& t,
		 map<string,bufferlist> *km,
		 const coll_t& coll,
		 const ghobject_t &log_oid,
		 bool require_rollback);

  static void write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors,
    bool require_rollback);

  static void write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid,
    const pg_missing_tracker_t &missing,
    bool require_rollback);

  static void _write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors,
    eversion_t dirty_to,
    eversion_t dirty_from,
    eversion_t writeout_from,
    const set<eversion_t> &trimmed,
    bool dirty_divergent_priors,
    bool touch_log,
    bool require_rollback,
    set<string> *log_keys_debug
    );

  static void _write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    eversion_t dirty_to,
    eversion_t dirty_from,
    eversion_t writeout_from,
    const set<eversion_t> &trimmed,
    const pg_missing_tracker_t &missing,
    bool touch_log,
    bool require_rollback,
    bool clear_divergent_priors,
    set<string> *log_keys_debug
    );

  void read_log_and_missing(
    ObjectStore *store, coll_t pg_coll,
    coll_t log_coll, ghobject_t log_oid,
    const pg_info_t &info,
    ostringstream &oss,
    bool debug_verify_stored_missing = false
    ) {
    return read_log_and_missing(
      store, pg_coll, log_coll, log_oid, info,
      log, missing, oss,
      &clear_divergent_priors,
      this,
      (pg_log_debug ? &log_keys_debug : 0),
      debug_verify_stored_missing);
  }

  template <typename missing_type>
  static void read_log_and_missing(ObjectStore *store, coll_t pg_coll,
    coll_t log_coll, ghobject_t log_oid,
    const pg_info_t &info,
    IndexedLog &log,
    missing_type &missing, ostringstream &oss,
    bool *clear_divergent_priors = NULL,
    const DoutPrefixProvider *dpp = NULL,
    set<string> *log_keys_debug = 0,
    bool debug_verify_stored_missing = false
    ) {
    ldpp_dout(dpp, 20) << "read_log_and_missing coll " << pg_coll
		       << " log_oid " << log_oid << dendl;

    // legacy?
    struct stat st;
    int r = store->stat(log_coll, log_oid, &st);
    assert(r == 0);
    assert(st.st_size == 0);

    // will get overridden below if it had been recorded
    eversion_t on_disk_can_rollback_to = info.last_update;
    eversion_t on_disk_rollback_info_trimmed_to = eversion_t();
    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(log_coll, log_oid);
    map<eversion_t, hobject_t> divergent_priors;
    bool has_divergent_priors = false;
    list<pg_log_entry_t> entries;
    if (p) {
      for (p->seek_to_first(); p->valid() ; p->next(false)) {
	// non-log pgmeta_oid keys are prefixed with _; skip those
	if (p->key()[0] == '_')
	  continue;
	bufferlist bl = p->value();//Copy bufferlist before creating iterator
	bufferlist::iterator bp = bl.begin();
	if (p->key() == "divergent_priors") {
	  ::decode(divergent_priors, bp);
	  ldpp_dout(dpp, 20) << "read_log_and_missing " << divergent_priors.size()
			     << " divergent_priors" << dendl;
	  has_divergent_priors = true;
	  debug_verify_stored_missing = false;
	} else if (p->key() == "can_rollback_to") {
	  ::decode(on_disk_can_rollback_to, bp);
	} else if (p->key() == "rollback_info_trimmed_to") {
	  ::decode(on_disk_rollback_info_trimmed_to, bp);
	} else if (p->key().substr(0, 7) == string("missing")) {
	  pair<hobject_t, pg_missing_item> p;
	  ::decode(p, bp);
	  missing.add(p.first, p.second.need, p.second.have);
	} else {
	  pg_log_entry_t e;
	  e.decode_with_checksum(bp);
	  ldpp_dout(dpp, 20) << "read_log_and_missing " << e << dendl;
	  if (!entries.empty()) {
	    pg_log_entry_t last_e(entries.back());
	    assert(last_e.version.version < e.version.version);
	    assert(last_e.version.epoch <= e.version.epoch);
	  }
	  entries.push_back(e);
	  if (log_keys_debug)
	    log_keys_debug->insert(e.get_key_name());
	}
      }
    }
    log = IndexedLog(
      info.last_update,
      info.log_tail,
      on_disk_can_rollback_to,
      on_disk_rollback_info_trimmed_to,
      std::move(entries));

    if (has_divergent_priors || debug_verify_stored_missing) {
      // build missing
      if (debug_verify_stored_missing || info.last_complete < info.last_update) {
	ldpp_dout(dpp, 10) << "read_log_and_missing checking for missing items over interval ("
			   << info.last_complete
			   << "," << info.last_update << "]" << dendl;

	set<hobject_t, hobject_t::BitwiseComparator> did;
	set<hobject_t, hobject_t::BitwiseComparator> checked;
	set<hobject_t, hobject_t::BitwiseComparator> skipped;
	for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	     i != log.log.rend();
	     ++i) {
	  if (!debug_verify_stored_missing && i->version <= info.last_complete) break;
	  if (cmp(i->soid, info.last_backfill, info.last_backfill_bitwise) > 0)
	    continue;
	  if (i->is_error())
	    continue;
	  if (did.count(i->soid)) continue;
	  did.insert(i->soid);

	  // has_divergent_priors -> !(i->is_rollforward())
          // rollforward entries mean that the on-disk object won't match the
          // the log until rolled-forward, skip in the check
	  if (i->version > log.get_can_rollback_to() &&
	      i->is_rollforward()) {
	    checked.insert(i->soid);
	    continue;
	  }

	  if (i->is_delete()) continue;

	  bufferlist bv;
	  int r = store->getattr(
	    pg_coll,
	    ghobject_t(i->soid, ghobject_t::NO_GEN, info.pgid.shard),
	    OI_ATTR,
	    bv);
	  if (r >= 0) {
	    object_info_t oi(bv);
	    if (oi.version < i->version) {
	      ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i
				 << " (have " << oi.version << ")" << dendl;
	      if (debug_verify_stored_missing) {
		auto miter = missing.get_items().find(i->soid);
		assert(miter != missing.get_items().end());
		assert(miter->second.need == i->version);
		assert(miter->second.have == oi.version);
		checked.insert(i->soid);
	      } else {
		missing.add(i->soid, i->version, oi.version);
	      }
	    }
	  } else {
	    ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;
	    if (debug_verify_stored_missing) {
	      auto miter = missing.get_items().find(i->soid);
	      assert(miter != missing.get_items().end());
	      assert(miter->second.need == i->version);
	      assert(miter->second.have == eversion_t());
	      checked.insert(i->soid);
	    } else {
	      missing.add(i->soid, i->version, eversion_t());
	    }
	  }
	}
	if (debug_verify_stored_missing) {
	  for (auto &&i: missing.get_items()) {
	    if (checked.count(i.first))
	      continue;
	    if (i.second.need > log.tail ||
	      cmp(i.first, info.last_backfill, info.last_backfill_bitwise) > 0) {
	      derr << __func__ << ": invalid missing set entry found "
		   << i.first
		   << dendl;
	      assert(0 == "invalid missing set entry found");
	    }
	    bufferlist bv;
	    int r = store->getattr(
	      pg_coll,
	      ghobject_t(i.first, ghobject_t::NO_GEN, info.pgid.shard),
	      OI_ATTR,
	      bv);
	    if (r >= 0) {
	      object_info_t oi(bv);
	      assert(oi.version == i.second.have);
	    } else {
	      assert(eversion_t() == i.second.have);
	    }
	  }
	} else {
	  assert(has_divergent_priors);
	  for (map<eversion_t, hobject_t>::reverse_iterator i =
		 divergent_priors.rbegin();
	       i != divergent_priors.rend();
	       ++i) {
	    if (i->first <= info.last_complete) break;
	    if (cmp(i->second, info.last_backfill, info.last_backfill_bitwise) > 0)
	      continue;
	    if (did.count(i->second)) continue;
	    did.insert(i->second);
	    bufferlist bv;
	    int r = store->getattr(
	      pg_coll,
	      ghobject_t(i->second, ghobject_t::NO_GEN, info.pgid.shard),
	      OI_ATTR,
	      bv);
	    if (r >= 0) {
	      object_info_t oi(bv);
	      /**
		 * 1) we see this entry in the divergent priors mapping
		 * 2) we didn't see an entry for this object in the log
		 *
		 * From 1 & 2 we know that either the object does not exist
		 * or it is at the version specified in the divergent_priors
		 * map since the object would have been deleted atomically
		 * with the addition of the divergent_priors entry, an older
		 * version would not have been recovered, and a newer version
		 * would show up in the log above.
		 */
	      assert(oi.version == i->first);
	    } else {
	      ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;
	      missing.add(i->second, i->first, eversion_t());
	    }
	  }
	}
	if (clear_divergent_priors)
	  (*clear_divergent_priors) = true;
      }
    }

    if (!has_divergent_priors) {
      if (clear_divergent_priors)
	(*clear_divergent_priors) = false;
      missing.flush();
    }
    ldpp_dout(dpp, 10) << "read_log_and_missing done" << dendl;
  }
};
  
#endif // CEPH_PG_LOG_H
