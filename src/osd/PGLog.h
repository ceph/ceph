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
#pragma once

// re-include our assert to clobber boost's
#include "include/ceph_assert.h"
#include "osd_types.h"
#include "os/ObjectStore.h"
#include <list>

constexpr auto PGLOG_INDEXED_OBJECTS          = 1 << 0;
constexpr auto PGLOG_INDEXED_CALLER_OPS       = 1 << 1;
constexpr auto PGLOG_INDEXED_EXTRA_CALLER_OPS = 1 << 2;
constexpr auto PGLOG_INDEXED_DUPS             = 1 << 3;
constexpr auto PGLOG_INDEXED_ALL              = PGLOG_INDEXED_OBJECTS 
                                              | PGLOG_INDEXED_CALLER_OPS 
                                              | PGLOG_INDEXED_EXTRA_CALLER_OPS 
                                              | PGLOG_INDEXED_DUPS;

class CephContext;

struct PGLog : DoutPrefixProvider {
  std::ostream& gen_prefix(std::ostream& out) const override {
    return out;
  }
  unsigned get_subsys() const override {
    return static_cast<unsigned>(ceph_subsys_osd);
  }
  CephContext *get_cct() const override {
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

public:
  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public pg_log_t {
    mutable ceph::unordered_map<hobject_t,pg_log_entry_t*> objects;  // ptrs into log.  be careful!
    mutable ceph::unordered_map<osd_reqid_t,pg_log_entry_t*> caller_ops;
    mutable ceph::unordered_multimap<osd_reqid_t,pg_log_entry_t*> extra_caller_ops;
    mutable ceph::unordered_map<osd_reqid_t,pg_log_dup_t*> dup_index;

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
    mempool::osd_pglog::list<pg_log_entry_t>::reverse_iterator
      rollback_info_trimmed_to_riter;

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
    { }

    template <typename... Args>
    explicit IndexedLog(Args&&... args) :
      pg_log_t(std::forward<Args>(args)...),
      complete_to(log.end()),
      last_requested(0),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin())
    {
      reset_rollback_info_trimmed_to_riter();
      index();
    }

    IndexedLog(const IndexedLog &rhs) :
      pg_log_t(rhs),
      complete_to(log.end()),
      last_requested(rhs.last_requested),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin())
    {
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

    mempool::osd_pglog::list<pg_log_entry_t> rewind_from_head(eversion_t newhead) {
      auto divergent = pg_log_t::rewind_from_head(newhead);
      index();
      reset_rollback_info_trimmed_to_riter();
      return divergent;
    }

    template <typename T>
    void scan_log_after(
      const eversion_t &bound, ///< [in] scan entries > bound
      T &&f) const {
      auto iter = log.rbegin();
      while (iter != log.rend() && iter->version > bound)
	++iter;

      while (true) {
	if (iter == log.rbegin())
	  break;
	f(*(--iter));
      }
    }

    /****/
    void claim_log_and_clear_rollback_info(const pg_log_t& o) {
      // we must have already trimmed the old entries
      ceph_assert(rollback_info_trimmed_to == head);
      ceph_assert(rollback_info_trimmed_to_riter == log.rbegin());

      *this = IndexedLog(o);

      skip_can_rollback_to_to_head();
      index();
    }

    void split_out_child(
      pg_t child_pgid,
      unsigned split_bits,
      IndexedLog *target);

    void zero() {
      // we must have already trimmed the old entries
      ceph_assert(rollback_info_trimmed_to == head);
      ceph_assert(rollback_info_trimmed_to_riter == log.rbegin());

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
      eversion_t *version,
      version_t *user_version,
      int *return_code) const
    {
      ceph_assert(version);
      ceph_assert(user_version);
      ceph_assert(return_code);
      ceph::unordered_map<osd_reqid_t,pg_log_entry_t*>::const_iterator p;
      if (!(indexed_data & PGLOG_INDEXED_CALLER_OPS)) {
        index_caller_ops();
      }
      p = caller_ops.find(r);
      if (p != caller_ops.end()) {
	*version = p->second->version;
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
	uint32_t idx = 0;
	for (auto i = p->second->extra_reqids.begin();
	     i != p->second->extra_reqids.end();
	     ++idx, ++i) {
	  if (i->first == r) {
	    *version = p->second->version;
	    *user_version = i->second;
	    *return_code = p->second->return_code;
	    if (*return_code >= 0) {
	      auto it = p->second->extra_reqid_return_codes.find(idx);
	      if (it != p->second->extra_reqid_return_codes.end()) {
		*return_code = it->second;
	      }
	    }
	    return true;
	  }
	}
	ceph_abort_msg("in extra_caller_ops but not extra_reqids");
      }

      if (!(indexed_data & PGLOG_INDEXED_DUPS)) {
        index_dups();
      }
      auto q = dup_index.find(r);
      if (q != dup_index.end()) {
	*version = q->second->version;
	*user_version = q->second->user_version;
	*return_code = q->second->return_code;
	return true;
      }

      return false;
    }

    /// get a (bounded) list of recent reqids for the given object
    void get_object_reqids(const hobject_t& oid, unsigned max,
			   mempool::osd_pglog::vector<pair<osd_reqid_t, version_t> > *pls,
			   mempool::osd_pglog::map<uint32_t, int> *return_codes) const {
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
	  if (i->reqid_is_indexed()) {
	    if (i->op == pg_log_entry_t::ERROR) {
	      // propagate op errors to the cache tier's PG log
	      return_codes->emplace(pls->size(), i->return_code);
	    }
	    pls->push_back(make_pair(i->reqid, i->user_version));
	  }

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
      // if to_index is 0, no need to run any of this code, especially
      // loop below; this can happen with copy constructor for
      // IndexedLog (and indirectly through assignment operator)
      if (!to_index) return;

      if (to_index & PGLOG_INDEXED_OBJECTS)
	objects.clear();
      if (to_index & PGLOG_INDEXED_CALLER_OPS)
	caller_ops.clear();
      if (to_index & PGLOG_INDEXED_EXTRA_CALLER_OPS)
	extra_caller_ops.clear();
      if (to_index & PGLOG_INDEXED_DUPS) {
	dup_index.clear();
	for (auto& i : dups) {
	  dup_index[i.reqid] = const_cast<pg_log_dup_t*>(&i);
	}
      }

      constexpr __u16 any_log_entry_index =
	PGLOG_INDEXED_OBJECTS |
	PGLOG_INDEXED_CALLER_OPS |
	PGLOG_INDEXED_EXTRA_CALLER_OPS;

      if (to_index & any_log_entry_index) {
	for (list<pg_log_entry_t>::const_iterator i = log.begin();
	     i != log.end();
	     ++i) {
	  if (to_index & PGLOG_INDEXED_OBJECTS) {
	    if (i->object_is_indexed()) {
	      objects[i->soid] = const_cast<pg_log_entry_t*>(&(*i));
	    }
	  }

	  if (to_index & PGLOG_INDEXED_CALLER_OPS) {
	    if (i->reqid_is_indexed()) {
	      caller_ops[i->reqid] = const_cast<pg_log_entry_t*>(&(*i));
	    }
	  }

	  if (to_index & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
	    for (auto j = i->extra_reqids.begin();
		 j != i->extra_reqids.end();
		 ++j) {
	      extra_caller_ops.insert(
		make_pair(j->first, const_cast<pg_log_entry_t*>(&(*i))));
	    }
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

    void index_dups() const {
      index(PGLOG_INDEXED_DUPS);
    }

    void index(pg_log_entry_t& e) {
      if ((indexed_data & PGLOG_INDEXED_OBJECTS) && e.object_is_indexed()) {
        if (objects.count(e.soid) == 0 ||
            objects[e.soid]->version < e.version)
          objects[e.soid] = &e;
      }
      if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
	// divergent merge_log indexes new before unindexing old
        if (e.reqid_is_indexed()) {
	  caller_ops[e.reqid] = &e;
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (auto j = e.extra_reqids.begin();
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
      dup_index.clear();
      indexed_data = 0;
    }

    void unindex(const pg_log_entry_t& e) {
      // NOTE: this only works if we remove from the _tail_ of the log!
      if (indexed_data & PGLOG_INDEXED_OBJECTS) {
	auto it = objects.find(e.soid);
        if (it != objects.end() && it->second->version == e.version)
          objects.erase(it);
      }
      if (e.reqid_is_indexed()) {
        if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
	  auto it = caller_ops.find(e.reqid);
	  // divergent merge_log indexes new before unindexing old
          if (it != caller_ops.end() && it->second == &e)
            caller_ops.erase(it);
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (auto j = e.extra_reqids.begin();
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

    void index(pg_log_dup_t& e) {
      if (indexed_data & PGLOG_INDEXED_DUPS) {
	dup_index[e.reqid] = &e;
      }
    }

    void unindex(const pg_log_dup_t& e) {
      if (indexed_data & PGLOG_INDEXED_DUPS) {
	auto i = dup_index.find(e.reqid);
	if (i != dup_index.end()) {
	  dup_index.erase(i);
	}
      }
    }

    // actors
    void add(const pg_log_entry_t& e, bool applied = true) {
      if (!applied) {
	ceph_assert(get_can_rollback_to() == head);
      }

      // make sure our buffers don't pin bigger buffers
      e.mod_desc.trim_bl();

      // add to log
      log.push_back(e);

      // riter previously pointed to the previous entry
      if (rollback_info_trimmed_to_riter == log.rbegin())
	++rollback_info_trimmed_to_riter;

      ceph_assert(e.version > head);
      ceph_assert(head.version == 0 || e.version.version > head.version);
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
        for (auto j = e.extra_reqids.begin();
	     j != e.extra_reqids.end();
	     ++j) {
	  extra_caller_ops.insert(make_pair(j->first, &(log.back())));
        }
      }

      if (!applied) {
	skip_can_rollback_to_to_head();
      }
    } // add

    void trim(
      CephContext* cct,
      eversion_t s,
      set<eversion_t> *trimmed,
      set<string>* trimmed_dups,
      eversion_t *write_from_dups);

    ostream& print(ostream& out) const;
  }; // IndexedLog


protected:
  //////////////////// data members ////////////////////

  pg_missing_tracker_t missing;
  IndexedLog  log;

  eversion_t dirty_to;         ///< must clear/writeout all keys <= dirty_to
  eversion_t dirty_from;       ///< must clear/writeout all keys >= dirty_from
  eversion_t writeout_from;    ///< must writout keys >= writeout_from
  set<eversion_t> trimmed;     ///< must clear keys in trimmed
  eversion_t dirty_to_dups;    ///< must clear/writeout all dups <= dirty_to_dups
  eversion_t dirty_from_dups;  ///< must clear/writeout all dups >= dirty_from_dups
  eversion_t write_from_dups;  ///< must write keys >= write_from_dups
  set<string> trimmed_dups;    ///< must clear keys in trimmed_dups
  CephContext *cct;
  bool pg_log_debug;
  /// Log is clean on [dirty_to, dirty_from)
  bool touched_log;
  bool clear_divergent_priors;
  bool rebuilt_missing_with_deletes = false;

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
  void mark_dirty_to_dups(eversion_t to) {
    if (to > dirty_to_dups)
      dirty_to_dups = to;
  }
  void mark_dirty_from_dups(eversion_t from) {
    if (from < dirty_from_dups)
      dirty_from_dups = from;
  }
public:
  bool is_dirty() const {
    return !touched_log ||
      (dirty_to != eversion_t()) ||
      (dirty_from != eversion_t::max()) ||
      (writeout_from != eversion_t::max()) ||
      !(trimmed.empty()) ||
      !missing.is_clean() ||
      !(trimmed_dups.empty()) ||
      (dirty_to_dups != eversion_t()) ||
      (dirty_from_dups != eversion_t::max()) ||
      (write_from_dups != eversion_t::max()) ||
      rebuilt_missing_with_deletes;
  }
  void mark_log_for_rewrite() {
    mark_dirty_to(eversion_t::max());
    mark_dirty_from(eversion_t());
    mark_dirty_to_dups(eversion_t::max());
    mark_dirty_from_dups(eversion_t());
    touched_log = false;
  }
  bool get_rebuilt_missing_with_deletes() const {
    return rebuilt_missing_with_deletes;
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
    trimmed_dups.clear();
    writeout_from = eversion_t::max();
    check();
    missing.flush();
    dirty_to_dups = eversion_t();
    dirty_from_dups = eversion_t::max();
    write_from_dups = eversion_t::max();
  }
public:

  // cppcheck-suppress noExplicitConstructor
  PGLog(CephContext *cct) :
    dirty_from(eversion_t::max()),
    writeout_from(eversion_t::max()),
    dirty_from_dups(eversion_t::max()),
    write_from_dups(eversion_t::max()),
    cct(cct),
    pg_log_debug(!(cct && !(cct->_conf->osd_debug_pg_log_writeout))),
    touched_log(false),
    clear_divergent_priors(false)
  { }

  void reset_backfill();

  void clear();

  //////////////////// get or set missing ////////////////////

  const pg_missing_tracker_t& get_missing() const { return missing; }

  void missing_add(const hobject_t& oid, eversion_t need, eversion_t have, bool is_delete=false) {
    missing.add(oid, need, have, is_delete);
  }

  void missing_add_next_entry(const pg_log_entry_t& e) {
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
    pg_info_t &info,
    bool transaction_applied = true,
    bool async = false);

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
    mark_dirty_to_dups(eversion_t::max());
  }

  void split_into(
      pg_t child_pgid,
      unsigned split_bits,
      PGLog *opg_log) {
    log.split_out_child(child_pgid, split_bits, &opg_log->log);
    missing.split_into(child_pgid, split_bits, &(opg_log->missing));
    opg_log->mark_dirty_to(eversion_t::max());
    opg_log->mark_dirty_to_dups(eversion_t::max());
    mark_dirty_to(eversion_t::max());
    mark_dirty_to_dups(eversion_t::max());
    if (missing.may_include_deletes)
      opg_log->rebuilt_missing_with_deletes = true;
  }

  void merge_from(
    const vector<PGLog*>& sources,
    eversion_t last_update) {
    unindex();
    missing.clear();

    vector<pg_log_t*> slogs;
    for (auto s : sources) {
      slogs.push_back(&s->log);
    }
    log.merge_from(slogs, last_update);

    index();

    mark_log_for_rewrite();
  }

  void recover_got(hobject_t oid, eversion_t v, pg_info_t &info) {
    if (missing.is_missing(oid, v)) {
      missing.got(oid, v);
      info.stats.stats.sum.num_objects_missing = missing.num_missing();

      // raise last_complete?
      if (missing.get_items().empty()) {
	log.complete_to = log.log.end();
	info.last_complete = info.last_update;
      }
      auto oldest_need = missing.get_oldest_need();
      while (log.complete_to != log.log.end()) {
	if (oldest_need <= log.complete_to->version)
	  break;
	if (info.last_complete < log.complete_to->version)
	  info.last_complete = log.complete_to->version;
	++log.complete_to;
      }
    }

    ceph_assert(log.get_can_rollback_to() >= v);
  }

  void reset_complete_to(pg_info_t *info) {
    log.complete_to = log.log.begin();
    ceph_assert(log.complete_to != log.log.end());
    auto oldest_need = missing.get_oldest_need();
    if (oldest_need != eversion_t()) {
      while (log.complete_to->version < oldest_need) {
        ++log.complete_to;
        ceph_assert(log.complete_to != log.log.end());
      }
    }
    if (!info)
      return;
    if (log.complete_to == log.log.begin()) {
      info->last_complete = eversion_t();
    } else {
      --log.complete_to;
      info->last_complete = log.complete_to->version;
      ++log.complete_to;
    }
  }

  void activate_not_complete(pg_info_t &info) {
    reset_complete_to(&info);
    log.last_requested = 0;
  }

  void proc_replica_log(pg_info_t &oinfo,
			const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from) const;

  void rebuild_missing_set_with_deletes(ObjectStore *store,
					ObjectStore::CollectionHandle& ch,
					const pg_info_t &info);

protected:
  static void split_by_object(
    mempool::osd_pglog::list<pg_log_entry_t> &entries,
    map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t>> *out_entries) {
    while (!entries.empty()) {
      auto &out_list = (*out_entries)[entries.front().soid];
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
    const mempool::osd_pglog::list<pg_log_entry_t> &orig_entries, ///< [in] entries for hoid to merge
    const pg_info_t &info,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &missing,               ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    ldpp_dout(dpp, 20) << __func__ << ": merging hoid " << hoid
		       << " entries: " << orig_entries << dendl;

    if (hoid > info.last_backfill) {
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " after last_backfill"
			 << dendl;
      return;
    }

    // entries is non-empty
    ceph_assert(!orig_entries.empty());
    // strip out and ignore ERROR entries
    mempool::osd_pglog::list<pg_log_entry_t> entries;
    eversion_t last;
    bool seen_non_error = false;
    for (list<pg_log_entry_t>::const_iterator i = orig_entries.begin();
	 i != orig_entries.end();
	 ++i) {
      // all entries are on hoid
      ceph_assert(i->soid == hoid);
      // did not see error entries before this entry and this entry is not error
      // then this entry is the first non error entry
      bool first_non_error = ! seen_non_error && ! i->is_error();
      if (! i->is_error() ) {
        // see a non error entry now
        seen_non_error = true;
      }
      
      // No need to check the first entry since it prior_version is unavailable
      // in the list
      // No need to check if the prior_version is the minimal version
      // No need to check the first non-error entry since the leading error
      // entries are not its prior version
      if (i != orig_entries.begin() && i->prior_version != eversion_t() &&
          ! first_non_error) {
	// in increasing order of version
	ceph_assert(i->version > last);
	// prior_version correct (unless it is an ERROR entry)
	ceph_assert(i->prior_version == last || i->is_error());
      }
      if (i->is_error()) {
	ldpp_dout(dpp, 20) << __func__ << ": ignoring " << *i << dendl;
      } else {
	ldpp_dout(dpp, 20) << __func__ << ": keeping " << *i << dendl;
	entries.push_back(*i);
	last = i->version;
      }
    }
    if (entries.empty()) {
      ldpp_dout(dpp, 10) << __func__ << ": no non-ERROR entries" << dendl;
      return;
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

      ceph_assert(objiter->second->version > last_divergent_update);

      // ensure missing has been updated appropriately
      if (objiter->second->is_update() ||
	  (missing.may_include_deletes && objiter->second->is_delete())) {
	ceph_assert(missing.is_missing(hoid) &&
	       missing.get_items().at(hoid).need == objiter->second->version);
      } else {
	ceph_assert(!missing.is_missing(hoid));
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
	missing.revise_need(hoid, prior_version, false);
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
	ceph_assert(i->can_rollback() && i->version > olog_can_rollback_to);
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
      missing.add(hoid, prior_version, eversion_t(), false);
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
    mempool::osd_pglog::list<pg_log_entry_t> &entries,       ///< [in] entries to merge
    const pg_info_t &oinfo,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &omissing,              ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t> > split;
    split_by_object(entries, &split);
    for (map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t>>::iterator i = split.begin();
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
    mempool::osd_pglog::list<pg_log_entry_t> entries;
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

  bool merge_log_dups(const pg_log_t& olog);

public:

  void rewind_divergent_log(eversion_t newhead,
                            pg_info_t &info,
                            LogEntryHandler *rollbacker,
                            bool &dirty_info,
                            bool &dirty_big_info);

  void merge_log(pg_info_t &oinfo,
		 pg_log_t &olog,
		 pg_shard_t from,
		 pg_info_t &info, LogEntryHandler *rollbacker,
		 bool &dirty_info, bool &dirty_big_info);

  template <typename missing_type>
  static bool append_log_entries_update_missing(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    bool maintain_rollback,
    IndexedLog *log,
    missing_type &missing,
    LogEntryHandler *rollbacker,
    const DoutPrefixProvider *dpp) {
    bool invalidate_stats = false;
    if (log && !entries.empty()) {
      ceph_assert(log->head < entries.begin()->version);
    }
    for (list<pg_log_entry_t>::const_iterator p = entries.begin();
	 p != entries.end();
	 ++p) {
      invalidate_stats = invalidate_stats || !p->is_error();
      if (log) {
	ldpp_dout(dpp, 20) << "update missing, append " << *p << dendl;
	log->add(*p);
      }
      if (p->soid <= last_backfill &&
	  !p->is_error()) {
	if (missing.may_include_deletes) {
	  missing.add_next_event(*p);
	} else {
	  if (p->is_delete()) {
	    missing.rm(p->soid, p->version);
	  } else {
	    missing.add_next_event(*p);
	  }
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
    }
    return invalidate_stats;
  }
  bool append_new_log_entries(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
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
      if (entries.begin()->is_lost_delete()) {
	// hack: since lost deletes queue recovery directly, and don't
	// go through activate_not_complete() again, our complete_to
	// iterator may still point at log.end(). Reset it to point
	// before these new lost_delete entries.  This only occurs
	// when lost+delete entries are initially added, which is
	// always in a list of solely lost_delete entries, so it is
	// sufficient to check whether the first entry is a
	// lost_delete
	reset_complete_to(nullptr);
      }
    }
    return invalidate_stats;
  }

  void write_log_and_missing(
    ObjectStore::Transaction& t,
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
    bool require_rollback,
    bool *rebuilt_missing_set_with_deletes);

  static void _write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors,
    eversion_t dirty_to,
    eversion_t dirty_from,
    eversion_t writeout_from,
    bool dirty_divergent_priors,
    bool touch_log,
    bool require_rollback,
    eversion_t dirty_to_dups,
    eversion_t dirty_from_dups,
    eversion_t write_from_dups,
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
    set<eversion_t> &&trimmed,
    set<string> &&trimmed_dups,
    const pg_missing_tracker_t &missing,
    bool touch_log,
    bool require_rollback,
    bool clear_divergent_priors,
    eversion_t dirty_to_dups,
    eversion_t dirty_from_dups,
    eversion_t write_from_dups,
    bool *rebuilt_missing_with_deletes,
    set<string> *log_keys_debug
    );

  void read_log_and_missing(
    ObjectStore *store,
    ObjectStore::CollectionHandle& ch,
    ghobject_t pgmeta_oid,
    const pg_info_t &info,
    ostringstream &oss,
    bool tolerate_divergent_missing_log,
    bool debug_verify_stored_missing = false
    ) {
    return read_log_and_missing(
      store, ch, pgmeta_oid, info,
      log, missing, oss,
      tolerate_divergent_missing_log,
      &clear_divergent_priors,
      this,
      (pg_log_debug ? &log_keys_debug : nullptr),
      debug_verify_stored_missing);
  }

  template <typename missing_type>
  static void read_log_and_missing(
    ObjectStore *store,
    ObjectStore::CollectionHandle &ch,
    ghobject_t pgmeta_oid,
    const pg_info_t &info,
    IndexedLog &log,
    missing_type &missing,
    ostringstream &oss,
    bool tolerate_divergent_missing_log,
    bool *clear_divergent_priors = nullptr,
    const DoutPrefixProvider *dpp = nullptr,
    set<string> *log_keys_debug = nullptr,
    bool debug_verify_stored_missing = false
    ) {
    ldpp_dout(dpp, 20) << "read_log_and_missing coll " << ch->cid
		       << " " << pgmeta_oid << dendl;

    // legacy?
    struct stat st;
    int r = store->stat(ch, pgmeta_oid, &st);
    ceph_assert(r == 0);
    ceph_assert(st.st_size == 0);

    // will get overridden below if it had been recorded
    eversion_t on_disk_can_rollback_to = info.last_update;
    eversion_t on_disk_rollback_info_trimmed_to = eversion_t();
    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(ch,
							      pgmeta_oid);
    map<eversion_t, hobject_t> divergent_priors;
    bool must_rebuild = false;
    missing.may_include_deletes = false;
    list<pg_log_entry_t> entries;
    list<pg_log_dup_t> dups;
    if (p) {
      for (p->seek_to_first(); p->valid() ; p->next()) {
	// non-log pgmeta_oid keys are prefixed with _; skip those
	if (p->key()[0] == '_')
	  continue;
	bufferlist bl = p->value();//Copy bufferlist before creating iterator
	auto bp = bl.cbegin();
	if (p->key() == "divergent_priors") {
	  decode(divergent_priors, bp);
	  ldpp_dout(dpp, 20) << "read_log_and_missing " << divergent_priors.size()
			     << " divergent_priors" << dendl;
	  must_rebuild = true;
	  debug_verify_stored_missing = false;
	} else if (p->key() == "can_rollback_to") {
	  decode(on_disk_can_rollback_to, bp);
	} else if (p->key() == "rollback_info_trimmed_to") {
	  decode(on_disk_rollback_info_trimmed_to, bp);
	} else if (p->key() == "may_include_deletes_in_missing") {
	  missing.may_include_deletes = true;
	} else if (p->key().substr(0, 7) == string("missing")) {
	  hobject_t oid;
	  pg_missing_item item;
	  decode(oid, bp);
	  decode(item, bp);
	  if (item.is_delete()) {
	    ceph_assert(missing.may_include_deletes);
	  }
	  missing.add(oid, item.need, item.have, item.is_delete());
	} else if (p->key().substr(0, 4) == string("dup_")) {
	  pg_log_dup_t dup;
	  decode(dup, bp);
	  if (!dups.empty()) {
	    ceph_assert(dups.back().version < dup.version);
	  }
	  dups.push_back(dup);
	} else {
	  pg_log_entry_t e;
	  e.decode_with_checksum(bp);
	  ldpp_dout(dpp, 20) << "read_log_and_missing " << e << dendl;
	  if (!entries.empty()) {
	    pg_log_entry_t last_e(entries.back());
	    ceph_assert(last_e.version.version < e.version.version);
	    ceph_assert(last_e.version.epoch <= e.version.epoch);
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
      std::move(entries),
      std::move(dups));

    if (must_rebuild || debug_verify_stored_missing) {
      // build missing
      if (debug_verify_stored_missing || info.last_complete < info.last_update) {
	ldpp_dout(dpp, 10)
	  << "read_log_and_missing checking for missing items over interval ("
	  << info.last_complete
	  << "," << info.last_update << "]" << dendl;

	set<hobject_t> did;
	set<hobject_t> checked;
	set<hobject_t> skipped;
	for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	     i != log.log.rend();
	     ++i) {
	  if (!debug_verify_stored_missing && i->version <= info.last_complete) break;
	  if (i->soid > info.last_backfill)
	    continue;
	  if (i->is_error())
	    continue;
	  if (did.count(i->soid)) continue;
	  did.insert(i->soid);

	  if (!missing.may_include_deletes && i->is_delete())
	    continue;

	  bufferlist bv;
	  int r = store->getattr(
	    ch,
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
		ceph_assert(miter != missing.get_items().end());
		ceph_assert(miter->second.need == i->version);
		// the 'have' version is reset if an object is deleted,
		// then created again
		ceph_assert(miter->second.have == oi.version || miter->second.have == eversion_t());
		checked.insert(i->soid);
	      } else {
		missing.add(i->soid, i->version, oi.version, i->is_delete());
	      }
	    }
	  } else {
	    ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;
	    if (debug_verify_stored_missing) {
	      auto miter = missing.get_items().find(i->soid);
	      if (i->is_delete()) {
		ceph_assert(miter == missing.get_items().end() ||
		       (miter->second.need == i->version &&
			miter->second.have == eversion_t()));
	      } else {
		ceph_assert(miter != missing.get_items().end());
		ceph_assert(miter->second.need == i->version);
		ceph_assert(miter->second.have == eversion_t());
	      }
	      checked.insert(i->soid);
	    } else {
	      missing.add(i->soid, i->version, eversion_t(), i->is_delete());
	    }
	  }
	}
	if (debug_verify_stored_missing) {
	  for (auto &&i: missing.get_items()) {
	    if (checked.count(i.first))
	      continue;
	    if (i.first > info.last_backfill) {
	      ldpp_dout(dpp, -1) << __func__ << ": invalid missing set entry "
				<< "found before last_backfill: "
				<< i.first << " " << i.second
				<< " last_backfill = " << info.last_backfill
				<< dendl;
	      ceph_abort_msg("invalid missing set entry found");
	    }
	    bufferlist bv;
	    int r = store->getattr(
	      ch,
	      ghobject_t(i.first, ghobject_t::NO_GEN, info.pgid.shard),
	      OI_ATTR,
	      bv);
	    if (r >= 0) {
	      object_info_t oi(bv);
	      ceph_assert(oi.version == i.second.have || eversion_t() == i.second.have);
	    } else {
	      ceph_assert(i.second.is_delete() || eversion_t() == i.second.have);
	    }
	  }
	} else {
	  ceph_assert(must_rebuild);
	  for (map<eversion_t, hobject_t>::reverse_iterator i =
		 divergent_priors.rbegin();
	       i != divergent_priors.rend();
	       ++i) {
	    if (i->first <= info.last_complete) break;
	    if (i->second > info.last_backfill)
	      continue;
	    if (did.count(i->second)) continue;
	    did.insert(i->second);
	    bufferlist bv;
	    int r = store->getattr(
	      ch,
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
	      	/**
		 * Unfortunately the assessment above is incorrect because of
		 * http://tracker.ceph.com/issues/17916 (we were incorrectly
		 * not removing the divergent_priors set from disk state!),
		 * so let's check that.
		 */
	      if (oi.version > i->first && tolerate_divergent_missing_log) {
		ldpp_dout(dpp, 0) << "read_log divergent_priors entry (" << *i
				  << ") inconsistent with disk state (" <<  oi
				  << "), assuming it is tracker.ceph.com/issues/17916"
				  << dendl;
	      } else {
		ceph_assert(oi.version == i->first);
	      }
	    } else {
	      ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;
	      missing.add(i->second, i->first, eversion_t(), false);
	    }
	  }
	}
	if (clear_divergent_priors)
	  (*clear_divergent_priors) = true;
      }
    }

    if (!must_rebuild) {
      if (clear_divergent_priors)
	(*clear_divergent_priors) = false;
      missing.flush();
    }
    ldpp_dout(dpp, 10) << "read_log_and_missing done" << dendl;
  } // static read_log_and_missing
}; // struct PGLog
