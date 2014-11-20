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

#include "PGLog.h"
#include "PG.h"
#include "SnapMapper.h"
#include "../include/unordered_map.h"

#define dout_subsys ceph_subsys_osd

static coll_t META_COLL("meta");

//////////////////// PGLog::IndexedLog ////////////////////

void PGLog::IndexedLog::advance_rollback_info_trimmed_to(
  eversion_t to,
  LogEntryHandler *h)
{
  assert(to <= can_rollback_to);

  if (to > rollback_info_trimmed_to)
    rollback_info_trimmed_to = to;

  while (rollback_info_trimmed_to_riter != log.rbegin()) {
    --rollback_info_trimmed_to_riter;
    if (rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to) {
      ++rollback_info_trimmed_to_riter;
      break;
    }
    h->trim(*rollback_info_trimmed_to_riter);
  }
}

void PGLog::IndexedLog::split_into(
  pg_t child_pgid,
  unsigned split_bits,
  PGLog::IndexedLog *olog)
{
  list<pg_log_entry_t> oldlog;
  oldlog.swap(log);

  eversion_t old_tail;
  olog->head = head;
  olog->tail = tail;
  unsigned mask = ~((~0)<<split_bits);
  for (list<pg_log_entry_t>::iterator i = oldlog.begin();
       i != oldlog.end();
       ) {
    if ((i->soid.hash & mask) == child_pgid.m_seed) {
      olog->log.push_back(*i);
    } else {
      log.push_back(*i);
    }
    oldlog.erase(i++);
  }


  olog->can_rollback_to = can_rollback_to;

  olog->index();
  index();
}

void PGLog::IndexedLog::trim(
  LogEntryHandler *handler,
  eversion_t s,
  set<eversion_t> *trimmed)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is "
		    << complete_to->version
		    << " on " << *this << dendl;
  }

  if (s > can_rollback_to)
    can_rollback_to = s;
  advance_rollback_info_trimmed_to(s, handler);

  while (!log.empty()) {
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    if (trimmed)
      trimmed->insert(e.version);

    unindex(e);         // remove from index,

    if (rollback_info_trimmed_to_riter == log.rend() ||
	e.version == rollback_info_trimmed_to_riter->version) {
      log.pop_front();
      rollback_info_trimmed_to_riter = log.rend();
    } else {
      log.pop_front();
    }
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

ostream& PGLog::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}

//////////////////// PGLog ////////////////////

void PGLog::reset_backfill()
{
  missing.clear();
  divergent_priors.clear();
  dirty_divergent_priors = true;
}

void PGLog::clear() {
  divergent_priors.clear();
  missing.clear();
  log.clear();
  log_keys_debug.clear();
  undirty();
}

void PGLog::clear_info_log(
  spg_t pgid,
  const hobject_t &infos_oid,
  const hobject_t &log_oid,
  ObjectStore::Transaction *t) {

  set<string> keys_to_remove;
  keys_to_remove.insert(PG::get_epoch_key(pgid));
  keys_to_remove.insert(PG::get_biginfo_key(pgid));
  keys_to_remove.insert(PG::get_info_key(pgid));

  t->remove(META_COLL, log_oid);
  t->omap_rmkeys(META_COLL, infos_oid, keys_to_remove);
}

void PGLog::trim(
  LogEntryHandler *handler,
  eversion_t trim_to,
  pg_info_t &info)
{
  // trim?
  if (trim_to > log.tail) {
    /* If we are trimming, we must be complete up to trim_to, time
     * to throw out any divergent_priors
     */
    divergent_priors.clear();
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(handler, trim_to, &trimmed);
    info.log_tail = log.tail;
  }
}

void PGLog::proc_replica_log(
  ObjectStore::Transaction& t,
  pg_info_t &oinfo, const pg_log_t &olog, pg_missing_t& omissing,
  pg_shard_t from) const
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  /*
    basically what we're doing here is rewinding the remote log,
    dropping divergent entries, until we find something that matches
    our master log.  we then reset last_update to reflect the new
    point up to which missing is accurate.

    later, in activate(), missing will get wound forward again and
    we will send the peer enough log to arrive at the same state.
  */

  for (map<hobject_t, pg_missing_t::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " before missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }

  list<pg_log_entry_t>::const_iterator fromiter = log.log.end();
  eversion_t lower_bound = log.tail;
  while (1) {
    if (fromiter == log.log.begin())
      break;
    --fromiter;
    if (fromiter->version <= olog.head) {
      dout(20) << "merge_log cut point (usually last shared) is "
	       << *fromiter << dendl;
      lower_bound = fromiter->version;
      ++fromiter;
      break;
    }
  }

  list<pg_log_entry_t> divergent;
  list<pg_log_entry_t>::const_iterator pp = olog.log.end();
  eversion_t lu(oinfo.last_update);
  while (true) {
    if (pp == olog.log.begin()) {
      if (pp != olog.log.end())   // no last_update adjustment if we discard nothing!
	lu = olog.tail;
      break;
    }
    --pp;
    const pg_log_entry_t& oe = *pp;

    // don't continue past the tail of our log.
    if (oe.version <= log.tail) {
      lu = oe.version;
      ++pp;
      break;
    }

    if (oe.version <= lower_bound) {
      lu = oe.version;
      ++pp;
      break;
    }

    divergent.push_front(oe);
  }    


  IndexedLog folog;
  folog.log.insert(folog.log.begin(), olog.log.begin(), pp);
  folog.index();
  _merge_divergent_entries(
    folog,
    divergent,
    oinfo,
    olog.can_rollback_to,
    omissing,
    0,
    0);

  if (lu < oinfo.last_update) {
    dout(10) << " peer osd." << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
  }

  if (omissing.have_missing()) {
    eversion_t first_missing =
      omissing.missing[omissing.rmissing.begin()->second].need;
    oinfo.last_complete = eversion_t();
    list<pg_log_entry_t>::const_iterator i = olog.log.begin();
    for (;
	 i != olog.log.end();
	 ++i) {
      if (i->version < first_missing)
	oinfo.last_complete = i->version;
      else
	break;
    }
  } else {
    oinfo.last_complete = oinfo.last_update;
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
void PGLog::_merge_object_divergent_entries(
  const IndexedLog &log,
  const hobject_t &hoid,
  const list<pg_log_entry_t> &entries,
  const pg_info_t &info,
  eversion_t olog_can_rollback_to,
  pg_missing_t &missing,
  boost::optional<pair<eversion_t, hobject_t> > *new_divergent_prior,
  LogEntryHandler *rollbacker
  )
{
  dout(10) << __func__ << ": merging hoid " << hoid
	   << " entries: " << entries << dendl;

  if (hoid > info.last_backfill) {
    dout(10) << __func__ << ": hoid " << hoid << " after last_backfill"
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

    if (rollbacker)
      rollbacker->trim(*i);
  }

  const eversion_t prior_version = entries.begin()->prior_version;
  const eversion_t first_divergent_update = entries.begin()->version;
  const eversion_t last_divergent_update = entries.rbegin()->version;
  const bool object_not_in_store =
    !missing.is_missing(hoid) &&
    entries.rbegin()->is_delete();
  dout(10) << __func__ << ": hoid " << hoid
	   << " prior_version: " << prior_version
	   << " first_divergent_update: " << first_divergent_update
	   << " last_divergent_update: " << last_divergent_update
	   << dendl;

  ceph::unordered_map<hobject_t, pg_log_entry_t*>::const_iterator objiter =
    log.objects.find(hoid);
  if (objiter != log.objects.end() &&
      objiter->second->version >= first_divergent_update) {
    /// Case 1)
    assert(objiter->second->version > last_divergent_update);

    dout(10) << __func__ << ": more recent entry found: "
	     << *objiter->second << ", already merged" << dendl;

    // ensure missing has been updated appropriately
    if (objiter->second->is_update()) {
      assert(missing.is_missing(hoid) &&
	     missing.missing[hoid].need == objiter->second->version);
    } else {
      assert(!missing.is_missing(hoid));
    }
    missing.revise_have(hoid, eversion_t());
    if (rollbacker && !object_not_in_store)
      rollbacker->remove(hoid);
    return;
  }

  dout(10) << __func__ << ": hoid " << hoid
	   <<" has no more recent entries in log" << dendl;
  if (prior_version == eversion_t() || entries.front().is_clone()) {
    /// Case 2)
    dout(10) << __func__ << ": hoid " << hoid
	     << " prior_version or op type indicates creation, deleting"
	     << dendl;
    if (missing.is_missing(hoid))
      missing.rm(missing.missing.find(hoid));
    if (rollbacker && !object_not_in_store)
      rollbacker->remove(hoid);
    return;
  }

  if (missing.is_missing(hoid)) {
    /// Case 3)
    dout(10) << __func__ << ": hoid " << hoid
	     << " missing, " << missing.missing[hoid]
	     << " adjusting" << dendl;

    if (missing.missing[hoid].have == prior_version) {
      dout(10) << __func__ << ": hoid " << hoid
	       << " missing.have is prior_version " << prior_version
	       << " removing from missing" << dendl;
      missing.rm(missing.missing.find(hoid));
    } else {
      dout(10) << __func__ << ": hoid " << hoid
	       << " missing.have is " << missing.missing[hoid].have
	       << ", adjusting" << dendl;
      missing.revise_need(hoid, prior_version);
      if (prior_version <= info.log_tail) {
	dout(10) << __func__ << ": hoid " << hoid
		 << " prior_version " << prior_version << " <= info.log_tail "
		 << info.log_tail << dendl;
	if (new_divergent_prior)
	  *new_divergent_prior = make_pair(prior_version, hoid);
      }
    }
    return;
  }

  dout(10) << __func__ << ": hoid " << hoid
	   << " must be rolled back or recovered, attempting to rollback"
	   << dendl;
  bool can_rollback = true;
  /// Distinguish between 4) and 5)
  for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
       i != entries.rend();
       ++i) {
    if (!i->mod_desc.can_rollback() || i->version <= olog_can_rollback_to) {
      dout(10) << __func__ << ": hoid " << hoid << " cannot rollback "
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
      assert(i->mod_desc.can_rollback() && i->version > olog_can_rollback_to);
      dout(10) << __func__ << ": hoid " << hoid
	       << " rolling back " << *i << dendl;
      if (rollbacker)
	rollbacker->rollback(*i);
    }
    dout(10) << __func__ << ": hoid " << hoid << " rolled back" << dendl;
    return;
  } else {
    /// Case 5)
    dout(10) << __func__ << ": hoid " << hoid << " cannot roll back, "
	     << "removing and adding to missing" << dendl;
    if (rollbacker && !object_not_in_store)
      rollbacker->remove(hoid);
    missing.add(hoid, prior_version, eversion_t());
    if (prior_version <= info.log_tail) {
      dout(10) << __func__ << ": hoid " << hoid
	       << " prior_version " << prior_version << " <= info.log_tail "
	       << info.log_tail << dendl;
      if (new_divergent_prior)
	*new_divergent_prior = make_pair(prior_version, hoid);
    }
  }
}

/**
 * rewind divergent entries at the head of the log
 *
 * This rewinds entries off the head of our log that are divergent.
 * This is used by replicas during activation.
 *
 * @param t transaction
 * @param newhead new head to rewind to
 */
void PGLog::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
				 pg_info_t &info, LogEntryHandler *rollbacker,
				 bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "rewind_divergent_log truncate divergent future " << newhead << dendl;
  assert(newhead >= log.tail);

  list<pg_log_entry_t>::iterator p = log.log.end();
  list<pg_log_entry_t> divergent;
  while (true) {
    if (p == log.log.begin()) {
      // yikes, the whole thing is divergent!
      divergent.swap(log.log);
      break;
    }
    --p;
    mark_dirty_from(p->version);
    if (p->version <= newhead) {
      ++p;
      divergent.splice(divergent.begin(), log.log, p, log.log.end());
      break;
    }
    assert(p->version > newhead);
    dout(10) << "rewind_divergent_log future divergent " << *p << dendl;
  }

  log.head = newhead;
  info.last_update = newhead;
  if (info.last_complete > newhead)
    info.last_complete = newhead;

  log.index();

  map<eversion_t, hobject_t> new_priors;
  _merge_divergent_entries(
    log,
    divergent,
    info,
    log.can_rollback_to,
    missing,
    &new_priors,
    rollbacker);
  for (map<eversion_t, hobject_t>::iterator i = new_priors.begin();
       i != new_priors.end();
       ++i) {
    add_divergent_prior(
      i->first,
      i->second);
  }

  if (info.last_update < log.can_rollback_to)
    log.can_rollback_to = info.last_update;

  dirty_info = true;
  dirty_big_info = true;
}

void PGLog::merge_log(ObjectStore::Transaction& t,
                      pg_info_t &oinfo, pg_log_t &olog, pg_shard_t fromosd,
                      pg_info_t &info, LogEntryHandler *rollbacker,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "merge_log " << olog << " from osd." << fromosd
           << " into " << log << dendl;

  // Check preconditions

  // If our log is empty, the incoming log needs to have not been trimmed.
  assert(!log.null() || olog.tail == eversion_t());
  // The logs must overlap.
  assert(log.head >= olog.tail && olog.head >= log.tail);

  for (map<hobject_t, pg_missing_t::item>::iterator i = missing.missing.begin();
       i != missing.missing.end();
       ++i) {
    dout(20) << "pg_missing_t sobject: " << i->first << dendl;
  }

  bool changed = false;

  // extend on tail?
  //  this is just filling in history.  it does not affect our
  //  missing set, as that should already be consistent with our
  //  current log.
  if (olog.tail < log.tail) {
    mark_dirty_to(log.log.begin()->version); // last clean entry
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<pg_log_entry_t>::iterator from = olog.log.begin();
    list<pg_log_entry_t>::iterator to;
    for (to = from;
	 to != olog.log.end();
	 ++to) {
      if (to->version > log.tail)
	break;
      log.index(*to);
      dout(15) << *to << dendl;
    }
      
    // splice into our log.
    log.log.splice(log.log.begin(),
		   olog.log, from, to);
      
    info.log_tail = log.tail = olog.tail;
    changed = true;
  }

  if (oinfo.stats.reported_seq < info.stats.reported_seq ||   // make sure reported always increases
      oinfo.stats.reported_epoch < info.stats.reported_epoch) {
    oinfo.stats.reported_seq = info.stats.reported_seq;
    oinfo.stats.reported_epoch = info.stats.reported_epoch;
  }
  if (info.last_backfill.is_max())
    info.stats = oinfo.stats;
  info.hit_set = oinfo.hit_set;

  // do we have divergent entries to throw out?
  if (olog.head < log.head) {
    rewind_divergent_log(t, olog.head, info, rollbacker, dirty_info, dirty_big_info);
    changed = true;
  }

  // extend on head?
  if (olog.head > log.head) {
    dout(10) << "merge_log extending head to " << olog.head << dendl;
      
    // find start point in olog
    list<pg_log_entry_t>::iterator to = olog.log.end();
    list<pg_log_entry_t>::iterator from = olog.log.end();
    eversion_t lower_bound = olog.tail;
    while (1) {
      if (from == olog.log.begin())
	break;
      --from;
      dout(20) << "  ? " << *from << dendl;
      if (from->version <= log.head) {
	dout(20) << "merge_log cut point (usually last shared) is " << *from << dendl;
	lower_bound = from->version;
	++from;
	break;
      }
    }
    mark_dirty_from(lower_bound);

    // index, update missing, delete deleted
    for (list<pg_log_entry_t>::iterator p = from; p != to; ++p) {
      pg_log_entry_t &ne = *p;
      dout(20) << "merge_log " << ne << dendl;
      log.index(ne);
      if (ne.soid <= info.last_backfill) {
	missing.add_next_event(ne);
	if (ne.is_delete())
	  rollbacker->remove(ne.soid);
      }
    }
      
    // move aside divergent items
    list<pg_log_entry_t> divergent;
    while (!log.empty()) {
      pg_log_entry_t &oe = *log.log.rbegin();
      /*
       * look at eversion.version here.  we want to avoid a situation like:
       *  our log: 100'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  new log: 122'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  lower_bound = 100'9
       * i.e, same request, different version.  If the eversion.version is > the
       * lower_bound, we it is divergent.
       */
      if (oe.version.version <= lower_bound.version)
	break;
      dout(10) << "merge_log divergent " << oe << dendl;
      divergent.push_front(oe);
      log.log.pop_back();
    }

    // splice
    log.log.splice(log.log.end(), 
		   olog.log, from, to);
    log.index();   

    info.last_update = log.head = olog.head;

    info.last_user_version = oinfo.last_user_version;
    info.purged_snaps = oinfo.purged_snaps;

    map<eversion_t, hobject_t> new_priors;
    _merge_divergent_entries(
      log,
      divergent,
      info,
      log.can_rollback_to,
      missing,
      &new_priors,
      rollbacker);
    for (map<eversion_t, hobject_t>::iterator i = new_priors.begin();
	 i != new_priors.end();
	 ++i) {
      add_divergent_prior(
	i->first,
	i->second);
    }

    // We cannot rollback into the new log entries
    log.can_rollback_to = log.head;

    changed = true;
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
  }
}

void PGLog::write_log(
  ObjectStore::Transaction& t, const hobject_t &log_oid)
{
  if (is_dirty()) {
    dout(10) << "write_log with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", dirty_divergent_priors: " << dirty_divergent_priors
	     << ", writeout_from: " << writeout_from
	     << ", trimmed: " << trimmed
	     << dendl;
    _write_log(
      t, log, log_oid, divergent_priors,
      dirty_to,
      dirty_from,
      writeout_from,
      trimmed,
      dirty_divergent_priors,
      !touched_log,
      (pg_log_debug ? &log_keys_debug : 0));
    undirty();
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

void PGLog::write_log(ObjectStore::Transaction& t, pg_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors)
{
  _write_log(
    t, log, log_oid,
    divergent_priors, eversion_t::max(), eversion_t(), eversion_t(),
    set<eversion_t>(),
    true, true, 0);
}

void PGLog::_write_log(
  ObjectStore::Transaction& t, pg_log_t &log,
  const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  const set<eversion_t> &trimmed,
  bool dirty_divergent_priors,
  bool touch_log,
  set<string> *log_keys_debug
  )
{
  set<string> to_remove;
  for (set<eversion_t>::const_iterator i = trimmed.begin();
       i != trimmed.end();
       ++i) {
    to_remove.insert(i->get_key_name());
    if (log_keys_debug) {
      assert(log_keys_debug->count(i->get_key_name()));
      log_keys_debug->erase(i->get_key_name());
    }
  }

//dout(10) << "write_log, clearing up to " << dirty_to << dendl;
  if (touch_log)
    t.touch(coll_t(), log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll_t(), log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll_t(), log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  map<string,bufferlist> keys;
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version < dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = keys.begin();
	 i != keys.end();
	 ++i) {
      assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  if (dirty_divergent_priors) {
    //dout(10) << "write_log: writing divergent_priors" << dendl;
    ::encode(divergent_priors, keys["divergent_priors"]);
  }
  ::encode(log.can_rollback_to, keys["can_rollback_to"]);
  ::encode(log.rollback_info_trimmed_to, keys["rollback_info_trimmed_to"]);

  if (!to_remove.empty())
    t.omap_rmkeys(META_COLL, log_oid, to_remove);
  t.omap_setkeys(META_COLL, log_oid, keys);
}

bool PGLog::read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
  const pg_info_t &info, map<eversion_t, hobject_t> &divergent_priors,
  IndexedLog &log,
  pg_missing_t &missing,
  ostringstream &oss,
  set<string> *log_keys_debug)
{
  dout(10) << "read_log" << dendl;
  bool rewrite_log = false;

  // legacy?
  struct stat st;
  int r = store->stat(META_COLL, log_oid, &st);
  assert(r == 0);
  if (st.st_size > 0) {
    read_log_old(store, coll, log_oid, info, divergent_priors, log, missing, oss, log_keys_debug);
    rewrite_log = true;
  } else {
    log.tail = info.log_tail;

    // will get overridden below if it had been recorded
    log.can_rollback_to = info.last_update;
    log.rollback_info_trimmed_to = eversion_t();

    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(META_COLL, log_oid);
    if (p) for (p->seek_to_first(); p->valid() ; p->next()) {
      bufferlist bl = p->value();//Copy bufferlist before creating iterator
      bufferlist::iterator bp = bl.begin();
      if (p->key() == "divergent_priors") {
	::decode(divergent_priors, bp);
	dout(20) << "read_log " << divergent_priors.size() << " divergent_priors" << dendl;
      } else if (p->key() == "can_rollback_to") {
	bufferlist bl = p->value();
	bufferlist::iterator bp = bl.begin();
	::decode(log.can_rollback_to, bp);
      } else if (p->key() == "rollback_info_trimmed_to") {
	bufferlist bl = p->value();
	bufferlist::iterator bp = bl.begin();
	::decode(log.rollback_info_trimmed_to, bp);
      } else {
	pg_log_entry_t e;
	e.decode_with_checksum(bp);
	dout(20) << "read_log " << e << dendl;
	if (!log.log.empty()) {
	  pg_log_entry_t last_e(log.log.back());
	  assert(last_e.version.version < e.version.version);
	  assert(last_e.version.epoch <= e.version.epoch);
	}
	log.log.push_back(e);
	log.head = e.version;
	if (log_keys_debug)
	  log_keys_debug->insert(e.get_key_name());
      }
    }
  }
  log.head = info.last_update;
  log.index();

  // build missing
  if (info.last_complete < info.last_update) {
    dout(10) << "read_log checking for missing items over interval (" << info.last_complete
	     << "," << info.last_update << "]" << dendl;

    set<hobject_t> did;
    for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 ++i) {
      if (i->version <= info.last_complete) break;
      if (i->soid > info.last_backfill) continue;
      if (did.count(i->soid)) continue;
      did.insert(i->soid);
      
      if (i->is_delete()) continue;
      
      bufferlist bv;
      int r = store->getattr(
	coll,
	ghobject_t(i->soid, ghobject_t::NO_GEN, info.pgid.shard),
	OI_ATTR,
	bv);
      if (r >= 0) {
	object_info_t oi(bv);
	if (oi.version < i->version) {
	  dout(15) << "read_log  missing " << *i << " (have " << oi.version << ")" << dendl;
	  missing.add(i->soid, i->version, oi.version);
	}
      } else {
	dout(15) << "read_log  missing " << *i << dendl;
	missing.add(i->soid, i->version, eversion_t());
      }
    }
    for (map<eversion_t, hobject_t>::reverse_iterator i =
	   divergent_priors.rbegin();
	 i != divergent_priors.rend();
	 ++i) {
      if (i->first <= info.last_complete) break;
      if (i->second > info.last_backfill) continue;
      if (did.count(i->second)) continue;
      did.insert(i->second);
      bufferlist bv;
      int r = store->getattr(
	coll, 
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
	dout(15) << "read_log  missing " << *i << dendl;
	missing.add(i->second, i->first, eversion_t());
      }
    }
  }
  dout(10) << "read_log done" << dendl;
  return rewrite_log;
}

void PGLog::read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
			 const pg_info_t &info, map<eversion_t, hobject_t> &divergent_priors,
			 IndexedLog &log,
			 pg_missing_t &missing, ostringstream &oss,
			 set<string> *log_keys_debug)
{
  // load bounds, based on old OndiskLog encoding.
  uint64_t ondisklog_tail = 0;
  uint64_t ondisklog_head = 0;
  bool ondisklog_has_checksums;

  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  {
    bufferlist::iterator bl = blb.begin();
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ondisklog_has_checksums = (struct_v >= 2);
    ::decode(ondisklog_tail, bl);
    ::decode(ondisklog_head, bl);
    if (struct_v >= 4) {
      uint64_t ondisklog_zero_to;
      ::decode(ondisklog_zero_to, bl);
    }
    if (struct_v >= 5)
      ::decode(divergent_priors, bl);
    DECODE_FINISH(bl);
  }
  uint64_t ondisklog_length = ondisklog_head - ondisklog_tail;
  dout(10) << "read_log " << ondisklog_tail << "~" << ondisklog_length << dendl;
 
  log.tail = info.log_tail;

  if (ondisklog_head > 0) {
    // read
    bufferlist bl;
    store->read(META_COLL, log_oid, ondisklog_tail, ondisklog_length, bl);
    if (bl.length() < ondisklog_length) {
      std::ostringstream oss;
      oss << "read_log got " << bl.length() << " bytes, expected "
	  << ondisklog_head << "-" << ondisklog_tail << "="
	  << ondisklog_length;
      throw read_log_error(oss.str().c_str());
    }
    
    pg_log_entry_t e;
    bufferlist::iterator p = bl.begin();
    assert(log.empty());
    eversion_t last;
    bool reorder = false;

    while (!p.end()) {
      uint64_t pos = ondisklog_tail + p.get_off();
      if (ondisklog_has_checksums) {
	bufferlist ebl;
	::decode(ebl, p);
	__u32 crc;
	::decode(crc, p);
	
	__u32 got = ebl.crc32c(0);
	if (crc == got) {
	  bufferlist::iterator q = ebl.begin();
	  ::decode(e, q);
	} else {
	  std::ostringstream oss;
	  oss << "read_log " << pos << " bad crc got " << got << " expected" << crc;
	  throw read_log_error(oss.str().c_str());
	}
      } else {
	::decode(e, p);
      }
      dout(20) << "read_log " << pos << " " << e << dendl;

      // [repair] in order?
      if (e.version < last) {
	dout(0) << "read_log " << pos << " out of order entry " << e << " follows " << last << dendl;
	oss << info.pgid << " log has out of order entry "
	      << e << " following " << last << "\n";
	reorder = true;
      }

      if (e.version <= log.tail) {
	dout(20) << "read_log  ignoring entry at " << pos << " below log.tail" << dendl;
	continue;
      }
      if (last.version == e.version.version) {
	dout(0) << "read_log  got dup " << e.version << " (last was " << last << ", dropping that one)" << dendl;
	log.log.pop_back();
	oss << info.pgid << " read_log got dup "
	      << e.version << " after " << last << "\n";
      }

      assert(!e.invalid_hash);

      if (e.invalid_pool) {
	e.soid.pool = info.pgid.pool();
      }

      e.offset = pos;
      uint64_t endpos = ondisklog_tail + p.get_off();
      log.log.push_back(e);
      if (log_keys_debug)
	log_keys_debug->insert(e.get_key_name());
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	oss << info.pgid << " log has extra data at "
	   << endpos << "~" << (ondisklog_head-endpos) << " after "
	   << info.last_update << "\n";

	dout(0) << "read_log " << endpos << " *** extra gunk at end of log, "
	        << "adjusting ondisklog_head" << dendl;
	ondisklog_head = endpos;
	break;
      }
    }
  
    if (reorder) {
      dout(0) << "read_log reordering log" << dendl;
      map<eversion_t, pg_log_entry_t> m;
      for (list<pg_log_entry_t>::iterator p = log.log.begin(); p != log.log.end(); ++p)
	m[p->version] = *p;
      log.log.clear();
      for (map<eversion_t, pg_log_entry_t>::iterator p = m.begin(); p != m.end(); ++p)
	log.log.push_back(p->second);
    }
  }
}
