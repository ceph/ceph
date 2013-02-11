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

#include "PG.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const PG *pg) {
  return *_dout << pg->gen_prefix();
}

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  assert(pi);
  info = *pi;
  auid = pi->auid;
  if (pi->get_snap_epoch() == map->get_epoch()) {
    pi->build_removed_snaps(newly_removed_snaps);
    newly_removed_snaps.subtract(cached_removed_snaps);
    cached_removed_snaps.union_of(newly_removed_snaps);
    snapc = pi->get_snap_context();
  } else {
    newly_removed_snaps.clear();
  }
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, pg_t p, const hobject_t& loid,
       const hobject_t& ioid) :
  osd(o), osdmap_ref(curmap), pool(_pool),
  _lock("PG::_lock"),
  ref(0), deleting(false), dirty_info(false), dirty_log(false),
  info(p), coll(p), log_oid(loid), biginfo_oid(ioid),
  recovery_item(this), scrub_item(this), scrub_finalize_item(this), snap_trim_item(this), stat_queue_item(this),
  recovery_ops_active(0),
  waiting_on_backfill(0),
  role(0),
  state(0),
  send_notify(false),
  need_up_thru(false),
  need_flush(false),
  last_peering_reset(0),
  heartbeat_peer_lock("PG::heartbeat_peer_lock"),
  backfill_target(-1),
  backfill_reserved(0),
  backfill_reserving(0),
  pg_stats_lock("PG::pg_stats_lock"),
  pg_stats_valid(false),
  osr(osd->osr_registry.lookup_or_create(p, (stringify(p)))),
  finish_sync_event(NULL),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(this)
{
}

PG::~PG()
{
}

void PG::lock(bool no_lockdep)
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);
  assert(!dirty_log);

  dout(30) << "lock" << dendl;
}

void PG::lock_with_map_lock_held(bool no_lockdep)
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);
  assert(!dirty_log);

  dout(30) << "lock_with_map_lock_held" << dendl;
}

void PG::reassert_lock_with_map_lock_held()
{
  assert(_lock.is_locked());
  osdmap_ref = osd->osdmap;

  dout(30) << "reassert_lock_with_map_lock_held" << dendl;
}

std::string PG::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  if (_lock.is_locked_by_me()) {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << info.pgid << "(unlocked)] ";
  }
  return out.str();
}
  


void PG::IndexedLog::trim(ObjectStore::Transaction& t, eversion_t s) 
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is " << complete_to->version
		    << " on " << *this << dendl;
  }

  while (!log.empty()) {
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    unindex(e);         // remove from index,
    log.pop_front();    // from log
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

/********* PG **********/

void PG::proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing, int from)
{
  dout(10) << "proc_master_log for osd." << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from);
  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  peer_missing[from].swap(omissing);
}
    
void PG::proc_replica_log(ObjectStore::Transaction& t,
			  pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing, int from)
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

  list<pg_log_entry_t>::const_reverse_iterator pp = olog.log.rbegin();
  eversion_t lu(oinfo.last_update);
  while (true) {
    if (pp == olog.log.rend()) {
      if (pp != olog.log.rbegin())   // no last_update adjustment if we discard nothing!
	lu = olog.tail;
      break;
    }
    const pg_log_entry_t& oe = *pp;

    // don't continue past the tail of our log.
    if (oe.version <= log.tail)
      break;

    if (!log.objects.count(oe.soid)) {
      dout(10) << " had " << oe << " new dne : divergent, ignoring" << dendl;
      ++pp;
      continue;
    }
      
    pg_log_entry_t& ne = *log.objects[oe.soid];
    if (ne.version == oe.version) {
      dout(10) << " had " << oe << " new " << ne << " : match, stopping" << dendl;
      lu = pp->version;
      break;
    }

    if (oe.soid > oinfo.last_backfill) {
      // past backfill line, don't care
      dout(10) << " had " << oe << " beyond last_backfill : skipping" << dendl;
      ++pp;
      continue;
    }

    if (ne.version > oe.version) {
      dout(10) << " had " << oe << " new " << ne << " : new will supercede" << dendl;
    } else {
      if (oe.is_delete()) {
	if (ne.is_delete()) {
	  // old and new are delete
	  dout(10) << " had " << oe << " new " << ne << " : both deletes" << dendl;
	} else {
	  // old delete, new update.
	  dout(10) << " had " << oe << " new " << ne << " : missing" << dendl;
	  omissing.add(ne.soid, ne.version, eversion_t());
	}
      } else {
	if (ne.is_delete()) {
	  // old update, new delete
	  dout(10) << " had " << oe << " new " << ne << " : new will supercede" << dendl;
	  omissing.rm(oe.soid, oe.version);
	} else {
	  // old update, new update
	  dout(10) << " had " << oe << " new " << ne << " : new will supercede" << dendl;
	  omissing.revise_need(ne.soid, ne.version);
	}
      }
    }

    ++pp;
  }    

  if (lu < oinfo.last_update) {
    dout(10) << " peer osd." << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
    if (lu < oinfo.last_complete)
      oinfo.last_complete = lu;
  }

  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  for (map<hobject_t, pg_missing_t::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " after missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }
  peer_missing[from].swap(omissing);
}

bool PG::proc_replica_info(int from, const pg_info_t &oinfo)
{
  map<int,pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);
  
  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();
  
  // stray?
  if (!is_acting(from)) {
    dout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end())
    update_heartbeat_peers();

  return true;
}

void PG::remove_object_with_snap_hardlinks(
  ObjectStore::Transaction& t, const hobject_t& soid)
{
  t.remove(coll, soid);
  if (soid.snap < CEPH_MAXSNAP) {
    bufferlist ba;
    int r = osd->store->getattr(coll, soid, OI_ATTR, ba);
    if (r >= 0) {
      // grr, need first snap bound, too.
      object_info_t oi(ba);
      if (oi.snaps.size() > 1)
	t.remove(coll_t(info.pgid, oi.snaps[oi.snaps.size() - 1]), soid);
      t.remove(coll_t(info.pgid, oi.snaps[0]), soid);
    }
  }
}


/*
 * merge an old (possibly divergent) log entry into the new log.  this 
 * happens _after_ new log items have been assimilated.  thus, we assume
 * the index already references newer entries (if present), and missing
 * has been updated accordingly.
 *
 * return true if entry is not divergent.
 */
bool PG::merge_old_entry(ObjectStore::Transaction& t, pg_log_entry_t& oe)
{
  if (oe.soid > info.last_backfill) {
    dout(20) << "merge_old_entry  had " << oe << " : beyond last_backfill" << dendl;
    return false;
  }
  if (log.objects.count(oe.soid)) {
    pg_log_entry_t &ne = *log.objects[oe.soid];  // new(er?) entry
    
    if (ne.version > oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : older, missing" << dendl;
      assert(ne.is_delete() || missing.is_missing(ne.soid));
      return false;
    }
    if (ne.version == oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : same" << dendl;
      return true;
    }
    if (oe.is_delete()) {
      if (ne.is_delete()) {
	// old and new are delete
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : both deletes" << dendl;
      } else {
	// old delete, new update.
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : missing" << dendl;
	missing.revise_need(ne.soid, ne.version);
      }
    } else {
      if (ne.is_delete()) {
	// old update, new delete
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : new delete supercedes" << dendl;
	missing.rm(oe.soid, oe.version);
      } else {
	// old update, new update
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : new item supercedes" << dendl;
	missing.revise_need(ne.soid, ne.version);
      }
    }
  } else if (oe.op == pg_log_entry_t::CLONE) {
    assert(oe.soid.snap != CEPH_NOSNAP);
    dout(20) << "merge_old_entry  had " << oe
	     << ", clone with no non-divergent log entries, "
	     << "deleting" << dendl;
    remove_object_with_snap_hardlinks(t, oe.soid);
    if (missing.is_missing(oe.soid))
      missing.rm(oe.soid, missing.missing[oe.soid].need);
  } else if (oe.prior_version > info.log_tail) {
    /**
     * oe.prior_version is a previously divergent log entry
     * oe.soid must have already been handled and the missing
     * set updated appropriately
     */
    dout(20) << "merge_old_entry  had oe " << oe
	     << " with divergent prior_version " << oe.prior_version
	     << " oe.soid " << oe.soid
	     << " must already have been merged" << dendl;
  } else {
    if (!oe.is_delete()) {
      dout(20) << "merge_old_entry  had " << oe << " deleting" << dendl;
      remove_object_with_snap_hardlinks(t, oe.soid);
    }
    dout(20) << "merge_old_entry  had " << oe << " updating missing to "
	     << oe.prior_version << dendl;
    if (oe.prior_version > eversion_t()) {
      ondisklog.add_divergent_prior(oe.prior_version, oe.soid);
      dirty_log = true;
      missing.revise_need(oe.soid, oe.prior_version);
    } else if (missing.is_missing(oe.soid)) {
      missing.rm(oe.soid, missing.missing[oe.soid].need);
    }
  }
  return false;
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
void PG::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead)
{
  dout(10) << "rewind_divergent_log truncate divergent future " << newhead << dendl;
  assert(newhead > log.tail);

  list<pg_log_entry_t>::iterator p = log.log.end();
  list<pg_log_entry_t> divergent;
  while (true) {
    if (p == log.log.begin()) {
      // yikes, the whole thing is divergent!
      divergent.swap(log.log);
      break;
    }
    --p;
    if (p->version == newhead) {
      ++p;
      divergent.splice(divergent.begin(), log.log, p, log.log.end());
      break;
    }
    assert(p->version > newhead);
    dout(10) << "rewind_divergent_log future divergent " << *p << dendl;
    log.unindex(*p);
  }

  log.head = newhead;
  info.last_update = newhead;
  if (info.last_complete > newhead)
    info.last_complete = newhead;

  for (list<pg_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); d++)
    merge_old_entry(t, *d);

  dirty_info = true;
  dirty_log = true;
}

void PG::merge_log(ObjectStore::Transaction& t,
		   pg_info_t &oinfo, pg_log_t &olog, int fromosd)
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
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<pg_log_entry_t>::iterator from = olog.log.begin();
    list<pg_log_entry_t>::iterator to;
    for (to = from;
	 to != olog.log.end();
	 to++) {
      if (to->version > log.tail)
	break;
      log.index(*to);
      dout(15) << *to << dendl;
    }
    assert(to != olog.log.end() ||
	   (olog.head == info.last_update));
      
    // splice into our log.
    log.log.splice(log.log.begin(),
		   olog.log, from, to);
      
    info.log_tail = log.tail = olog.tail;
    changed = true;
  }

  if (oinfo.stats.reported < info.stats.reported)   // make sure reported always increases
    oinfo.stats.reported = info.stats.reported;
  if (info.last_backfill.is_max())
    info.stats = oinfo.stats;

  // do we have divergent entries to throw out?
  if (olog.head < log.head) {
    rewind_divergent_log(t, olog.head);
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
      from--;
      dout(20) << "  ? " << *from << dendl;
      if (from->version <= log.head) {
	dout(20) << "merge_log cut point (usually last shared) is " << *from << dendl;
	lower_bound = from->version;
	from++;
	break;
      }
    }

    // index, update missing, delete deleted
    for (list<pg_log_entry_t>::iterator p = from; p != to; p++) {
      pg_log_entry_t &ne = *p;
      dout(20) << "merge_log " << ne << dendl;
      log.index(ne);
      if (ne.soid <= info.last_backfill) {
	missing.add_next_event(ne);
	if (ne.is_delete())
	  remove_object_with_snap_hardlinks(t, ne.soid);
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
      log.unindex(oe);
      log.log.pop_back();
    }

    // splice
    log.log.splice(log.log.end(), 
		   olog.log, from, to);
    log.index();   

    info.last_update = log.head = olog.head;

    // process divergent items
    if (!divergent.empty()) {
      for (list<pg_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); d++)
	merge_old_entry(t, *d);
    }

    changed = true;
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_log = true;
  }
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PG::search_for_missing(const pg_info_t &oinfo, const pg_missing_t *omissing,
			    int fromosd)
{
  bool stats_updated = false;
  bool found_missing = false;

  // take note that we've probed this peer, for
  // all_unfound_are_queried_or_lost()'s benefit.
  peer_missing[fromosd];

  // found items?
  for (map<hobject_t,pg_missing_t::item>::iterator p = missing.missing.begin();
       p != missing.missing.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (oinfo.last_update < need) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (last_update " << oinfo.last_update << " < needed " << need << ")"
	       << dendl;
      continue;
    }
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (past last_backfill " << oinfo.last_backfill << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_complete < need) {
      if (!omissing) {
	// We know that the peer lacks some objects at the revision we need.
	// Without the peer's missing set, we don't know whether it has this
	// particular object or not.
	dout(10) << __func__ << " " << soid << " " << need
		 << " might also be missing on osd." << fromosd << dendl;
	continue;
      }

      if (omissing->is_missing(soid)) {
	dout(10) << "search_for_missing " << soid << " " << need
		 << " also missing on osd." << fromosd << dendl;
	continue;
      }
    }
    dout(10) << "search_for_missing " << soid << " " << need
	     << " is on osd." << fromosd << dendl;

    map<hobject_t, set<int> >::iterator ml = missing_loc.find(soid);
    if (ml == missing_loc.end()) {
      map<hobject_t, list<OpRequestRef> >::iterator wmo =
	waiting_for_missing_object.find(soid);
      if (wmo != waiting_for_missing_object.end()) {
	requeue_ops(wmo->second);
      }
      stats_updated = true;
      missing_loc[soid].insert(fromosd);
      missing_loc_sources.insert(fromosd);
    }
    else {
      ml->second.insert(fromosd);
      missing_loc_sources.insert(fromosd);
    }
    found_missing = true;
  }
  if (stats_updated) {
    update_stats();
  }

  dout(20) << "search_for_missing missing " << missing.missing << dendl;
  return found_missing;
}

void PG::discover_all_missing(map< int, map<pg_t,pg_query_t> > &query_map)
{
  assert(missing.have_missing());

  dout(10) << __func__ << " "
	   << missing.num_missing() << " missing, "
	   << get_num_unfound() << " unfound"
	   << dendl;

  std::set<int>::const_iterator m = might_have_unfound.begin();
  std::set<int>::const_iterator mend = might_have_unfound.end();
  for (; m != mend; ++m) {
    int peer(*m);
    
    if (!get_osdmap()->is_up(peer)) {
      dout(20) << __func__ << " skipping down osd." << peer << dendl;
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    dout(10) << __func__ << ": osd." << peer << ": requesting pg_missing_t"
	     << dendl;
    peer_missing_requested.insert(peer);
    query_map[peer][info.pgid] =
      pg_query_t(pg_query_t::MISSING, info.history, get_osdmap()->get_epoch());
  }
}


ostream& PG::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       p++) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}





/******* PG ***********/
bool PG::needs_recovery() const
{
  assert(is_primary());

  bool ret = false;

  if (missing.num_missing()) {
    dout(10) << __func__ << " primary has " << missing.num_missing() << dendl;
    ret = true;
  }

  vector<int>::const_iterator end = acting.end();
  vector<int>::const_iterator a = acting.begin();
  assert(a != end);
  ++a;
  for (; a != end; ++a) {
    int peer = *a;
    map<int, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << " osd." << peer << " don't have missing set" << dendl;
      ret = true;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << " osd." << peer << " has " << pm->second.num_missing() << " missing" << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " is recovered" << dendl;
  return ret;
}

bool PG::needs_backfill() const
{
  assert(is_primary());

  bool ret = false;

  vector<int>::const_iterator end = acting.end();
  vector<int>::const_iterator a = acting.begin();
  assert(a != end);
  ++a;
  for (; a != end; ++a) {
    int peer = *a;
    map<int,pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (pi->second.last_backfill != hobject_t::get_max()) {
      dout(10) << __func__ << " osd." << peer << " has last_backfill " << pi->second.last_backfill << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " does not need backfill" << dendl;
  return ret;
}

bool PG::_calc_past_interval_range(epoch_t *start, epoch_t *end)
{
  *end = info.history.same_interval_since;

  // Do we already have the intervals we want?
  map<epoch_t,pg_interval_t>::const_iterator pif = past_intervals.begin();
  if (pif != past_intervals.end()) {
    if (pif->first <= info.history.last_epoch_clean) {
      dout(10) << __func__ << ": already have past intervals back to "
	       << info.history.last_epoch_clean << dendl;
      return false;
    }
    *end = past_intervals.begin()->first;
  }

  *start = MAX(MAX(info.history.epoch_created,
		   info.history.last_epoch_clean),
	       osd->get_superblock().oldest_map);
  if (*start >= *end) {
    dout(10) << __func__ << " start epoch " << *start << " >= end epoch " << *end
	     << ", nothing to do" << dendl;
    return false;
  }

  return true;
}


void PG::generate_past_intervals()
{
  epoch_t cur_epoch, end_epoch;
  if (!_calc_past_interval_range(&cur_epoch, &end_epoch)) {
    return;
  }

  OSDMapRef last_map, cur_map;
  vector<int> acting, up, old_acting, old_up;

  cur_map = osd->get_map(cur_epoch);
  cur_map->pg_to_up_acting_osds(get_pgid(), up, acting);
  epoch_t same_interval_since = cur_epoch;
  dout(10) << __func__ << " over epochs " << cur_epoch << "-"
	   << end_epoch << dendl;
  ++cur_epoch;
  for (; cur_epoch <= end_epoch; ++cur_epoch) {
    last_map.swap(cur_map);
    old_up.swap(up);
    old_acting.swap(acting);

    cur_map = osd->get_map(cur_epoch);
    cur_map->pg_to_up_acting_osds(get_pgid(), up, acting);

    std::stringstream debug;
    bool new_interval = pg_interval_t::check_new_interval(
      old_acting,
      acting,
      old_up,
      up,
      same_interval_since,
      info.history.last_epoch_clean,
      cur_map,
      last_map,
      info.pgid.pool(),
      info.pgid,
      &past_intervals,
      &debug);
    if (new_interval) {
      dout(10) << debug.str() << dendl;
      same_interval_since = cur_epoch;
    }
  }

  // record our work.
  dirty_info = true;
}

/*
 * Trim past_intervals.
 *
 * This gets rid of all the past_intervals that happened before last_epoch_clean.
 */
void PG::trim_past_intervals()
{
  std::map<epoch_t,pg_interval_t>::iterator pif = past_intervals.begin();
  std::map<epoch_t,pg_interval_t>::iterator end = past_intervals.end();
  while (pif != end) {
    if (pif->second.last >= info.history.last_epoch_clean)
      return;
    dout(10) << __func__ << ": trimming " << pif->second << dendl;
    past_intervals.erase(pif++);
  }
}


bool PG::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = get_osdmap()->get_up_thru(osd->whoami);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    dout(10) << "adjust_need_up_thru now " << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

void PG::remove_down_peer_info(const OSDMapRef osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  map<int,pg_info_t>::iterator p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first)) {
      dout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      p++;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PG::all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const
{
  assert(is_primary());

  set<int>::const_iterator peer = might_have_unfound.begin();
  set<int>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(*peer));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  dout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound " << might_have_unfound 
	   << " have been queried or are marked lost" << dendl;
  return true;
}

void PG::build_prior(std::auto_ptr<PriorSet> &prior_set)
{
  if (1) {
    // sanity check
    for (map<int,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 it++) {
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }
  prior_set.reset(new PriorSet(*get_osdmap(),
				 past_intervals,
				 up,
				 acting,
				 info,
				 this));
  PriorSet &prior(*prior_set.get());
				 
  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(osd->whoami) < info.history.same_interval_since) {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " < same_since " << info.history.same_interval_since
	     << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " >= same_since " << info.history.same_interval_since
	     << ", all is well" << dendl;
    need_up_thru = false;
  }
  set_probe_targets(prior_set->probe);
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  stray_purged.clear();
  might_have_unfound.clear();

  last_update_ondisk = eversion_t();

  snap_trimq.clear();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();
  missing_loc_sources.clear();

  log.reset_recovery_pointers();

  scrubber.reserved_peers.clear();
  scrub_after_recovery = false;

  osd->recovery_wq.dequeue(this);
  osd->snap_trim_wq.dequeue(this);
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<int, pg_info_t>::const_iterator PG::find_best_info(const map<int, pg_info_t> &infos) const
{
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;
  for (map<int, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found < i->second.last_epoch_started) {
      min_last_update_acceptable = eversion_t::max();
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
    if (max_last_epoch_started_found == i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  assert(min_last_update_acceptable != eversion_t::max());

  map<int, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update.  if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<int, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disquality anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (p->second.last_update < best->second.last_update)
      continue;
    if (p->second.last_update > best->second.last_update) {
      best = p;
      continue;
    }
    // Prefer longer tail if it brings another peer into contiguity
    for (map<int, pg_info_t>::const_iterator q = infos.begin();
	 q != infos.end();
	 ++q) {
      if (q->second.is_incomplete())
	continue;  // don't care about log contiguity
      if (q->second.last_update < best->second.log_tail &&
	  q->second.last_update >= p->second.log_tail) {
	dout(10) << "calc_acting prefer osd." << p->first
		 << " because it brings osd." << q->first << " into log contiguity" << dendl;
	best = p;
	continue;
      }
      if (q->second.last_update < p->second.log_tail &&
	  q->second.last_update >= best->second.log_tail) {
	continue;
      }
    }
    // prefer current primary (usually the caller), all things being equal
    if (p->first == acting[0]) {
      dout(10) << "calc_acting prefer osd." << p->first
	       << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
bool PG::calc_acting(int& newest_update_osd_id, vector<int>& want) const
{
  map<int, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[osd->whoami] = info;

  for (map<int,pg_info_t>::iterator p = all_info.begin(); p != all_info.end(); ++p) {
    dout(10) << "calc_acting osd." << p->first << " " << p->second << dendl;
  }

  map<int, pg_info_t>::const_iterator newest_update_osd = find_best_info(all_info);

  if (newest_update_osd == all_info.end()) {
    if (up != acting) {
      dout(10) << "calc_acting no suitable info found (incomplete backfills?), reverting to up" << dendl;
      want = up;
      return true;
    } else {
      dout(10) << "calc_acting no suitable info found (incomplete backfills?)" << dendl;
      return false;
    }
  }


  dout(10) << "calc_acting newest update on osd." << newest_update_osd->first
	   << " with " << newest_update_osd->second << dendl;
  newest_update_osd_id = newest_update_osd->first;
  
  // select primary
  map<int,pg_info_t>::const_iterator primary;
  if (up.size() &&
      !all_info[up[0]].is_incomplete() &&
      all_info[up[0]].last_update >= newest_update_osd->second.log_tail) {
    dout(10) << "up[0](osd." << up[0] << ") selected as primary" << dendl;
    primary = all_info.find(up[0]);         // prefer up[0], all thing being equal
  } else if (!newest_update_osd->second.is_incomplete()) {
    dout(10) << "up[0] needs backfill, osd." << newest_update_osd_id
	     << " selected as primary instead" << dendl;
    primary = newest_update_osd;
  } else {
    map<int, pg_info_t> complete_infos;
    for (map<int, pg_info_t>::iterator i = all_info.begin();
	 i != all_info.end();
	 ++i) {
      if (!i->second.is_incomplete())
	complete_infos.insert(*i);
    }
    primary = find_best_info(complete_infos);
    if (primary == complete_infos.end() ||
	primary->second.last_update < newest_update_osd->second.log_tail) {
      dout(10) << "calc_acting no acceptable primary, reverting to up " << up << dendl;
      want = up;
      return true;
    } else {
      dout(10) << "up[0] and newest_update_osd need backfill, osd."
	       << newest_update_osd_id
	       << " selected as primary instead" << dendl;
    }
  }


  dout(10) << "calc_acting primary is osd." << primary->first
	   << " with " << primary->second << dendl;
  want.push_back(primary->first);
  unsigned usable = 1;
  unsigned backfill = 0;

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds 
  for (vector<int>::const_iterator i = up.begin();
       i != up.end();
       ++i) {
    if (*i == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(*i)->second;
    if (cur_info.is_incomplete() || cur_info.last_update < primary->second.log_tail) {
      if (backfill < 1) {
	dout(10) << " osd." << *i << " (up) accepted (backfill) " << cur_info << dendl;
	want.push_back(*i);
	backfill++;
      } else {
	dout(10) << " osd." << *i << " (up) rejected" << cur_info << dendl;
      }
    } else {
      want.push_back(*i);
      usable++;
      dout(10) << " osd." << *i << " (up) accepted " << cur_info << dendl;
    }
  }

  for (vector<int>::const_iterator i = acting.begin();
       i != acting.end();
       ++i) {
    if (usable >= get_osdmap()->get_pg_size(info.pgid))
      break;

    // skip up osds we already considered above
    if (*i == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), *i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(*i)->second;
    if (cur_info.is_incomplete() || cur_info.last_update < primary->second.log_tail) {
      dout(10) << " osd." << *i << " (stray) REJECTED " << cur_info << dendl;
    } else {
      want.push_back(*i);
      dout(10) << " osd." << *i << " (stray) accepted " << cur_info << dendl;
      usable++;
    }
  }

  for (map<int,pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (usable >= get_osdmap()->get_pg_size(info.pgid))
      break;

    // skip up osds we already considered above
    if (i->first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i->first);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(acting.begin(), acting.end(), i->first);
    if (acting_it != acting.end())
      continue;

    if (i->second.is_incomplete() || i->second.last_update < primary->second.log_tail) {
      dout(10) << " osd." << i->first << " (stray) REJECTED " << i->second << dendl;
    } else {
      want.push_back(i->first);
      dout(10) << " osd." << i->first << " (stray) accepted " << i->second << dendl;
      usable++;
    }
  }

  return true;
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 */
bool PG::choose_acting(int& newest_update_osd)
{
  vector<int> want;

  if (!calc_acting(newest_update_osd, want)) {
    dout(10) << "choose_acting failed" << dendl;
    assert(want_acting.empty());
    return false;
  }

  if (want.size() < pool.info.min_size) {
    want_acting.clear();
    return false;
  }

  if (want != acting) {
    dout(10) << "choose_acting want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;
    want_acting = want;
    if (want == up) {
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid, empty);
    } else
      osd->queue_want_pg_temp(info.pgid, want);
    return false;
  } else {
    want_acting.clear();
  }
  dout(10) << "choose_acting want " << want << " (== acting)" << dendl;
  return true;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PG::build_might_have_unfound()
{
  assert(might_have_unfound.empty());
  assert(is_primary());

  dout(10) << __func__ << dendl;

  // Make sure that we have past intervals.
  generate_past_intervals();

  // We need to decide who might have unfound objects that we need
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator end = past_intervals.rend();
  for (; p != end; ++p) {
    const pg_interval_t &interval(p->second);
    // We already have all the objects that exist at last_epoch_clean,
    // so there's no need to look at earlier intervals.
    if (interval.last < info.history.last_epoch_clean)
      break;

    // If nothing changed, we don't care about this interval.
    if (!interval.maybe_went_rw)
      continue;

    std::vector<int>::const_iterator a = interval.acting.begin();
    std::vector<int>::const_iterator a_end = interval.acting.end();
    for (; a != a_end; ++a) {
      if (*a != osd->whoami)
	might_have_unfound.insert(*a);
    }
  }

  // include any (stray) peers
  for (map<int,pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       p++)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

struct C_PG_ActivateCommitted : public Context {
  PG *pg;
  epoch_t epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e)
    : pg(p), epoch(e) {}
  void finish(int r) {
    pg->_activate_committed(epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t query_epoch,
		  list<Context*>& tfin,
		  map< int, map<pg_t,pg_query_t> >& query_map,
		  map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map)
{
  assert(!is_active());
  assert(scrubber.callbacks.empty());
  assert(callbacks_for_degraded_object.empty());

  // -- crash recovery?
  if (is_primary() &&
      pool.info.crash_replay_interval > 0 &&
      may_need_replay(get_osdmap())) {
    replay_until = ceph_clock_now(g_ceph_context);
    replay_until += pool.info.crash_replay_interval;
    dout(10) << "activate starting replay interval for " << pool.info.crash_replay_interval
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);

    // TODOSAM: osd->osd-> is no good
    osd->osd->replay_queue_lock.Lock();
    osd->osd->replay_queue.push_back(pair<pg_t,utime_t>(info.pgid, replay_until));
    osd->osd->replay_queue_lock.Unlock();
  }

  // twiddle pg state
  state_set(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  info.last_epoch_started = query_epoch;

  if (is_primary()) {
    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (missing.have_missing()) {
      build_might_have_unfound();
    }
  }
 
  if (role == 0) {    // primary state
    last_update_ondisk = info.last_update;
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }
  last_update_applied = info.last_update;


  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_log = true;

  // clean up stray objects
  clean_up_local(t); 

  // find out when we commit
  get();   // for callback
  tfin.push_back(new C_PG_ActivateCommitted(this, query_epoch));
  
  // initialize snap_trimq
  if (is_primary()) {
    snap_trimq = pool.cached_removed_snaps;
    snap_trimq.subtract(info.purged_snaps);
    dout(10) << "activate - snap_trimq " << snap_trimq << dendl;
    if (!snap_trimq.empty() && is_clean())
      queue_snap_trim();
  }

  // Check local snaps
  adjust_local_snaps();

  // init complete pointer
  if (missing.num_missing() == 0) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    log.complete_to = log.log.begin();
    while (log.complete_to->version <
	   missing.missing[missing.rmissing.begin()->second].need)
      log.complete_to++;
    assert(log.complete_to != log.log.end());
    if (log.complete_to == log.log.begin()) {
      info.last_complete = eversion_t();
    } else {
      log.complete_to--;
      info.last_complete = log.complete_to->version;
      log.complete_to++;
    }
    log.last_requested = 0;
    dout(10) << "activate -     complete_to = " << log.complete_to->version
	     << dendl;
    if (is_primary()) {
      dout(10) << "activate - starting recovery" << dendl;
      osd->queue_for_recovery(this);
      if (have_unfound())
	discover_all_missing(query_map);
    }
  }
    
  log_weirdness();

  // if primary..
  if (is_primary()) {
    // start up replicas

    // count replicas that are not backfilling
    unsigned active = 1;

    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_info.count(peer));
      pg_info_t& pi = peer_info[peer];

      dout(10) << "activate peer osd." << peer << " " << pi << dendl;

      MOSDPGLog *m = 0;
      pg_missing_t& pm = peer_missing[peer];

      bool needs_past_intervals = pi.dne();

      if (pi.last_update == info.last_update) {
        // empty log
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd." << peer << " is up to date, queueing in pending_activators" << dendl;
	  (*activator_map)[peer].push_back(
	    make_pair(
	      pg_notify_t(
		get_osdmap()->get_epoch(),
		get_osdmap()->get_epoch(),
		info),
	      past_intervals));
	} else {
	  dout(10) << "activate peer osd." << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(get_osdmap()->get_epoch(), info);
	}
      } else if (log.tail > pi.last_update || pi.last_backfill == hobject_t()) {
	// backfill
	osd->clog.info() << info.pgid << " restarting backfill on osd." << peer
			 << " from (" << pi.log_tail << "," << pi.last_update << "] " << pi.last_backfill
			 << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.last_backfill = hobject_t();
	pi.history = info.history;
	pi.stats.stats.clear();

	m = new MOSDPGLog(get_osdmap()->get_epoch(), pi);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(log, g_conf->osd_min_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	assert(log.tail <= pi.last_update);
	m = new MOSDPGLog(get_osdmap()->get_epoch(), info);
	// send new stuff to append to replicas log
	m->log.copy_after(log, pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (needs_past_intervals)
	m->past_intervals = past_intervals;

      if (pi.last_backfill == hobject_t::get_max())
	active++;

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (list<pg_log_entry_t>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             p++)
	  if (p->soid <= pi.last_backfill)
	    pm.add_next_event(*p);
      }
      
      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log << dendl;
	//m->log.print(cout);
	osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
      }

      // peer now has 
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd." << peer << " " << pi << " uptodate" << dendl;
      } else {
        dout(10) << "activate peer osd." << peer << " " << pi << " missing " << pm << dendl;
      }
    }

    // degraded?
    if (get_osdmap()->get_pg_size(info.pgid) > active)
      state_set(PG_STATE_DEGRADED);

    // all clean?
    if (needs_recovery()) {
      dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            DoRecovery())));
    } else if (needs_backfill()) {
      dout(10) << "activate queueing backfill" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            RequestBackfill())));
    } else {
      dout(10) << "activate all replicas clean, no recovery" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            AllReplicasRecovered())));
    }

    update_stats();
  }

  // we need to flush this all out before doing anything else..
  need_flush = true;

  // waiters
  if (!is_replay()) {
    requeue_ops(waiting_for_active);
  }

  on_activate();
}

void PG::do_pending_flush()
{
  assert(is_locked());
  if (need_flush) {
    dout(10) << "do_pending_flush doing pending flush" << dendl;
    osr->flush();
    need_flush = false;
    dout(10) << "do_pending_flush done" << dendl;
  }
}

bool PG::op_has_sufficient_caps(OpRequestRef op)
{
  // only check MOSDOp
  if (op->request->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->request);

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  string key = req->get_object_locator().key;
  if (key.length() == 0)
    key = req->get_oid().name;

  bool cap = caps.is_capable(pool.name, pool.auid, key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps pool=" << pool.id << " (" << pool.name
	   << ") owner=" << pool.auid
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::do_request(OpRequestRef op)
{
  // do any pending flush
  do_pending_flush();

  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }
  if (must_delay_request(op)) {
    waiting_for_map.push_back(op);
    return;
  }
  if (can_discard_request(op)) {
    return;
  }
  if (!flushed) {
    waiting_for_active.push_back(op);
    return;
  }

  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (is_replay() || !is_active()) {
      waiting_for_active.push_back(op);
      return;
    }
    do_op(op); // do it now
    break;

  case MSG_OSD_SUBOP:
    do_sub_op(op);
    break;

  case MSG_OSD_SUBOPREPLY:
    do_sub_op_reply(op);
    break;

  case MSG_OSD_PG_SCAN:
    do_scan(op);
    break;

  case MSG_OSD_PG_BACKFILL:
    do_backfill(op);
    break;

  default:
    assert(0 == "bad message type in do_request");
  }
}


void PG::replay_queued_ops()
{
  assert(is_replay() && is_active());
  eversion_t c = info.last_update;
  list<OpRequestRef> replay;
  dout(10) << "replay_queued_ops" << dendl;
  state_clear(PG_STATE_REPLAY);

  for (map<eversion_t,OpRequestRef>::iterator p = replay_queue.begin();
       p != replay_queue.end();
       p++) {
    if (p->first.version != c.version+1) {
      dout(10) << "activate replay " << p->first
	       << " skipping " << c.version+1 - p->first.version 
	       << " ops"
	       << dendl;      
      c = p->first;
    }
    dout(10) << "activate replay " << p->first << " "
             << *p->second->request << dendl;
    replay.push_back(p->second);
  }
  replay_queue.clear();
  requeue_ops(replay);
  requeue_ops(waiting_for_active);

  update_stats();
}

void PG::_activate_committed(epoch_t e)
{
  lock();
  if (e < last_peering_reset) {
    dout(10) << "_activate_committed " << e << ", that was an old interval" << dendl;
  } else if (is_primary()) {
    peer_activated.insert(osd->whoami);
    dout(10) << "_activate_committed " << e << " peer_activated now " << peer_activated 
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_interval_since " << info.history.same_interval_since << dendl;
    if (peer_activated.size() == acting.size())
      all_activated_and_committed();
  } else {
    dout(10) << "_activate_committed " << e << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(e);
    pg_notify_t i = pg_notify_t(get_osdmap()->get_epoch(),
				get_osdmap()->get_epoch(),
				info);
    i.info.history.last_epoch_started = e;
    m->pg_list.push_back(make_pair(i, pg_interval_map_t()));
    osd->send_message_osd_cluster(acting[0], m, get_osdmap()->get_epoch());
  }

  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_info(*t);
    int tr = osd->store->queue_transaction(osr.get(), t);
    assert(tr == 0);
  }

  unlock();
  put();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PG::all_activated_and_committed()
{
  dout(10) << "all_activated_and_committed" << dendl;
  assert(is_primary());
  assert(peer_activated.size() == acting.size());

  // info.last_epoch_started is set during activate()
  info.history.last_epoch_started = info.last_epoch_started;

  share_pg_info();
  update_stats();

  queue_peering_event(
    CephPeeringEvtRef(
      new CephPeeringEvt(
        get_osdmap()->get_epoch(),
        get_osdmap()->get_epoch(),
        AllReplicasActivated())));
}

void PG::queue_snap_trim()
{
  if (osd->queue_for_snap_trim(this))
    dout(10) << "queue_snap_trim -- queuing" << dendl;
  else
    dout(10) << "queue_snap_trim -- already trimming" << dendl;
}

bool PG::queue_scrub()
{
  assert(_lock.is_locked());
  if (is_scrubbing()) {
    return false;
  }
  scrubber.must_scrub = false;
  state_set(PG_STATE_SCRUBBING);
  if (scrubber.must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
    scrubber.must_deep_scrub = false;
  }
  if (scrubber.must_repair) {
    state_set(PG_STATE_REPAIR);
    scrubber.must_repair = false;
  }
  osd->queue_for_scrub(this);
  return true;
}

struct C_PG_FinishRecovery : public Context {
  PG *pg;
  C_PG_FinishRecovery(PG *p) : pg(p) {
    pg->get();
  }
  void finish(int r) {
    pg->_finish_recovery(this);
  }
};

void PG::mark_clean()
{
  // only mark CLEAN if we have the desired number of replicas AND we
  // are not remapped.
  if (acting.size() == get_osdmap()->get_pg_size(info.pgid) &&
      up == acting)
    state_set(PG_STATE_CLEAN);

  // NOTE: this is actually a bit premature: we haven't purged the
  // strays yet.
  info.history.last_epoch_clean = get_osdmap()->get_epoch();

  trim_past_intervals();

  if (is_clean() && !snap_trimq.empty())
    queue_snap_trim();

  dirty_info = true;
}

void PG::finish_recovery(list<Context*>& tfin)
{
  dout(10) << "finish_recovery" << dendl;
  assert(info.last_complete == info.last_update);

  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  tfin.push_back(finish_sync_event);
}

void PG::_finish_recovery(Context *c)
{
  lock();
  if (c == finish_sync_event) {
    dout(10) << "_finish_recovery" << dendl;
    finish_sync_event = 0;
    purge_strays();

    update_stats();

    if (scrub_after_recovery) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      scrub_after_recovery = false;
      queue_scrub();
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  unlock();
  put();
}

void PG::start_recovery_op(const hobject_t& soid)
{
  dout(10) << "start_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")"
#endif
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid) == 0);
  recovering_oids.insert(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->start_recovery_op(this, soid);
}

void PG::finish_recovery_op(const hobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")" 
#endif
	   << dendl;
  assert(recovery_ops_active > 0);
  recovery_ops_active--;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid));
  recovering_oids.erase(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->finish_recovery_op(this, soid, dequeue);
}

void PG::IndexedLog::split_into(
  pg_t child_pgid,
  unsigned split_bits,
  PG::IndexedLog *olog)
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
      if (log.empty())
	tail = i->version;
    } else {
      log.push_back(*i);
      if (olog->empty())
	olog->tail = i->version;
    }
    oldlog.erase(i++);
  }

  if (log.empty())
    tail = head;
  else
    head = log.rbegin()->version;

  if (olog->empty())
    olog->tail = olog->head;
  else
    olog->head = olog->log.rbegin()->version;

  olog->index();
  index();
}

static void split_list(
  list<OpRequestRef> *from,
  list<OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (list<OpRequestRef>::iterator i = from->begin();
       i != from->end();
    ) {
    if (PG::split_request(*i, match, bits)) {
      to->push_back(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

static void split_replay_queue(
  map<eversion_t, OpRequestRef> *from,
  map<eversion_t, OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (map<eversion_t, OpRequestRef>::iterator i = from->begin();
       i != from->end();
       ) {
    if (PG::split_request(i->second, match, bits)) {
      to->insert(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

void PG::split_ops(PG *child, unsigned split_bits) {
  unsigned match = child->info.pgid.m_seed;
  assert(waiting_for_map.empty());
  assert(waiting_for_all_missing.empty());
  assert(waiting_for_missing_object.empty());
  assert(waiting_for_degraded_object.empty());
  assert(waiting_for_ack.empty());
  assert(waiting_for_ondisk.empty());
  split_replay_queue(&replay_queue, &(child->replay_queue), match, split_bits);

  osd->dequeue_pg(this, &waiting_for_active);
  split_list(&waiting_for_active, &(child->waiting_for_active), match, split_bits);
}

void PG::split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  child->osdmap_ref = osdmap_ref;

  child->pool = pool;

  // Log
  log.split_into(child_pgid, split_bits, &(child->log));
  child->info.last_complete = info.last_complete;

  info.last_update = log.head;
  child->info.last_update = child->log.head;

  info.log_tail = log.tail;
  child->info.log_tail = child->log.tail;

  if (info.last_complete < log.tail)
    info.last_complete = log.tail;
  if (child->info.last_complete < child->log.tail)
    child->info.last_complete = child->log.tail;

  // Missing
  missing.split_into(child_pgid, split_bits, &(child->missing));

  // Info
  child->info.history = info.history;
  child->info.purged_snaps = info.purged_snaps;
  child->info.last_backfill = info.last_backfill;

  child->info.stats = info.stats;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.last_epoch_started = info.last_epoch_started;

  child->snap_trimq = snap_trimq;

  get_osdmap()->pg_to_up_acting_osds(child->info.pgid, child->up, child->acting);
  child->role = get_osdmap()->calc_pg_role(osd->whoami, child->acting);
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap()->get_epoch();

  // History
  child->past_intervals = past_intervals;

  split_ops(child, split_bits);
  _split_into(child_pgid, child, split_bits);

  child->dirty_info = true;
  child->dirty_log = true;
  dirty_info = true;
  dirty_log = true;
}

void PG::defer_recovery()
{
  // TODOSAM: osd->osd-> not good
  osd->osd->defer_recovery(this);
}

void PG::clear_recovery_state() 
{
  dout(10) << "clear_recovery_state" << dendl;

  log.reset_recovery_pointers();
  finish_sync_event = 0;

  hobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    finish_recovery_op(soid, true);
  }

  backfill_target = -1;
  backfill_info.clear();
  peer_backfill_info.clear();
  waiting_on_backfill = false;
  _clear_recovery_state();  // pg impl specific hook
}

void PG::cancel_recovery()
{
  dout(10) << "cancel_recovery" << dendl;
  clear_recovery_state();
}


void PG::purge_strays()
{
  dout(10) << "purge_strays " << stray_set << dendl;
  
  bool removed = false;
  for (set<int>::iterator p = stray_set.begin();
       p != stray_set.end();
       p++) {
    if (get_osdmap()->is_up(*p)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<pg_t> to_remove;
      to_remove.push_back(info.pgid);
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap()->get_epoch(),
	to_remove);
      osd->send_message_osd_cluster(*p, m, get_osdmap()->get_epoch());
      stray_purged.insert(*p);
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_info.erase(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PG::set_probe_targets(const set<int> &probe_set)
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets = probe_set;
}

void PG::clear_probe_targets()
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers()
{
  assert(is_locked());

  set<int> new_peers;
  if (role == 0) {
    for (unsigned i=0; i<acting.size(); i++)
      new_peers.insert(acting[i]);
    for (unsigned i=0; i<up.size(); i++)
      new_peers.insert(up[i]);
    for (map<int,pg_info_t>::iterator p = peer_info.begin(); p != peer_info.end(); ++p)
      new_peers.insert(p->first);
  }

  bool need_update = false;
  heartbeat_peer_lock.Lock();
  if (new_peers == heartbeat_peers) {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " unchanged" << dendl;
  } else {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " -> " << new_peers << dendl;
    heartbeat_peers.swap(new_peers);
    need_update = true;
  }
  heartbeat_peer_lock.Unlock();

  if (need_update)
    osd->need_heartbeat_peer_update();
}

void PG::update_stats()
{
  pg_stats_lock.Lock();
  if (is_primary()) {
    // update our stat summary
    info.stats.reported.inc(info.history.same_primary_since);
    info.stats.version = info.last_update;
    info.stats.created = info.history.epoch_created;
    info.stats.last_scrub = info.history.last_scrub;
    info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
    info.stats.last_deep_scrub = info.history.last_deep_scrub;
    info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
    info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
    info.stats.last_epoch_clean = info.history.last_epoch_clean;

    if (info.stats.stats.sum.num_scrub_errors)
      state_set(PG_STATE_INCONSISTENT);
    else
      state_clear(PG_STATE_INCONSISTENT);

    utime_t now = ceph_clock_now(g_ceph_context);
    info.stats.last_fresh = now;
    if (info.stats.state != state) {
      info.stats.state = state;
      info.stats.last_change = now;
    }
    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    info.stats.last_unstale = now;

    info.stats.log_size = ondisklog.length();
    info.stats.ondisk_log_size = ondisklog.length();
    info.stats.log_start = log.tail;
    info.stats.ondisk_log_start = log.tail;

    pg_stats_valid = true;
    pg_stats_stable = info.stats;

    // calc copies, degraded
    unsigned target = MAX(get_osdmap()->get_pg_size(info.pgid), acting.size());
    pg_stats_stable.stats.calc_copies(target);
    pg_stats_stable.stats.sum.num_objects_degraded = 0;
    if ((is_degraded() || !is_clean()) && is_active()) {
      // NOTE: we only generate copies, degraded, unfound values for
      // the summation, not individual stat categories.
      uint64_t num_objects = pg_stats_stable.stats.sum.num_objects;

      uint64_t degraded = 0;

      // if the acting set is smaller than we want, add in those missing replicas
      if (acting.size() < target)
	degraded += (target - acting.size()) * num_objects;

      // missing on primary
      pg_stats_stable.stats.sum.num_objects_missing_on_primary = missing.num_missing();
      degraded += missing.num_missing();
      
      for (unsigned i=1; i<acting.size(); i++) {
	assert(peer_missing.count(acting[i]));

	// in missing set
	degraded += peer_missing[acting[i]].num_missing();

	// not yet backfilled
	degraded += num_objects - peer_info[acting[i]].stats.stats.sum.num_objects;
      }
      pg_stats_stable.stats.sum.num_objects_degraded = degraded;
      pg_stats_stable.stats.sum.num_objects_unfound = get_num_unfound();
    }

    dout(15) << "update_stats " << pg_stats_stable.reported << dendl;
  } else {
    pg_stats_valid = false;
    dout(15) << "update_stats -- not primary" << dendl;
  }
  pg_stats_lock.Unlock();

  if (is_primary())
    osd->pg_stat_queue_enqueue(this);
}

void PG::clear_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_lock.Lock();
  pg_stats_valid = false;
  pg_stats_lock.Unlock();

  osd->pg_stat_queue_dequeue(this);
}

/**
 * initialize a newly instantiated pg
 *
 * Initialize PG state, as when a PG is initially created, or when it
 * is first instantiated on the current node.
 *
 * @param role our role/rank
 * @param newup up set
 * @param newacting acting set
 * @param history pg history
 * @param pi past_intervals
 * @param t transaction to write out our new state in
 */
void PG::init(int role, vector<int>& newup, vector<int>& newacting, pg_history_t& history,
	      pg_interval_map_t& pi,
	      ObjectStore::Transaction *t)
{
  dout(10) << "init role " << role << " up " << newup << " acting " << newacting
	   << " history " << history
	   << " " << pi.size() << " past_intervals"
	   << dendl;

  set_role(role);
  acting = newacting;
  up = newup;

  info.history = history;
  past_intervals.swap(pi);

  info.stats.up = up;
  info.stats.acting = acting;
  info.stats.mapping_epoch = info.history.same_interval_since;

  reg_next_scrub();

  write_info(*t);
  write_log(*t);
}

void PG::write_info(ObjectStore::Transaction& t)
{
  // pg state
  bufferlist infobl;
  __u8 struct_v = 5;
  ::encode(struct_v, infobl);
  ::encode(get_osdmap()->get_epoch(), infobl);
  t.collection_setattr(coll, "info", infobl);
 
  // potentially big stuff
  bufferlist bigbl;
  ::encode(past_intervals, bigbl);
  ::encode(snap_collections, bigbl);
  ::encode(info, bigbl);
  dout(20) << "write_info bigbl " << bigbl.length() << dendl;
  t.truncate(coll_t::META_COLL, biginfo_oid, 0);
  t.write(coll_t::META_COLL, biginfo_oid, 0, bigbl.length(), bigbl);

  dirty_info = false;
}

epoch_t PG::peek_map_epoch(ObjectStore *store, coll_t coll, bufferlist *bl)
{
  assert(bl);
  store->collection_getattr(coll, "info", *bl);
  bufferlist::iterator bp = bl->begin();
  __u8 struct_v = 0;
  ::decode(struct_v, bp);
  if (struct_v < 5)
    return 0;
  epoch_t cur_epoch = 0;
  ::decode(cur_epoch, bp);
  return cur_epoch;
}

void PG::write_log(ObjectStore::Transaction& t)
{
  dout(10) << "write_log" << dendl;

  // assemble buffer
  bufferlist bl;

  // build buffer
  ondisklog.tail = 0;
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end();
       p++) {
    uint64_t startoff = bl.length();

    bufferlist ebl(sizeof(*p)*2);
    ::encode(*p, ebl);
    __u32 crc = ebl.crc32c(0);
    ::encode(ebl, bl);
    ::encode(crc, bl);

    p->offset = startoff;
  }
  ondisklog.head = bl.length();
  ondisklog.has_checksums = true;

  // write it
  t.remove(coll_t::META_COLL, log_oid );
  t.write(coll_t::META_COLL, log_oid , 0, bl.length(), bl);

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(coll, "ondisklog", blb);
  
  dout(10) << "write_log to " << ondisklog.tail << "~" << ondisklog.length() << dendl;
  dirty_log = false;
}

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_info)
    write_info(t);
  if (dirty_log)
    write_log(t);
}

void PG::trim(ObjectStore::Transaction& t, eversion_t trim_to)
{
  // trim?
  if (trim_to > log.tail) {
    /* If we are trimming, we must be complete up to trim_to, time
     * to throw out any divergent_priors
     */
    ondisklog.divergent_priors.clear();
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(t, trim_to);
    info.log_tail = log.tail;
    trim_ondisklog(t);
  }
}

void PG::trim_ondisklog(ObjectStore::Transaction& t) 
{
  uint64_t new_tail;
  if (log.empty()) {
    new_tail = ondisklog.head;
  } else {
    new_tail = log.log.front().offset;
  }
  bool same_block = (new_tail & ~4095) == (ondisklog.tail & ~4095);
  dout(15) << "trim_ondisklog tail " << ondisklog.tail << " -> " << new_tail
	   << ", now " << new_tail << "~" << (ondisklog.head - new_tail)
	   << " " << (same_block ? "(same block)" : "(different block)")
	   << dendl;
  assert(new_tail >= ondisklog.tail);
  
  if (same_block)
    return;

  ondisklog.tail = new_tail;

  if (!g_conf->osd_preserve_trimmed_log) {
    uint64_t zt = new_tail & ~4095;
    if (zt > ondisklog.zero_to) {
      t.zero(coll_t::META_COLL, log_oid, ondisklog.zero_to, zt - ondisklog.zero_to);
      dout(15) << "trim_ondisklog zeroing from " << ondisklog.zero_to
	       << " to " << zt << dendl;
      ondisklog.zero_to = zt;
    }
  }

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(coll, "ondisklog", blb);
}

void PG::trim_peers()
{
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  if (pg_trim_to != eversion_t()) {
    for (unsigned i=1; i<acting.size(); i++)
      osd->send_message_osd_cluster(acting[i],
				    new MOSDPGTrim(get_osdmap()->get_epoch(), info.pgid,
						   pg_trim_to),
				    get_osdmap()->get_epoch());
  }
}

void PG::add_log_entry(pg_log_entry_t& e, bufferlist& log_bl)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;
  
  // raise last_update.
  assert(e.version > info.last_update);
  info.last_update = e.version;

  // log mutation
  log.add(e);
  if (ondisklog.has_checksums) {
    bufferlist ebl(sizeof(e)*2);
    ::encode(e, ebl);
    __u32 crc = ebl.crc32c(0);
    ::encode(ebl, log_bl);
    ::encode(crc, log_bl);
  } else {
    ::encode(e, log_bl);
  }
  dout(10) << "add_log_entry " << e << dendl;
}


void PG::append_log(vector<pg_log_entry_t>& logv, eversion_t trim_to, ObjectStore::Transaction &t)
{
  dout(10) << "append_log " << log << " " << logv << dendl;

  bufferlist bl;
  for (vector<pg_log_entry_t>::iterator p = logv.begin();
       p != logv.end();
       p++) {
    p->offset = ondisklog.head + bl.length();
    add_log_entry(*p, bl);
  }

  dout(10) << "append_log  " << ondisklog.tail << "~" << ondisklog.length()
	   << " adding " << bl.length() << dendl;

  t.write(coll_t::META_COLL, log_oid, ondisklog.head, bl.length(), bl );
  ondisklog.head += bl.length();

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(coll, "ondisklog", blb);
  dout(10) << "append_log  now " << ondisklog.tail << "~" << ondisklog.length() << dendl;

  trim(t, trim_to);

  // update the local pg, pg log
  write_info(t);
}

bool PG::check_log_for_corruption(ObjectStore *store)
{
  OndiskLog bounds;
  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  bufferlist::iterator p = blb.begin();
  ::decode(bounds, p);

  dout(10) << "check_log_for_corruption: tail " << bounds.tail << " head " << bounds.head << dendl;

  stringstream ss;
  ss << "CORRUPT pg " << info.pgid << " log: ";

  bool ok = true;
  uint64_t pos = 0;
  if (bounds.head > 0) {
    // read
    struct stat st;
    store->stat(coll_t::META_COLL, log_oid, &st);
    bufferlist bl;
    store->read(coll_t::META_COLL, log_oid, bounds.tail, bounds.length(), bl);
    if (st.st_size != (int)bounds.head) {
      ss << "mismatched bounds " << bounds.tail << ".." << bounds.head << " and file size " << st.st_size;
      ok = false;
    } else if (bl.length() < bounds.length()) {
      dout(0) << " got " << bl.length() << " bytes, expected " 
	      << bounds.tail << ".." << bounds.head << "="
	      << bounds.length()
	      << dendl;
      ss << "short log, " << bl.length() << " bytes, expected " << bounds.length();
      ok = false;
    } else {
      pg_log_entry_t e;
      bufferlist::iterator p = bl.begin();
      while (!p.end()) {
	pos = bounds.tail + p.get_off();
	try {
	  ::decode(e, p);
	}
	catch (const buffer::error &e) {
	  dout(0) << "corrupt entry at " << pos << dendl;
	  ss << "corrupt entry at offset " << pos;
	  ok = false;
	  break;
	}
	catch(const std::bad_alloc &a) {
	  dout(0) << "corrupt entry at " << pos << dendl;
	  ss << "corrupt entry at offset " << pos;
	  ok = false;
	  break;
	}
	dout(30) << " " << pos << " " << e << dendl;
      }
    }
  }
  if (!ok) {
    stringstream f;
    f << "/tmp/pglog_bad_" << info.pgid;
    string filename;
    getline(f, filename);
    blb.write_file(filename.c_str(), 0644);
    ss << ", saved to " << filename;
    osd->clog.error(ss);
  }
  return ok;
}

//! Get the name we're going to save our corrupt page log as
std::string PG::get_corrupt_pg_log_name() const
{
  const int MAX_BUF = 512;
  char buf[MAX_BUF];
  struct tm tm_buf;
  time_t my_time(time(NULL));
  const struct tm *t = localtime_r(&my_time, &tm_buf);
  int ret = strftime(buf, sizeof(buf), "corrupt_log_%Y-%m-%d_%k:%M_", t);
  if (ret == 0) {
    dout(0) << "strftime failed" << dendl;
    return "corrupt_log_unknown_time";
  }
  info.pgid.print(buf + ret, MAX_BUF - ret);
  return buf;
}

int PG::read_info(ObjectStore *store, const coll_t coll, bufferlist &bl,
  pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
  hobject_t &biginfo_oid, interval_set<snapid_t>  &snap_collections)
{
  bufferlist::iterator p = bl.begin();
  __u8 struct_v;

  // info
  ::decode(struct_v, p);
  if (struct_v < 4)
    ::decode(info, p);
  if (struct_v < 2) {
    ::decode(past_intervals, p);
  
    // snap_collections
    bl.clear();
    store->collection_getattr(coll, "snap_collections", bl);
    p = bl.begin();
    ::decode(struct_v, p);
  } else {
    bl.clear();
    int r = store->read(coll_t::META_COLL, biginfo_oid, 0, 0, bl);
    if (r < 0)
       return r;
    p = bl.begin();
    ::decode(past_intervals, p);
  }

  if (struct_v < 3) {
    set<snapid_t> snap_collections_temp;
    ::decode(snap_collections_temp, p);
    snap_collections.clear();
    for (set<snapid_t>::iterator i = snap_collections_temp.begin();
	 i != snap_collections_temp.end();
	 ++i) {
      snap_collections.insert(*i);
    }
  } else {
    ::decode(snap_collections, p);
    if (struct_v >= 4)
      ::decode(info, p);
  }
  return 0;
}

void PG::read_state(ObjectStore *store, bufferlist &bl)
{
  int r = read_info(store, coll, bl, info, past_intervals, biginfo_oid,
    snap_collections);
  assert(r >= 0);

  try {
    ostringstream oss;
    read_log(store, coll, log_oid, info, ondisklog, log, missing, oss, this);
    if (oss.str().length())
      osd->clog.error() << oss;
  }
  catch (const buffer::error &e) {
    string cr_log_coll_name(get_corrupt_pg_log_name());
    dout(0) << "Got exception '" << e.what() << "' while reading log. "
            << "Moving corrupted log file to '" << cr_log_coll_name
	    << "' for later " << "analysis." << dendl;

    ondisklog.zero();

    // clear log index
    log.head = log.tail = info.last_update;

    // reset info
    info.log_tail = info.last_update;

    // Move the corrupt log to a new place and create a new zero-length log entry.
    ObjectStore::Transaction t;
    coll_t cr_log_coll(cr_log_coll_name);
    t.create_collection(cr_log_coll);
    t.collection_move(cr_log_coll, coll_t::META_COLL, log_oid);
    t.touch(coll_t::META_COLL, log_oid);
    write_info(t);
    store->apply_transaction(t);

    info.last_backfill = hobject_t();
    info.stats.stats.clear();
  }

  // log any weirdness
  log_weirdness();
}

void PG::log_weirdness()
{
  if (log.tail != info.log_tail)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.tail " << log.tail
		      << " != info.log_tail " << info.log_tail
		      << "\n";
  if (log.head != info.last_update)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.head " << log.head
		      << " != info.last_update " << info.last_update
		      << "\n";

  if (log.empty()) {
    // shoudl it be?
    if (log.head != log.tail)
      osd->clog.error() << info.pgid
			<< " log bound mismatch, empty but (" << log.tail << "," << log.head << "]\n";
  } else {
    if ((log.log.begin()->version <= log.tail) || // sloppy check
	(log.log.rbegin()->version != log.head && !(log.head == log.tail)))
      osd->clog.error() << info.pgid
			<< " log bound mismatch, info (" << log.tail << "," << log.head << "]"
			<< " actual ["
			<< log.log.begin()->version << "," << log.log.rbegin()->version << "]"
			<< "\n";
  }
  
  if (info.last_complete < log.tail)
    osd->clog.error() << info.pgid
		      << " last_complete " << info.last_complete
		      << " < log.tail " << log.tail
		      << "\n";
}

coll_t PG::make_snap_collection(ObjectStore::Transaction& t, snapid_t s)
{
  coll_t c(info.pgid, s);
  if (!snap_collections.contains(s)) {
    snap_collections.insert(s);
    write_info(t);
    dout(10) << "create_snap_collection " << c << ", set now " << snap_collections << dendl;
    t.create_collection(c);
  }
  return c;
}

void PG::update_snap_collections(vector<pg_log_entry_t> &log_entries,
				 ObjectStore::Transaction &t)
{
  for (vector<pg_log_entry_t>::iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    if (i->snaps.length() > 0) {
      vector<snapid_t> snaps;
      bufferlist::iterator p = i->snaps.begin();
      try {
	::decode(snaps, p);
      } catch (...) {
	snaps.clear();
      }
      if (snaps.size()) {
	make_snap_collection(t, snaps[0]);
	if (snaps.size() > 1)
	  make_snap_collection(t, *(snaps.rbegin()));
      }
    }
  }
}

/**
 * filter trimming|trimmed snaps out of snapcontext
 */
void PG::filter_snapc(SnapContext& snapc)
{
  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snapc.snaps.begin();
       p != snapc.snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snapc << dendl;
	newsnaps.insert(newsnaps.begin(), snapc.snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snapc.snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snapc << dendl;
  }
}


void PG::adjust_local_snaps()
{
  interval_set<snapid_t> to_remove;
  to_remove.intersection_of(snap_collections, info.purged_snaps);
  if (!to_remove.empty()) {
    queue_snap_trim();
  }
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m)
{
  for (map<hobject_t, list<OpRequestRef> >::iterator it = m.begin();
       it != m.end();
       it++)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(PGRef(this), *i));
  }
  ls.clear();
}


// ==========================================================================================
// SCRUB

/*
 * when holding pg and sched_scrub_lock, then the states are:
 *   scheduling:
 *     scrubber.reserved = true
 *     scrub_rserved_peers includes whoami
 *     osd->scrub_pending++
 *   scheduling, replica declined:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers includes -1
 *     osd->scrub_pending++
 *   pending:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers.size() == acting.size();
 *     pg on scrub_wq
 *     osd->scrub_pending++
 *   scrubbing:
 *     scrubber.reserved = false;
 *     scrubber.reserved_peers empty
 *     osd->scrubber.active++
 */

// returns true if a scrub has been newly kicked off
bool PG::sched_scrub()
{
  assert(_lock.is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return false;
  }

  bool ret = true;
  if (!scrubber.reserved) {
    assert(scrubber.reserved_peers.empty());
    if (osd->inc_scrubs_pending()) {
      dout(20) << "sched_scrub: reserved locally, reserving replicas" << dendl;
      scrubber.reserved = true;
      scrubber.reserved_peers.insert(osd->whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << "sched_scrub: failed to reserve locally" << dendl;
      ret = false;
    }
  }
  if (scrubber.reserved) {
    if (scrubber.reserve_failed) {
      dout(20) << "sched_scrub: failed, a peer declined" << dendl;
      clear_scrub_reserved();
      scrub_unreserve_replicas();
      ret = false;
    } else if (scrubber.reserved_peers.size() == acting.size()) {
      dout(20) << "sched_scrub: success, reserved self and replicas" << dendl;
      if (ceph_clock_now(g_ceph_context) >
	  info.history.last_deep_scrub_stamp + g_conf->osd_deep_scrub_interval) {
	dout(10) << "sched_scrub: scrub will be deep" << dendl;
	state_set(PG_STATE_DEEP_SCRUB);
      }
      queue_scrub();
    } else {
      // none declined, since scrubber.reserved is set
      dout(20) << "sched_scrub: reserved " << scrubber.reserved_peers << ", waiting for replicas" << dendl;
    }
  }

  return ret;
}

void PG::reg_next_scrub()
{
  if (scrubber.must_scrub) {
    scrubber.scrub_reg_stamp = utime_t();
  } else {
    scrubber.scrub_reg_stamp = info.history.last_scrub_stamp;
  }
  osd->reg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::unreg_next_scrub()
{
  osd->unreg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::sub_op_scrub_map(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp *)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_map" << dendl;

  if (m->map_epoch < info.history.same_interval_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << m->map_epoch << " < " << info.history.same_interval_since << dendl;
    return;
  }

  op->mark_started();

  int from = m->get_source().num();

  dout(10) << " got osd." << from << " scrub map" << dendl;
  bufferlist::iterator p = m->get_data().begin();

  if (scrubber.is_chunky) { // chunky scrub
    scrubber.received_maps[from].decode(p, info.pgid.pool());
    dout(10) << "map version is " << scrubber.received_maps[from].valid_through << dendl;
  } else {               // classic scrub
    if (scrubber.received_maps.count(from)) {
      ScrubMap incoming;
      incoming.decode(p, info.pgid.pool());
      dout(10) << "from replica " << from << dendl;
      dout(10) << "map version is " << incoming.valid_through << dendl;
      scrubber.received_maps[from].merge_incr(incoming);
    } else {
      scrubber.received_maps[from].decode(p, info.pgid.pool());
    }
  }

  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(from);

  if (scrubber.waiting_on == 0) {
    if (scrubber.is_chunky) { // chunky scrub
      osd->scrub_wq.queue(this);
    } else {                  // classic scrub
      if (scrubber.finalizing) { // incremental lists received
        osd->scrub_finalize_wq.queue(this);
      } else {                // initial lists received
        scrubber.block_writes = true;
        if (last_update_applied == info.last_update) {
          scrubber.finalizing = true;
          scrub_gather_replica_maps();
          ++scrubber.waiting_on;
          scrubber.waiting_on_whom.insert(osd->whoami);
          osd->scrub_wq.queue(this);
        }
      }
    }
  }
}

/* 
 * pg lock may or may not be held
 */
void PG::_scan_list(ScrubMap &map, vector<hobject_t> &ls, bool deep)
{
  dout(10) << "_scan_list scanning " << ls.size() << " objects"
           << (deep ? " deeply" : "") << dendl;
  int i = 0;
  for (vector<hobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++) {
    hobject_t poid = *p;

    struct stat st;
    int r = osd->store->stat(coll, poid, &st);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      o.nlinks = st.st_nlink;
      assert(!o.negative);
      osd->store->getattrs(coll, poid, o.attrs);

      // calculate the CRC32 on deep scrubs
      if (deep) {
        bufferhash h, oh;
        bufferlist bl, hdrbl;
        int r;
        __u64 pos = 0;
        while ( (r = osd->store->read(coll, poid, pos,
                                      g_conf->osd_deep_scrub_stride, bl)) > 0) {
          h << bl;
          pos += bl.length();
          bl.clear();
        }
        o.digest = h.digest();
        o.digest_present = true;

        bl.clear();
        r = osd->store->omap_get_header(coll, poid, &hdrbl);
        if (r == 0) {
          dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
          ::encode(hdrbl, bl);
          oh << bl;
          bl.clear();
        }

        ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
          coll, poid);
        assert(iter);
        for (iter->seek_to_first(); iter->valid() ; iter->next()) {
          dout(25) << "CRC key " << iter->key() << " value "
            << string(iter->value().c_str(), iter->value().length()) << dendl;

          ::encode(iter->key(), bl);
          ::encode(iter->value(), bl);
          oh << bl;
          bl.clear();
        }

        //Store final calculated CRC32 of omap header & key/values
        o.omap_digest = oh.digest();
        o.omap_digest_present = true;
      }

      if (poid.snap != CEPH_SNAPDIR && poid.snap != CEPH_NOSNAP) {
	// Check snap collections
	check_snap_collections(st.st_ino, poid, o.attrs, &o.snapcolls);
      }
      dout(25) << "_scan_list  " << poid << dendl;
    } else {
      dout(25) << "_scan_list  " << poid << " got " << r << ", skipping" << dendl;
    }
  }
}

// send scrub v2-compatible messages (classic scrub)
void PG::_request_scrub_map_classic(int replica, eversion_t version)
{
  assert(replica != osd->whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(info.pgid, version,
					      last_update_applied,
                                              get_osdmap()->get_epoch());
  osd->send_message_osd_cluster(replica, repscrubop, get_osdmap()->get_epoch());
}

// send scrub v3 messages (chunky scrub)
void PG::_request_scrub_map(int replica, eversion_t version,
                            hobject_t start, hobject_t end,
                            bool deep)
{
  assert(replica != osd->whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(info.pgid, version,
                                              get_osdmap()->get_epoch(),
                                              start, end, deep);
  osd->send_message_osd_cluster(replica, repscrubop, get_osdmap()->get_epoch());
}

void PG::sub_op_scrub_reserve(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_reserve" << dendl;

  if (scrubber.reserved) {
    dout(10) << "Ignoring reserve request: Already reserved" << dendl;
    return;
  }

  op->mark_started();

  scrubber.reserved = osd->inc_scrubs_pending();

  MOSDSubOpReply *reply = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  ::encode(scrubber.reserved, reply->get_data());
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::sub_op_scrub_reserve_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = (MOSDSubOpReply*)op->request;
  assert(reply->get_header().type == MSG_OSD_SUBOPREPLY);
  dout(7) << "sub_op_scrub_reserve_reply" << dendl;

  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  op->mark_started();

  int from = reply->get_source().num();
  bufferlist::iterator p = reply->get_data().begin();
  bool reserved;
  ::decode(reserved, p);

  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    if (reserved) {
      dout(10) << " osd." << from << " scrub reserve = success" << dendl;
      scrubber.reserved_peers.insert(from);
    } else {
      /* One decline stops this pg from being scheduled for scrubbing. */
      dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
      scrubber.reserve_failed = true;
    }
    sched_scrub();
  }
}

void PG::sub_op_scrub_unreserve(OpRequestRef op)
{
  assert(op->request->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_unreserve" << dendl;

  op->mark_started();

  clear_scrub_reserved();
}

void PG::sub_op_scrub_stop(OpRequestRef op)
{
  op->mark_started();

  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_stop" << dendl;

  // see comment in sub_op_scrub_reserve
  scrubber.reserved = false;

  MOSDSubOpReply *reply = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::clear_scrub_reserved()
{
  osd->scrub_wq.dequeue(this);
  scrubber.reserved_peers.clear();
  scrubber.reserve_failed = false;

  if (scrubber.reserved) {
    scrubber.reserved = false;
    osd->dec_scrubs_pending();
  }
}

void PG::scrub_reserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting reserve from osd." << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_RESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(acting[i], subop, get_osdmap()->get_epoch());
  }
}

void PG::scrub_unreserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting unreserve from osd." << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_UNRESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(acting[i], subop, get_osdmap()->get_epoch());
  }
}

/*
 * build a scrub map over a chunk without releasing the lock
 * only used by chunky scrub
 */
int PG::build_scrub_map_chunk(ScrubMap &map,
                              hobject_t start, hobject_t end, bool deep)
{
  dout(10) << "build_scrub_map" << dendl;
  dout(20) << "scrub_map_chunk [" << start << "," << end << ")" << dendl;

  map.valid_through = info.last_update;

  // objects
  vector<hobject_t> ls;
  int ret = osd->store->collection_list_range(coll, start, end, 0, &ls);
  if (ret < 0) {
    dout(5) << "collection_list_range error: " << ret << dendl;
    return ret;
  }

  _scan_list(map, ls, deep);

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  // log
  osd->store->read(coll_t(), log_oid, 0, 0, map.logbl);
  dout(10) << " done.  pg log is " << map.logbl.length() << " bytes" << dendl;

  return 0;
}

/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 * called while holding pg lock
 */ 
void PG::build_scrub_map(ScrubMap &map)
{
  dout(10) << "build_scrub_map" << dendl;

  map.valid_through = info.last_update;
  epoch_t epoch = info.history.same_interval_since;

  unlock();

  // wait for any writes on our pg to flush to disk first.  this avoids races
  // with scrub starting immediately after trim or recovery completion.
  osr->flush();

  // objects
  vector<hobject_t> ls;
  osd->store->collection_list(coll, ls);

  _scan_list(map, ls, false);
  lock();

  if (epoch != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    return;
  }


  dout(10) << "PG relocked, finalizing" << dendl;

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  // log
  osd->store->read(coll_t(), log_oid, 0, 0, map.logbl);
  dout(10) << " done.  pg log is " << map.logbl.length() << " bytes" << dendl;
}


/* 
 * build a summary of pg content changed starting after v
 * called while holding pg lock
 */
void PG::build_inc_scrub_map(ScrubMap &map, eversion_t v)
{
  map.valid_through = last_update_applied;
  map.incr_since = v;
  vector<hobject_t> ls;
  list<pg_log_entry_t>::iterator p;
  if (v == log.tail) {
    p = log.log.begin();
  } else if (v > log.tail) {
    p = log.find_entry(v);
    p++;
  } else {
    assert(0);
  }
  
  for (; p != log.log.end(); p++) {
    if (p->is_update()) {
      ls.push_back(p->soid);
      map.objects[p->soid].negative = false;
    } else if (p->is_delete()) {
      map.objects[p->soid].negative = true;
    }
  }

  _scan_list(map, ls, false);
  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  // log
  osd->store->read(coll_t(), log_oid, 0, 0, map.logbl);
}

void PG::repair_object(const hobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer)
{
  dout(10) << "repair_object " << soid << " bad_peer osd." << bad_peer << " ok_peer osd." << ok_peer << dendl;
  eversion_t v;
  bufferlist bv;
  bv.push_back(po->attrs[OI_ATTR]);
  object_info_t oi(bv);
  if (bad_peer != acting[0]) {
    peer_missing[bad_peer].add(soid, oi.version, eversion_t());
  } else {
    // We should only be scrubbing if the PG is clean.
    assert(waiting_for_missing_object.empty());

    missing.add(soid, oi.version, eversion_t());
    missing_loc[soid].insert(ok_peer);
    missing_loc_sources.insert(ok_peer);

    log.last_requested = 0;
  }
}

/* replica_scrub
 *
 * Classic behavior:
 *
 * If msg->scrub_from is not set, replica_scrub calls build_scrubmap to
 * build a complete map (with the pg lock dropped).
 *
 * If msg->scrub_from is set, replica_scrub sets scrubber.finalizing.
 * Similarly to scrub, if last_update_applied is behind info.last_update
 * replica_scrub returns to be requeued by sub_op_modify_applied.
 * replica_scrub then builds an incremental scrub map with the 
 * pg lock held.
 *
 * Chunky behavior:
 *
 * Wait for last_update_applied to match msg->scrub_to as above. Wait
 * for pushes to complete in case of recent recovery. Build a single
 * scrubmap of objects that are in the range [msg->start, msg->end).
 */
void PG::replica_scrub(MOSDRepScrub *msg)
{
  assert(!scrubber.active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_interval_since) {
    if (scrubber.finalizing) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrubber.finalizing = 0;
    } else {
      dout(10) << "replica_scrub discarding old replica_scrub from "
	       << msg->map_epoch << " < " << info.history.same_interval_since 
	       << dendl;
    }
    return;
  }

  ScrubMap map;

  if (msg->chunky) { // chunky scrub
    if (last_update_applied < msg->scrub_to) {
      dout(10) << "waiting for last_update_applied to catch up" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    if (active_pushes > 0) {
      dout(10) << "waiting for active pushes to finish" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    build_scrub_map_chunk(map, msg->start, msg->end, msg->deep);

  } else {
    if (msg->scrub_from > eversion_t()) {
      if (scrubber.finalizing) {
        assert(last_update_applied == info.last_update);
        assert(last_update_applied == msg->scrub_to);
      } else {
        scrubber.finalizing = 1;
        if (last_update_applied != msg->scrub_to) {
          scrubber.active_rep_scrub = msg;
          msg->get();
          return;
        }
      }
      build_inc_scrub_map(map, msg->scrub_from);
      scrubber.finalizing = 0;
    } else {
      build_scrub_map(map);
    }

    if (msg->map_epoch < info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      return;
    }
  }

  vector<OSDOp> scrub(1);
  scrub[0].op.op = CEPH_OSD_OP_SCRUB_MAP;
  hobject_t poid;
  eversion_t v;
  osd_reqid_t reqid;
  MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
				   msg->map_epoch, osd->get_tid(), v);
  ::encode(map, subop->get_data());
  subop->ops = scrub;

  osd->send_message_osd_cluster(subop, msg->get_connection());
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * scrub will be chunky if all OSDs in PG support chunky scrub
 * scrub will fall back to classic in any other case
 */
void PG::scrub()
{
  lock();
  if (deleting) {
    unlock();
    put();
    return;
  }

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_DEEP_SCRUB);
    update_stats();
    unlock();
    return;
  }

  // when we're starting a scrub, we need to determine which type of scrub to do
  if (!scrubber.active) {
    OSDMapRef curmap = osd->get_osdmap();
    scrubber.is_chunky = true;
    for (unsigned i=1; i<acting.size(); i++) {
      ConnectionRef con = osd->get_con_osd_cluster(acting[i], get_osdmap()->get_epoch());
      if (!con)
	continue;
      if (!(con->features & CEPH_FEATURE_CHUNKY_SCRUB)) {
        dout(20) << "OSD " << acting[i]
                 << " does not support chunky scrubs, falling back to classic"
                 << dendl;
        scrubber.is_chunky = false;
        break;
      }
    }

    if (scrubber.is_chunky) {
      scrubber.deep = state_test(PG_STATE_DEEP_SCRUB);
    } else {
      state_clear(PG_STATE_DEEP_SCRUB);
    }

    dout(10) << "starting a new " << (scrubber.is_chunky ? "chunky" : "classic") << " scrub" << dendl;
  }

  if (scrubber.is_chunky) {
    chunky_scrub();
  } else {
    classic_scrub();
  }

  unlock();
}

/*
 * Classic scrub is a two stage scrub: an initial scrub with writes enabled
 * followed by a finalize with writes blocked.
 *
 * A request is sent out to all replicas for initial scrub maps. Once they reply
 * (sub_op_scrub_map) writes are blocked for all objects in the PG.
 *
 * Finalize: Primaries and replicas wait for all writes in the log to be applied
 * (op_applied), then builds an incremental scrub of all the changes since the
 * beginning of the scrub.
 *
 * Once the primary has received all maps, it compares them and performs
 * repairs.
 *
 * The initial stage of the scrub is handled by scrub_wq and the final stage by
 * scrub_finalize_wq.
 *
 * Relevant variables:
 *
 * scrubber.waiting_on (int)
 * scrubber.waiting_on_whom
 *    Number of people who still need to build an initial/incremental scrub map.
 *    This is decremented in sub_op_scrub_map.
 *
 * last_update_applied
 *    The last update that's hit the disk. In the finalize stage, we block
 *    writes and wait for all writes to flush by checking:
 *
 *      last_update_appied == info.last_update
 *
 *    This is checked in op_applied.
 *
 *  scrubber.block_writes
 *    Flag to determine if writes are blocked.
 *
 *  finalizing scrub
 *    Flag set when we're in the finalize stage.
 *
 */
void PG::classic_scrub()
{
  if (!scrubber.active) {
    dout(10) << "scrub start" << dendl;
    scrubber.active = true;

    update_stats();
    scrubber.received_maps.clear();
    scrubber.epoch_start = info.history.same_interval_since;

    osd->inc_scrubs_active(scrubber.reserved);
    if (scrubber.reserved) {
      scrubber.reserved = false;
      scrubber.reserved_peers.clear();
    }

    /* scrubber.waiting_on == 0 iff all replicas have sent the requested maps and
     * the primary has done a final scrub (which in turn can only happen if
     * last_update_applied == info.last_update)
     */
    scrubber.waiting_on = acting.size();
    scrubber.waiting_on_whom.insert(acting.begin(), acting.end());

    // request maps from replicas
    for (unsigned i=1; i<acting.size(); i++) {
      _request_scrub_map_classic(acting[i], eversion_t());
    }

    // Unlocks and relocks...
    scrubber.primary_scrubmap = ScrubMap();
    build_scrub_map(scrubber.primary_scrubmap);

    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }

    --scrubber.waiting_on;
    scrubber.waiting_on_whom.erase(osd->whoami);

    if (scrubber.waiting_on == 0) {
      // the replicas have completed their scrub map, so lock out writes
      scrubber.block_writes = true;
    } else {
      dout(10) << "wait for replicas to build initial scrub map" << dendl;
      return;
    }

    if (last_update_applied != info.last_update) {
      dout(10) << "wait for cleanup" << dendl;
      return;
    }

    // fall through if last_update_applied == info.last_update and scrubber.waiting_on == 0

    // request incrementals from replicas
    scrub_gather_replica_maps();
    ++scrubber.waiting_on;
    scrubber.waiting_on_whom.insert(osd->whoami);
  }
    
  dout(10) << "clean up scrub" << dendl;
  assert(last_update_applied == info.last_update);

  scrubber.finalizing = true;

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    return;
  }
  
  if (scrubber.primary_scrubmap.valid_through != log.head) {
    ScrubMap incr;
    build_inc_scrub_map(incr, scrubber.primary_scrubmap.valid_through);
    scrubber.primary_scrubmap.merge_incr(incr);
  }
  
  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(osd->whoami);
  if (scrubber.waiting_on == 0) {
    assert(last_update_applied == info.last_update);
    osd->scrub_finalize_wq.queue(this);
  }
}

/*
 * Chunky scrub scrubs objects one chunk at a time with writes blocked for that
 * chunk.
 *
 * The object store is partitioned into chunks which end on hash boundaries. For
 * each chunk, the following logic is performed:
 *
 *  (1) Block writes on the chunk
 *  (2) Request maps from replicas
 *  (3) Wait for pushes to be applied (after recovery)
 *  (4) Wait for writes to flush on the chunk
 *  (5) Wait for maps from replicas
 *  (6) Compare / repair all scrub maps
 *
 * This logic is encoded in the very linear state machine:
 *
 *           +------------------+
 *  _________v__________        |
 * |                    |       |
 * |      INACTIVE      |       |
 * |____________________|       |
 *           |                  |
 *           |   +----------+   |
 *  _________v___v______    |   |
 * |                    |   |   |
 * |      NEW_CHUNK     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |     WAIT_PUSHES    |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |  WAIT_LAST_UPDATE  |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |      BUILD_MAP     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    WAIT_REPLICAS   |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    COMPARE_MAPS    |   |   |
 * |____________________|   |   |
 *           |   |          |   |
 *           |   +----------+   |
 *  _________v__________        |
 * |                    |       |
 * |       FINISH       |       |
 * |____________________|       |
 *           |                  |
 *           +------------------+
 *
 * The primary determines the last update from the subset by walking the log. If
 * it sees a log entry pertaining to a file in the chunk, it tells the replicas
 * to wait until that update is applied before building a scrub map. Both the
 * primary and replicas will wait for any active pushes to be applied.
 *
 * In contrast to classic_scrub, chunky_scrub is entirely handled by scrub_wq.
 *
 * scrubber.state encodes the current state of the scrub (refer to state diagram
 * for details).
 */
void PG::chunky_scrub() {
  // check for map changes
  if (scrubber.is_chunky_scrub_active()) {
    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }
  }

  bool done = false;
  int ret;

  while (!done) {
    dout(20) << "scrub state " << Scrubber::state_string(scrubber.state) << dendl;

    switch (scrubber.state) {
      case PG::Scrubber::INACTIVE:
        dout(10) << "scrub start" << dendl;

        update_stats();
        scrubber.epoch_start = info.history.same_interval_since;
        scrubber.active = true;

	osd->inc_scrubs_active(scrubber.reserved);
	if (scrubber.reserved) {
	  scrubber.reserved = false;
	  scrubber.reserved_peers.clear();
	}

        scrubber.start = hobject_t();
        scrubber.state = PG::Scrubber::NEW_CHUNK;

        break;

      case PG::Scrubber::NEW_CHUNK:
        scrubber.primary_scrubmap = ScrubMap();
        scrubber.received_maps.clear();

        {

          // get the start and end of our scrub chunk
          //
          // start and end need to lie on a hash boundary. We test for this by
          // requesting a list and searching backward from the end looking for a
          // boundary. If there's no boundary, we request a list after the first
          // list, and so forth.

          bool boundary_found = false;
          hobject_t start = scrubber.start;
          while (!boundary_found) {
            vector<hobject_t> objects;
            ret = osd->store->collection_list_partial(coll, start,
                                                      5, 5, 0,
                                                      &objects, &scrubber.end);
            assert(ret >= 0);

            // in case we don't find a boundary: start again at the end
            start = scrubber.end;

            // special case: reached end of file store, implicitly a boundary
            if (objects.size() == 0) {
              break;
            }

            // search backward from the end looking for a boundary
            objects.push_back(scrubber.end);
            while (!boundary_found && objects.size() > 1) {
              hobject_t end = objects.back().get_boundary();
              objects.pop_back();

              if (objects.back().get_filestore_key() != end.get_filestore_key()) {
                scrubber.end = end;
                boundary_found = true;
              }
            }
          }
        }

        scrubber.block_writes = true;

        // walk the log to find the latest update that affects our chunk
        scrubber.subset_last_update = eversion_t();
        for (list<pg_log_entry_t>::iterator p = log.log.begin();
             p != log.log.end();
             ++p) {
          if (p->soid >= scrubber.start && p->soid < scrubber.end)
            scrubber.subset_last_update = p->version;
        }

        // ask replicas to wait until last_update_applied >= scrubber.subset_last_update and then scan
        scrubber.waiting_on_whom.insert(osd->whoami);
        ++scrubber.waiting_on;

        // request maps from replicas
        for (unsigned i=1; i<acting.size(); i++) {
          _request_scrub_map(acting[i], scrubber.subset_last_update,
                             scrubber.start, scrubber.end, scrubber.deep);
          scrubber.waiting_on_whom.insert(acting[i]);
          ++scrubber.waiting_on;
        }

        scrubber.state = PG::Scrubber::WAIT_PUSHES;

        break;

      case PG::Scrubber::WAIT_PUSHES:
        if (active_pushes == 0) {
          scrubber.state = PG::Scrubber::WAIT_LAST_UPDATE;
        } else {
          dout(15) << "wait for pushes to apply" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::WAIT_LAST_UPDATE:
        if (last_update_applied >= scrubber.subset_last_update) {
          scrubber.state = PG::Scrubber::BUILD_MAP;
        } else {
          // will be requeued by op_applied
          dout(15) << "wait for writes to flush" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::BUILD_MAP:
        assert(last_update_applied >= scrubber.subset_last_update);

        // build my own scrub map
        ret = build_scrub_map_chunk(scrubber.primary_scrubmap,
                                    scrubber.start, scrubber.end,
                                    scrubber.deep);
        if (ret < 0) {
          dout(5) << "error building scrub map: " << ret << ", aborting" << dendl;
          scrub_clear_state();
          scrub_unreserve_replicas();
          return;
        }

        --scrubber.waiting_on;
        scrubber.waiting_on_whom.erase(osd->whoami);

        scrubber.state = PG::Scrubber::WAIT_REPLICAS;
        break;

      case PG::Scrubber::WAIT_REPLICAS:
        if (scrubber.waiting_on > 0) {
          // will be requeued by sub_op_scrub_map
          dout(10) << "wait for replicas to build scrub map" << dendl;
          done = true;
        } else {
          scrubber.state = PG::Scrubber::COMPARE_MAPS;
        }
        break;

      case PG::Scrubber::COMPARE_MAPS:
        assert(last_update_applied >= scrubber.subset_last_update);
        assert(scrubber.waiting_on == 0);

        scrub_compare_maps();
        scrubber.block_writes = false;
	scrubber.run_callbacks();

        // requeue the writes from the chunk that just finished
        requeue_ops(waiting_for_active);

        if (scrubber.end < hobject_t::get_max()) {
          // schedule another leg of the scrub
          scrubber.start = scrubber.end;

          scrubber.state = PG::Scrubber::NEW_CHUNK;
          osd->scrub_wq.queue(this);
          done = true;
        } else {
          scrubber.state = PG::Scrubber::FINISH;
        }

        break;

      case PG::Scrubber::FINISH:
        scrub_finish();
        scrubber.state = PG::Scrubber::INACTIVE;
        done = true;

        break;

      default:
        assert(0);
    }
  }
}

void PG::scrub_clear_state()
{
  assert(_lock.is_locked());
  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);
  update_stats();

  // active -> nothing.
  if (scrubber.active)
    osd->dec_scrubs_active();

  requeue_ops(waiting_for_active);

  if (scrubber.queue_snap_trim) {
    dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
    queue_snap_trim();
  }

  scrubber.reset();

  // type-specific state clear
  _scrub_clear_state();
}

bool PG::scrub_gather_replica_maps() {
  assert(scrubber.waiting_on == 0);
  assert(_lock.is_locked());

  for (map<int,ScrubMap>::iterator p = scrubber.received_maps.begin();
       p != scrubber.received_maps.end();
       p++) {
    
    if (scrubber.received_maps[p->first].valid_through != log.head) {
      scrubber.waiting_on++;
      scrubber.waiting_on_whom.insert(p->first);
      // Need to request another incremental map
      _request_scrub_map_classic(p->first, p->second.valid_through);
    }
  }
  
  if (scrubber.waiting_on > 0) {
    return false;
  } else {
    return true;
  }
}

bool PG::_compare_scrub_objects(ScrubMap::object &auth,
				ScrubMap::object &candidate,
				ostream &errorstream)
{
  bool ok = true;
  if (auth.size != candidate.size) {
    ok = false;
    errorstream << "size " << candidate.size 
		<< " != known size " << auth.size;
  }
  if (auth.digest_present && candidate.digest_present) {
    if (auth.digest != candidate.digest) {
      if (!ok)
        errorstream << ", ";
      ok = false;

      errorstream << "digest " << candidate.digest
                  << " != known digest " << auth.digest;
    }
  }
  if (auth.omap_digest_present && candidate.omap_digest_present) {
    if (auth.omap_digest != candidate.omap_digest) {
      if (!ok)
        errorstream << ", ";
      ok = false;

      errorstream << "omap_digest " << candidate.omap_digest
                  << " != known omap_digest " << auth.omap_digest;
    }
  }
  for (map<string,bufferptr>::const_iterator i = auth.attrs.begin();
       i != auth.attrs.end();
       i++) {
    if (!candidate.attrs.count(i->first)) {
      if (!ok)
	errorstream << ", ";
      ok = false;
      errorstream << "missing attr " << i->first;
    } else if (candidate.attrs.find(i->first)->second.cmp(i->second)) {
      if (!ok)
	errorstream << ", ";
      ok = false;
      errorstream << "attr value mismatch " << i->first;
    }
  }
  for (map<string,bufferptr>::const_iterator i = candidate.attrs.begin();
       i != candidate.attrs.end();
       i++) {
    if (!auth.attrs.count(i->first)) {
      if (!ok)
	errorstream << ", ";
      ok = false;
      errorstream << "extra attr " << i->first;
    }
  }
  return ok;
}



map<int, ScrubMap *>::const_iterator PG::_select_auth_object(
  const hobject_t &obj,
  const map<int,ScrubMap*> &maps)
{
  map<int, ScrubMap *>::const_iterator auth = maps.end();
  // Select first present replica
  for (map<int, ScrubMap *>::const_iterator j = maps.begin();
       j != maps.end();
       ++j) {
    if (!j->second->objects.count(obj)) {
      continue;
    }
    if (auth == maps.end()) {
      auth = j;
      continue;
    }
  }
  return auth;
}

void PG::_compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			    map<hobject_t, set<int> > &missing,
			    map<hobject_t, set<int> > &inconsistent,
			    map<hobject_t, int> &authoritative,
			    map<hobject_t, set<int> > &invalid_snapcolls,
			    ostream &errorstream)
{
  map<hobject_t,ScrubMap::object>::const_iterator i;
  map<int, ScrubMap *>::const_iterator j;
  set<hobject_t> master_set;

  // Construct master set
  for (j = maps.begin(); j != maps.end(); j++) {
    for (i = j->second->objects.begin(); i != j->second->objects.end(); i++) {
      master_set.insert(i->first);
    }
  }

  // Check maps against master set and each other
  for (set<hobject_t>::const_iterator k = master_set.begin();
       k != master_set.end();
       k++) {
    map<int, ScrubMap *>::const_iterator auth = _select_auth_object(*k, maps);
    assert(auth != maps.end());
    set<int> cur_missing;
    set<int> cur_inconsistent;
    for (j = maps.begin(); j != maps.end(); j++) {
      if (j->second->objects.count(*k)) {
	// Check snapcolls
	if (k->snap < CEPH_MAXSNAP) {
	  if (_report_snap_collection_errors(
		*k,
		acting[j->first],
		j->second->objects[*k].attrs,
		j->second->objects[*k].snapcolls,
		j->second->objects[*k].nlinks,
		errorstream)) {
	    invalid_snapcolls[*k].insert(j->first);
	  }
	}
	// Compare
	stringstream ss;
	if (!_compare_scrub_objects(auth->second->objects[*k],
	    j->second->objects[*k],
	    ss)) {
	  cur_inconsistent.insert(j->first);
	  ++scrubber.errors;
	  errorstream << info.pgid << " osd." << acting[j->first]
		      << ": soid " << *k << " " << ss.str() << std::endl;
	}
      } else {
	cur_missing.insert(j->first);
	++scrubber.errors;
	errorstream << info.pgid
		    << " osd." << acting[j->first] 
		    << " missing " << *k << std::endl;
      }
    }
    assert(auth != maps.end());
    if (cur_missing.size()) {
      missing[*k] = cur_missing;
    }
    if (cur_inconsistent.size()) {
      inconsistent[*k] = cur_inconsistent;
    }
    if (cur_inconsistent.size() || cur_missing.size()) {
      authoritative[*k] = auth->first;
    }
  }
}

void PG::scrub_compare_maps() {
  dout(10) << "scrub_compare_maps has maps, analyzing" << dendl;
  if (acting.size() > 1) {
    dout(10) << "scrub  comparing replica scrub maps" << dendl;

    stringstream ss;

    // Map from object with errors to good peer
    map<hobject_t, int> authoritative;
    map<int,ScrubMap *> maps;

    dout(2) << "scrub   osd." << acting[0] << " has " 
	    << scrubber.primary_scrubmap.objects.size() << " items" << dendl;
    maps[0] = &scrubber.primary_scrubmap;
    for (unsigned i=1; i<acting.size(); i++) {
      dout(2) << "scrub   osd." << acting[i] << " has " 
	      << scrubber.received_maps[acting[i]].objects.size() << " items" << dendl;
      maps[i] = &scrubber.received_maps[acting[i]];
    }

    _compare_scrubmaps(
      maps,
      scrubber.missing,
      scrubber.inconsistent,
      authoritative,
      scrubber.inconsistent_snapcolls,
      ss);
    dout(2) << ss.str() << dendl;

    if (authoritative.size() || scrubber.inconsistent_snapcolls.size()) {
      osd->clog.error(ss);
    }

    for (map<hobject_t, int>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      scrubber.authoritative.insert(
	make_pair(
	  i->first,
	  make_pair(maps[i->second]->objects[i->first], i->second)));
    }
  }

  // ok, do the pg-type specific scrubbing
  _scrub(scrubber.primary_scrubmap);
}

void PG::scrub_process_inconsistent() {
  dout(10) << "process_inconsistent() checking authoritative" << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  if (scrubber.authoritative.size() || scrubber.inconsistent.size()) {
    stringstream ss;
    for (map<hobject_t, set<int> >::iterator obj =
	   scrubber.inconsistent_snapcolls.begin();
	 obj != scrubber.inconsistent_snapcolls.end();
	 ++obj) {
      for (set<int>::iterator j = obj->second.begin();
	   j != obj->second.end();
	   ++j) {
	++scrubber.errors;
	ss << info.pgid << " " << mode << " " << " object " << obj->first
	   << " has inconsistent snapcolls on " << *j << std::endl;
      }
    }

    ss << info.pgid << " " << mode << " " << scrubber.missing.size() << " missing, "
       << scrubber.inconsistent.size() << " inconsistent objects\n";
    dout(2) << ss.str() << dendl;
    osd->clog.error(ss);
    if (repair) {
      state_clear(PG_STATE_CLEAN);
      scrub_after_recovery = true;
      for (map<hobject_t, pair<ScrubMap::object, int> >::iterator i =
	     scrubber.authoritative.begin();
	   i != scrubber.authoritative.end();
	   i++) {
	set<int>::iterator j;
	
	if (scrubber.missing.count(i->first)) {
	  for (j = scrubber.missing[i->first].begin();
	       j != scrubber.missing[i->first].end(); 
	       j++) {
	    repair_object(i->first, 
	      &(i->second.first),
	      acting[*j],
	      acting[i->second.second]);
	    ++scrubber.fixed;
	  }
	}
	if (scrubber.inconsistent.count(i->first)) {
	  for (j = scrubber.inconsistent[i->first].begin(); 
	       j != scrubber.inconsistent[i->first].end(); 
	       j++) {
	    repair_object(i->first, 
	      &(i->second.first),
	      acting[*j],
	      acting[i->second.second]);
	    ++scrubber.fixed;
	  }
	}
      }
    }
  }
}

void PG::scrub_finalize() {
  lock();
  if (deleting) {
    unlock();
    return;
  }

  assert(last_update_applied == info.last_update);

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    unlock();
    return;
  }

  if (!scrub_gather_replica_maps()) {
    dout(10) << "maps not yet up to date, sent out new requests" << dendl;
    unlock();
    return;
  }

  scrub_compare_maps();

  scrub_finish();

  dout(10) << "scrub done" << dendl;
  unlock();
}

// the part that actually finalizes a scrub
void PG::scrub_finish() {
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // type-specific finish (can tally more errors)
  _scrub_finish();

  scrub_process_inconsistent();

  {
    stringstream oss;
    oss << info.pgid << " " << mode << " ";
    if (scrubber.errors)
      oss << scrubber.errors << " errors";
    else
      oss << "ok";
    if (repair)
      oss << ", " << scrubber.fixed << " fixed";
    oss << "\n";
    if (scrubber.errors)
      osd->clog.error(oss);
    else
      osd->clog.info(oss);
  }

  // finish up
  unreg_next_scrub();
  utime_t now = ceph_clock_now(g_ceph_context);
  info.history.last_scrub = info.last_update;
  info.history.last_scrub_stamp = now;
  if (scrubber.deep) {
    info.history.last_deep_scrub = info.last_update;
    info.history.last_deep_scrub_stamp = now;
  }
  if (scrubber.errors == 0)
    info.history.last_clean_scrub_stamp = now;
  info.stats.stats.sum.num_scrub_errors = scrubber.errors;
  reg_next_scrub();

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_info(*t);
    int tr = osd->store->queue_transaction(osr.get(), t);
    assert(tr == 0);
  }


  if (scrubber.fixed) {
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  DoRecovery())));
  }

  scrub_clear_state();
  scrub_unreserve_replicas();

  if (is_active() && is_primary()) {
    share_pg_info();
  }
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new pg_info_t with replicas
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_info.count(i)) {
      peer_info[i].last_epoch_started = info.last_epoch_started;
      peer_info[i].history.merge(info.history);
    }
    MOSDPGInfo *m = new MOSDPGInfo(get_osdmap()->get_epoch());
    m->pg_list.push_back(
      make_pair(
	pg_notify_t(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  info),
	pg_interval_map_t()));
    osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
  }
}

/*
 * Share a new segment of this PG's log with some replicas, after PG is active.
 *
 * Updates peer_missing and peer_info.
 */
void PG::share_pg_log()
{
  dout(10) << __func__ << dendl;
  assert(is_primary());

  vector<int>::const_iterator a = acting.begin();
  assert(a != acting.end());
  vector<int>::const_iterator end = acting.end();
  while (++a != end) {
    int peer(*a);
    pg_missing_t& pmissing(peer_missing[peer]);
    pg_info_t& pinfo(peer_info[peer]);

    MOSDPGLog *m = new MOSDPGLog(info.last_update.epoch, info);
    m->log.copy_after(log, pinfo.last_update);

    for (list<pg_log_entry_t>::const_iterator i = m->log.log.begin();
	 i != m->log.log.end();
	 ++i) {
      pmissing.add_next_event(*i);
    }
    pinfo.last_update = m->log.head;

    osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
  }
}

void PG::update_history_from_master(pg_history_t new_history)
{
  unreg_next_scrub();
  info.history.merge(new_history);
  reg_next_scrub();
}

void PG::fulfill_info(int from, const pg_query_t &query, 
		      pair<int, pg_info_t> &notify_info)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type == pg_query_t::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(int from, const pg_query_t &query, epoch_t query_epoch)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type != pg_query_t::INFO);

  MOSDPGLog *mlog = new MOSDPGLog(get_osdmap()->get_epoch(),
				  info, query_epoch);
  mlog->missing = missing;

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    dout(10) << " sending info+missing+log since " << query.since
	     << dendl;
    if (query.since != eversion_t() && query.since < log.tail) {
      osd->clog.error() << info.pgid << " got broken pg_query_t::LOG since " << query.since
			<< " when my log.tail is " << log.tail
			<< ", sending full log instead\n";
      mlog->log = log;           // primary should not have requested this!!
    } else
      mlog->log.copy_after(log, query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    dout(10) << " sending info+missing+full log" << dendl;
    mlog->log = log;
  }

  dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  ConnectionRef con = osd->get_con_osd_cluster(from, get_osdmap()->get_epoch());
  if (con) {
    osd->osd->_share_map_outgoing(from, con.get(), get_osdmap());
    osd->send_message_osd_cluster(mlog, con.get());
  } else {
    mlog->put();
  }
}


// true if all OSDs in prior intervals may have crashed, and we need to replay
// false positives are okay, false negatives are not.
bool PG::may_need_replay(const OSDMapRef osdmap) const
{
  bool crashed = false;

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       p++) {
    const pg_interval_t &interval = p->second;
    dout(10) << "may_need_replay " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at whether any of the osds during this interval survived
    // past the end of the interval (i.e., didn't crash and
    // potentially fail to COMMIT a write that it ACKed).
    bool any_survived_interval = false;

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];

      const osd_info_t *pinfo = 0;
      if (osdmap->exists(o))
	pinfo = &osdmap->get_info(o);

      // does this osd appear to have survived through the end of the
      // interval?
      if (pinfo) {
	if (pinfo->up_from <= interval.first && pinfo->up_thru > interval.last) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " up_thru " << pinfo->up_thru
		   << " survived the interval" << dendl;
	  any_survived_interval = true;
	}
	else if (pinfo->up_from <= interval.first &&
		 (std::find(acting.begin(), acting.end(), o) != acting.end() ||
		  std::find(up.begin(), up.end(), o) != up.end())) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " and is in acting|up,"
		   << " assumed to have survived the interval" << dendl;
	  // (if it hasn't, we will rebuild PriorSet)
	  any_survived_interval = true;
	}
	else if (pinfo->up_from > interval.last &&
		 pinfo->last_clean_begin <= interval.first &&
		 pinfo->last_clean_end > interval.last) {
	  dout(10) << "may_need_replay  prior osd." << o
		   << " up_from " << pinfo->up_from
		   << " and last clean interval ["
		   << pinfo->last_clean_begin << "," << pinfo->last_clean_end
		   << ") survived the interval" << dendl;
	  any_survived_interval = true;
	}
      }
    }

    if (!any_survived_interval) {
      dout(3) << "may_need_replay  no known survivors of interval "
	      << interval.first << "-" << interval.last
	      << ", may need replay" << dendl;
      crashed = true;
      break;
    }
  }

  return crashed;
}

bool PG::is_split(OSDMapRef lastmap, OSDMapRef nextmap)
{
  return info.pgid.is_split(
    lastmap->get_pg_num(pool.id),
    nextmap->get_pg_num(pool.id),
    0);
}

bool PG::acting_up_affected(const vector<int>& newup, const vector<int>& newacting)
{
  if (acting != newacting || up != newup) {
    dout(20) << "acting_up_affected newup " << newup << " newacting " << newacting << dendl;
    return true;
  } else {
    return false;
  }
}

bool PG::old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch)
{
  if (last_peering_reset > reply_epoch ||
      last_peering_reset > query_epoch) {
    dout(10) << "old_peering_msg reply_epoch " << reply_epoch << " query_epoch " << query_epoch
	     << " last_peering_reset " << last_peering_reset
	     << dendl;
    return true;
  }
  return false;
}

void PG::set_last_peering_reset()
{
  dout(20) << "set_last_peering_reset " << get_osdmap()->get_epoch() << dendl;
  last_peering_reset = get_osdmap()->get_epoch();
}

struct FlushState {
  PG *pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {
    pg->get();
  }
  ~FlushState() {
    pg->lock();
    pg->queue_flushed(epoch);
    pg->unlock();
    pg->put();
  }
};
typedef std::tr1::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush(ObjectStore::Transaction *t,
		     list<Context *> *on_applied,
		     list<Context *> *on_safe)
{
  // flush in progress ops
  FlushStateRef flush_trigger(
    new FlushState(this, get_osdmap()->get_epoch()));
  t->nop();
  flushed = false;
  on_applied->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
  on_safe->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
  DeletingStateRef del = osd->deleting_pgs.lookup(info.pgid);
  if (del)
    del->register_on_delete(new ContainerContext<FlushStateRef>(flush_trigger));
}

/* Called before initializing peering during advance_map */
void PG::start_peering_interval(const OSDMapRef lastmap,
				const vector<int>& newup,
				const vector<int>& newacting)
{
  const OSDMapRef osdmap = get_osdmap();

  // -- there was a change! --
  kick();

  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();
  int oldprimary = get_primary();
  acting.swap(oldacting);
  up.swap(oldup);

  up = newup;
  acting = newacting;
  want_acting.clear();

  if (info.stats.up != up ||
      info.stats.acting != acting) {
    info.stats.up = up;
    info.stats.acting = acting;
    info.stats.mapping_epoch = info.history.same_interval_since;
  }

  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  int role = osdmap->calc_pg_role(osd->whoami, acting, acting.size());
  set_role(role);

  // did acting, up, primary|acker change?
  if (!lastmap) {
    dout(10) << " no lastmap" << dendl;
    dirty_info = true;
  } else {
    bool new_interval = pg_interval_t::check_new_interval(
      oldacting, newacting,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap, info.pgid.pool(), info.pgid, &past_intervals);
    if (new_interval) {
      dout(10) << " noting past " << past_intervals.rbegin()->second << dendl;
      dirty_info = true;
    }
  }

  if (oldacting != acting || oldup != up || is_split(lastmap, osdmap)) {
    info.history.same_interval_since = osdmap->get_epoch();
  }
  if (oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  if (oldprimary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  dout(10) << " up " << oldup << " -> " << up 
	   << ", acting " << oldacting << " -> " << acting 
	   << ", role " << oldrole << " -> " << role << dendl; 

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERING);

  peer_missing.clear();
  peer_purged.clear();

  // reset primary state?
  if (oldrole == 0 || get_role() == 0)
    clear_primary_state();

    
  // pg->on_*
  on_change();

  assert(!deleting);

  if (role != oldrole) {
    // old primary?
    if (oldrole == 0) {
      state_clear(PG_STATE_CLEAN);
      clear_stats();
	
      // take replay queue waiters
      list<OpRequestRef> ls;
      for (map<eversion_t,OpRequestRef>::iterator it = replay_queue.begin();
	   it != replay_queue.end();
	   it++)
	ls.push_back(it->second);
      replay_queue.clear();
      requeue_ops(ls);
    }

    on_role_change();

    // take active waiters
    requeue_ops(waiting_for_active);

    // should we tell the primary we are here?
    send_notify = (role != 0);
      
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != oldprimary) {    
      // we need to announce
      send_notify = true;
        
      dout(10) << *this << " " << oldacting << " -> " << acting 
	       << ", acting primary " 
	       << oldprimary << " -> " << get_primary() 
	       << dendl;
    } else {
      // primary is the same.
      if (role == 0) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);
	  
	dout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }
  // make sure we clear out any pg_temp change requests
  osd->remove_want_pg_temp(info.pgid);
  cancel_recovery();

  if (acting.empty() && up.size() && up[0] == osd->whoami) {
    dout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    osd->queue_want_pg_temp(info.pgid, acting);
  }
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  assert(!is_primary());

  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();

  // Handle changes to purged_snaps ONLY IF we have caught up
  if (last_complete_ondisk.epoch >= info.history.last_epoch_started) {
    interval_set<snapid_t> p;
    p.union_of(oinfo.purged_snaps, info.purged_snaps);
    p.subtract(info.purged_snaps);
    info.purged_snaps = oinfo.purged_snaps;
    if (!p.empty()) {
      dout(10) << " purged_snaps " << info.purged_snaps
	       << " -> " << oinfo.purged_snaps
	       << " removed " << p << dendl;
      adjust_local_snaps();
    }
    dirty_info = true;
  }
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info
      << " " << pg.up;
  if (pg.acting != pg.up)
    out << "/" << pg.acting;
  out << " r=" << pg.get_role();
  out << " lpr=" << pg.get_last_peering_reset();

  if (pg.past_intervals.size()) {
    out << " pi=" << pg.past_intervals.begin()->first << "-" << pg.past_intervals.rbegin()->second.last
	<< "/" << pg.past_intervals.size();
  }

  if (pg.is_active() &&
      pg.last_update_ondisk != pg.info.last_update)
    out << " luod=" << pg.last_update_ondisk;

  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  if (pg.log.tail != pg.info.log_tail ||
      pg.log.head != pg.info.last_update)
    out << " (info mismatch, " << pg.log << ")";

  if (pg.log.empty()) {
    // shoudl it be?
    if (pg.log.head.version - pg.log.tail.version != 0) {
      out << " (log bound mismatch, empty)";
    }
  } else {
    if ((pg.log.log.begin()->version <= pg.log.tail) || // sloppy check
        (pg.log.log.rbegin()->version != pg.log.head && !(pg.log.head == pg.log.tail))) {
      out << " (log bound mismatch, actual=["
	  << pg.log.log.begin()->version << ","
	  << pg.log.log.rbegin()->version << "]";
      //out << "len=" << pg.log.log.size();
      out << ")";
    }
  }

  if (pg.get_backfill_target() >= 0)
    out << " bft=" << pg.get_backfill_target();

  if (pg.last_complete_ondisk != pg.info.last_complete)
    out << " lcod " << pg.last_complete_ondisk;

  if (pg.get_role() == 0) {
    out << " mlcod " << pg.min_last_complete_ondisk;
  }

  out << " " << pg_state_string(pg.get_state());
  if (pg.should_send_notify())
    out << " NOTIFY";

  if (pg.scrubber.must_repair)
    out << " MUST_REPAIR";
  if (pg.scrubber.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (pg.scrubber.must_scrub)
    out << " MUST_SCRUB";

  //out << " (" << pg.log.tail << "," << pg.log.head << "]";
  if (pg.missing.num_missing()) {
    out << " m=" << pg.missing.num_missing();
    if (pg.is_primary()) {
      int unfound = pg.get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (pg.snap_trimq.size())
    out << " snaptrimq=" << pg.snap_trimq;

  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;
  if (OSD::op_is_discardable(m)) {
    return true;
  } else if (op->may_write() &&
	     (!is_primary() ||
	      !same_for_modify_since(m->get_map_epoch()))) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (op->may_read() &&
	     !same_for_read_since(m->get_map_epoch())) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (is_replay()) {
    if (m->get_version().version > 0) {
      dout(7) << " queueing replay at " << m->get_version()
	      << " for " << *m << dendl;
      replay_queue[m->get_version()] = op;
      op->mark_delayed("waiting for replay");
      return true;
    }
  }
  return false;
}

bool PG::can_discard_subop(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp *)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (old_peering_msg(m->map_epoch, m->map_epoch)) {
    dout(10) << "handle_sub_op pg changed " << info.history
	     << " after " << m->map_epoch
	     << ", dropping" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_scan(OpRequestRef op)
{
  MOSDPGScan *m = (MOSDPGScan *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = (MOSDPGBackfill *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old backfill, ignoring" << dendl;
    return true;
  }

  return false;

}

bool PG::can_discard_request(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  case MSG_OSD_SUBOP:
    return can_discard_subop(op);
  case MSG_OSD_SUBOPREPLY:
    return false;
  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);

  case MSG_OSD_PG_BACKFILL:
    return can_discard_backfill(op);
  }
  return true;
}

bool PG::split_request(OpRequestRef op, unsigned match, unsigned bits)
{
  unsigned mask = ~((~0)<<bits);
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return (static_cast<MOSDOp*>(op->request)->get_pg().m_seed & mask) == match;
  case MSG_OSD_SUBOP:
    return false;
  case MSG_OSD_SUBOPREPLY:
    return false;
  case MSG_OSD_PG_SCAN:
    return false;
  case MSG_OSD_PG_BACKFILL:
    return false;
  }
  return false;
}

bool PG::must_delay_request(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !require_same_or_newer_map(
      static_cast<MOSDOp*>(op->request)->get_map_epoch());

  case MSG_OSD_SUBOP:
    return !require_same_or_newer_map(
      static_cast<MOSDSubOp*>(op->request)->map_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !require_same_or_newer_map(
      static_cast<MOSDSubOpReply*>(op->request)->map_epoch);

  case MSG_OSD_PG_SCAN:
    return !require_same_or_newer_map(
      static_cast<MOSDPGScan*>(op->request)->map_epoch);

  case MSG_OSD_PG_BACKFILL:
    return !require_same_or_newer_map(
      static_cast<MOSDPGBackfill*>(op->request)->map_epoch);
  }
  assert(0);
  return false;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  requeue_ops(waiting_for_map);
  for (list<CephPeeringEvtRef>::iterator i = peering_waiters.begin();
       i != peering_waiters.end();
       ++i) osd->queue_for_peering(this);
  peering_queue.splice(peering_queue.begin(), peering_waiters,
		       peering_waiters.begin(), peering_waiters.end());
}

void PG::handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx)
{
  dout(10) << "handle_peering_event: " << evt->get_desc() << dendl;
  if (!require_same_or_newer_map(evt->get_epoch_sent())) {
    dout(10) << "deferring event " << evt->get_desc() << dendl;
    peering_waiters.push_back(evt);
    return;
  }
  if (old_peering_evt(evt))
    return;
  recovery_state.handle_event(evt, rctx);
}

void PG::queue_peering_event(CephPeeringEvtRef evt)
{
  if (old_peering_evt(evt))
    return;
  peering_queue.push_back(evt);
  osd->queue_for_peering(this);
}

void PG::queue_notify(epoch_t msg_epoch,
		      epoch_t query_epoch,
		      int from, pg_notify_t& i)
{
  dout(10) << "notify " << i << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MNotifyRec(from, i))));
}

void PG::queue_info(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     int from, pg_info_t& i)
{
  dout(10) << "info " << i << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MInfoRec(from, i, msg_epoch))));
}

void PG::queue_log(epoch_t msg_epoch,
		   epoch_t query_epoch,
		   int from,
		   MOSDPGLog *msg)
{
  dout(10) << "log " << *msg << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MLogRec(from, msg))));
}

void PG::queue_null(epoch_t msg_epoch,
		    epoch_t query_epoch)
{
  dout(10) << "null" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 NullEvt())));
}

void PG::queue_flushed(epoch_t e)
{
  dout(10) << "flushed" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(e, e,
					 FlushedEvt())));
}

void PG::queue_query(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     int from, const pg_query_t& q)
{
  dout(10) << "handle_query " << q << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MQuery(from, q, query_epoch))));
}

void PG::handle_advance_map(OSDMapRef osdmap, OSDMapRef lastmap,
			    vector<int>& newup, vector<int>& newacting,
			    RecoveryCtx *rctx)
{
  assert(osdmap->get_epoch() == (lastmap->get_epoch() + 1));
  assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map " << newup << "/" << newacting << dendl;
  osdmap_ref = osdmap;
  pool.update(osdmap);
  AdvMap evt(osdmap, lastmap, newup, newacting);
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  ActMap evt;
  recovery_state.handle_event(evt, rctx);
  dirty_info = true;
}

void PG::handle_loaded(RecoveryCtx *rctx)
{
  dout(10) << "handle_loaded" << dendl;
  Load evt;
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_create(RecoveryCtx *rctx)
{
  dout(10) << "handle_create" << dendl;
  Initialize evt;
  recovery_state.handle_event(evt, rctx);
  ActMap evt2;
  recovery_state.handle_event(evt2, rctx);
}

void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  QueryState q(f);
  recovery_state.handle_event(q, 0);
}



std::ostream& operator<<(std::ostream& oss,
			 const struct PG::PriorSet &prior)
{
  oss << "PriorSet[probe=" << prior.probe << " "
      << "down=" << prior.down << " "
      << "blocked_by=" << prior.blocked_by << "]";
  return oss;
}

/*---------------------------------------------------*/
// Handle staitc function so it can use dout()
#undef dout_prefix
#define dout_prefix if (passedpg) _prefix(_dout, passedpg)

void PG::read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
  const pg_info_t &info, OndiskLog &ondisklog, IndexedLog &log,
  pg_missing_t &missing, ostringstream &oss, const PG *passedpg)
{
  // load bounds
  ondisklog.tail = ondisklog.head = 0;

  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  bufferlist::iterator p = blb.begin();
  ::decode(ondisklog, p);

  dout(10) << "read_log " << ondisklog.tail << "~" << ondisklog.length() << dendl;

  log.tail = info.log_tail;

  // In case of sobject_t based encoding, may need to list objects in the store
  // to find hashes
  bool listed_collection = false;
  vector<hobject_t> ls;
  
  if (ondisklog.head > 0) {
    // read
    bufferlist bl;
    store->read(coll_t::META_COLL, log_oid, ondisklog.tail, ondisklog.length(), bl);
    if (bl.length() < ondisklog.length()) {
      std::ostringstream oss;
      oss << "read_log got " << bl.length() << " bytes, expected "
	  << ondisklog.head << "-" << ondisklog.tail << "="
	  << ondisklog.length();
      throw read_log_error(oss.str().c_str());
    }
    
    pg_log_entry_t e;
    bufferlist::iterator p = bl.begin();
    assert(log.empty());
    eversion_t last;
    bool reorder = false;
    while (!p.end()) {
      uint64_t pos = ondisklog.tail + p.get_off();
      if (ondisklog.has_checksums) {
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

      if (e.invalid_hash) {
	// We need to find the object in the store to get the hash
	if (!listed_collection) {
	  store->collection_list(coll, ls);
	  listed_collection = true;
	}
	bool found = false;
	for (vector<hobject_t>::iterator i = ls.begin();
	     i != ls.end();
	     ++i) {
	  if (i->oid == e.soid.oid && i->snap == e.soid.snap) {
	    e.soid = *i;
	    found = true;
	    break;
	  }
	}
	if (!found) {
	  // Didn't find the correct hash
	  std::ostringstream oss;
	  oss << "Could not find hash for hoid " << e.soid << std::endl;
	  throw read_log_error(oss.str().c_str());
	}
      }

      if (e.invalid_pool) {
	e.soid.pool = info.pgid.pool();
      }

      e.offset = pos;
      uint64_t endpos = ondisklog.tail + p.get_off();
      log.log.push_back(e);
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	oss << info.pgid << " log has extra data at "
	   << endpos << "~" << (ondisklog.head-endpos) << " after "
	   << info.last_update << "\n";

	dout(0) << "read_log " << endpos << " *** extra gunk at end of log, "
	        << "adjusting ondisklog.head" << dendl;
	ondisklog.head = endpos;
	break;
      }
    }
  
    if (reorder) {
      dout(0) << "read_log reordering log" << dendl;
      map<eversion_t, pg_log_entry_t> m;
      for (list<pg_log_entry_t>::iterator p = log.log.begin(); p != log.log.end(); p++)
	m[p->version] = *p;
      log.log.clear();
      for (map<eversion_t, pg_log_entry_t>::iterator p = m.begin(); p != m.end(); p++)
	log.log.push_back(p->second);
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
	 i++) {
      if (i->version <= info.last_complete) break;
      if (did.count(i->soid)) continue;
      did.insert(i->soid);
      
      if (i->is_delete()) continue;
      
      bufferlist bv;
      int r = store->getattr(coll, i->soid, OI_ATTR, bv);
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
	   ondisklog.divergent_priors.rbegin();
	 i != ondisklog.divergent_priors.rend();
	 ++i) {
      if (i->first <= info.last_complete) break;
      if (did.count(i->second)) continue;
      did.insert(i->second);
      bufferlist bv;
      int r = store->getattr(coll, i->second, OI_ATTR, bv);
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
}

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
PG::RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx)
{
  state_name = "Crashed";
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
PG::RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx)
{
  state_name = "Initial";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::Initial::react(const Load& l)
{
  PG *pg = context< RecoveryMachine >().pg;

  // do we tell someone we're here?
  pg->send_notify = (pg->role != 0);

  return transit< Reset >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MNotifyRec& notify)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(notify.from, notify.notify.info);
  pg->update_heartbeat_peers();
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MInfoRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MLogRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PG::RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Started-------*/
PG::RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}

boost::statechart::result
PG::RecoveryState::Started::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->flushed = true;
  pg->requeue_ops(pg->waiting_for_active);
  return discard_event();
}


boost::statechart::result PG::RecoveryState::Started::react(const AdvMap& advmap)
{
  dout(10) << "Started advmap" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->acting_up_affected(advmap.newup, advmap.newacting) ||
    pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, transitioning to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  pg->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Started::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------Reset---------*/
PG::RecoveryState::Reset::Reset(my_context ctx)
  : my_base(ctx)
{
  state_name = "Reset";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->set_last_peering_reset();
}

boost::statechart::result
PG::RecoveryState::Reset::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->flushed = true;
  pg->requeue_ops(pg->waiting_for_active);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Reset advmap" << dendl;

  // make sure we have past_intervals filled in.  hopefully this will happen
  // _before_ we are active.
  pg->generate_past_intervals();

  pg->remove_down_peer_info(advmap.osdmap);
  if (pg->acting_up_affected(advmap.newup, advmap.newacting) ||
    pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, calling start_peering_interval again"
	     << dendl;
    pg->start_peering_interval(advmap.lastmap, advmap.newup, advmap.newacting);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->info),
					     pg->past_intervals);
  }

  pg->update_heartbeat_peers();
  pg->take_waiters();

  return transit< Started >();
}

boost::statechart::result PG::RecoveryState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Reset::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-------Start---------*/
PG::RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx)
{
  state_name = "Start";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    dout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    dout(1) << "transitioning to Stray" << dendl; 
    post_event(MakeStray());
  }
}

void PG::RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Primary--------*/
PG::RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(notevt.from, notevt.notify.info);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(const ActMap&)
{
  dout(7) << "handle ActMap primary" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->update_stats();
  pg->take_waiters();
  return discard_event();
}

void PG::RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Peering--------*/
PG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx), flushed(false)
{
  state_name = "Started/Primary/Peering";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result PG::RecoveryState::Peering::react(const AdvMap& advmap) 
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Peering advmap" << dendl;
  if (prior_set.get()->affected_by_map(advmap.osdmap, pg)) {
    dout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  
  pg->adjust_need_up_thru(advmap.osdmap);
  
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Peering::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  for (map<epoch_t,pg_interval_t>::iterator p = pg->past_intervals.begin();
       p != pg->past_intervals.end();
       ++p) {
    q.f->open_object_section("past_interval");
    p->second.dump(q.f);
    q.f->close_section();
  }
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<int>::iterator p = prior_set->probe.begin(); p != prior_set->probe.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  if (prior_set->pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set->down.begin(); p != prior_set->down.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set->blocked_by.begin();
       p != prior_set->blocked_by.end();
       p++) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::Peering::exit()
{
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();
}


/*------Backfilling-------*/
PG::RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Backfilling";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = true;
  pg->osd->queue_for_recovery(pg);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL);
}

void PG::RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_BACKFILL);
}

template <class EVT>
struct QueuePeeringEvt : Context {
  boost::intrusive_ptr<PG> pg;
  epoch_t epoch;
  EVT evt;
  QueuePeeringEvt(PG *pg, epoch_t epoch, EVT evt) :
    pg(pg), epoch(epoch), evt(evt) {}
  void finish(int r) {
    pg->lock();
    pg->queue_peering_event(PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  epoch,
	  epoch,
	  evt)));
    pg->unlock();
  }
};

/*--WaitRemoteBackfillReserved--*/

PG::RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitRemoteBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  ConnectionRef con = pg->osd->get_con_osd_cluster(
    pg->backfill_target, pg->get_osdmap()->get_epoch());
  if (con) {
    if ((con->features & CEPH_FEATURE_BACKFILL_RESERVATION)) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	  MBackfillReserve::REQUEST,
	  pg->info.pgid,
	  pg->get_osdmap()->get_epoch()),
	con.get());
    } else {
      post_event(RemoteBackfillReserved());
    }
  }
}

void PG::RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  return transit<Backfilling>();
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  Mutex::Locker lock(pg->osd->backfill_request_lock);
  pg->osd->backfill_request_timer.add_event_after(
    g_conf->osd_backfill_retry_interval,
    new QueuePeeringEvt<RequestBackfill>(
      pg, pg->get_osdmap()->get_epoch(),
      RequestBackfill()));

  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PG::RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalBackfillReserved()));
}

void PG::RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----NotBackfilling------*/
PG::RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/NotBackfilling";
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepNotRecovering----*/
PG::RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepNotRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepWaitRecoveryReserved--*/
PG::RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->osd->remote_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      RemoteRecoveryReserved()));
}

boost::statechart::result
PG::RecoveryState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->acting[0],
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      pg->info.pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

void PG::RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-RepWaitBackfillReserved*/
PG::RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  int64_t kb      = pg->osd->osd->osd_stat.kb,
          kb_used = pg->osd->osd->osd_stat.kb_used;
  int64_t max = kb * g_conf->osd_backfill_full_ratio;
  if (kb_used >= max) {
    dout(10) << "backfill reservation rejected: kb used >= max: "
             << kb_used << " >= " << max << dendl;
    post_event(RemoteReservationRejected());
  } else {
    pg->osd->remote_reserver.request_reservation(
      pg->info.pgid,
      new QueuePeeringEvt<RemoteBackfillReserved>(
        pg, pg->get_osdmap()->get_epoch(),
        RemoteBackfillReserved()));
  }
}

void PG::RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->acting[0],
    new MBackfillReserve(
      MBackfillReserve::GRANT,
      pg->info.pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->acting[0],
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      pg->info.pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PG::RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
}

/*------Activating--------*/
PG::RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Activating";
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalRecoveryReserved()));
}

void PG::RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    acting_osd_it(context< Active >().sorted_acting_set.begin())
{
  state_name = "Started/Primary/Active/WaitRemoteRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  PG *pg = context< RecoveryMachine >().pg;

  if (acting_osd_it != context< Active >().sorted_acting_set.end()) {
    // skip myself
    if (*acting_osd_it == pg->osd->whoami)
      ++acting_osd_it;
  }

  if (acting_osd_it != context< Active >().sorted_acting_set.end()) {
    ConnectionRef con = pg->osd->get_con_osd_cluster(*acting_osd_it, pg->get_osdmap()->get_epoch());
    if (con) {
      if ((con->features & CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->osd->send_message_osd_cluster(
          new MRecoveryReserve(MRecoveryReserve::REQUEST,
			       pg->info.pgid,
			       pg->get_osdmap()->get_epoch()),
	  con.get());
      } else {
	post_event(RemoteRecoveryReserved());
      }
    }
    ++acting_osd_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Recovering";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_set(PG_STATE_RECOVERING);
  pg->osd->queue_for_recovery(pg);
}

void PG::RecoveryState::Recovering::release_reservations()
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->missing.have_missing());
  pg->state_clear(PG_STATE_RECOVERING);

  // release remote reservations
  for (set<int>::const_iterator i = context< Active >().sorted_acting_set.begin();
        i != context< Active >().sorted_acting_set.end();
        ++i) {
    if (*i == pg->osd->whoami) // skip myself
      continue;
    ConnectionRef con = pg->osd->get_con_osd_cluster(*i, pg->get_osdmap()->get_epoch());
    if (con) {
      if ((con->features & CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->osd->send_message_osd_cluster(
          new MRecoveryReserve(MRecoveryReserve::RELEASE,
			       pg->info.pgid,
			       pg->get_osdmap()->get_epoch()),
	  con.get());
      }
    }
  }
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<WaitRemoteBackfillReserved>();
}

void PG::RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx)
{
  int newest_update_osd;

  state_name = "Started/Primary/Active/Recovered";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED is appropriate.
  if (pg->get_osdmap()->get_pg_size(pg->info.pgid) <= pg->acting.size())
    pg->state_clear(PG_STATE_DEGRADED);

  // adjust acting set?  (e.g. because backfill completed...)
  if (pg->acting != pg->up && !pg->choose_acting(newest_update_osd))
    assert(pg->want_acting.size());

  assert(!pg->needs_recovery());

  if (context< Active >().all_replicas_activated)
    post_event(GoClean());
}

void PG::RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Clean";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  if (pg->info.last_complete != pg->info.last_update) {
    assert(0);
  }
  pg->finish_recovery(*context< RecoveryMachine >().get_on_safe_context_list());
  pg->mark_clean();

  pg->share_pg_info();
  pg->update_stats();

}

void PG::RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
}

/*---------Active---------*/
PG::RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    sorted_acting_set(context< RecoveryMachine >().pg->acting.begin(),
                      context< RecoveryMachine >().pg->acting.end()),
    all_replicas_activated(false)
{
  state_name = "Started/Primary/Active";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  assert(!pg->backfill_reserving);
  assert(!pg->backfill_reserved);
  assert(pg->is_primary());
  dout(10) << "In Active, about to call activate" << dendl;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       pg->get_osdmap()->get_epoch(),
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       *context< RecoveryMachine >().get_query_map(),
	       context< RecoveryMachine >().get_info_map());
  assert(pg->is_active());
  dout(10) << "Activate Finished" << dendl;
}

boost::statechart::result PG::RecoveryState::Active::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  if (!pg->pool.newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool.newly_removed_snaps);
    dout(10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
  }
  pg->check_recovery_sources(pg->get_osdmap());

  for (vector<int>::iterator p = pg->want_acting.begin();
       p != pg->want_acting.end(); ++p) {
    if (!advmap.osdmap->is_up(*p)) {
      assert((std::find(pg->acting.begin(), pg->acting.end(), *p) !=
	      pg->acting.end()) ||
	     (std::find(pg->up.begin(), pg->up.end(), *p) !=
	      pg->up.end()));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(pg->info.pgid) !=
      pg->get_osdmap()->get_pg_size(pg->info.pgid)) {
    unsigned active = pg->acting.size();
    if (pg->backfill_target != -1)
      --active;
    if (pg->get_osdmap()->get_pg_size(pg->info.pgid) <= active)
      pg->state_clear(PG_STATE_DEGRADED);
    else
      pg->state_set(PG_STATE_DEGRADED);
    pg->update_stats(); // degraded may have changed
  }
  return forward_event();
}
    
boost::statechart::result PG::RecoveryState::Active::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_active());
  assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (g_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->osd->store);

  int unfound = pg->missing.num_missing() - pg->missing_loc.size();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) {
    if (g_conf->osd_auto_mark_unfound_lost) {
      pg->osd->clog.error() << pg->info.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically marking lost but NOT IMPLEMENTED\n";
      //pg->mark_all_unfound_lost(*context< RecoveryMachine >().get_cur_transaction());
    } else
      pg->osd->clog.error() << pg->info.pgid << " has " << unfound << " objects unfound and apparently lost\n";
  }

  if (!pg->snap_trimq.empty() &&
      pg->is_clean()) {
    dout(10) << "Active: queuing snap trim" << dendl;
    pg->queue_snap_trim();
  }

  if (!pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
    pg->osd->queue_for_recovery(pg);
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MNotifyRec& notevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", already have info from that osd, ignoring" 
	     << dendl;
  } else if (pg->peer_purged.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from
	     << ", already purged that peer, ignoring"
	     << dendl;
  } else {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", calling proc_replica_info and discover_all_missing"
	     << dendl;
    pg->proc_replica_info(notevt.from, notevt.notify.info);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());

  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_acting(infoevt.from)) {
    assert(pg->info.history.last_epoch_started < 
	   pg->info.history.same_interval_since);
    assert(infoevt.info.history.last_epoch_started >= 
	   pg->info.history.same_interval_since);
    dout(10) << " peer osd." << infoevt.from << " activated and committed" 
	     << dendl;
    pg->peer_activated.insert(infoevt.from);
  }

  if (pg->peer_activated.size() == pg->acting.size()) {
    pg->all_activated_and_committed();
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);
  if (got_missing)
    pg->osd->queue_for_recovery(pg);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<int>::iterator p = pg->might_have_unfound.begin();
	 p != pg->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_int("osd", *p);
      if (pg->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (pg->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!pg->get_osdmap()->is_up(*p)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    pg->dump_recovery_info(q.f);
    q.f->close_section();
  }

  {
    q.f->open_object_section("scrub");
    q.f->dump_stream("scrubber.epoch_start") << pg->scrubber.epoch_start;
    q.f->dump_int("scrubber.active", pg->scrubber.active);
    q.f->dump_int("scrubber.block_writes", pg->scrubber.block_writes);
    q.f->dump_int("scrubber.finalizing", pg->scrubber.finalizing);
    q.f->dump_int("scrubber.waiting_on", pg->scrubber.waiting_on);
    {
      q.f->open_array_section("scrubber.waiting_on_whom");
      for (set<int>::iterator p = pg->scrubber.waiting_on_whom.begin();
	   p != pg->scrubber.waiting_on_whom.end();
	   ++p) {
	q.f->dump_int("osd", *p);
      }
      q.f->close_section();
    }
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const AllReplicasActivated &evt)
{
  all_replicas_activated = true;
  return discard_event();
}

void PG::RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_DEGRADED);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_REPLAY);
}

/*------ReplicaActive-----*/
PG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive";

  context< RecoveryMachine >().log_enter(state_name);
}


boost::statechart::result PG::RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  map< int, map< pg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       actevt.query_epoch,
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       query_map, NULL);
  dout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "received log from " << logevt.from << dendl;
  pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		logevt.msg->info, logevt.msg->log, logevt.from);

  assert(pg->log.head == pg->info.last_update);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->info),
					     pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(query.query.type == pg_query_t::MISSING);
  pg->update_history_from_master(query.query.history);
  pg->fulfill_log(query.from, query.query, query.query_epoch);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("scrubber.finalizing", pg->scrubber.finalizing);
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
}

/*-------Stray---*/
PG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx) {
  state_name = "Started/Stray";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
}

boost::statechart::result PG::RecoveryState::Stray::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  dout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    pg->unreg_next_scrub();
    pg->info = msg->info;
    pg->reg_next_scrub();
    pg->dirty_info = true;
    pg->dirty_log = true;
    pg->log.claim_log(msg->log);
    pg->missing.clear();
  } else {
    pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		  msg->info, msg->log, logevt.from);
  }

  assert(pg->log.head == pg->info.last_update);

  post_event(Activate(logevt.msg->get_epoch()));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    pg->rewind_divergent_log(*context< RecoveryMachine >().get_cur_transaction(),
			     infoevt.info.last_update);
    pg->info.stats = infoevt.info.stats;
  }
  
  assert(infoevt.info.last_update == pg->info.last_update);
  assert(pg->log.head == pg->info.last_update);

  post_event(Activate(infoevt.msg_epoch));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::INFO) {
    pair<int, pg_info_t> notify_info;
    pg->update_history_from_master(query.query.history);
    pg->fulfill_info(query.from, query.query, notify_info);
    context< RecoveryMachine >().send_notify(notify_info.first,
					     pg_notify_t(query.query_epoch,
							 pg->get_osdmap()->get_epoch(),
							 notify_info.second),
					     pg->past_intervals);
  } else {
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Stray::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->info),
					     pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

void PG::RecoveryState::Stray::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------GetInfo---------*/
PG::RecoveryState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx) 
{
  state_name = "Started/Primary/Peering/GetInfo";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->generate_past_intervals();
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  if (!prior_set.get())
    pg->build_prior(prior_set);

  pg->update_stats();

  get_infos();
  if (peer_info_requested.empty() && !prior_set->pg_down) {
    post_event(GotInfo());
  }
}

void PG::RecoveryState::GetInfo::get_infos()
{
  PG *pg = context< RecoveryMachine >().pg;
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  for (set<int>::const_iterator it = prior_set->probe.begin();
       it != prior_set->probe.end();
       ++it) {
    int peer = *it;
    if (peer == pg->osd->whoami) {
      continue;
    }
    if (pg->peer_info.count(peer)) {
      dout(10) << " have osd." << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      dout(10) << " already requested info from osd." << peer << dendl;
    } else if (!pg->get_osdmap()->is_up(peer)) {
      dout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      dout(10) << " querying info from osd." << peer << dendl;
      context< RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 pg->info.history,
			 pg->get_osdmap()->get_epoch()));
      peer_info_requested.insert(peer);
    }
  }
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  set<int>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end())
    peer_info_requested.erase(p);

  PG *pg = context< RecoveryMachine >().pg;
  epoch_t old_start = pg->info.history.last_epoch_started;
  if (pg->proc_replica_info(infoevt.from, infoevt.notify.info)) {
    // we got something new ...
    auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;
    if (old_start < pg->info.history.last_epoch_started) {
      dout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      pg->build_prior(prior_set);

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<int>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set->probe.count(*p) == 0) {
	  dout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set->pg_down) {
      /*
       * make sure we have at least one !incomplete() osd from the
       * last rw interval.  the incomplete (backfilling) replicas
       * get a copy of the log, but they don't get all the object
       * updates, so they are insufficient to recover changes during
       * that interval.
       */
      if (pg->info.history.last_epoch_started) {
	for (map<epoch_t,pg_interval_t>::reverse_iterator p = pg->past_intervals.rbegin();
	     p != pg->past_intervals.rend();
	     ++p) {
	  if (p->first < pg->info.history.last_epoch_started)
	    break;
	  if (!p->second.maybe_went_rw)
	    continue;
	  pg_interval_t& interval = p->second;
	  dout(10) << " last maybe_went_rw interval was " << interval << dendl;
	  OSDMapRef osdmap = pg->get_osdmap();

	  /*
	   * this mirrors the PriorSet calculation: we wait if we
	   * don't have an up (AND !incomplete) node AND there are
	   * nodes down that might be usable.
	   */
	  bool any_up_complete_now = false;
	  bool any_down_now = false;
	  for (unsigned i=0; i<interval.acting.size(); i++) {
	    int o = interval.acting[i];
	    if (!osdmap->exists(o) || osdmap->get_info(o).lost_at > interval.first)
	      continue;  // dne or lost
	    if (osdmap->is_up(o)) {
	      pg_info_t *pinfo;
	      if (o == pg->osd->whoami) {
		pinfo = &pg->info;
	      } else {
		assert(pg->peer_info.count(o));
		pinfo = &pg->peer_info[o];
	      }
	      if (!pinfo->is_incomplete())
		any_up_complete_now = true;
	    } else {
	      any_down_now = true;
	    }
	  }
	  if (!any_up_complete_now && any_down_now) {
	    dout(10) << " no osds up+complete from interval " << interval << dendl;
	    pg->state_set(PG_STATE_DOWN);
	    return discard_event();
	  }
	  break;
	}
      }
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<int>::iterator p = peer_info_requested.begin(); p != peer_info_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      pg->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetInfo::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetLog------------*/
PG::RecoveryState::GetLog::GetLog(my_context ctx) : 
  my_base(ctx), newest_update_osd(-1), msg(0)
{
  state_name = "Started/Primary/Peering/GetLog";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  // adjust acting?
  if (!pg->choose_acting(newest_update_osd)) {
    if (pg->want_acting.size()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (newest_update_osd == pg->osd->whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = pg->peer_info[newest_update_osd];

  // am i broken?
  if (pg->info.last_update < best.log_tail) {
    dout(10) << " not contiguous with osd." << newest_update_osd << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->info.last_update;
  for (vector<int>::iterator p = pg->acting.begin() + 1; p != pg->acting.end(); ++p) {
    pg_info_t& ri = pg->peer_info[*p];
    if (ri.last_update >= best.log_tail && ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  dout(10) << " requesting log from osd." << newest_update_osd << dendl;
  context<RecoveryMachine>().send_query(
    newest_update_osd,
    pg_query_t(pg_query_t::LOG, request_log_from, pg->info.history,
	       pg->get_osdmap()->get_epoch()));
}

boost::statechart::result PG::RecoveryState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(newest_update_osd)) {
    dout(10) << "GetLog: newest_update_osd osd." << newest_update_osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const MLogRec& logevt)
{
  assert(!msg);
  if (logevt.from != newest_update_osd) {
    dout(10) << "GetLog: discarding log from "
	     << "non-newest_update_osd osd." << logevt.from << dendl;
    return discard_event();
  }
  dout(10) << "GetLog: recieved master log from osd" 
	   << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const GotLog&)
{
  dout(10) << "leaving GetLog" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (msg) {
    dout(10) << "processing master log" << dendl;
    pg->proc_master_log(*context<RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing, 
			newest_update_osd);
  }
  return transit< GetMissing >();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("newest_update_osd", newest_update_osd);
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetLog::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------WaitActingChange--------*/
PG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitActingChange";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  pg->remove_down_peer_info(osdmap);

  dout(10) << "verifying no want_acting " << pg->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->want_acting.begin(); p != pg->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      dout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitActingChange::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Incomplete--------*/
PG::RecoveryState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/Incomplete";
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);
  pg->update_stats();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const AdvMap &advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  int64_t poolnum = pg->info.pgid.pool();

  // Reset if min_size changed, pg might now be able to go active
  if (advmap.lastmap->get_pools().find(poolnum)->second.min_size !=
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

void PG::RecoveryState::Incomplete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
}

/*------GetMissing--------*/
PG::RecoveryState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/GetMissing";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  for (vector<int>::iterator i = pg->acting.begin() + 1;
       i != pg->acting.end();
       ++i) {
    const pg_info_t& pi = pg->peer_info[*i];

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->log.tail) {
      dout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->peer_missing[*i];
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      dout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->peer_missing[*i];
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i];
      pg->search_for_missing(pi, &pg->peer_missing[*i], *i);
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    eversion_t since(pi.last_epoch_started, 0);
    assert(pi.last_update >= pg->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      dout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryMachine >().send_query(
	*i,
	pg_query_t(pg_query_t::LOG, since, pg->info.history,
		   pg->get_osdmap()->get_epoch()));
    } else {
      dout(10) << " requesting fulllog+missing from osd." << *i
	       << " (want since " << since << " < log.tail " << pi.log_tail << ")"
	       << dendl;
      context< RecoveryMachine >().send_query(
	*i, pg_query_t(pg_query_t::FULLLOG,
		       pg->info.history, pg->get_osdmap()->get_epoch()));
    }
    peer_missing_requested.insert(*i);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(CheckRepops());
  }
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(*context<RecoveryMachine>().get_cur_transaction(),
		       logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    post_event(CheckRepops());
  }
  return discard_event();
};

boost::statechart::result PG::RecoveryState::GetMissing::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<int>::iterator p = peer_missing_requested.begin(); p != peer_missing_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      pg->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetMissing::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---WaitFlushedPeering---*/
PG::RecoveryState::WaitFlushedPeering::WaitFlushedPeering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitFlushedPeering";
  PG *pg = context< RecoveryMachine >().pg;
  context< RecoveryMachine >().log_enter(state_name);
  if (context< RecoveryMachine >().pg->flushed)
    post_event(Activate(pg->get_osdmap()->get_epoch()));
}

boost::statechart::result
PG::RecoveryState::WaitFlushedPeering::react(const FlushedEvt &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->flushed = true;
  pg->requeue_ops(pg->waiting_for_active);
  return transit< WaitFlushedPeering >();
}

boost::statechart::result
PG::RecoveryState::WaitFlushedPeering::react(const QueryState &q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for flush");
  return forward_event();
}

/*------WaitUpThru--------*/
PG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitUpThru";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const ActMap& am)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->need_up_thru) {
    post_event(CheckRepops());
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);

  // hmm.. should we?
  (void)got_missing;
  //if (got_missing)
  //pg->osd->queue_for_recovery(pg);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitUpThru::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void PG::RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  dout(20) << "enter " << state_name << dendl;
  pg->osd->pg_recovery_stats.log_enter(state_name);
}

void PG::RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now(g_ceph_context) - enter_time;
  dout(20) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, ceph_clock_now(g_ceph_context) - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix (*_dout << (debug_pg ? debug_pg->gen_prefix() : string()) << " PriorSet: ")

PG::PriorSet::PriorSet(const OSDMap &osdmap,
		       const map<epoch_t, pg_interval_t> &past_intervals,
		       const vector<int> &up,
		       const vector<int> &acting,
		       const pg_info_t &info,
		       const PG *debug_pg)
  : pg_down(false)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike) 
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i=0; i<acting.size(); i++)
    probe.insert(acting[i]);
  // It may be possible to exlude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i=0; i<up.size(); i++)
    probe.insert(up[i]);

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       p++) {
    const pg_interval_t &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at candidate osds during this interval.  each falls into
    // one of three categories: up, down (but potentially
    // interesting), or lost (down, but we won't wait for it).
    bool any_up_now = false;    // any candidates up now
    bool any_down_now = false;  // any candidates down now (that might have useful data)

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];

      const osd_info_t *pinfo = 0;
      if (osdmap.exists(o))
	pinfo = &osdmap.get_info(o);

      if (osdmap.is_up(o)) {
	// include past acting osds if they are up.
	probe.insert(o);
	any_up_now = true;
      } else if (!pinfo) {
	dout(10) << "build_prior  prior osd." << o << " no longer exists" << dendl;
	down.insert(o);
      } else if (pinfo->lost_at > interval.first) {
	dout(10) << "build_prior  prior osd." << o << " is down, but lost_at " << pinfo->lost_at << dendl;
	down.insert(o);
      } else {
	dout(10) << "build_prior  prior osd." << o << " is down" << dendl;
	down.insert(o);
	any_down_now = true;
      }
    }

    // if nobody survived this interval, and we may have gone rw,
    // then we need to wait for one of those osds to recover to
    // ensure that we haven't lost any information.
    if (!any_up_now && any_down_now) {
      // fixme: how do we identify a "clean" shutdown anyway?
      dout(10) << "build_prior  possibly went active+rw, none up; including down osds" << dendl;
      for (vector<int>::const_iterator i = interval.acting.begin();
	   i != interval.acting.end();
	   ++i) {
	if (osdmap.exists(*i) &&   // if it doesn't exist, we already consider it lost.
	    osdmap.is_down(*i)) {
	  probe.insert(*i);
	  pg_down = true;

	  // make note of when any down osd in the cur set was lost, so that
	  // we can notice changes in prior_set_affected.
	  blocked_by[*i] = osdmap.get_info(*i).lost_at;
	}
      }
    }
  }

  dout(10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
}

// true if the given map affects the prior set
bool PG::PriorSet::affected_by_map(const OSDMapRef osdmap, const PG *debug_pg) const
{
  for (set<int>::iterator p = probe.begin();
       p != probe.end();
       ++p) {
    int o = *p;

    // did someone in the prior set go down?
    if (osdmap->is_down(o) && down.count(o) == 0) {
      dout(10) << "affected_by_map osd." << o << " now down" << dendl;
      return true;
    }

    // did a down osd in cur get (re)marked as lost?
    map<int,epoch_t>::const_iterator r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (!osdmap->exists(o)) {
	dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
	return true;
      }
      if (osdmap->get_info(o).lost_at != r->second) {
	dout(10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
	return true;
      }
    }
  }

  // did someone in the prior down set go up?
  for (set<int>::const_iterator p = down.begin();
       p != down.end();
       ++p) {
    int o = *p;

    if (osdmap->is_up(o)) {
      dout(10) << "affected_by_map osd." << *p << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap->exists(o)) {
      dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
      return true;
    }
  }

  return false;
}

void intrusive_ptr_add_ref(PG *pg) { pg->get(); }
void intrusive_ptr_release(PG *pg) { pg->put(); }
