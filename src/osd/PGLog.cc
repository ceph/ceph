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

#define dout_subsys ceph_subsys_osd

//////////////////// PGLog ////////////////////

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

void PGLog::IndexedLog::trim(ObjectStore::Transaction& t, hobject_t& log_oid, eversion_t s)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is " << complete_to->version
		    << " on " << *this << dendl;
  }

  set<string> keys_to_rm;
  while (!log.empty()) {
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    unindex(e);         // remove from index,
    keys_to_rm.insert(e.get_key_name());
    log.pop_front();    // from log
  }
  t.omap_rmkeys(coll_t::META_COLL, log_oid, keys_to_rm);

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

void PGLog::clear_info_log(
  pg_t pgid,
  const hobject_t &infos_oid,
  const hobject_t &log_oid,
  ObjectStore::Transaction *t) {

  set<string> keys_to_remove;
  keys_to_remove.insert(PG::get_epoch_key(pgid));
  keys_to_remove.insert(PG::get_biginfo_key(pgid));
  keys_to_remove.insert(PG::get_info_key(pgid));

  t->remove(coll_t::META_COLL, log_oid);
  t->omap_rmkeys(coll_t::META_COLL, infos_oid, keys_to_remove);
}

void PGLog::trim(ObjectStore::Transaction& t, eversion_t trim_to, pg_info_t &info, hobject_t &log_oid)
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
    log.trim(t, log_oid, trim_to);
    info.log_tail = log.tail;
  }
}

void PGLog::proc_replica_log(ObjectStore::Transaction& t,
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

/*
 * merge an old (possibly divergent) log entry into the new log.  this 
 * happens _after_ new log items have been assimilated.  thus, we assume
 * the index already references newer entries (if present), and missing
 * has been updated accordingly.
 *
 * return true if entry is not divergent.
 */
bool PGLog::merge_old_entry(ObjectStore::Transaction& t, pg_log_entry_t& oe, pg_info_t& info, list<hobject_t>& remove_snap, bool &dirty_log)
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
    remove_snap.push_back(oe.soid);
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
      remove_snap.push_back(oe.soid);
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
void PGLog::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
                      pg_info_t &info, list<hobject_t>& remove_snap,
                      bool &dirty_log, bool &dirty_info, bool &dirty_big_info)
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

  for (list<pg_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); ++d)
    merge_old_entry(t, *d, info, remove_snap, dirty_log);

  dirty_info = true;
  dirty_big_info = true;
  dirty_log = true;
}

void PGLog::merge_log(ObjectStore::Transaction& t,
                      pg_info_t &oinfo, pg_log_t &olog, int fromosd,
                      pg_info_t &info, list<hobject_t>& remove_snap,
                      bool &dirty_log, bool &dirty_info, bool &dirty_big_info)
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
	 ++to) {
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
    rewind_divergent_log(t, olog.head, info, remove_snap, dirty_log, dirty_info, dirty_big_info);
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

    // index, update missing, delete deleted
    for (list<pg_log_entry_t>::iterator p = from; p != to; ++p) {
      pg_log_entry_t &ne = *p;
      dout(20) << "merge_log " << ne << dendl;
      log.index(ne);
      if (ne.soid <= info.last_backfill) {
	missing.add_next_event(ne);
	if (ne.is_delete())
	  remove_snap.push_back(ne.soid);
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
    info.purged_snaps = oinfo.purged_snaps;

    // process divergent items
    if (!divergent.empty()) {
      for (list<pg_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); ++d)
	merge_old_entry(t, *d, info, remove_snap, dirty_log);
    }

    changed = true;
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
    dirty_log = true;
  }
}

void PGLog::write_log(ObjectStore::Transaction& t, pg_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors)
{
  //dout(10) << "write_log" << dendl;
  t.remove(coll_t::META_COLL, log_oid);
  t.touch(coll_t::META_COLL, log_oid);
  map<string,bufferlist> keys;
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end();
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }
  //dout(10) << "write_log " << keys.size() << " keys" << dendl;

  ::encode(divergent_priors, keys["divergent_priors"]);

  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);
}

bool PGLog::read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
  const pg_info_t &info, OndiskLog &ondisklog, IndexedLog &log,
  pg_missing_t &missing, ostringstream &oss)
{
  dout(10) << "read_log" << dendl;
  bool rewrite_log = false;

  // legacy?
  struct stat st;
  int r = store->stat(coll_t::META_COLL, log_oid, &st);
  assert(r == 0);
  if (st.st_size > 0) {
    read_log_old(store, coll, log_oid, info, ondisklog, log, missing, oss);
    rewrite_log = true;
  } else {
    log.tail = info.log_tail;
    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(coll_t::META_COLL, log_oid);
    if (p) for (p->seek_to_first(); p->valid() ; p->next()) {
      bufferlist bl = p->value();//Copy bufferlist before creating iterator
      bufferlist::iterator bp = bl.begin();
      if (p->key() == "divergent_priors") {
	::decode(ondisklog.divergent_priors, bp);
	dout(20) << "read_log " << ondisklog.divergent_priors.size() << " divergent_priors" << dendl;
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
  return rewrite_log;
}

void PGLog::read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
  const pg_info_t &info, OndiskLog &ondisklog, IndexedLog &log,
  pg_missing_t &missing, ostringstream &oss)
{
  // load bounds, based on old OndiskLog encoding.
  uint64_t ondisklog_tail = 0;
  uint64_t ondisklog_head = 0;
  uint64_t ondisklog_zero_to;
  bool ondisklog_has_checksums;

  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  {
    bufferlist::iterator bl = blb.begin();
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ondisklog_has_checksums = (struct_v >= 2);
    ::decode(ondisklog_tail, bl);
    ::decode(ondisklog_head, bl);
    if (struct_v >= 4)
      ::decode(ondisklog_zero_to, bl);
    else
      ondisklog_zero_to = 0;
    if (struct_v >= 5)
      ::decode(ondisklog.divergent_priors, bl);
    DECODE_FINISH(bl);
  }
  uint64_t ondisklog_length = ondisklog_head - ondisklog_tail;
  dout(10) << "read_log " << ondisklog_tail << "~" << ondisklog_length << dendl;
 
  log.tail = info.log_tail;

  // In case of sobject_t based encoding, may need to list objects in the store
  // to find hashes
  vector<hobject_t> ls;
  
  if (ondisklog_head > 0) {
    // read
    bufferlist bl;
    store->read(coll_t::META_COLL, log_oid, ondisklog_tail, ondisklog_length, bl);
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
    bool listed_collection = false;

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
      uint64_t endpos = ondisklog_tail + p.get_off();
      log.log.push_back(e);
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
