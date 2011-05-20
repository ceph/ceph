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

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include <sstream>

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix _prefix(this)
static ostream& _prefix(const PG *pg) {
  return *_dout << pg->gen_prefix();
}

std::string PG::gen_prefix() const {
  stringstream out;
  out << "osd" << osd->whoami 
      << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) 
      << " " << *this << " ";
  return out.str();
}
  
/******* PGLog ********/

void PG::Log::copy_after(const Log &other, eversion_t v) 
{
  head = other.head;
  tail = other.tail;
  for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       i++) {
    if (i->version <= v) {
      tail = i->version;
      break;
    }
    assert(i->version > v);
    log.push_front(*i);
  }
}

bool PG::Log::copy_after_unless_divergent(const Log &other, eversion_t split, eversion_t floor) 
{
  assert(split >= other.tail);
  assert(floor >= other.tail);
  assert(floor <= split);
  head = tail = other.head;
  
  /* runs on replica.  split is primary's log.head.  floor is how much they want.
     split tell us if the primary is divergent.. e.g.:
     -> i am A, B is primary, split is 2'6, floor is 2'2.
A     B     C
2'2         2'2
2'3   2'3   2'3
2'4   2'4   2'4
3'5 | 2'5   2'5
3'6 | 2'6
3'7 |
3'8 |
3'9 |
      -> i return full backlog.
  */

  for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       i++) {
    // is primary divergent? 
    // e.g. my 3'6 vs their 2'6 split
    if (i->version.version == split.version && i->version.epoch > split.epoch) {
      clear();
      return false;   // divergent!
    }
    if (i->version == floor) break;
    assert(i->version > floor);

    // e.g. my 2'23 > '12
    log.push_front(*i);
  }
  tail = floor;
  return true;
}

void PG::Log::copy_non_backlog(const Log &other)
{
  if (other.backlog) {
    head = other.head;
    tail = other.tail;
    for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
         i != other.log.rend();
         i++) 
      if (i->version > tail)
        log.push_front(*i);
      else
        break;
  } else {
    *this = other;
  }
}



void PG::IndexedLog::trim(ObjectStore::Transaction& t, eversion_t s) 
{
  if (backlog && s < tail)
    s = tail;

  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is " << complete_to->version
		    << " on " << *this << dendl;
  }

  while (!log.empty()) {
    Entry &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    unindex(e);         // remove from index,
    log.pop_front();    // from log
  }

  // raise tail?
  if (backlog)
    backlog = false;
  if (tail < s)
    tail = s;
}


void PG::IndexedLog::trim_write_ahead(eversion_t last_update) 
{
  while (!log.empty() &&
         log.rbegin()->version > last_update) {
    // remove from index
    unindex(*log.rbegin());
    
    // remove
    log.pop_back();
  }
}

void PG::trim_write_ahead()
{
  if (info.last_update < log.head) {
    dout(10) << "trim_write_ahead (" << info.last_update << "," << log.head << "]" << dendl;
    log.trim_write_ahead(info.last_update);
  } else {
    assert(info.last_update == log.head);
    dout(10) << "trim_write_ahead last_update=head=" << info.last_update << dendl;
  }

}

void PG::proc_master_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, Missing& omissing, int from)
{
  dout(10) << "proc_master_log for osd" << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from);
  peer_info[from] = oinfo;
  dout(10) << " peer osd" << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  peer_missing[from].swap(omissing);
}
    
void PG::proc_replica_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, Missing& omissing, int from)
{
  dout(10) << "proc_replica_log for osd" << from << ": " << oinfo << " " << olog << " " << omissing << dendl;

  // This was a side effect in _process_pg_info. At this point I don't
  // think it's necessary, since we're in the GetMissing state,
  // but we'll keep it here for now
  osd->unreg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);
  info.history.merge(oinfo.history);
  osd->reg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);

  /*
    basically what we're doing here is rewinding the remote log,
    dropping divergent entries, until we find something that matches
    our master log.  we then reset last_update to reflect the new
    point up to which missing is accurate.

    later, in activate(), missing will get wound forward again and
    we will send the peer enough log to arrive at the same state.
  */

  for (map<sobject_t, Missing::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(10) << "Missing sobject: " << i->first << dendl;
  }
  list<Log::Entry>::const_reverse_iterator pp = olog.log.rbegin();
  eversion_t lu(oinfo.last_update);
  while (true) {
    if (pp == olog.log.rend()) {
      lu = olog.tail;
      break;
    }
    const Log::Entry& oe = *pp;

    // don't continue past the tail of our log.
    if (oe.version <= log.tail)
      break;

    if (!log.objects.count(oe.soid)) {
      dout(10) << " had " << oe << " new dne : divergent, ignoring" << dendl;
      ++pp;
      continue;
    }
      
    Log::Entry& ne = *log.objects[oe.soid];
    if (ne.version == oe.version) {
      dout(10) << " had " << oe << " new " << ne << " : match, stopping" << dendl;
      lu = pp->version;
      break;
    }
    if (ne.version > oe.version) {
      dout(10) << " had " << oe << " new " << ne << " : new will supercede" << dendl;
    } else {
      if (oe.is_delete()) {
	if (ne.is_delete()) {
	  // old and new are delete
	  dout(20) << " had " << oe << " new " << ne << " : both deletes" << dendl;
	} else {
	  // old delete, new update.
	  dout(20) << " had " << oe << " new " << ne << " : missing" << dendl;
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
    dout(10) << " peer osd" << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
    if (lu < oinfo.last_complete)
      oinfo.last_complete = lu;
  }

  peer_info[from] = oinfo;
  dout(10) << " peer osd" << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  for (map<sobject_t, Missing::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(10) << "Final Missing sobject: " << i->first << dendl;
  }
  peer_missing[from].swap(omissing);
}

bool PG::proc_replica_info(int from, Info &oinfo)
{
  map<int,PG::Info>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd" << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  dout(10) << " got osd" << from << " " << oinfo << dendl;
  assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);
  
  osd->unreg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);
  info.history.merge(oinfo.history);
  osd->reg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);
  
  // stray?
  if (!is_acting(from)) {
    dout(10) << " osd" << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }
  update_stats();

  return true;
}


/*
 * merge an old (possibly divergent) log entry into the new log.  this 
 * happens _after_ new log items have been assimilated.  thus, we assume
 * the index already references newer entries (if present), and missing
 * has been updated accordingly.
 *
 * return true if entry is not divergent.
 */
bool PG::merge_old_entry(ObjectStore::Transaction& t, Log::Entry& oe)
{
  if (log.objects.count(oe.soid)) {
    Log::Entry &ne = *log.objects[oe.soid];  // new(er?) entry
    
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
	assert(missing.is_missing(oe.soid));
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
  } else {
    if (oe.is_delete()) {
      dout(20) << "merge_old_entry  had " << oe << " new dne : ok" << dendl;      
    } else {
      dout(20) << "merge_old_entry  had " << oe << " new dne : deleting" << dendl;
      t.remove(coll, oe.soid);
      missing.rm(oe.soid, oe.version);
    }
  }
  return false;
}

void PG::merge_log(ObjectStore::Transaction& t,
		   Info &oinfo, Log &olog, int fromosd)
{
  dout(10) << "merge_log " << olog << " from osd" << fromosd
           << " into " << log << dendl;
  bool changed = false;

  if (log.empty() ||
      (olog.tail > log.head && olog.backlog)) { // e.g. log=(0,20] olog=(40,50]+backlog) 

    if (is_primary()) {
      // we should have our own backlog already; see peer() code where
      // we request this.
    } else {
      // primary should have requested our backlog during peer().
    }

    hash_map<sobject_t, Log::Entry*> old_objects;
    old_objects.swap(log.objects);

    // swap in other log and index
    log.log.swap(olog.log);
    log.index();

    // first, find split point (old log.head) in new log.
    list<Log::Entry>::iterator p = log.log.end();
    while (p != log.log.begin()) {
      p--;
      if (p->version <= log.head) {
	dout(10) << "merge_log split point is " << *p << dendl;

	hash_map<sobject_t,Log::Entry*>::const_iterator oldobj = old_objects.find(p->soid);
	if (oldobj != old_objects.end() &&
	    oldobj->second->version == p->version)
	  p++;       // move past the split point, if it also exists in our old log...
	break;
      }
    }
    
    // then add all new items (_after_ split) to missing
    for (; p != log.log.end(); p++) {
      Log::Entry &ne = *p;
      dout(20) << "merge_log merging " << ne << dendl;
      missing.add_next_event(ne);
      if (ne.is_delete())
	t.remove(coll, ne.soid);
    }

    // find any divergent or removed items in old log.
    //  skip anything not in the index.
    for (p = olog.log.begin();
	 p != olog.log.end();
	 p++) {
      Log::Entry &oe = *p;                      // old entry
      if (old_objects.count(oe.soid) &&
	  old_objects[oe.soid] == &oe)
	merge_old_entry(t, oe);
    }

    info.last_update = log.head = olog.head;
    info.log_tail = log.tail = olog.tail;
    info.log_backlog = log.backlog = olog.backlog;
    if (oinfo.stats.reported < info.stats.reported)   // make sure reported always increases
      oinfo.stats.reported = info.stats.reported;
    info.stats = oinfo.stats;
    changed = true;
  } 

  else {
    // try to merge the two logs?

    // extend on tail?
    //  this is just filling in history.  it does not affect our
    //  missing set, as that should already be consistent with our
    //  current log.
    // FIXME: what if we have backlog, but they have lower tail?
    if (olog.tail < log.tail && olog.head >= log.tail && !log.backlog) {
      dout(10) << "merge_log extending tail to " << olog.tail
               << (olog.backlog ? " +backlog":"")
	       << dendl;
      list<Log::Entry>::iterator from = olog.log.begin();
      list<Log::Entry>::iterator to;
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
      info.log_backlog = log.backlog = olog.backlog;
      changed = true;
    }
    
    // extend on head?
    if (olog.head > log.head &&
        olog.tail <= log.head) {
      dout(10) << "merge_log extending head to " << olog.head << dendl;
      
      // find start point in olog
      list<Log::Entry>::iterator to = olog.log.end();
      list<Log::Entry>::iterator from = olog.log.end();
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
      for (list<Log::Entry>::iterator p = from; p != to; p++) {
	Log::Entry &ne = *p;
        dout(20) << "merge_log " << ne << dendl;
	log.index(ne);
	missing.add_next_event(ne);
	if (ne.is_delete())
	  t.remove(coll, ne.soid);
      }
      
      // move aside divergent items
      list<Log::Entry> divergent;
      while (!log.log.empty()) {
	Log::Entry &oe = *log.log.rbegin();
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
      
      info.last_update = log.head = olog.head;
      if (oinfo.stats.reported < info.stats.reported)   // make sure reported always increases
	oinfo.stats.reported = info.stats.reported;
      info.stats = oinfo.stats;

      // process divergent items
      if (!divergent.empty()) {
	// removing items screws screws our index
	log.index();   
	for (list<Log::Entry>::iterator d = divergent.begin(); d != divergent.end(); d++)
	  merge_old_entry(t, *d);
      }

      changed = true;
    }
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;
  //log.print(cout);

  if (changed) {
    write_info(t);
    write_log(t);
  }
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PG::search_for_missing(const Info &oinfo, const Missing *omissing,
			    int fromosd)
{
  bool stats_updated = false;
  bool found_missing = false;

  // found items?
  for (map<sobject_t,Missing::item>::iterator p = missing.missing.begin();
       p != missing.missing.end();
       ++p) {
    const sobject_t &soid(p->first);
    eversion_t need = p->second.need;
    eversion_t have = p->second.have;
    if (oinfo.last_update < need) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd" << fromosd
	       << " (last_update " << oinfo.last_update << " < needed " << need << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_complete < need) {
      if (!omissing) {
	// We know that the peer lacks some objects at the revision we need.
	// Without the peer's missing set, we don't know whether it has this
	// particular object or not.
	dout(10) << __func__ << " " << soid << " " << need
		 << " might also be missing on osd" << fromosd << dendl;
	continue;
      }

      if (omissing->is_missing(soid)) {
	dout(10) << "search_for_missing " << soid << " " << need
		 << " also missing on osd" << fromosd << dendl;
	continue;
      }
    }
    dout(10) << "search_for_missing " << soid << " " << need
	     << " is on osd" << fromosd << dendl;

    map<sobject_t, set<int> >::iterator ml = missing_loc.find(soid);
    if (ml == missing_loc.end()) {
      map<sobject_t, list<class Message*> >::iterator wmo =
	waiting_for_missing_object.find(soid);
      if (wmo != waiting_for_missing_object.end()) {
	osd->take_waiters(wmo->second);
      }
      stats_updated = true;
      missing_loc[soid].insert(fromosd);
    }
    else {
      ml->second.insert(fromosd);
    }
    found_missing = true;
  }
  if (stats_updated) {
    update_stats();
  }

  dout(20) << "search_for_missing missing " << missing.missing << dendl;
  return found_missing;
}

void PG::discover_all_missing(map< int, map<pg_t,PG::Query> > &query_map)
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
    
    if (!osd->osdmap->is_up(peer)) {
      dout(20) << __func__ << " skipping down osd" << peer << dendl;
      continue;
    }

    // If we've requested any of this stuff, the Missing information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      dout(20) << __func__ << ": osd" << peer
	       << ": we already have Missing" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      dout(20) << __func__ << ": osd" << peer
	       << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_backlog_requested.find(peer) != peer_backlog_requested.end()) {
      dout(20) << __func__ << ": osd" << peer
	       << ": in peer_backlog_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      dout(20) << __func__ << ": osd" << peer
	       << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    dout(10) << __func__ << ": osd" << peer << ": requesting Missing"
	     << dendl;
    peer_missing_requested.insert(peer);
    query_map[peer][info.pgid] =
      PG::Query(PG::Query::MISSING, info.history);
  }
}

// ===============================================================
// BACKLOG

bool PG::build_backlog_map(map<eversion_t,Log::Entry>& omap)
{
  dout(10) << "build_backlog_map requested epoch " << generate_backlog_epoch << dendl;

  unlock();

  vector<sobject_t> olist;
  osd->store->collection_list(coll, olist);

  for (vector<sobject_t>::iterator it = olist.begin();
       it != olist.end();
       it++) {
    sobject_t poid = *it;

    Log::Entry e;
    e.soid = poid;
    bufferlist bv;
    int r = osd->store->getattr(coll, poid, OI_ATTR, bv);
    if (r < 0)
      continue;  // musta just been deleted!
    object_info_t oi(bv);
    e.version = oi.version;
    e.prior_version = oi.prior_version;
    e.reqid = oi.last_reqid;
    e.mtime = oi.mtime;
    if (e.soid.snap && e.soid.snap < CEPH_NOSNAP) {
      e.op = Log::Entry::CLONE;
      ::encode(oi.snaps, e.snaps);
    } else {
      e.op = Log::Entry::BACKLOG;           // FIXME if/when we do smarter op codes!
    }

    omap[e.version] = e;

    lock();
    dout(10) << "build_backlog_map  " << e << dendl;
    if (!generate_backlog_epoch || osd->is_stopping()) {
      dout(10) << "build_backlog_map aborting" << dendl;
      return false;
    }
    unlock();
  }
  lock();
  dout(10) << "build_backlog_map done: " << omap.size() << " objects" << dendl;
  if (!generate_backlog_epoch) {
    dout(10) << "build_backlog_map aborting" << dendl;
    return false;
  }
  return true;
}

void PG::assemble_backlog(map<eversion_t,Log::Entry>& omap)
{
  dout(10) << "assemble_backlog for " << log << ", " << omap.size() << " objects" << dendl;
  
  assert(!log.backlog);
  log.backlog = true;
  info.log_backlog = true;

  /*
   * note that we don't create prior_version backlog entries for
   * objects that no longer exist (i.e., those for which there is a
   * delete entry in the log).  that's maybe a bit sloppy, but not a
   * problem, since we mainly care about generating an accurate
   * missing map, and an object that was deleted should obviously not
   * end up as missing.
   */

  /*
   * first pass inserts any prior_version backlog entries into the
   * ordered map.  second pass adds it to the log.
   */

  map<eversion_t,Log::Entry>::iterator i = omap.begin();
  while (i != omap.end()) {
    Log::Entry& be = i->second;

    dout(15) << " " << be << dendl;

    /*
     * we can skip an object if
     *  - is already in the log AND
     *    - it is a totally new object OR
     *    - the prior_version is also already in the log
     * otherwise, if we have the object, include a prior_version backlog entry.
     */
    if (log.objects.count(be.soid)) {
      Log::Entry *le = log.objects[be.soid];
      
      // note the prior version
      if (le->prior_version == eversion_t() ||  // either new object, or
	  le->prior_version >= log.tail) {      // prior_version also already in log
	dout(15) << " skipping " << be << " (have " << *le << ")" << dendl;
      } else {
	be.version = le->prior_version;
	be.reqid = osd_reqid_t();
	dout(15) << "   adding prior_version " << be << " (have " << *le << ")" << dendl;
	omap[be.version] = be;
	// don't try to index: this is the prior_version backlog entry
      }
      omap.erase(i++);
    } else {
      i++;
    }
  }

  for (map<eversion_t,Log::Entry>::reverse_iterator i = omap.rbegin();
       i != omap.rend();
       i++) {
    Log::Entry& be = i->second;
    dout(15) << "   adding " << be << dendl;
    log.log.push_front(be);
    if (be.reqid != osd_reqid_t())   // don't index prior_version backlog entries
      log.index( *log.log.begin() );
  }
}


void PG::drop_backlog()
{
  dout(10) << "drop_backlog for " << log << dendl;
  //log.print(cout);

  assert(log.backlog);
  log.backlog = false;
  info.log_backlog = false;
  
  while (!log.log.empty()) {
    Log::Entry &e = *log.log.begin();
    if (e.version > log.tail) break;

    dout(15) << "drop_backlog trimming " << e << dendl;
    log.unindex(e);
    log.log.pop_front();
  }
}





ostream& PG::Log::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<Entry>::const_iterator p = log.begin();
       p != log.end();
       p++) 
    out << *p << std::endl;
  return out;
}

ostream& PG::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<Entry>::const_iterator p = log.begin();
       p != log.end();
       p++) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}





/******* PG ***********/
bool PG::is_all_uptodate() const
{
  assert(is_primary());

  bool uptodate = true;

  if (up != acting) {
    dout(10) << __func__ << ": the set of UP osds is not the same as the "
	     << "set of ACTING osds." << dendl;
    uptodate = false;
  }

  if (missing.num_missing()) {
    dout(10) << __func__ << ": primary has " << missing.num_missing() << dendl;
    uptodate = false;
  }

  vector<int>::const_iterator end = acting.end();
  vector<int>::const_iterator a = acting.begin();
  assert(a != end);
  ++a;
  for (; a != end; ++a) {
    int peer = *a;
    map<int, Missing>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << ": osd" << peer << " don't have missing set" << dendl;
      uptodate = false;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << ": osd" << peer << " has " << pm->second.num_missing() << " missing" << dendl;
      uptodate = false;
    }
  }

  if (uptodate)
    dout(10) << __func__ << ": everyone is uptodate" << dendl;
  return uptodate;
}

void PG::generate_past_intervals()
{
  // Do we already have the intervals we want?
  map<epoch_t,Interval>::const_iterator pif = past_intervals.begin();
  if (pif != past_intervals.end()) {
    if (pif->first <= info.history.last_epoch_clean) {
      dout(10) << __func__ << ": already have past intervals back to "
	       << info.history.last_epoch_clean << dendl;
      return;
    }
    past_intervals.clear();
  }

  epoch_t first_epoch = 0;
  epoch_t stop = MAX(info.history.epoch_created, info.history.last_epoch_clean);
  epoch_t last_epoch = info.history.same_acting_since - 1;
  dout(10) << __func__ << " over epochs " << stop << "-" << last_epoch << dendl;

  OSDMap *nextmap = osd->get_map(last_epoch);
  for (;
       last_epoch >= stop;
       last_epoch = first_epoch - 1) {
    OSDMap *lastmap = nextmap;
    vector<int> tup, tacting;
    lastmap->pg_to_up_acting_osds(get_pgid(), tup, tacting);
    
    // calc first_epoch, first_map
    for (first_epoch = last_epoch; first_epoch > stop; first_epoch--) {
      nextmap = osd->get_map(first_epoch-1);
      vector<int> t;
      nextmap->pg_to_acting_osds(get_pgid(), t);
      if (t != tacting)
	break;
    }

    Interval &i = past_intervals[first_epoch];
    i.first = first_epoch;
    i.last = last_epoch;
    i.up.swap(tup);
    i.acting.swap(tacting);
    if (i.acting.size()) {
      i.maybe_went_rw = 
	(lastmap->get_up_thru(i.acting[0]) >= first_epoch &&
	 lastmap->get_up_from(i.acting[0]) <= first_epoch) ||
	(first_epoch == info.history.epoch_created);
      dout(10) << "generate_past_intervals " << i
	       << " : primary up " << lastmap->get_up_from(i.acting[0])
	       << "-" << lastmap->get_up_thru(i.acting[0])
	       << dendl;
    } else {
      i.maybe_went_rw = false;
      dout(10) << "generate_past_intervals " << i << " : empty" << dendl;
    }
  }
}

/*
 * Trim past_intervals.
 *
 * This gets rid of all the past_intervals that happened before last_epoch_clean.
 */
void PG::trim_past_intervals()
{
  std::map<epoch_t,Interval>::iterator pif = past_intervals.begin();
  std::map<epoch_t,Interval>::iterator end = past_intervals.end();
  while (pif != end) {
    if (pif->second.last >= info.history.last_epoch_clean)
      return;
    dout(10) << __func__ << ": trimming " << pif->second << dendl;
    past_intervals.erase(pif++);
  }
}



// true if the given map affects the prior set
bool PG::prior_set_affected(PgPriorSet &prior, const OSDMap *osdmap) const
{
  for (set<int>::iterator p = prior.cur.begin();
       p != prior.cur.end();
       ++p)
  {
    int o = *p;

    // did someone in the prior set go down?
    if (osdmap->is_down(o) && prior.down.count(o) == 0) {
      dout(10) << "prior_set_affected: osd" << o << " now down" << dendl;
      return true;
    }

    // If someone in the prior set is marked as lost, it would also have to be
    // marked as down. So don't check for newly lost osds here.
  }

  // did someone in the prior down set go up?
  for (set<int>::iterator p = prior.down.begin();
       p != prior.down.end();
       ++p)
  {
    int o = *p;

    if (osdmap->is_up(o)) {
      dout(10) << "prior_set_affected: osd" << *p << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap->exists(o)) {
      dout(10) << "prior_set_affected: osd" << o << " no longer exists" << dendl;
      return true;
    }
      
    const osd_info_t& pinfo(osdmap->get_info(o));
    if (pinfo.lost_at > pinfo.up_from) {
      set<int>::const_iterator pl = prior.lost.find(o);
      if (pl == prior.lost.end()) {
	dout(10) << "prior_set_affected: osd" << o << " now lost" << dendl;
	return true;
      }
    }
  }

  // did a significant osd's up_thru change?
  for (map<int,epoch_t>::const_iterator p = prior.up_thru.begin();
       p != prior.up_thru.end();
       ++p)
    if (p->second != osdmap->get_up_thru(p->first)) {
      dout(10) << "prior_set_affected: primary osd" << p->first
	       << " up_thru " << p->second
	       << " -> " << osdmap->get_up_thru(p->first) 
	       << dendl;
      return true;
    }

  return false;
}

bool PG::adjust_need_up_thru(const OSDMap *osdmap)
{
  epoch_t up_thru = osd->osdmap->get_up_thru(osd->whoami);
  if (need_up_thru &&
      up_thru >= info.history.same_acting_since) {
    dout(10) << "adjust_need_up_thru now " << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PG::all_unfound_are_lost(const OSDMap* osdmap) const
{
  assert(is_primary());

  set<int>::const_iterator peer = might_have_unfound.begin();
  set<int>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    const osd_info_t &osd_info(osdmap->get_info(*peer));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  dout(10) << "all_unfound_are_lost all of might_have_unfound " << might_have_unfound 
	   << " are marked lost!" << dendl;
  return true;
}

/* Mark an object as lost
 */
void PG::mark_obj_as_lost(ObjectStore::Transaction& t,
			  const sobject_t &lost_soid)
{
  // Wake anyone waiting for this object. Now that it's been marked as lost,
  // we will just return an error code.
  map<sobject_t, list<class Message*> >::iterator wmo =
    waiting_for_missing_object.find(lost_soid);
  if (wmo != waiting_for_missing_object.end()) {
    osd->take_waiters(wmo->second);
  }

  // Tell the object store that this object is lost.
  bufferlist b1;
  int r = osd->store->getattr(coll, lost_soid, OI_ATTR, b1);

  object_locator_t oloc;
  oloc.clear();
  oloc.pool = info.pgid.pool();
  object_info_t oi(lost_soid, oloc);

  if (r >= 0) {
    // Some version of this lost object exists in our filestore.
    // So, we can fetch its attributes and preserve most of them.
    oi = object_info_t(b1);
  }
  else {
    // The object doesn't exist in the filestore yet. Make sure that
    // we create it there.
    t.touch(coll, lost_soid);
  }

  oi.lost = true;
  oi.version = info.last_update;
  bufferlist b2;
  oi.encode(b2);
  t.setattr(coll, lost_soid, OI_ATTR, b2);
}

/* Mark all unfound objects as lost.
 */
void PG::mark_all_unfound_as_lost(ObjectStore::Transaction& t)
{
  dout(3) << __func__ << dendl;

  dout(30) << __func__  << ": log before:\n";
  log.print(*_dout);
  *_dout << dendl;

  utime_t mtime = g_clock.now();
  eversion_t old_last_update = info.last_update;
  info.last_update.epoch = osd->osdmap->get_epoch();
  map<sobject_t, Missing::item>::iterator m = missing.missing.begin();
  map<sobject_t, Missing::item>::iterator mend = missing.missing.end();
  while (m != mend) {
    const sobject_t &soid(m->first);
    if (missing_loc.find(soid) != missing_loc.end()) {
      // We only care about unfound objects
      ++m;
      continue;
    }

    // Add log entry
    info.last_update.version++;
    Log::Entry e(Log::Entry::LOST, soid, info.last_update,
		 m->second.need, osd_reqid_t(), mtime);
    log.add(e);

    // Object store stuff
    mark_obj_as_lost(t, soid);

    // Remove from missing set
    missing.got(m++);
  }

  dout(30) << __func__ << ": log after:\n";
  log.print(*_dout);
  *_dout << dendl;

  if (missing.num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
    write_info(t);
  }

  // Send out the PG log to all replicas
  // So that they know what is lost
  share_pg_log(old_last_update);
}

void PG::build_prior(std::auto_ptr<PgPriorSet> &prior_set)
{
  if (1) {
    // sanity check
    for (map<int,Info>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 it++) {
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }
  stringstream out;
  prior_set.reset(new PgPriorSet(osd->whoami,
				 *osd->osdmap,
				 past_intervals,
				 up,
				 acting,
				 info,
				 this));
  dout(10) << out << dendl;
  PgPriorSet &prior(*prior_set.get());
				 
  // take note that we care about the primary's up_thru.  if it
  // changes later, it will affect our prior_set, and we'll want
  // to rebuild it!
  if (prior.crashed) {
    state_set(PG_STATE_CRASHED);
  }
  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (prior.some_down) {
    for (vector<Interval>::iterator i = prior.inter_up_thru.begin();
	 i != prior.inter_up_thru.end();
	 ++i) {
      OSDMap *lastmap = osd->get_map(i->last);
      prior.up_thru[i->acting[0]] = lastmap->get_up_thru(i->acting[0]);
    }
  }
  
  // NOTE: we can skip the up_thru check if this is a new PG and there
  // were no prior intervals.
  if (info.history.epoch_created < info.history.same_acting_since &&
      osd->osdmap->get_up_thru(osd->whoami) < info.history.same_acting_since) {
    dout(10) << "up_thru " << osd->osdmap->get_up_thru(osd->whoami)
	     << " < same_since " << info.history.same_acting_since
	     << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    dout(10) << "up_thru " << osd->osdmap->get_up_thru(osd->whoami)
	     << " >= same_since " << info.history.same_acting_since
	     << ", all is well" << dendl;
  }
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  have_master_log = false;
  stray_set.clear();
  peer_log_requested.clear();
  peer_backlog_requested.clear();
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
  log.reset_recovery_pointers();

  scrub_reserved_peers.clear();
  osd->recovery_wq.dequeue(this);
  osd->snap_trim_wq.dequeue(this);
}

bool PG::choose_acting(int newest_update_osd) const
{
  vector<int> want;

  map<int, Info> all_info(peer_info.begin(), peer_info.end());
  all_info[osd->whoami] = info;

  const Info &best_info = all_info.find(newest_update_osd)->second;
  dout(10) << "choose_acting best_info is " << best_info
	   << " from osd" << newest_update_osd << dendl;
  for (vector<int>::const_iterator i = up.begin();
       i != up.end();
       ++i) {
    const Info &cur_info = all_info.find(*i)->second;
    if (best_info.log_tail <= cur_info.last_update || best_info.log_backlog) {
      // Can be brought up to date without stopping to generate a backlog
      want.push_back(*i);
      dout(10) << " osd" << *i << " (up) accepted" << dendl;
    } else {
      dout(10) << " osd" << *i << " (up) REJECTED" << dendl;
    }
  }

  for (map<int, Info>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (want.size() >= osd->osdmap->get_pg_size(info.pgid))
      break;

    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i->first);
    if (up_it != up.end())
      continue;

    if (best_info.log_tail <= i->second.last_update || best_info.log_backlog) {
      // Can be brought up to date without stopping to generate a backlog
      want.push_back(i->first);
      dout(10) << " osd" << i->first << " (stray) accepted" << dendl;
    } else {
      dout(10) << " osd" << i->first << " (stray) REJECTED" << dendl;
    }
  }

  if (want != acting) {
    dout(10) << "choose_acting  want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;
    if (want == up) {
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid, empty);
    } else
      osd->queue_want_pg_temp(info.pgid, want);
    return false;
  }
  dout(10) << "choose_acting want " << want << " (== acting)" << dendl;
  return true;
}

bool PG::choose_log_location(const PgPriorSet &prior_set,
			     bool &need_backlog,
			     bool &wait_on_backlog,
			     int &pull_from,
			     eversion_t &newest_update,
			     eversion_t &oldest_update) const
{
  pull_from = -1;
  const Info *best_info = NULL;
  need_backlog = false;
  wait_on_backlog = false;
  map<int, Info> all_info(peer_info.begin(), peer_info.end());
  all_info[osd->whoami] = info;

  for (map<int, Info>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (prior_set.cur.find(i->first) == prior_set.cur.end()) {
      dout(10) << "osd" << i->first << " not in current prior set, skipping" << dendl;
      continue;
    }

    // We always want the latest last update, but we prefer one that
    // has a backlog. If last update and backlog status are the same,
    // prefer the one with the longer log.
    if (!best_info ||
	i->second.last_update > best_info->last_update ||
	((i->second.last_update == best_info->last_update) &&
	 (((i->second.log_tail < best_info->log_tail &&
	   (i->second.log_backlog || !best_info->log_backlog))) ||
	  (i->second.log_backlog && !best_info->log_backlog)))) {
      best_info = &(i->second);
      pull_from = i->first;
      newest_update = i->second.last_update;
    }
    if (oldest_update > i->second.last_update) {
      oldest_update = i->second.last_update;
    }
  }

  dout(10) << "choose_log_location newest_update " << newest_update
	   << " on osd" << pull_from << dendl;

  if (!choose_acting(pull_from)) {
    return false;
  }

  for (vector<int>::const_iterator it = ++acting.begin();
       it != acting.end();
       ++it) {
    const Info &pi = peer_info.find(*it)->second;
    if (best_info->log_tail > pi.last_update) {
      wait_on_backlog = true;
      need_backlog = true;
    }
  }

  for (vector<int>::const_iterator it = up.begin();
       it != up.end();
       ++it) {
    if (*it == osd->whoami) continue;
    const Info &pi = peer_info.find(*it)->second;

    vector<int>::const_iterator acting_it = find(acting.begin(), acting.end(), *it);
    if (acting_it != acting.end())
      continue;

    if (pi.last_update < best_info->log_tail) {
      dout(10) << "must generate backlog for up but !acting peer osd" << *it
	       << " whose last_update " << pi.last_update
	       << " < best_info->tail " << best_info->log_tail << dendl;
      need_backlog = true;
    }
  }

  // check our own info -- we aren't in peer_info
  if (best_info->log_tail > info.last_update) {
    wait_on_backlog = true;
    need_backlog = true;
  }

  // do i need a backlog due to mis-trimmed log?  (compensate for past(!) bugs)
  if (info.last_complete < info.log_tail && !info.log_backlog) {
    dout(10) << "must generate backlog because my last_complete " << info.last_complete
	     << " < log.tail " << info.log_tail << " and no backlog" << dendl;
    need_backlog = true;
    wait_on_backlog = true;
  }
  for (vector<int>::const_iterator it = ++acting.begin();
       it != acting.end();
       ++it) {
    const Info& pi = peer_info.find(*it)->second;
    if (pi.last_complete < pi.log_tail && !pi.log_backlog &&
	pi.last_complete < info.log_tail) {
      dout(10) << "must generate backlog for replica peer osd" << *it
	       << " who has a last_complete " << pi.last_complete
	       << " < their log.tail " << pi.log_tail << " and no backlog" << dendl;
      need_backlog = true;
      wait_on_backlog = true;
      break;
    }
  }

  dout(10) << "choose_log_location"
	   << (need_backlog ? " need_backlog" : "")
	   << (wait_on_backlog ? " wait_on_backlog" : "")
	   << dendl;
  return true;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive Missing from each OSD in might_have_unfound, we
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
  std::map<epoch_t,Interval>::const_reverse_iterator p = past_intervals.rbegin();
  std::map<epoch_t,Interval>::const_reverse_iterator end = past_intervals.rend();
  for (; p != end; ++p) {
    const Interval &interval(p->second);
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
  for (map<int,Info>::iterator p = peer_info.begin();
       p != peer_info.end();
       p++)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

struct C_PG_ActivateCommitted : public Context {
  PG *pg;
  epoch_t epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e) : pg(p), epoch(e) {}
  void finish(int r) {
    pg->_activate_committed(epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t, list<Context*>& tfin,
		  map< int, map<pg_t,Query> >& query_map,
		  map<int, MOSDPGInfo*> *activator_map)
{
  assert(!is_active());
  // -- crash recovery?
  if (is_crashed()) {
    replay_until = g_clock.now();
    replay_until += g_conf.osd_replay_window;
    dout(10) << "crashed, allowing op replay for " << g_conf.osd_replay_window
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);
    osd->replay_queue_lock.Lock();
    osd->replay_queue.push_back(pair<pg_t,utime_t>(info.pgid, replay_until));
    osd->replay_queue_lock.Unlock();
  }

  // twiddle pg state
  state_set(PG_STATE_ACTIVE);
  state_clear(PG_STATE_STRAY);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_PEERING);
  state_clear(PG_STATE_CRASHED);
  if (is_primary() && 
      osd->osdmap->get_pg_size(info.pgid) != acting.size())
    state_set(PG_STATE_DEGRADED);
  else
    state_clear(PG_STATE_DEGRADED);

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

  assert(info.last_complete >= log.tail || log.backlog);

  need_up_thru = false;

  // if we are building a backlog, cancel it!
  if (up == acting)
    osd->cancel_generate_backlog(this);

  // write pg info, log
  write_info(t);
  write_log(t);

  // clean up stray objects
  clean_up_local(t); 

  // find out when we commit
  tfin.push_back(new C_PG_ActivateCommitted(this, info.history.same_acting_since));
  
  // initialize snap_trimq
  if (is_primary()) {
    snap_trimq = pool->cached_removed_snaps;
    snap_trimq.subtract(info.purged_snaps);
    dout(10) << "activate - snap_trimq " << snap_trimq << dendl;
    if (!snap_trimq.empty() && is_clean())
      queue_snap_trim();
  }

  // Check local snaps
  adjust_local_snaps(t, info.purged_snaps);

  // init complete pointer
  if (missing.num_missing() == 0 &&
      info.last_complete != info.last_update) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
  }

  if (info.last_complete == info.last_update) {
    dout(10) << "activate - complete" << dendl;
    log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    log.complete_to = log.log.begin();
    while (log.complete_to->version <= info.last_complete)
      log.complete_to++;
    assert(log.complete_to != log.log.end());
    log.last_requested = eversion_t();
    dout(10) << "activate -     complete_to = " << log.complete_to->version << dendl;
    if (is_primary()) {
      dout(10) << "activate - starting recovery" << dendl;
      osd->queue_for_recovery(this);
      if (have_unfound())
	discover_all_missing(query_map);
    }
  }

  // if primary..
  if (is_primary()) {
    // start up replicas
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_info.count(peer));
      PG::Info& pi = peer_info[peer];

      MOSDPGLog *m = 0;

      dout(10) << "activate peer osd" << peer << " " << pi << dendl;

      bool need_old_log_entries = pi.log_tail > pi.last_complete && !pi.log_backlog;

      if (pi.last_update == info.last_update && !need_old_log_entries) {
        // empty log
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd" << peer << " is up to date, queueing in pending_activators" << dendl;
	  if (activator_map->count(peer) == 0)
	    (*activator_map)[peer] = new MOSDPGInfo(osd->osdmap->get_epoch());
	  (*activator_map)[peer]->pg_info.push_back(info);
	} else {
	  dout(10) << "activate peer osd" << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(osd->osdmap->get_epoch(), info);
	}
      } 
      else {
	m = new MOSDPGLog(osd->osdmap->get_epoch(), info);
	if (need_old_log_entries) {
	  // the replica's tail is after it's last_complete and it has no backlog.
	  // ick, this shouldn't normally happen.  but we can compensate!
	  dout(10) << "activate peer osd" << peer << " has last_complete < log tail and no backlog, compensating" << dendl;
	  if (log.tail >= pi.last_complete) {
	    // _our_ log is sufficient, phew!
	    m->log.copy_after(log, pi.last_complete);
	  } else {
	    assert(log.backlog);
	    m->log = log;
	  }
	} else if (log.tail > pi.last_update) {
	  // our tail is too new; send the full backlog.
	  assert(log.backlog);
	  m->log = log;
	} else {
	  // send new stuff to append to replicas log
	  assert(info.last_update > pi.last_update);
	  m->log.copy_after(log, pi.last_update);
	}
      }

      Missing& pm = peer_missing[peer];

      // update local version of peer's missing list!
      if (m) {
        eversion_t plu = pi.last_update;
        for (list<Log::Entry>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             p++) 
          if (p->version > plu)
            pm.add_event(*p);
      }
      
      if (m) {
	dout(10) << "activate peer osd" << peer << " sending " << m->log << " " << m->missing << dendl;
	//m->log.print(cout);
	osd->cluster_messenger->send_message(m, osd->osdmap->get_cluster_inst(peer));
      }

      // peer now has 
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd" << peer << " already uptodate, " << pi << dendl;
      } else {
        dout(10) << "activate peer osd" << peer << " " << pi
                 << " missing " << pm << dendl;
      }
    }

    // all clean?
    if (is_all_uptodate()) 
      finish_recovery(t, tfin);
    else {
      dout(10) << "activate not all replicas are uptodate, queueing recovery" << dendl;
      osd->queue_for_recovery(this);
    }

    update_stats();
  }

  // waiters
  if (!is_replay()) {
    osd->take_waiters(waiting_for_active);
  }
}


void PG::replay_queued_ops()
{
  assert(is_replay() && is_active() && !is_crashed());
  eversion_t c = info.last_update;
  list<Message*> replay;
  dout(10) << "replay_queued_ops" << dendl;
  state_clear(PG_STATE_REPLAY);

  for (map<eversion_t,MOSDOp*>::iterator p = replay_queue.begin();
       p != replay_queue.end();
       p++) {
    if (p->first.version != c.version+1) {
      dout(10) << "activate replay " << p->first
	       << " skipping " << c.version+1 - p->first.version 
	       << " ops"
	       << dendl;      
      c = p->first;
    }
    dout(10) << "activate replay " << p->first << " " << *p->second << dendl;
    replay.push_back(p->second);
  }
  replay_queue.clear();
  osd->take_waiters(replay);
  osd->take_waiters(waiting_for_active);
  state_clear(PG_STATE_REPLAY);
  update_stats();
}

void PG::_activate_committed(epoch_t e)
{
  if (e < info.history.same_acting_since) {
    dout(10) << "_activate_committed " << e << ", that was an old interval" << dendl;
    return;
  }

  if (is_primary()) {
    peer_activated.insert(osd->whoami);
    dout(10) << "_activate_committed " << e << " peer_activated now " << peer_activated 
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_acting_since " << info.history.same_acting_since << dendl;
    if (peer_activated.size() == acting.size())
      all_activated_and_committed();
  } else {
    dout(10) << "_activate_committed " << e << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(osd->osdmap->get_epoch());
    PG::Info i = info;
    i.history.last_epoch_started = e;
    m->pg_info.push_back(i);
    osd->cluster_messenger->send_message(m, osd->osdmap->get_cluster_inst(acting[0]));
  }
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

  info.history.last_epoch_started = osd->osdmap->get_epoch();
  share_pg_info();

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  write_info(*t);
  int tr = osd->store->queue_transaction(&osr, t);
  assert(tr == 0);
}

void PG::queue_snap_trim()
{
  if (osd->snap_trim_wq.queue(this))
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
  state_set(PG_STATE_SCRUBBING);
  osd->scrub_wq.queue(this);
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

void PG::finish_recovery(ObjectStore::Transaction& t, list<Context*>& tfin)
{
  dout(10) << "finish_recovery" << dendl;
  state_set(PG_STATE_CLEAN);
  assert(info.last_complete == info.last_update);

  // NOTE: this is actually a bit premature: we haven't purged the
  // strays yet.
  info.history.last_epoch_clean = osd->osdmap->get_epoch();
  share_pg_info();

  clear_recovery_state();

  write_info(t);

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  tfin.push_back(finish_sync_event);
}

void PG::_finish_recovery(Context *c)
{
  osd->map_lock.get_read();  // avoid race with advance_map, etc..
  lock();
  if (c == finish_sync_event) {
    dout(10) << "_finish_recovery" << dendl;
    finish_sync_event = 0;
    purge_strays();

    update_stats();

    if (state_test(PG_STATE_INCONSISTENT)) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      queue_scrub();
    } else if (log.backlog) {
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      drop_backlog();
      write_info(*t);
      write_log(*t);
      int tr = osd->store->queue_transaction(&osr, t);
      assert(tr == 0);
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  osd->map_lock.put_read();
  unlock();
  put();
}

void PG::start_recovery_op(const sobject_t& soid)
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
  osd->start_recovery_op(this, soid);
}

void PG::finish_recovery_op(const sobject_t& soid, bool dequeue)
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
  osd->finish_recovery_op(this, soid, dequeue);
}


void PG::defer_recovery()
{
  osd->defer_recovery(this);
}

void PG::clear_recovery_state() 
{
  dout(10) << "clear_recovery_state" << dendl;

  log.reset_recovery_pointers();
  finish_sync_event = 0;

  sobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    finish_recovery_op(soid, true);
  }

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
  
  for (set<int>::iterator p = stray_set.begin();
       p != stray_set.end();
       p++) {
    if (osd->osdmap->is_up(*p)) {
      dout(10) << "sending PGRemove to osd" << *p << dendl;
      osd->queue_for_removal(*p, info.pgid);
      stray_purged.insert(*p);
    } else {
      dout(10) << "not sending PGRemove to down osd" << *p << dendl;
    }
    peer_info.erase(*p);
  }

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_backlog_requested.clear();
  peer_missing_requested.clear();
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
    pg_stats_valid = true;
    pg_stats_stable = info.stats;
    pg_stats_stable.state = state;
    pg_stats_stable.up = up;
    pg_stats_stable.acting = acting;

    pg_stats_stable.log_size = ondisklog.length();
    pg_stats_stable.ondisk_log_size = ondisklog.length();
    pg_stats_stable.log_start = log.tail;
    pg_stats_stable.ondisk_log_start = log.tail;

    pg_stats_stable.num_object_copies = pg_stats_stable.num_objects * osd->osdmap->get_pg_size(info.pgid);

    if (is_degraded())
      pg_stats_stable.num_objects_degraded =
	pg_stats_stable.num_objects * (osd->osdmap->get_pg_size(info.pgid) - acting.size());
    else
      pg_stats_stable.num_objects_degraded = 0;
    if (!is_clean() && is_active()) {
      pg_stats_stable.num_objects_missing_on_primary = missing.num_missing();
      int degraded = missing.num_missing();
      for (unsigned i=1; i<acting.size(); i++) {
	assert(peer_missing.count(acting[i]));
	degraded += peer_missing[acting[i]].num_missing();
      }
      pg_stats_stable.num_objects_degraded += degraded;

      pg_stats_stable.num_objects_unfound = get_num_unfound();
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


void PG::write_info(ObjectStore::Transaction& t)
{
  // pg state
  bufferlist infobl;
  __u8 struct_v = 3;
  ::encode(struct_v, infobl);
  ::encode(info, infobl);
  dout(20) << "write_info info " << infobl.length() << dendl;
  t.collection_setattr(coll, "info", infobl);
 
  // potentially big stuff
  bufferlist bigbl;
  ::encode(past_intervals, bigbl);
  ::encode(snap_collections, bigbl);
  dout(20) << "write_info bigbl " << bigbl.length() << dendl;
  t.truncate(coll_t::META_COLL, biginfo_oid, 0);
  t.write(coll_t::META_COLL, biginfo_oid, 0, bigbl.length(), bigbl);

  dirty_info = false;
}

void PG::write_log(ObjectStore::Transaction& t)
{
  dout(10) << "write_log" << dendl;

  // assemble buffer
  bufferlist bl;

  // build buffer
  ondisklog.tail = 0;
  ondisklog.block_map.clear();
  for (list<Log::Entry>::iterator p = log.log.begin();
       p != log.log.end();
       p++) {
    uint64_t startoff = bl.length();

    bufferlist ebl(sizeof(*p)*2);
    ::encode(*p, ebl);
    __u32 crc = ebl.crc32c(0);
    ::encode(ebl, bl);
    ::encode(crc, bl);
    uint64_t endoff = ebl.length();
    if (startoff / 4096 != endoff / 4096) {
      // we reached a new block. *p was the last entry with bytes in previous block
      ondisklog.block_map[startoff] = p->version;
    }
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

void PG::trim(ObjectStore::Transaction& t, eversion_t trim_to)
{
  // trim?
  if (trim_to > log.tail) {
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(t, trim_to);
    info.log_tail = log.tail;
    info.log_backlog = log.backlog;
    trim_ondisklog_to(t, trim_to);
  }
}

void PG::trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v) 
{
  dout(15) << "trim_ondisk_log_to v " << v << dendl;

  map<uint64_t,eversion_t>::iterator p = ondisklog.block_map.begin();
  while (p != ondisklog.block_map.end()) {
    //dout(15) << "    " << p->first << " -> " << p->second << dendl;
    p++;
    if (p == ondisklog.block_map.end() ||
        p->second > v) {  // too far!
      p--;                // back up
      break;
    }
  }
  //dout(15) << "  * " << p->first << " -> " << p->second << dendl;
  if (p == ondisklog.block_map.begin()) 
    return;  // can't trim anything!
  
  // we can trim!
  uint64_t trim = p->first;
  dout(10) << "  " << ondisklog.tail << "~" << ondisklog.length()
	   << " -> " << trim << "~" << (ondisklog.head-trim)
	   << dendl;
  assert(trim >= ondisklog.tail);
  ondisklog.tail = trim;

  // adjust block_map
  while (p != ondisklog.block_map.begin()) 
    ondisklog.block_map.erase(ondisklog.block_map.begin());
  
  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(coll, "ondisklog", blb);

  if (!g_conf.osd_preserve_trimmed_log)
    t.zero(coll_t::META_COLL, log_oid, 0, ondisklog.tail & ~4095);
}

void PG::trim_peers()
{
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  if (pg_trim_to != eversion_t()) {
    for (unsigned i=1; i<acting.size(); i++)
      osd->cluster_messenger->send_message(new MOSDPGTrim(osd->osdmap->get_epoch(), info.pgid,
						  pg_trim_to),
				   osd->osdmap->get_cluster_inst(acting[i]));
  }
}

void PG::add_log_entry(Log::Entry& e, bufferlist& log_bl)
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


void PG::append_log(ObjectStore::Transaction &t, bufferlist& bl,
		    eversion_t log_version)
{
  dout(10) << "append_log " << ondisklog.tail << "~" << ondisklog.length()
	   << " adding " << bl.length() <<  dendl;
 
  // update block map?
  if (ondisklog.head % 4096 < (ondisklog.head + bl.length()) % 4096)
    ondisklog.block_map[ondisklog.head] = log_version;  // log_version is last event in prev. block

  t.write(coll_t::META_COLL, log_oid, ondisklog.head, bl.length(), bl );
  
  ondisklog.head += bl.length();

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(coll, "ondisklog", blb);
  dout(10) << "append_log  now " << ondisklog.tail << "~" << ondisklog.length() << dendl;
}

void PG::read_log(ObjectStore *store)
{
  // load bounds
  ondisklog.tail = ondisklog.head = 0;

  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  bufferlist::iterator p = blb.begin();
  ::decode(ondisklog, p);

  dout(10) << "read_log " << ondisklog.tail << "~" << ondisklog.length() << dendl;

  log.backlog = info.log_backlog;
  log.tail = info.log_tail;
  
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
    
    PG::Log::Entry e;
    bufferlist::iterator p = bl.begin();
    assert(log.log.empty());
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
	osd->clog.error() << info.pgid << " log has out of order entry "
	      << e << " following " << last << "\n";
	reorder = true;
      }

      if (e.version <= log.tail && !log.backlog) {
	dout(20) << "read_log  ignoring entry at " << pos << " below log.tail" << dendl;
	continue;
      }
      if (last.version == e.version.version) {
	dout(0) << "read_log  got dup " << e.version << " (last was " << last << ", dropping that one)" << dendl;
	log.log.pop_back();
	osd->clog.error() << info.pgid << " read_log got dup "
	      << e.version << " after " << last << "\n";
      }
      
      uint64_t endpos = ondisklog.tail + p.get_off();
      if (endpos / 4096 != pos / 4096)
	ondisklog.block_map[pos] = e.version;  // last event in prior block
      log.log.push_back(e);
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	osd->clog.error() << info.pgid << " log has extra data at "
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
      map<eversion_t, Log::Entry> m;
      for (list<Log::Entry>::iterator p = log.log.begin(); p != log.log.end(); p++)
	m[p->version] = *p;
      log.log.clear();
      for (map<eversion_t, Log::Entry>::iterator p = m.begin(); p != m.end(); p++)
	log.log.push_back(p->second);
    }
  }

  log.head = info.last_update;
  log.index();

  // build missing
  if (info.last_complete < info.last_update) {
    dout(10) << "read_log checking for missing items over interval (" << info.last_complete
	     << "," << info.last_update << "]" << dendl;

    set<sobject_t> did;
    for (list<Log::Entry>::reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 i++) {
      if (i->version <= info.last_complete) break;
      if (did.count(i->soid)) continue;
      did.insert(i->soid);
      
      if (i->is_delete()) continue;
      
      bufferlist bv;
      int r = osd->store->getattr(coll, i->soid, OI_ATTR, bv);
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
  }
  dout(10) << "read_log done" << dendl;
}

bool PG::check_log_for_corruption(ObjectStore *store)
{
  OndiskLog bounds;
  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  bufferlist::iterator p = blb.begin();
  ::decode(bounds, p);

  dout(10) << "check_log_for_corruption: tail " << bounds.tail << " head " << bounds.head
	   << " block_map " << bounds.block_map << dendl;

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
      PG::Log::Entry e;
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

void PG::read_state(ObjectStore *store)
{
  bufferlist bl;
  bufferlist::iterator p;
  __u8 struct_v;

  // info
  store->collection_getattr(coll, "info", bl);
  p = bl.begin();
  ::decode(struct_v, p);
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
    store->read(coll_t::META_COLL, biginfo_oid, 0, 0, bl);
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
  }

  try {
    read_log(store);
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
    info.log_backlog = false;

    // Move the corrupt log to a new place and create a new zero-length log entry.
    ObjectStore::Transaction t;
    coll_t cr_log_coll(cr_log_coll_name);
    t.create_collection(cr_log_coll);
    t.collection_add(cr_log_coll, coll_t::META_COLL, log_oid);
    t.collection_remove(coll_t::META_COLL, log_oid);
    t.touch(coll_t::META_COLL, log_oid);
    write_info(t);
    store->apply_transaction(t);
  }
}

coll_t PG::make_snap_collection(ObjectStore::Transaction& t, snapid_t s)
{
  coll_t c(info.pgid, s);
  if (!snap_collections.contains(s)) {
    snap_collections.insert(s);
    dout(10) << "create_snap_collection " << c << ", set now " << snap_collections << dendl;
    bufferlist bl;
    ::encode(snap_collections, bl);
    t.collection_setattr(coll, "snap_collections", bl);
    t.create_collection(c);
  }
  return c;
}

void PG::adjust_local_snaps(ObjectStore::Transaction &t, interval_set<snapid_t> &to_check)
{
  interval_set<snapid_t> to_remove;
  to_remove.intersection_of(snap_collections, to_check);
  while (!to_remove.empty()) {
    snapid_t current = to_remove.range_start();
    coll_t c(info.pgid, current);
    t.remove_collection(c);
    to_remove.erase(current);
    snap_collections.erase(current);
  }
  write_info(t);
}


void PG::take_object_waiters(map<sobject_t, list<Message*> >& m)
{
  for (map<sobject_t, list<Message*> >::iterator it = m.begin();
       it != m.end();
       it++)
    osd->take_waiters(it->second);
  m.clear();
}


// ==========================================================================================
// SCRUB

/*
 * when holding pg and sched_scrub_lock, then the states are:
 *   scheduling:
 *     scrub_reserved = true
 *     scrub_rserved_peers includes whoami
 *     osd->scrub_pending++
 *   scheduling, replica declined:
 *     scrub_reserved = true
 *     scrub_reserved_peers includes -1
 *     osd->scrub_pending++
 *   pending:
 *     scrub_reserved = true
 *     scrub_reserved_peers.size() == acting.size();
 *     pg on scrub_wq
 *     osd->scrub_pending++
 *   scrubbing:
 *     scrub_reserved = false;
 *     scrub_reserved_peers empty
 *     osd->scrub_active++
 */

// returns false if waiting for a reply
bool PG::sched_scrub()
{
  assert(_lock.is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return true;
  }

  // just scrubbed?
  if (info.history.last_scrub_stamp + g_conf.osd_scrub_min_interval > g_clock.now()) {
    dout(20) << "sched_scrub: just scrubbed, skipping" << dendl;
    return true;
  }

  bool ret = false;
  if (!scrub_reserved) {
    assert(scrub_reserved_peers.empty());
    if (osd->inc_scrubs_pending()) {
      dout(20) << "sched_scrub: reserved locally, reserving replicas" << dendl;
      scrub_reserved = true;
      scrub_reserved_peers.insert(osd->whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << "sched_scrub: failed to reserve locally" << dendl;
    }
  }
  if (scrub_reserved) {
    if (scrub_reserve_failed) {
      dout(20) << "sched_scrub: failed, a peer declined" << dendl;
      clear_scrub_reserved();
      scrub_unreserve_replicas();
      ret = true;
    } else if (scrub_reserved_peers.size() == acting.size()) {
      dout(20) << "sched_scrub: success, reserved self and replicas" << dendl;
      queue_scrub();
      ret = true;
    } else {
      // none declined, since scrub_reserved is set
      dout(20) << "sched_scrub: reserved " << scrub_reserved_peers << ", waiting for replicas" << dendl;
    }
  }

  return ret;
}


void PG::sub_op_scrub_map(MOSDSubOp *op)
{
  dout(7) << "sub_op_scrub_map" << dendl;

  if (op->map_epoch < info.history.same_acting_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << op->map_epoch << " < " << info.history.same_acting_since << dendl;
    op->put();
    return;
  }

  int from = op->get_source().num();

  dout(10) << " got osd" << from << " scrub map" << dendl;
  bufferlist::iterator p = op->get_data().begin();
  if (scrub_received_maps.count(from)) {
    ScrubMap incoming;
    incoming.decode(p);
    dout(10) << "from replica " << from << dendl;
    dout(10) << "map version is " << incoming.valid_through << dendl;
    scrub_received_maps[from].merge_incr(incoming);
  } else {
    scrub_received_maps[from].decode(p);
  }

  if (--scrub_waiting_on == 0) {
    assert(last_update_applied == info.last_update);
    osd->scrub_finalize_wq.queue(this);
  }

  op->put();
}

/* 
 * pg lock may or may not be held
 */
void PG::_scan_list(ScrubMap &map, vector<sobject_t> &ls)
{
  dout(10) << "_scan_list scanning " << ls.size() << " objects" << dendl;
  int i = 0;
  for (vector<sobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++) {
    sobject_t poid = *p;

    struct stat st;
    int r = osd->store->stat(coll, poid, &st);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      osd->store->getattrs(coll, poid, o.attrs);
      dout(25) << "_scan_list  " << poid << dendl;
    } else {
      dout(25) << "_scan_list  " << poid << " got " << r << ", skipping" << dendl;
    }
  }
}

void PG::_request_scrub_map(int replica, eversion_t version)
{
    dout(10) << "scrub  requesting scrubmap from osd" << replica << dendl;
    MOSDRepScrub *repscrubop = new MOSDRepScrub(info.pgid, version, 
						osd->osdmap->get_epoch());
    osd->cluster_messenger->send_message(repscrubop,
					 osd->osdmap->get_cluster_inst(replica));
}

void PG::sub_op_scrub_reserve(MOSDSubOp *op)
{
  dout(7) << "sub_op_scrub_reserve" << dendl;

  if (scrub_reserved) {
    dout(10) << "Ignoring reserve request: Already reserved" << dendl;
    op->put();
    return;
  }

  scrub_reserved = osd->inc_scrubs_pending();

  MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
  ::encode(scrub_reserved, reply->get_data());
  osd->cluster_messenger->send_message(reply, op->get_connection());

  op->put();
}

void PG::sub_op_scrub_reserve_reply(MOSDSubOpReply *op)
{
  dout(7) << "sub_op_scrub_reserve_reply" << dendl;

  if (!scrub_reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    op->put();
    return;
  }

  int from = op->get_source().num();
  bufferlist::iterator p = op->get_data().begin();
  bool reserved;
  ::decode(reserved, p);

  if (scrub_reserved_peers.find(from) != scrub_reserved_peers.end()) {
    dout(10) << " already had osd" << from << " reserved" << dendl;
  } else {
    if (reserved) {
      dout(10) << " osd" << from << " scrub reserve = success" << dendl;
      scrub_reserved_peers.insert(from);
    } else {
      /* One decline stops this pg from being scheduled for scrubbing. */
      dout(10) << " osd" << from << " scrub reserve = fail" << dendl;
      scrub_reserve_failed = true;
    }
    sched_scrub();
  }

  op->put();
}

void PG::sub_op_scrub_unreserve(MOSDSubOp *op)
{
  dout(7) << "sub_op_scrub_unreserve" << dendl;

  clear_scrub_reserved();

  op->put();
}

void PG::sub_op_scrub_stop(MOSDSubOp *op)
{
  dout(7) << "sub_op_scrub_stop" << dendl;

  // see comment in sub_op_scrub_reserve
  scrub_reserved = false;

  MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK); 
  osd->cluster_messenger->send_message(reply, op->get_connection());

  op->put();
}

void PG::clear_scrub_reserved()
{
  osd->scrub_wq.dequeue(this);
  scrub_reserved_peers.clear();
  scrub_reserve_failed = false;

  if (scrub_reserved) {
    scrub_reserved = false;
    osd->dec_scrubs_pending();
  }
}

void PG::scrub_reserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting reserve from osd" << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_RESERVE;
    sobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     osd->osdmap->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->cluster_messenger->send_message(subop, osd->osdmap->get_cluster_inst(acting[i]));
  }
}

void PG::scrub_unreserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting unreserve from osd" << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_UNRESERVE;
    sobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     osd->osdmap->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->cluster_messenger->send_message(subop, osd->osdmap->get_cluster_inst(acting[i]));
  }
}

/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 * called while holding pg lock
 */ 
void PG::build_scrub_map(ScrubMap &map)
{
  dout(10) << "build_scrub_map" << dendl;

  map.valid_through = info.last_update;
  epoch_t epoch = info.history.same_acting_since;

  unlock();

  // wait for any writes on our pg to flush to disk first.  this avoids races
  // with scrub starting immediately after trim or recovery completion.
  osr.flush();

  // objects
  vector<sobject_t> ls;
  osd->store->collection_list(coll, ls);

  _scan_list(map, ls);
  lock();

  if (epoch != info.history.same_acting_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    return;
  }


  dout(10) << "PG relocked, finalizing" << dendl;

  // Catch up
  ScrubMap incr;
  build_inc_scrub_map(incr, map.valid_through);
  map.merge_incr(incr);

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
  vector<sobject_t> ls;
  list<Log::Entry>::iterator p;
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
    } else if (p->is_delete()) {
      map.objects[p->soid];
      map.objects[p->soid].negative = 1;
    }
  }

  _scan_list(map, ls);
  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  // log
  osd->store->read(coll_t(), log_oid, 0, 0, map.logbl);
}

void PG::repair_object(const sobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer)
{
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

    log.last_requested = eversion_t();
  }
  osd->queue_for_recovery(this);
}

/* replica_scrub
 *
 * If msg->scrub_from is not set, replica_scrub calls build_scrubmap to
 * build a complete map (with the pg lock dropped).
 *
 * If msg->scrub_from is set, replica_scrub sets finalizing_scrub.
 * Similarly to scrub, if last_update_applied is behind info.last_update
 * replica_scrub returns to be requeued by sub_op_modify_applied.
 * replica_scrub then builds an incremental scrub map with the 
 * pg lock held.
 */
void PG::replica_scrub(MOSDRepScrub *msg)
{
  assert(!active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_acting_since) {
    if (finalizing_scrub) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      finalizing_scrub = 0;
    } else {
      dout(10) << "replica_scrub discarding old replica_scrub from "
	       << msg->map_epoch << " < " << info.history.same_acting_since 
	       << dendl;
    }
    msg->put();
    return;
  }

  ScrubMap map;
  if (msg->scrub_from > eversion_t()) {
    if (finalizing_scrub) {
      assert(last_update_applied == info.last_update);
    } else {
      finalizing_scrub = 1;
      if (last_update_applied != info.last_update) {
	active_rep_scrub = msg;
	return;
      }
    }
    build_inc_scrub_map(map, msg->scrub_from);
    finalizing_scrub = 0;
  } else {
    build_scrub_map(map);
  }

  if (msg->map_epoch < info.history.same_acting_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    msg->put();
    return;
  }

  vector<OSDOp> scrub(1);
  scrub[0].op.op = CEPH_OSD_OP_SCRUB_MAP;
  sobject_t poid;
  eversion_t v;
  osd_reqid_t reqid;
  MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
				   msg->map_epoch, osd->get_tid(), v);
  ::encode(map, subop->get_data());
  subop->ops = scrub;

  osd->cluster_messenger->send_message(subop, msg->get_connection());

  msg->put();
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * Once the initial scrub has completed and the requests have gone out to 
 * replicas for maps, finalizing_scrub is set.  scrub_waiting_on is set to 
 * the number of maps outstanding (active.size()).
 *
 * If last_update_applied is behind the head of the log, scrub returns to be
 * requeued by op_applied.
 *
 * Once last_update_applied == info.last_update, scrub catches itself up and
 * decrements scrub_waiting_on.
 *
 * sub_op_scrub_map similarly decrements scrub_waiting_on for each map received.
 * 
 * Once scrub_waiting_on hits 0 (either in scrub or sub_op_scrub_map) 
 * scrub_finalize is queued.
 * 
 * In scrub_finalize, if any replica maps are too old, new ones are requested,
 * scrub_waiting_on is reset, and scrub_finalize returns to be requeued by
 * sub_op_scrub_map.  If all maps are up to date, scrub_finalize checks 
 * the maps and performs repairs.
 */
void PG::scrub()
{

  osd->map_lock.get_read();
  lock();

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_SCRUBBING);
    clear_scrub_reserved();
    unlock();
    osd->map_lock.put_read();
    return;
  }

  if (!finalizing_scrub) {
    dout(10) << "scrub start" << dendl;
    update_stats();
    scrub_received_maps.clear();
    scrub_epoch_start = info.history.same_acting_since;

    osd->sched_scrub_lock.Lock();
    if (scrub_reserved) {
      --(osd->scrubs_pending);
      assert(osd->scrubs_pending >= 0);
      scrub_reserved = false;
      scrub_reserved_peers.clear();
    }
    ++(osd->scrubs_active);
    osd->sched_scrub_lock.Unlock();

    /* scrub_waiting_on == 0 iff all replicas have sent the requested maps and
     * the primary has done a final scrub (which in turn can only happen if
     * last_update_applied == info.last_update)
     */
    scrub_waiting_on = acting.size();

    // request maps from replicas
    for (unsigned i=1; i<acting.size(); i++) {
      _request_scrub_map(acting[i], eversion_t());
    }
    osd->map_lock.put_read();

    // Unlocks and relocks...
    primary_scrubmap = ScrubMap();
    build_scrub_map(primary_scrubmap);

    if (scrub_epoch_start != info.history.same_acting_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      unlock();
      return;
    }

    finalizing_scrub = true;
    if (last_update_applied != info.last_update) {
      dout(10) << "wait for cleanup" << dendl;
      unlock();
      return;
    }
  } else {
    osd->map_lock.put_read();
  }
    

  dout(10) << "clean up scrub" << dendl;
  assert(last_update_applied == info.last_update);
  
  if (scrub_epoch_start != info.history.same_acting_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    unlock();
    return;
  }
  
  if (primary_scrubmap.valid_through != log.head) {
    ScrubMap incr;
    build_inc_scrub_map(incr, primary_scrubmap.valid_through);
    primary_scrubmap.merge_incr(incr);
  }
  
  --scrub_waiting_on;
  if (scrub_waiting_on == 0) {
    assert(last_update_applied == info.last_update);
    osd->scrub_finalize_wq.queue(this);
  }
  
  unlock();
}

void PG::scrub_clear_state()
{
  assert(_lock.is_locked());
  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_REPAIR);
  update_stats();

  // active -> nothing.
  osd->dec_scrubs_active();

  scrub_unreserve_replicas();
  
  osd->take_waiters(waiting_for_active);

  finalizing_scrub = false;
}

bool PG::scrub_gather_replica_maps() {
  assert(scrub_waiting_on == 0);
  assert(_lock.is_locked());

  for (map<int,ScrubMap>::iterator p = scrub_received_maps.begin();
       p != scrub_received_maps.end();
       p++) {
    
    if (scrub_received_maps[p->first].valid_through != log.head) {
      scrub_waiting_on++;
      // Need to request another incremental map
      _request_scrub_map(p->first, p->second.valid_through);
    }
  }
  
  if (scrub_waiting_on > 0) {
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

void PG::_compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			    map<sobject_t, set<int> > &missing,
			    map<sobject_t, set<int> > &inconsistent,
			    map<sobject_t, int> &authoritative,
			    ostream &errorstream)
{
  map<sobject_t,ScrubMap::object>::const_iterator i;
  map<int, ScrubMap *>::const_iterator j;
  set<sobject_t> master_set;

  // Construct master set
  for (j = maps.begin(); j != maps.end(); j++) {
    for (i = j->second->objects.begin(); i != j->second->objects.end(); i++) {
      master_set.insert(i->first);
    }
  }

  // Check maps against master set and each other
  for (set<sobject_t>::const_iterator k = master_set.begin();
       k != master_set.end();
       k++) {
    map<int, ScrubMap *>::const_iterator auth = maps.end();
    set<int> cur_missing;
    set<int> cur_inconsistent;
    for (j = maps.begin(); j != maps.end(); j++) {
      if (j->second->objects.count(*k)) {
	if (auth == maps.end()) {
	  // Take first osd to have it as authoritative
	  auth = j;
	} else {
	  // Compare 
	  stringstream ss;
	  if (!_compare_scrub_objects(auth->second->objects[*k],
				      j->second->objects[*k],
				      ss)) {
	    cur_inconsistent.insert(j->first);
	    errorstream << info.pgid << " osd" << acting[j->first]
			<< ": soid " << *k << ss.str() << std::endl;
	  }
	}
      } else {
	cur_missing.insert(j->first);
	errorstream << info.pgid
		    << " osd" << acting[j->first] 
		    << " missing " << *k << std::endl;
      }
    }
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

void PG::scrub_finalize() {
  osd->map_lock.get_read();
  lock();
  assert(last_update_applied == info.last_update);

  if (scrub_epoch_start != info.history.same_acting_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    unlock();
    osd->map_lock.put_read();
    return;
  }
  
  if (!scrub_gather_replica_maps()) {
    dout(10) << "maps not yet up to date, sent out new requests" << dendl;
    unlock();
    osd->map_lock.put_read();
    return;
  }
  osd->map_lock.put_read();

  dout(10) << "scrub_finalize has maps, analyzing" << dendl;
  int errors = 0, fixed = 0;
  bool repair = state_test(PG_STATE_REPAIR);
  const char *mode = repair ? "repair":"scrub";
  if (acting.size() > 1) {
    dout(10) << "scrub  comparing replica scrub maps" << dendl;

    stringstream ss;

    // Maps from objects with erros to missing/inconsistent peers
    map<sobject_t, set<int> > missing;
    map<sobject_t, set<int> > inconsistent;

    // Map from object with errors to good peer
    map<sobject_t, int> authoritative;
    map<int,ScrubMap *> maps;

    dout(2) << "scrub   osd" << acting[0] << " has " 
	    << primary_scrubmap.objects.size() << " items" << dendl;
    maps[0] = &primary_scrubmap;
    for (unsigned i=1; i<acting.size(); i++) {
      dout(2) << "scrub   osd" << acting[i] << " has " 
	      << scrub_received_maps[acting[i]].objects.size() << " items" << dendl;
      maps[i] = &scrub_received_maps[acting[i]];
    }

    _compare_scrubmaps(maps, missing, inconsistent, authoritative, ss);

    if (authoritative.size()) {
      ss << info.pgid << " " << mode << " " << missing.size() << " missing, "
	 << inconsistent.size() << " inconsistent objects\n";
      dout(2) << ss.str() << dendl;
      osd->clog.error(ss);
      state_set(PG_STATE_INCONSISTENT);
      if (repair) {
	state_clear(PG_STATE_CLEAN);
	for (map<sobject_t, int>::iterator i = authoritative.begin();
	     i != authoritative.end();
	     i++) {
	  set<int>::iterator j;
	  
	  if (missing.count(i->first)) {
	    for (j = missing[i->first].begin();
		 j != missing[i->first].end(); 
		 j++) {
	      repair_object(i->first, 
			    &maps[i->second]->objects[i->first],
			    acting[*j],
			    acting[i->second]);
	    }
	  }
	  if (inconsistent.count(i->first)) {
	    for (j = inconsistent[i->first].begin(); 
		 j != inconsistent[i->first].end(); 
		 j++) {
	      repair_object(i->first, 
			    &maps[i->second]->objects[i->first],
			    acting[*j],
			    acting[i->second]);
	    }
	  }

	}
      }
    }
  }

  // ok, do the pg-type specific scrubbing
  _scrub(primary_scrubmap, errors, fixed);

  {
    stringstream oss;
    oss << info.pgid << " " << mode << " ";
    if (errors)
      oss << errors << " errors";
    else
      oss << "ok";
    if (repair)
      oss << ", " << fixed << " fixed";
    oss << "\n";
    if (errors)
      osd->clog.error(oss);
    else
      osd->clog.info(oss);
  }

  if (errors == 0 || (repair && (errors - fixed) == 0))
    state_clear(PG_STATE_INCONSISTENT);

  // finish up
  osd->unreg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);
  info.history.last_scrub = info.last_update;
  info.history.last_scrub_stamp = g_clock.now();
  osd->reg_last_pg_scrub(info.pgid, info.history.last_scrub_stamp);

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_info(*t);
    int tr = osd->store->queue_transaction(&osr, t);
    assert(tr == 0);
  }

  scrub_clear_state();
  unlock();

  osd->map_lock.get_read();
  lock();
  if (is_active() && is_primary()) {
    share_pg_info();
  }
  unlock();
  osd->map_lock.put_read();

  dout(10) << "scrub done" << dendl;
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new PG::Info with replicas
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    MOSDPGInfo *m = new MOSDPGInfo(osd->osdmap->get_epoch());
    m->pg_info.push_back(info);
    osd->cluster_messenger->send_message(m, osd->osdmap->get_cluster_inst(peer));
  }
}

/*
 * Share some of this PG's log with some replicas.
 *
 * The log will be shared from the current version (last_update) back to
 * 'oldver.'
 *
 * Updates peer_missing and peer_info.
 */
void PG::share_pg_log(const eversion_t &oldver)
{
  dout(10) << __func__ << dendl;

  assert(is_primary());

  vector<int>::const_iterator a = acting.begin();
  assert(a != acting.end());
  vector<int>::const_iterator end = acting.end();
  while (++a != end) {
    int peer(*a);
    MOSDPGLog *m = new MOSDPGLog(info.last_update.version, info);
    m->log.head = log.head;
    m->log.tail = log.tail;
    for (list<Log::Entry>::const_reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 ++i) {
      if (i->version <= oldver) {
	m->log.tail = i->version;
	break;
      }
      const Log::Entry &entry(*i);
      switch (entry.op) {
	case Log::Entry::LOST: {
	  PG::Missing& pmissing(peer_missing[peer]);
	  pmissing.add(entry.soid, entry.version, eversion_t());
	  break;
        }
	default: {
	  // To do this right, you need to figure out what impact this log
	  // entry has on the peer missing and the peer info
	  assert(0);
	  break;
        }
      }

      m->log.log.push_front(*i);
    }
    PG::Info& pinfo(peer_info[peer]);
    pinfo.last_update = log.head;
    // shouldn't move pinfo.log.tail
    pinfo.last_update = log.head;
    // no change in pinfo.last_complete

    m->missing = missing;
    osd->cluster_messenger->send_message(m, osd->osdmap->
					 get_cluster_inst(peer));
  }
}

void PG::fulfill_info(int from, const Query &query, 
		      pair<int, Info> &notify_info)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type == PG::Query::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(int from, const Query &query)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type != PG::Query::INFO);
  if (query.type == PG::Query::BACKLOG &&
      !log.backlog) {
    assert(0); // generated in the state machine
  } else {
    MOSDPGLog *mlog = new MOSDPGLog(osd->osdmap->get_epoch(), info);
    mlog->missing = missing;
	
    // primary -> other, when building master log
    if (query.type == PG::Query::LOG) {
      dout(10) << " sending info+missing+log since " << query.since
	       << dendl;
      mlog->log.copy_after(log, query.since);
    }
	
    if (query.type == PG::Query::BACKLOG) {
      dout(10) << "sending info+missing+backlog" << dendl;
      assert(log.backlog);
      mlog->log = log;
    } 
    else if (query.type == PG::Query::FULLLOG) {
      dout(10) << " sending info+missing+full log" << dendl;
      mlog->log.copy_non_backlog(log);
    }
	
    dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;
	
    osd->_share_map_outgoing(osd->osdmap->get_cluster_inst(from));
    osd->cluster_messenger->send_message(mlog, 
					 osd->osdmap->get_cluster_inst(from));
  }
}

bool PG::acting_up_affected(const vector<int>& newup, const vector<int>& newacting)
{
  if (acting != newacting || up != newup) {
    return true;
  } else {
    return false;
  }
}

bool PG::old_peering_msg(const epoch_t &msg_epoch)
{
  return (last_warm_restart > msg_epoch);
}

/* Called before initializing peering during advance_map */
void PG::warm_restart(const OSDMap& lastmap, const vector<int>& newup, const vector<int>& newacting)
{
  const OSDMap &osdmap = *osd->osdmap;

  // -- there was a change! --
  kick();

  last_warm_restart = osdmap.get_epoch();

  vector<int> oldacting, oldup;
  int oldrole = get_role();
  int oldprimary = get_primary();
  acting.swap(oldacting);
  up.swap(oldup);

  up = newup;
  acting = newacting;

  int role = osdmap.calc_pg_role(osd->whoami, acting, acting.size());
  set_role(role);

  // did acting, up, primary|acker change?
  if (acting != oldacting || up != oldup) {
    // remember past interval
    PG::Interval& i = past_intervals[info.history.same_acting_since];
    i.first = info.history.same_acting_since;
    i.last = osdmap.get_epoch() - 1;
    i.acting = oldacting;
    i.up = oldup;

    if (oldacting != acting || oldup != up) {
      info.history.same_acting_since = osdmap.get_epoch();
    }
    if (oldup != up) {
      info.history.same_up_since = osdmap.get_epoch();
    }

    if (i.acting.size()) {
      i.maybe_went_rw = 
	(lastmap.get_up_thru(i.acting[0]) >= i.first &&
	 lastmap.get_up_from(i.acting[0]) <= i.first) ||
	i.first == info.history.epoch_created;
    } else {
      i.maybe_went_rw = 0;
    }

    if (oldprimary != get_primary()) {
      info.history.same_primary_since = osdmap.get_epoch();
    }

    dout(10) << " noting past " << i << dendl;
    dirty_info = true;
  }

  dout(10) << " up " << oldup << " -> " << up 
	   << ", acting " << oldacting << " -> " << acting 
	   << ", role " << oldrole << " -> " << role << dendl; 

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_PEERING);  // we'll need to restart peering
  state_clear(PG_STATE_DEGRADED);
  state_clear(PG_STATE_REPLAY);
  state_clear(PG_STATE_CRASHED);

  osd->cancel_generate_backlog(this);

  peer_missing.clear();

  if (is_primary()) {
    if (osdmap.get_pg_size(info.pgid) != acting.size())
      state_set(PG_STATE_DEGRADED);
  }

  // reset primary state?
  if (oldrole == 0 || get_role() == 0)
    clear_primary_state();

    
  // pg->on_*
  /* TODO on_osd_failure does NOTHING! */
#if 0
  for (unsigned i=0; i<oldacting.size(); i++)
    if (osdmap.is_down(oldacting[i]))
      ->on_osd_failure(oldacting[i]);
#endif

  on_change();

  if (deleting) {
    dout(10) << *this << " canceling deletion!" << dendl;
    deleting = false;
    osd->remove_wq.dequeue(this);
  }
    
  if (role != oldrole) {
    // old primary?
    if (oldrole == 0) {
      state_clear(PG_STATE_CLEAN);
      clear_stats();
	
      // take replay queue waiters
      list<Message*> ls;
      for (map<eversion_t,MOSDOp*>::iterator it = replay_queue.begin();
	   it != replay_queue.end();
	   it++)
	ls.push_back(it->second);
      replay_queue.clear();
      osd->take_waiters(ls);
    }

    on_role_change();

    // take active waiters
    osd->take_waiters(waiting_for_active);

    // new primary?
    if (role == 0) {
      // i am new primary
      state_clear(PG_STATE_STRAY);
    } else {
      // i am now replica|stray.  we need to send a notify.
      state_set(PG_STATE_STRAY);
      have_master_log = false;
    }
      
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != oldprimary) {    
      // we need to announce
      state_set(PG_STATE_STRAY);
        
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
  osd->pg_temp_wanted.erase(info.pgid);
  cancel_recovery();

  if (acting.empty() && up.size() && up[0] == osd->whoami) {
    dout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    osd->queue_want_pg_temp(info.pgid, acting);
  }
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const Info &oinfo)
{
  assert(is_replica());
  assert(is_active());
  info.stats = oinfo.stats;

  // Handle changes to purged_snaps
  interval_set<snapid_t> p;
  p.union_of(oinfo.purged_snaps, info.purged_snaps);
  p.subtract(info.purged_snaps);
  info.purged_snaps = oinfo.purged_snaps;
  if (!p.empty()) {
    dout(10) << " purged_snaps " << info.purged_snaps
	     << " -> " << oinfo.purged_snaps
	     << " removed " << p << dendl;
    adjust_local_snaps(t, p);
  }
  write_info(t);
}

unsigned int PG::Missing::num_missing() const
{
  return missing.size();
}

bool PG::Missing::have_missing() const
{
  return !missing.empty();
}

void PG::Missing::swap(Missing& o)
{
  missing.swap(o.missing);
  rmissing.swap(o.rmissing);
}

bool PG::Missing::is_missing(const sobject_t& oid) const
{
  return (missing.find(oid) != missing.end());
}

bool PG::Missing::is_missing(const sobject_t& oid, eversion_t v) const
{
  map<sobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return false;
  const Missing::item &item(m->second);
  if (item.need > v)
    return false;
  return true;
}

eversion_t PG::Missing::have_old(const sobject_t& oid) const
{
  map<sobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return eversion_t();
  const Missing::item &item(m->second);
  return item.have;
}

/*
 * this needs to be called in log order as we extend the log.  it
 * assumes missing is accurate up through the previous log entry.
 */
void PG::Missing::add_next_event(Log::Entry& e)
{
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
    } else if (e.is_backlog()) {
      missing[e.soid].need = e.version;
    } else {
      // not missing, we must have prior_version (if any)
      missing[e.soid] = item(e.version, e.prior_version);
    }
    rmissing[e.version] = e.soid;
  } else
    rm(e.soid, e.version);
}

void PG::Missing::add_event(Log::Entry& e)
{
  if (e.is_update()) {
    if (missing.count(e.soid)) {
      if (missing[e.soid].need >= e.version)
	return;   // already missing same or newer.
      // missing older, revise need
      rmissing.erase(missing[e.soid].need);
      missing[e.soid].need = e.version;
    } else if (e.is_backlog()) {
      missing[e.soid].need = e.version;
    } else
      // not missing => have prior_version (if any)
      missing[e.soid] = item(e.version, e.prior_version);
    rmissing[e.version] = e.soid;
  } else
    rm(e.soid, e.version);
}

void PG::Missing::revise_need(sobject_t oid, eversion_t need)
{
  if (missing.count(oid)) {
    rmissing.erase(missing[oid].need);
    missing[oid].need = need;            // no not adjust .have
  } else {
    missing[oid] = item(need, eversion_t());
  }
  rmissing[need] = oid;
}

void PG::Missing::add(const sobject_t& oid, eversion_t need, eversion_t have)
{
  missing[oid] = item(need, have);
  rmissing[need] = oid;
}

void PG::Missing::rm(const sobject_t& oid, eversion_t v)
{
  if (missing.count(oid) && missing[oid].need <= v) {
    rmissing.erase(missing[oid].need);
    missing.erase(oid);
  }
}

void PG::Missing::got(const sobject_t& oid, eversion_t v)
{
  assert(missing.count(oid));
  assert(missing[oid].need <= v);
  rmissing.erase(missing[oid].need);
  missing.erase(oid);
}

void PG::Missing::got(const std::map<sobject_t, Missing::item>::iterator &m)
{
  rmissing.erase(m->second.need);
  missing.erase(m);
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info
      << " " << pg.up;
  if (pg.acting != pg.up)
    out << "/" << pg.acting;
  out << " r=" << pg.get_role();

  if (pg.is_active() &&
      pg.last_update_ondisk != pg.info.last_update)
    out << " luod=" << pg.last_update_ondisk;

  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  if (pg.log.tail != pg.info.log_tail ||
      pg.log.head != pg.info.last_update)
    out << " (info mismatch, " << pg.log << ")";

  if (pg.log.log.empty()) {
    // shoudl it be?
    if (pg.log.head.version - pg.log.tail.version != 0) {
      out << " (log bound mismatch, empty)";
    }
  } else {
    if (((pg.log.log.begin()->version.version <= pg.log.tail.version) &&  // sloppy check
         !pg.log.backlog) ||
        (pg.log.log.rbegin()->version.version != pg.log.head.version)) {
      out << " (log bound mismatch, actual=["
	  << pg.log.log.begin()->version << ","
	  << pg.log.log.rbegin()->version << "]";
      //out << "len=" << pg.log.log.size();
      out << ")";
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

  if (pg.deleting)
    out << " DELETING";
  out << "]";


  return out;
}

std::ostream& operator<<(std::ostream& oss,
			 const struct PG::PgPriorSet &prior)
{
  oss << "[[ cur=" << prior.cur << ", "
      << "down=" << prior.down << ", "
      << "lost=" << prior.lost << " ]]";
  return oss;
}

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
PG::RecoveryState::Crashed::Crashed(my_context ctx) : my_base(ctx) {
  state_name = "Crashed";
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
PG::RecoveryState::Initial::Initial(my_context ctx) : my_base(ctx) {
  state_name = "Initial";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::Initial::react(const MNotifyRec& notify) {
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(notify.from, notify.info);
  return transit< Primary >();
}

boost::statechart::result 
PG::RecoveryState::Initial::react(const MInfoRec& i) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result 
PG::RecoveryState::Initial::react(const MLogRec& i) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PG::RecoveryState::Initial::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Started-------*/
PG::RecoveryState::Started::Started(my_context ctx) : my_base(ctx) {
  state_name = "Started";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::Started::react(const AdvMap& advmap) {
  dout(10) << "Started advmap" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->acting_up_affected(advmap.newup, advmap.newacting)) {
    dout(10) << "up or acting affected, transitioning to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  return discard_event();
}

void PG::RecoveryState::Started::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------Reset---------*/
PG::RecoveryState::Reset::Reset(my_context ctx) : my_base(ctx) {
  state_name = "Reset";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::Reset::react(const AdvMap& advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Reset advmap" << dendl;
  if (pg->acting_up_affected(advmap.newup, advmap.newacting)) {
    dout(10) << "up or acting affected, calling warm_restart again" << dendl;
    pg->warm_restart(advmap.lastmap, advmap.newup, advmap.newacting);
  }
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Reset::react(const ActMap&) {
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_stray() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg->info);
  }
  return transit< Started >();
}

void PG::RecoveryState::Reset::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-------Start---------*/
PG::RecoveryState::Start::Start(my_context ctx) : my_base(ctx) {
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

void PG::RecoveryState::Start::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Primary--------*/
PG::RecoveryState::Primary::Primary(my_context ctx) : my_base(ctx) {
  state_name = "Started/Primary";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::Primary::react(const BacklogComplete&) {
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "BacklogComplete" << dendl;
  pg->choose_acting(pg->osd->whoami);
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Primary::react(const MNotifyRec& notevt) {
  dout(7) << "handle_pg_notify from osd" << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.info.last_update) {
    dout(10) << *pg << " got dup osd" << notevt.from << " info " << notevt.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(notevt.from, notevt.info);
  }
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Primary::react(const ActMap&) {
  dout(7) << "handle ActMap primary" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->update_stats();
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::Primary::react(const AdvMap& advmap) 
{
  PG *pg = context< RecoveryMachine >().pg;
  OSDMap &osdmap = advmap.osdmap;

  // Remove any downed osds from peer_info
  map<int,PG::Info>::iterator p = pg->peer_info.begin();
  while (p != pg->peer_info.end()) {
    if (!osdmap.is_up(p->first)) {
      dout(10) << " dropping down osd" << p->first << " info " << p->second << dendl;
      pg->peer_missing.erase(p->first);
      pg->peer_info.erase(p++);
    } else
      p++;
  }
  return forward_event();
}

void PG::RecoveryState::Primary::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Peering--------*/
PG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx) {
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
  if (pg->prior_set_affected(*prior_set.get(), &advmap.osdmap)) {
    dout(1) << "Peering, priors_set_affected, going to Reset" << dendl;
    pg->state_clear(PG_STATE_PEERING);
    post_event(advmap);
    return transit< Reset >();
  }
  
  pg->adjust_need_up_thru(&advmap.osdmap);
  
  return forward_event();
}

void PG::RecoveryState::Peering::exit() {
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Active---------*/
PG::RecoveryState::Active::Active(my_context ctx) : my_base(ctx) {
  state_name = "Started/Primary/Active";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_primary());
  dout(10) << "In Active, about to call activate" << dendl;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       *context< RecoveryMachine >().get_context_list(),
	       *context< RecoveryMachine >().get_query_map(),
	       context< RecoveryMachine >().get_info_map());
  assert(pg->is_active());
  dout(10) << "Activate Finished" << dendl;
}

boost::statechart::result 
PG::RecoveryState::Active::react(const AdvMap& advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  if (!pg->pool->newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool->newly_removed_snaps);
    dout(10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
  }
  return forward_event();
}
    
boost::statechart::result 
PG::RecoveryState::Active::react(const ActMap&) {
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_active());
  assert(pg->is_primary());
  pg->check_recovery_op_pulls(pg->osd->osdmap);
	
  if (g_conf.osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->osd->store);

  if (pg->missing.num_missing() > pg->missing_loc.size()) {
    if (pg->all_unfound_are_lost(pg->osd->osdmap)) {
      pg->mark_all_unfound_as_lost(*context< RecoveryMachine >().get_cur_transaction());
    }
  }

  if (!pg->snap_trimq.empty() &&
      pg->is_clean()) {
    dout(10) << "Active: queuing snap trim" << dendl;
    pg->queue_snap_trim();
  }

  if (pg->is_all_uptodate()) {
    dout(10) << "Active: all up to date, going clean" << dendl;
    pg->finish_recovery(*context< RecoveryMachine >().get_cur_transaction(),
			*context< RecoveryMachine >().get_context_list());
  }

  pg->choose_acting(pg->osd->whoami);

  return forward_event();
}

boost::statechart::result 
PG::RecoveryState::Active::react(const MNotifyRec& notevt) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", already have info from that osd, ignoring" 
	     << dendl;
  } else {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", calling proc_replica_info and discover_all_missing"
	     << dendl;
    pg->proc_replica_info(notevt.from, notevt.info);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Active::react(const MInfoRec& infoevt) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());

  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_acting(infoevt.from)) {
    assert(pg->info.history.last_epoch_started < 
	   pg->info.history.same_acting_since);
    assert(infoevt.info.history.last_epoch_started >= 
	   pg->info.history.same_acting_since);
    dout(10) << " peer osd" << infoevt.from << " activated and committed" 
	     << dendl;
    pg->peer_activated.insert(infoevt.from);
  }

  if (pg->peer_activated.size() == pg->acting.size()) {
    pg->all_activated_and_committed();
  }
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::Active::react(const MLogRec& logevt) {
  dout(10) << "searching osd" << logevt.from
           << " log for unfound items" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);
  if (got_missing)
    pg->osd->queue_for_recovery(pg);
  return discard_event();
}

void PG::RecoveryState::Active::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------ReplicaActive-----*/
PG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx) {
  state_name = "Started/ReplicaActive";

  context< RecoveryMachine >().log_enter(state_name);
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  map< int, map< pg_t, Query> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       *context< RecoveryMachine >().get_context_list(),
	       query_map, NULL);
  dout(10) << "Activate Finished" << dendl;
}

boost::statechart::result 
PG::RecoveryState::ReplicaActive::react(const MInfoRec& infoevt) {
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::ReplicaActive::react(const ActMap&) {
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_stray() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg->info);
  }
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::ReplicaActive::react(const MQuery& query) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(query.query.type == Query::MISSING);
  pg->fulfill_log(query.from, query.query);
  return discard_event();
}

void PG::RecoveryState::ReplicaActive::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-------Stray---*/
PG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx), backlog_requested(false) {
  state_name = "Started/Stray";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
}

boost::statechart::result 
PG::RecoveryState::Stray::react(const MLogRec& logevt) {
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg;
  dout(10) << "received log from " << logevt.from << dendl;
  pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		msg->info, msg->log, logevt.from);

  assert(pg->log.tail <= pg->info.last_complete || pg->log.backlog);
  assert(pg->log.head == pg->info.last_update);

  pg->write_info(*context<RecoveryMachine>().get_cur_transaction());
  dout(10) << "activating!" << dendl;
  post_event(Activate());
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Stray::react(const MInfoRec& infoevt) {
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "received info from " << infoevt.from << dendl;

  Log empty_log;
  pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		infoevt.info, empty_log, infoevt.from);

  assert(pg->log.tail <= pg->info.last_complete || pg->log.backlog);
  assert(pg->log.head == pg->info.last_update);

  pg->write_info(*context<RecoveryMachine>().get_cur_transaction());
  dout(10) << "activating!" << dendl;
  post_event(Activate());
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Stray::react(const BacklogComplete&) {
  PG *pg = context< RecoveryMachine >().pg;
  assert(backlog_requested);
  dout(10) << "BacklogComplete" << dendl;
  for (map<int, Query>::iterator i = pending_queries.begin();
       i != pending_queries.end();
       pending_queries.erase(i++)) {
    dout(10) << "sending log to " << i->first << dendl;
    pg->fulfill_log(i->first, i->second);
  }
  backlog_requested = false;
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::Stray::react(const MQuery& query) {
  PG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == Query::BACKLOG) {
    if (!pg->log.backlog) {
      dout(10) << "Stray, need a backlog!" 
	       << dendl;
      pending_queries[query.from] = query.query;
      if (!backlog_requested) {
	dout(10) << "Stray, generating a backlog!" 
		 << dendl;
	backlog_requested = true;
	pg->osd->queue_generate_backlog(pg);
      }
      return discard_event();
    }
  }

  if (query.query.type == Query::INFO) {
    pair<int, Info> notify_info;
    pg->fulfill_info(query.from, query.query, notify_info);
    context< RecoveryMachine >().send_notify(notify_info.first, notify_info.second);
  } else {
    pg->fulfill_log(query.from, query.query);
  }
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::Stray::react(const ActMap&) {
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_stray() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg->info);
  }
  return discard_event();
}

void PG::RecoveryState::Stray::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------GetInfo---------*/
PG::RecoveryState::GetInfo::GetInfo(my_context ctx) : my_base(ctx) 
{
  state_name = "Started/Primary/Peering/GetInfo";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->generate_past_intervals();
  auto_ptr<PgPriorSet> &prior_set = context< Peering >().prior_set;

  if (!prior_set.get())
    pg->build_prior(prior_set);

  if (pg->need_up_thru)
    pg->osd->queue_want_up_thru(pg->info.history.same_acting_since);

  get_infos();
}

void PG::RecoveryState::GetInfo::get_infos()
{
  PG *pg = context< RecoveryMachine >().pg;
  auto_ptr<PgPriorSet> &prior_set = context< Peering >().prior_set;

  for (set<int>::const_iterator it = prior_set->cur.begin();
       it != prior_set->cur.end();
       ++it) {
    int peer = *it;
    if (peer == pg->osd->whoami) {
      continue;
    }
    if (pg->peer_info.count(peer)) {
      dout(10) << " have osd" << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      dout(10) << " already requested info from osd" << peer << dendl;
    } else if (!pg->osd->osdmap->is_up(peer)) {
      dout(10) << " not querying info from down osd" << peer << dendl;
    } else {
      dout(10) << " querying info from osd" << peer << dendl;
      context< RecoveryMachine >().send_query(peer, Query(Query::INFO, pg->info.history));
      peer_info_requested.insert(peer);
    }
  }
  
  if (peer_info_requested.empty() && !prior_set->some_down) {
    post_event(GotInfo());
  }
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  set<int>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end())
    peer_info_requested.erase(p);

  PG *pg = context< RecoveryMachine >().pg;
  epoch_t old_start = pg->info.history.last_epoch_started;
  if (pg->proc_replica_info(infoevt.from, infoevt.info)) {
    // we got something new ...
    auto_ptr<PgPriorSet> &prior_set = context< Peering >().prior_set;
    if (old_start < pg->info.history.last_epoch_started) {
      dout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      pg->build_prior(prior_set);
      get_infos();
    } else {
      // are we done getting everything?
      if (peer_info_requested.empty() && !prior_set->some_down)
	post_event(GotInfo());
    }
  }
  return discard_event();
}

void PG::RecoveryState::GetInfo::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetLog------------*/
PG::RecoveryState::GetLog::GetLog(my_context ctx) : 
  my_base(ctx), newest_update_osd(-1), need_backlog(false),
  wait_on_backlog(false), msg(0)
{
  state_name = "Started/Primary/Peering/GetLog";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  eversion_t newest_update;
  eversion_t oldest_update;
  if (!pg->choose_log_location(*context< Peering >().prior_set.get(),
			       need_backlog,
			       wait_on_backlog,
			       newest_update_osd,
			       newest_update,
			       oldest_update)) {
    post_event(NeedNewMap());
  } else {
    if (need_backlog && !pg->log.backlog) {
      pg->osd->queue_generate_backlog(pg);
    }

    if (newest_update_osd != pg->osd->whoami) {
      dout(10) << " requesting master log from osd" << newest_update_osd << dendl;
      context<RecoveryMachine>().send_query(newest_update_osd,
					    wait_on_backlog ? 
					    Query(Query::BACKLOG, pg->info.history) :
					    Query(Query::LOG, oldest_update, pg->info.history));
    }

    if (pg->log.backlog) {
      wait_on_backlog = false;
    }

    if (!wait_on_backlog && newest_update_osd == pg->osd->whoami) {
      dout(10) << " neither backlog nor master log needed, "
	       << "moving to GetMissing" << dendl;
      post_event(GotLog());
    }
  }
}

boost::statechart::result 
PG::RecoveryState::GetLog::react(const MLogRec& logevt) {
  assert(!msg);
  if (logevt.from != newest_update_osd) {
    dout(10) << "GetLog: discarding log from "
	     << "non-newest_update_osd osd" << logevt.from << dendl;
    return discard_event();
  }
  dout(10) << "GetLog: recieved master log from osd" 
	   << logevt.from << dendl;
  msg = logevt.msg;
  msg->get();
  if (!wait_on_backlog) {
    dout(10) << "GetLog: already have/don't need backlog." 
	     << "moving on to GetMissing" << dendl;
    post_event(GotLog());
  }
  dout(10) << "GetLog: Still need backlog" << dendl;
  return discard_event();
}

boost::statechart::result 
PG::RecoveryState::GetLog::react(const BacklogComplete&) {
  wait_on_backlog = false;
  PG *pg = context< RecoveryMachine >().pg;
  if (msg || newest_update_osd == pg->osd->whoami) {
    dout(10) << "GetLog: already have/don't need master log." 
	     << "moving on to GetMissing" << dendl;
    post_event(GotLog());
  } else {
    dout(10) << "GetLog: Still need master log" << dendl;
  }
  return forward_event();
}

boost::statechart::result 
PG::RecoveryState::GetLog::react(const GotLog&) {
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

void PG::RecoveryState::GetLog::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

PG::RecoveryState::GetLog::~GetLog() {
  if (msg)
    msg->put();
}

/*------WaitActingChange--------*/
PG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx) : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitActingChange";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::WaitActingChange::react(const MLogRec& logevt) {
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

void PG::RecoveryState::WaitActingChange::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetMissing--------*/
PG::RecoveryState::GetMissing::GetMissing(my_context ctx) : my_base(ctx)
{
  state_name = "Started/Primary/Peering/GetMissing";
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  for (vector<int>::iterator i = pg->acting.begin() + 1;
       i != pg->acting.end();
       ++i) {
    const Info& pi = pg->peer_info[*i];

    if (pi.is_empty())
      continue;                                // Does not have the PG yet

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd" << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i];
      pg->search_for_missing(pi, &pg->peer_missing[*i], *i);
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    dout(10) << " requesting missing from osd" << *i << dendl;
    context< RecoveryMachine >().send_query(*i,
      Query(Query::LOG,
	    eversion_t(pi.history.last_epoch_started, 0),
	    pg->info.history));
    peer_missing_requested.insert(*i);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate());
  }
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(*context<RecoveryMachine>().get_cur_transaction(),
		       msg->info, msg->log, msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    post_event(Activate());
  }
  return discard_event();
};

void PG::RecoveryState::GetMissing::exit() {
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------WaitUpThru--------*/
PG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx) : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitUpThru";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const ActMap& am) {
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->need_up_thru) {
    post_event(Activate());
    return discard_event();
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "searching osd" << logevt.from
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

void PG::RecoveryState::WaitUpThru::exit() {
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
  utime_t dur = g_clock.now() - enter_time;
  dout(20) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, g_clock.now() - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*----RecoveryState Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << machine.pg->gen_prefix() 

void PG::RecoveryState::handle_notify(int from, PG::Info& i,
				      RecoveryCtx *rctx)
{
  dout(10) << "handle_notify " << i << " from osd" << from << dendl;
  start_handle(rctx);
  machine.process_event(MNotifyRec(from, i));
  end_handle();
}

void PG::RecoveryState::handle_info(int from, PG::Info& i,
				    RecoveryCtx *rctx)
{
  dout(10) << "handle_info " << i << " from osd" << from << dendl;
  start_handle(rctx);
  machine.process_event(MInfoRec(from, i));
  end_handle();
}

void PG::RecoveryState::handle_log(int from,
				   MOSDPGLog *msg,
				   RecoveryCtx *rctx)
{
  dout(10) << "handle_log " << *msg << " from osd" << from << dendl;
  start_handle(rctx);
  machine.process_event(MLogRec(from, msg));
  end_handle();
}

void PG::RecoveryState::handle_query(int from, const PG::Query& q,
				     RecoveryCtx *rctx)
{
  dout(10) << "handle_query " << q << " from osd" << from << dendl;
  start_handle(rctx);
  machine.process_event(MQuery(from, q));
  end_handle();
}

void PG::RecoveryState::handle_advance_map(OSDMap &osdmap, OSDMap &lastmap,
					   vector<int>& newup, vector<int>& newacting,
					   RecoveryCtx *rctx)
{
  dout(10) << "handle_advance_map " << newup << "/" << newacting << dendl;
  start_handle(rctx);
  machine.process_event(AdvMap(osdmap, lastmap, newup, newacting));
  end_handle();
}

void PG::RecoveryState::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  start_handle(rctx);
  machine.process_event(ActMap());
  end_handle();
}

void PG::RecoveryState::handle_backlog_generated(RecoveryCtx *rctx)
{
  dout(10) << "handle_backlog_generated" << dendl;
  start_handle(rctx);
  machine.process_event(BacklogComplete());
  end_handle();
}

void PG::RecoveryState::handle_loaded(RecoveryCtx *rctx)
{
  dout(10) << "handle_backlog_loaded" << dendl;
  start_handle(rctx);
  machine.process_event(Load());
  end_handle();
}

void PG::RecoveryState::handle_create(RecoveryCtx *rctx)
{
  dout(10) << "handle_create" << dendl;
  start_handle(rctx);
  machine.process_event(Initialize());
  end_handle();
}

/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix (*_dout << pg->gen_prefix() << "PgPriorSet: ")

PG::PgPriorSet::PgPriorSet(int whoami,
			   const OSDMap &osdmap,
			   const map<epoch_t, Interval> &past_intervals,
			   const vector<int> &up,
			   const vector<int> &acting,
			   const PG::Info &info,
			   const PG* pg)
  : crashed(false), pg_down(false), some_down(false), pg(pg)
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
   *  2:   B   alive_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   alive_thru[B]=0
   *  3:   B   alive_thru[B]=2
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
        written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */
  // current up and/or acting nodes, of course.
  for (unsigned i=0; i<up.size(); i++)
    cur.insert(up[i]);
  for (unsigned i=0; i<acting.size(); i++)
    cur.insert(acting[i]);

  // see if i have ever started since joining the pg.  this is important only
  // if we want to exclude lost osds.
  set<int> started_since_joining;
  for (vector<int>::const_iterator q = acting.begin(); q != acting.end(); q++) {
    int o = *q;
    
    for (map<epoch_t,Interval>::const_reverse_iterator p = past_intervals.rbegin();
	 p != past_intervals.rend();
	 p++) {
      const Interval &interval = p->second;
      if (interval.last < info.history.last_epoch_started)
	break;  // we don't care
      if (!interval.maybe_went_rw)
	continue;
      if (std::find(interval.acting.begin(), interval.acting.end(), o)
	  != interval.acting.end())
	started_since_joining.insert(o);
      break;
    }
  }

  dout(10) << "build_prior " << started_since_joining << " have started since joining this pg" << dendl;

  for (map<epoch_t,Interval>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       p++) {
    const Interval &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    int need_down = 0;
    bool any_survived = false;

    // consider UP osds
    for (unsigned i=0; i<interval.up.size(); i++) {
      int o = interval.up[i];

      if (osdmap.is_up(o)) // is up now
	cur.insert(o);
    }

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];

      const osd_info_t *pinfo = 0;
      if (osdmap.exists(o))
	pinfo = &osdmap.get_info(o);

      // if the osd restarted after this interval but is not known to have
      // cleanly survived through this interval, we mark the pg crashed.
      if (pinfo && (pinfo->up_from > interval.last &&
		    !(pinfo->last_clean_first <= interval.first &&
		      pinfo->last_clean_last >= interval.last))) {
	dout(10) << "build_prior  prior osd" << o
	    << " up_from " << pinfo->up_from
	    << " and last clean interval " << pinfo->last_clean_first << "-" << pinfo->last_clean_last
	    << " does not include us" << dendl;
	crashed = true;
      }

      if (osdmap.is_up(o)) {  // is up now
	// include past acting osds if they are up
	cur.insert(o);

	// did any osds survive _this_ interval?
	any_survived = true;
      } else if (!pinfo || pinfo->lost_at > interval.first) {
	down.insert(0);
	if (started_since_joining.size()) {
	  if (pinfo)
	    dout(10) << "build_prior  prior osd" << o
		<< " is down, but marked lost at " << pinfo->lost_at
		<< ", and " << started_since_joining << " have started since joining pg"
		<< dendl;
	  else
	    dout(10) << "build_prior  prior osd" << o
		<< " no longer exists, and " << started_since_joining << " have started since joining pg"
		<< dendl;

	} else {
	  if (pinfo)
	    dout(10) << "build_prior  prior osd" << o
		<< " is down, but marked lost at " << pinfo->lost_at
		<< ", and NO acting osds have started since joining pg, so i may not have any pg state :/"
		<< dendl;
	  else
	    dout(10) << "build_prior  prior osd" << o
		     << " no longer exists, and NO acting osds have started since joining pg, so i may not have any pg state :/"
		     << dendl;
	  need_down++;
	}
      } else {
	dout(10) << "build_prior  prior osd" << o
	    << " is down" << dendl;
	need_down++;
	down.insert(o);
      }
    }

    // if nobody survived this interval, and we may have gone rw,
    // then we need to wait for one of those osds to recover to
    // ensure that we haven't lost any information.
    if (!any_survived && need_down && interval.maybe_went_rw) {
      // fixme: how do we identify a "clean" shutdown anyway?
      dout(10) << "build_prior  " << need_down
	  << " osds possibly went active+rw, no survivors, including" << dendl;
      for (unsigned i=0; i<interval.acting.size(); i++)
	if (osdmap.is_down(interval.acting[i])) {
	  cur.insert(interval.acting[i]);
	  pg_down = true;
	}
      some_down = true;
      inter_up_thru.push_back(interval);
    }

    if (crashed) {
      dout(10) << "build_prior  one of " << interval.acting
	  << " possibly crashed, marking pg crashed" << dendl;
    }
  }

  // Build prior_set.lost
  for (set<int>::const_iterator i = cur.begin();
       i != cur.end(); ++i) {
    int o = *i;
    const osd_info_t& pinfo(osdmap.get_info(o));
    if (pinfo.lost_at > pinfo.up_from) {
      lost.insert(o);
    }
  }

  dout(10) << "build_prior final: cur " << cur << " down " << down << " lost " << lost
	   << " up_thru " << up_thru
	   << " inter_up_thru " << inter_up_thru
	   << (crashed ? " crashed":"")
	   << (pg_down ? " pg_down":"")
	   << (some_down ? " some_down":"")
	   << dendl;
}

