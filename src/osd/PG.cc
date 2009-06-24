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
#include "config.h"
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
#define dout_prefix _prefix(this, osd->whoami, osd->osdmap)
static ostream& _prefix(PG *pg, int whoami, OSDMap *osdmap) {
  return *_dout << dbeginl<< pthread_self() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " " << *pg << " ";
}


/******* PGLog ********/

void PG::Log::copy_after(const Log &other, eversion_t v) 
{
  top = other.top;
  bottom = other.bottom;
  for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       i++) {
    if (i->version <= v) {
      bottom = i->version;
      break;
    }
    assert(i->version > v);
    log.push_front(*i);
  }
}

bool PG::Log::copy_after_unless_divergent(const Log &other, eversion_t split, eversion_t floor) 
{
  assert(split >= other.bottom);
  assert(floor >= other.bottom);
  assert(floor <= split);
  top = bottom = other.top;
  
  /* runs on replica.  split is primary's log.top.  floor is how much they want.
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
  bottom = floor;
  return true;
}

void PG::Log::copy_non_backlog(const Log &other)
{
  if (other.backlog) {
    top = other.top;
    bottom = other.bottom;
    for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
         i != other.log.rend();
         i++) 
      if (i->version > bottom)
        log.push_front(*i);
      else
        break;
  } else {
    *this = other;
  }
}



void PG::IndexedLog::trim(ObjectStore::Transaction& t, eversion_t s) 
{
  if (backlog && s < bottom)
    s = bottom;

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

  // raise bottom?
  if (backlog)
    backlog = false;
  if (bottom < s)
    bottom = s;
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
  if (info.last_update < log.top) {
    dout(10) << "trim_write_ahead (" << info.last_update << "," << log.top << "]" << dendl;
    log.trim_write_ahead(info.last_update);
  } else {
    assert(info.last_update == log.top);
    dout(10) << "trim_write_ahead last_update=top=" << info.last_update << dendl;
  }

}

void PG::proc_replica_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, Missing& omissing, int from)
{
  dout(10) << "proc_replica_log for osd" << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active());

  if (!have_master_log) {
    // merge log into our own log to build master log.  no need to
    // make any adjustments to their missing map; we are taking their
    // log to be authoritative (i.e., their entries are by definitely
    // non-divergent).
    merge_log(t, oinfo, olog, omissing, from);

  } else if (is_acting(from)) {
    // replica.  have master log. 
    // populate missing; check for divergence
    
    /*
      basically what we're doing here is rewinding the remote log,
      dropping divergent entries, until we find something that matches
      our master log.  we then reset last_update to reflect the new
      point up to which missing is accurate.

      later, in activate(), missing will get wound forward again and
      we will send the peer enough log to arrive at the same state.
    */

    list<Log::Entry>::reverse_iterator pp = olog.log.rbegin();
    eversion_t lu = oinfo.last_update;
    while (pp != olog.log.rend()) {
      Log::Entry& oe = *pp;

      // don't continue past the bottom of our log.
      if (oe.version < log.bottom)
	break;

      if (!log.objects.count(oe.soid)) {
        dout(10) << " had " << oe << " new dne : divergent, ignoring" << dendl;
        ++pp;
        continue;
      }
      
      Log::Entry& ne = *log.objects[oe.soid];
      if (ne.version == oe.version) {
	dout(10) << " had " << oe << " new " << ne << " : match, stopping" << dendl;
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
      if (pp != olog.log.rend())
        lu = pp->version;
      else
        lu = olog.bottom;
    }    

    if (lu < oinfo.last_update) {
      dout(10) << " peer osd" << from << " last_update now " << lu << dendl;
      oinfo.last_update = lu;
      if (lu < oinfo.last_complete)
	oinfo.last_complete = lu;
      if (lu < oldest_update) {
        dout(10) << " oldest_update now " << lu << dendl;
        oldest_update = lu;
      }
    }
  }

  peer_info[from] = oinfo;
  dout(10) << " peer osd" << from << " now " << oinfo << dendl;

  search_for_missing(olog, omissing, from);
  peer_missing[from].swap(omissing);
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
      t.remove(info.pgid.to_coll(), oe.soid);
      missing.rm(oe.soid, oe.version);
    }
  }
  return false;
}

void PG::merge_log(ObjectStore::Transaction& t,
		   Info &oinfo, Log &olog, Missing &omissing, int fromosd)
{
  dout(10) << "merge_log " << olog << " from osd" << fromosd
           << " into " << log << dendl;
  bool changed = false;

  if (log.empty() ||
      (olog.bottom > log.top && olog.backlog)) { // e.g. log=(0,20] olog=(40,50]+backlog) 

    if (is_primary()) {
      // we should have our own backlog already; see peer() code where
      // we request this.
    } else {
      // primary should have requested our backlog during peer().
    }
    assert(log.backlog || log.top == eversion_t());

    hash_map<sobject_t, Log::Entry*> old_objects;
    old_objects.swap(log.objects);

    // swap in other log and index
    log.log.swap(olog.log);
    log.index();

    // first, find split point (old log.top) in new log.
    // adjust oldest_updated as needed.
    list<Log::Entry>::iterator p = log.log.end();
    while (p != log.log.begin()) {
      p--;
      if (p->version <= log.top) {
	dout(10) << "merge_log split point is " << *p << dendl;

	if (p->version < log.top && p->version < oldest_update) {
	  dout(10) << "merge_log oldest_update " << oldest_update << " -> "
		   << p->version << dendl;
	  oldest_update = p->version;
	}

	if (p->version == log.top)
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
	t.remove(info.pgid.to_coll(), ne.soid);
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

    info.last_update = log.top = olog.top;
    info.log_bottom = log.bottom = olog.bottom;
    info.log_backlog = log.backlog = olog.backlog;
    if (oinfo.stats.reported < info.stats.reported)   // make sure reported always increases
      oinfo.stats.reported = info.stats.reported;
    info.stats = oinfo.stats;
    changed = true;
  } 

  else {
    // try to merge the two logs?

    // extend on bottom?
    //  this is just filling in history.  it does not affect our
    //  missing set, as that should already be consistent with our
    //  current log.
    // FIXME: what if we have backlog, but they have lower bottom?
    if (olog.bottom < log.bottom && olog.top >= log.bottom && !log.backlog) {
      dout(10) << "merge_log extending bottom to " << olog.bottom
               << (olog.backlog ? " +backlog":"")
	       << dendl;
      list<Log::Entry>::iterator from = olog.log.begin();
      list<Log::Entry>::iterator to;
      for (to = from;
           to != olog.log.end();
           to++) {
        if (to->version > log.bottom)
	  break;
        log.index(*to);
        dout(15) << *to << dendl;
      }
      assert(to != olog.log.end());
      
      // splice into our log.
      log.log.splice(log.log.begin(),
                     olog.log, from, to);
      
      info.log_bottom = log.bottom = olog.bottom;
      info.log_backlog = log.backlog = olog.backlog;
      changed = true;
    }
    
    // extend on top?
    if (olog.top > log.top &&
        olog.bottom <= log.top) {
      dout(10) << "merge_log extending top to " << olog.top << dendl;
      
      // find start point in olog
      list<Log::Entry>::iterator to = olog.log.end();
      list<Log::Entry>::iterator from = olog.log.end();
      eversion_t lower_bound = olog.bottom;
      while (1) {
        if (from == olog.log.begin())
	  break;
        from--;
        dout(20) << "  ? " << *from << dendl;
        if (from->version <= log.top) {
	  dout(20) << "merge_log last shared is " << *from << dendl;
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
	  t.remove(info.pgid.to_coll(), ne.soid);
      }
      
      // move aside divergent items
      list<Log::Entry> divergent;
      while (!log.log.empty()) {
	Log::Entry &oe = *log.log.rbegin();
	if (oe.version <= lower_bound)
	  break;
	dout(10) << "merge_log divergent " << oe << dendl;
	divergent.push_front(oe);
	log.log.pop_back();
      }

      // splice
      log.log.splice(log.log.end(), 
                     olog.log, from, to);
      
      info.last_update = log.top = olog.top;
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
 * process replica's missing map to determine if they have
 * any objects that i need
 */
void PG::search_for_missing(Log &olog, Missing &omissing, int fromosd)
{
  // found items?
  for (map<sobject_t,Missing::item>::iterator p = missing.missing.begin();
       p != missing.missing.end();
       p++) {
    eversion_t need = p->second.need;
    eversion_t have = p->second.have;
    if (omissing.is_missing(p->first)) {
      dout(10) << "search_for_missing " << p->first << " " << need
	       << " also missing on osd" << fromosd << dendl;
    } 
    else if (need <= olog.top) {
      dout(10) << "search_for_missing " << p->first << " " << need
               << " is on osd" << fromosd << dendl;
      missing_loc[p->first].insert(fromosd);
    } else {
      dout(10) << "search_for_missing " << p->first << " " << need
               << " > olog.top " << olog.top << ", also missing on osd" << fromosd
               << dendl;
    }
  }

  dout(20) << "search_for_missing missing " << missing.missing << dendl;
}



// ===============================================================
// BACKLOG

bool PG::build_backlog_map(map<eversion_t,Log::Entry>& omap)
{
  dout(10) << "build_backlog_map requested epoch " << generate_backlog_epoch << dendl;

  unlock();

  vector<sobject_t> olist;
  osd->store->collection_list(info.pgid.to_coll(), olist);

  for (vector<sobject_t>::iterator it = olist.begin();
       it != olist.end();
       it++) {
    sobject_t poid = *it;

    Log::Entry e;
    e.soid = poid;
    bufferlist bv;
    osd->store->getattr(info.pgid.to_coll(), poid, OI_ATTR, bv);
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

    /*
     * we can skip an object if
     *  - is already in the log AND
     *    - it is a totally new object OR
     *    - the prior_version is also already in the log
     * otherwise, if we have the object, include a prior_version backlog entry.
     */
    if (log.objects.count(be.soid)) {
      Log::Entry *le = log.objects[be.soid];
      
      assert(!le->is_delete());  // if it's a deletion, we are corrupt..

      // note the prior version
      if (le->prior_version == eversion_t() ||  // either new object, or
	  le->prior_version >= log.bottom) {    // prior_version also already in log
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
  
  while (!log.log.empty()) {
    Log::Entry &e = *log.log.begin();
    if (e.version > log.bottom) break;

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
    //assert(logged_object(p->soid));
    assert(p->op == Entry::BACKLOG || logged_req(p->reqid));
  }
  return out;
}





/******* PG ***********/

void PG::generate_past_intervals()
{
  epoch_t first_epoch = 0;
  epoch_t stop = MAX(1, info.history.last_epoch_started);
  epoch_t last_epoch = info.history.same_since - 1;

  dout(10) << "generate_past_intervals over epochs " << stop << "-" << last_epoch << dendl;

  OSDMap *nextmap = osd->get_map(last_epoch);
  for (;
       last_epoch >= stop;
       last_epoch = first_epoch - 1) {
    OSDMap *lastmap = nextmap;
    vector<int> tacting;
    lastmap->pg_to_acting_osds(get_pgid(), tacting);
    
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
    i.acting.swap(tacting);
    if (i.acting.size()) {
      i.maybe_went_rw = 
	lastmap->get_up_thru(i.acting[0]) >= first_epoch &&
	lastmap->get_up_from(i.acting[0]) <= first_epoch;
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

void PG::trim_past_intervals()
{
  while (past_intervals.size() &&
	 past_intervals.begin()->second.last < info.history.last_epoch_started) {
    dout(10) << "trim_past_intervals trimming " << past_intervals.begin()->second << dendl;
    past_intervals.erase(past_intervals.begin());
  }
}



// true if the given map affects the prior set
bool PG::prior_set_affected(OSDMap *osdmap)
{
  // did someone in the prior set go down?
  for (set<int>::iterator p = prior_set.begin();
       p != prior_set.end();
       p++)
    if (osdmap->is_down(*p) && prior_set_down.count(*p) == 0) {
      dout(10) << "prior_set_affected: osd" << *p << " now down" << dendl;
      return true;
    }

  // did someone in the prior down set go up?
  for (set<int>::iterator p = prior_set_down.begin();
       p != prior_set_down.end();
       p++)
    if (osdmap->is_up(*p)) {
      dout(10) << "prior_set_affected: osd" << *p << " now up" << dendl;
      return true;
    }
  
  // did a significant osd's up_thru change?
  for (map<int,epoch_t>::iterator p = prior_set_up_thru.begin();
       p != prior_set_up_thru.end();
       p++)
    if (p->second != osdmap->get_up_thru(p->first)) {
      dout(10) << "prior_set_affected: primary osd" << p->first
	       << " up_thru " << p->second
	       << " -> " << osdmap->get_up_thru(p->first) 
	       << dendl;
      return true;
    }

  return false;
}

void PG::clear_prior()
{
  dout(10) << "clear_prior" << dendl;
  prior_set.clear();
  prior_set_down.clear();
  prior_set_up_thru.clear();
  must_notify_mon = false;
}


void PG::build_prior()
{
  if (1) {
    // sanity check
    for (map<int,Info>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 it++)
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
  }

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
   * any OSDs in the prior set are down until we send an MOSDAlive to
   * the monitor such that the OSDMap sets osd_up_thru to an epoch.
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
   * intervene in some as-yet-undetermined way.  :)
   */

  clear_prior();

  // current nodes, of course.
  for (unsigned i=1; i<acting.size(); i++)
    prior_set.insert(acting[i]);

  // and prior PG mappings.  move backwards in time.
  state_clear(PG_STATE_CRASHED);
  state_clear(PG_STATE_DOWN);
  bool any_up_now = false;
  bool some_down = false;

  // generate past intervals, if we don't have them.
  if (info.history.same_since > info.history.last_epoch_started &&
      (past_intervals.empty() ||
       past_intervals.begin()->first > info.history.last_epoch_started))
    generate_past_intervals();
  
  for (map<epoch_t,Interval>::reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       p++) {
    Interval &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      continue;  // we don't care

    if (interval.acting.empty())
      continue;

    OSDMap *lastmap = osd->get_map(interval.last);

    int crashed = 0;
    int need_down = 0;
    bool any_survived = false;
    for (unsigned i=0; i<interval.acting.size(); i++) {
      const osd_info_t& pinfo = osd->osdmap->get_info(interval.acting[i]);

      // if the osd restarted after this interval but is not known to have
      // cleanly survived through this interval, we mark the pg crashed.
      if (pinfo.up_from > interval.last &&
	  !(pinfo.last_clean_first <= interval.first &&
	    pinfo.last_clean_last >= interval.last)) {
	dout(10) << "build_prior  prior osd" << interval.acting[i]
		 << " up_from " << pinfo.up_from
		 << " and last clean interval " << pinfo.last_clean_first << "-" << pinfo.last_clean_last
		 << " does not include us" << dendl;
	crashed++;
      }

      if (osd->osdmap->is_up(interval.acting[i])) {  // is up now
	if (interval.acting[i] != osd->whoami)       // and is not me
	  prior_set.insert(interval.acting[i]);

	// did any osds survive _this_ interval?
	any_survived = true;

	// are any osds alive from the last interval started?
	if (interval.first <= info.history.last_epoch_started &&
	    interval.last >= info.history.last_epoch_started)
	  any_up_now = true;
      } else if (pinfo.lost_at > interval.first) {
	dout(10) << "build_prior  prior osd" << interval.acting[i]
		 << " is down, but marked lost at " << pinfo.lost_at << dendl;
	prior_set_down.insert(interval.acting[i]);
      } else {
	dout(10) << "build_prior  prior osd" << interval.acting[i]
		 << " is down, must notify mon" << dendl;
	must_notify_mon = true;
	need_down++;
	prior_set_down.insert(interval.acting[i]);
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
	if (osd->osdmap->is_down(interval.acting[i])) {
	  prior_set.insert(interval.acting[i]);
	  state_set(PG_STATE_DOWN);
	}
      some_down = true;
      
      // take note that we care about the primary's up_thru.  if it
      // changes later, it will affect our prior_set, and we'll want
      // to rebuild it!
      prior_set_up_thru[interval.acting[0]] = lastmap->get_up_thru(interval.acting[0]);
    }

    if (crashed) {
      dout(10) << "build_prior  one of " << interval.acting 
	       << " possibly crashed, marking pg crashed" << dendl;
      state_set(PG_STATE_CRASHED);
    }
  }

  dout(10) << "build_prior = " << prior_set
	   << " down = " << prior_set_down << " ..."
	   << (is_crashed() ? " crashed":"")
	   << (is_down() ? " down":"")
	   << (some_down ? " some_down":"")
	   << (must_notify_mon ? " must_notify_mon":"")
	   << dendl;
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  have_master_log = false;
  prior_set.clear();
  prior_set_down.clear();
  prior_set_up_thru.clear();
  stray_set.clear();
  uptodate_set.clear();
  peer_info_requested.clear();
  peer_log_requested.clear();
  peer_summary_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  min_last_complete_ondisk = eversion_t();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();
  log.reset_recovery_pointers();

  stat_object_temp_rd.clear();

  peer_scrub_map.clear();
  osd->recovery_wq.dequeue(this);
}


// if false, stop.
bool PG::recover_master_log(map< int, map<pg_t,Query> >& query_map)
{
  /** GET ALL PG::Info *********/

  // -- query info from everyone in prior_set.
  bool missing_info = false;
  for (set<int>::iterator it = prior_set.begin();
       it != prior_set.end();
       it++) {
    if (peer_info.count(*it)) {
      dout(10) << " have info from osd" << *it 
               << ": " << peer_info[*it]
               << dendl;      
      continue;
    }
    missing_info = true;

    if (peer_info_requested.count(*it)) {
      dout(10) << " waiting for osd" << *it << dendl;
      continue;
    }
    
    if (osd->osdmap->is_up(*it)) {
      dout(10) << " querying info from osd" << *it << dendl;
      query_map[*it][info.pgid] = Query(Query::INFO, info.history);
      peer_info_requested.insert(*it);
    } else {
      dout(10) << " not querying down osd" << *it << dendl;
    }
  }
  if (missing_info)
    return false;

  
  // -- ok, we have all (prior_set) info.  (and maybe others.)

  dout(10) << " have prior_set info.  min_last_complete_ondisk " << min_last_complete_ondisk << dendl;


  /** CREATE THE MASTER PG::Log *********/

  // who (of all priors and active) has the latest PG version?
  eversion_t newest_update = info.last_update;
  int newest_update_osd = osd->whoami;

  oldest_update = info.last_update;  // only of acting (current) osd set.
  int oldest_who = osd->whoami;
  min_last_complete_ondisk = info.last_complete;
  
  for (map<int,Info>::iterator it = peer_info.begin();
       it != peer_info.end();
       it++) {
    if (osd->osdmap->is_down(it->first))
      continue;
    dout(10) << " have info from osd" << it->first << ": " << it->second << dendl;
    if (it->second.last_update > newest_update ||
	(it->second.last_update == newest_update &&    // prefer osds in the prior set
	 prior_set.count(newest_update_osd) == 0)) {
      newest_update = it->second.last_update;
      newest_update_osd = it->first;
    }
    if (is_acting(it->first)) {
      if (it->second.last_update < oldest_update) {
        oldest_update = it->second.last_update;
	oldest_who = it->first;
      }
      if (it->second.last_complete < min_last_complete_ondisk)
        min_last_complete_ondisk = it->second.last_complete;
    }
  }
  if (newest_update == info.last_update)   // or just me, if nobody better.
    newest_update_osd = osd->whoami;
  
  // gather log(+missing) from that person!
  if (newest_update_osd != osd->whoami) {
    Info& pi = peer_info[newest_update_osd];
    if (pi.log_bottom <= log.top) {
      if (peer_log_requested.count(newest_update_osd)) {
	dout(10) << " newest update on osd" << newest_update_osd
		 << " v " << newest_update 
		 << ", already queried log" 
		 << dendl;
      } else {
	// we'd _like_ it back to oldest_update, but take what we can get.
	dout(10) << " newest update on osd" << newest_update_osd
		 << " v " << newest_update 
		 << ", querying since oldest_update " << oldest_update
		 << dendl;
	query_map[newest_update_osd][info.pgid] = Query(Query::LOG, oldest_update, info.history);
	peer_log_requested.insert(newest_update_osd);
      }
    } else {
      dout(10) << " newest update on osd" << newest_update_osd
	       << ", whose log.bottom " << pi.log_bottom
	       << " > my log.top " << log.top
	       << ", i will need backlog for me+them." << dendl;
      // it's possible another peer could fill in the missing bits, but
      // pretty unlikely.  someday it may be worth the complexity to
      // try.  until then, just get the full backlogs.
      if (!log.backlog) {
	osd->queue_generate_backlog(this);
	return false;
      }
      
      if (peer_summary_requested.count(newest_update_osd)) {
	dout(10) << " newest update on osd" << newest_update_osd
		 << " v " << newest_update 
		 << ", already queried summary/backlog" 
		 << dendl;
      } else {
	dout(10) << " newest update on osd" << newest_update_osd
		 << " v " << newest_update 
		 << ", querying entire summary/backlog"
		 << dendl;
	query_map[newest_update_osd][info.pgid] = Query(Query::BACKLOG, info.history);
	peer_summary_requested.insert(newest_update_osd);
      }
    }
    return false;
  } else {
    dout(10) << " newest_update " << info.last_update << " (me)" << dendl;
  }

  dout(10) << " oldest_update " << oldest_update << " (osd" << oldest_who << ")" << dendl;

  if (is_down()) {
    dout(10) << " down.  we wait." << dendl;    
    return false;
  }

  have_master_log = true;


  // -- do i need to generate backlog for any of my peers?
  if (oldest_update < log.bottom && !log.backlog) {
    dout(10) << "must generate backlog for some peers, my bottom " 
             << log.bottom << " > oldest_update " << oldest_update
             << dendl;
    osd->queue_generate_backlog(this);
    return false;
  }

  return true;
}


void PG::peer(ObjectStore::Transaction& t, 
              map< int, map<pg_t,Query> >& query_map,
	      map<int, MOSDPGInfo*> *activator_map)
{
  dout(10) << "peer acting is " << acting << dendl;

  if (!is_active())
    state_set(PG_STATE_PEERING);
  
  if (prior_set.empty())
    build_prior();

  dout(10) << "peer prior_set is " << prior_set << dendl;
  
  
  if (!have_master_log)
    if (!recover_master_log(query_map))
      return;


  /** COLLECT MISSING+LOG FROM PEERS **********/
  /*
    we also detect divergent replicas here by pulling the full log
    from everyone.  

    for example:
   0:    1:    2:    
    2'6   2'6    2'6
    2'7   2'7    2'7
    3'8 | 2'8    2'8
    3'9 |        2'9
    
  */  

  // gather missing from peers
  bool have_all_missing = true;
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    Info& pi = peer_info[peer];
    dout(10) << " peer osd" << peer << " " << pi << dendl;

    if (pi.is_empty())
      continue;
    if (peer_missing.count(peer) == 0) {
      dout(10) << " still need log+missing from osd" << peer << dendl;
      have_all_missing = false;
    }
    if (peer_log_requested.count(peer) ||
        peer_summary_requested.count(peer)) continue;
   
    assert(pi.last_update <= log.top);

    if (pi.last_update < log.bottom) {
      // we need the full backlog in order to build this node's missing map.
      dout(10) << " osd" << peer << " last_update " << pi.last_update
	       << " < log.bottom " << log.bottom
	       << ", pulling missing+backlog" << dendl;
      query_map[peer][info.pgid] = Query(Query::BACKLOG, info.history);
      peer_summary_requested.insert(peer);
    } else {
      // we need just enough log to get any divergent items so that we
      // can appropriate adjust the missing map.  that can be as far back
      // as the peer's last_epoch_started.
      eversion_t from(pi.history.last_epoch_started, 0);
      dout(10) << " osd" << peer << " last_update " << pi.last_update
	       << ", pulling missing+log from it's last_epoch_started " << from << dendl;
      query_map[peer][info.pgid] = Query(Query::LOG, from, info.history);
      peer_log_requested.insert(peer);
    }
  }
  if (!have_all_missing)
    return;

  dout(10) << " min_last_complete_ondisk " << min_last_complete_ondisk << dendl;

  
  // -- ok.  and have i located all pg contents?
  if (missing.num_missing() > missing_loc.size()) {
    dout(10) << "there are still " << (missing.num_missing() - missing_loc.size()) << " lost objects" << dendl;

    // let's pull info+logs from _everyone_ (strays included, this
    // time) in search of missing objects.

    for (map<int,Info>::iterator it = peer_info.begin();
         it != peer_info.end();
         it++) {
      int peer = it->first;

      if (osd->osdmap->is_down(peer))
	continue;

      if (peer_summary_requested.count(peer)) {
        dout(10) << " already requested summary/backlog from osd" << peer << dendl;
        continue;
      }

      dout(10) << " requesting summary/backlog from osd" << peer << dendl;      
      query_map[peer][info.pgid] = Query(Query::BACKLOG, info.history);
      peer_summary_requested.insert(peer);
      return;
    }
    
    dout(10) << (missing.num_missing() - missing_loc.size())
	     << " objects are still lost, waiting+hoping for a notify from someone else!" << dendl;
    return;
  }

  // sanity check
  assert(missing.num_missing() == missing_loc.size());
  assert(info.last_complete >= log.bottom || log.backlog);

  // -- do need to notify the monitor?
  if (must_notify_mon) {
    if (osd->osdmap->get_up_thru(osd->whoami) < info.history.same_since) {
      dout(10) << "up_thru " << osd->osdmap->get_up_thru(osd->whoami)
	       << " < same_since " << info.history.same_since
	       << ", must notify monitor" << dendl;
      need_up_thru = true;
      osd->queue_want_up_thru(info.history.same_since);
      return;
    } else {
      dout(10) << "up_thru " << osd->osdmap->get_up_thru(osd->whoami)
	       << " >= same_since " << info.history.same_since
	       << ", all is well" << dendl;
    }
  }

  // -- crash recovery?
  if (is_crashed()) {
    replay_until = g_clock.now();
    replay_until += g_conf.osd_replay_window;
    dout(10) << "crashed, allowing op replay for " << g_conf.osd_replay_window
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);
    state_clear(PG_STATE_PEERING);
    osd->replay_queue_lock.Lock();
    osd->replay_queue.push_back(pair<pg_t,utime_t>(info.pgid, replay_until));
    osd->replay_queue_lock.Unlock();
  } 
  else if (!is_active()) {
    // -- ok, activate!
    activate(t, activator_map);
  }
  else if (is_all_uptodate()) 
    finish_recovery();
}


void PG::activate(ObjectStore::Transaction& t,
		  map<int, MOSDPGInfo*> *activator_map)
{
  assert(!is_active());

  // twiddle pg state
  state_set(PG_STATE_ACTIVE);
  state_clear(PG_STATE_STRAY);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_PEERING);
  if (is_crashed()) {
    //assert(is_replay());      // HELP.. not on replica?
    state_clear(PG_STATE_CRASHED);
    state_clear(PG_STATE_REPLAY);
  }
  if (is_primary() && 
      osd->osdmap->get_pg_size(info.pgid) != acting.size())
    state_set(PG_STATE_DEGRADED);
  else
    state_clear(PG_STATE_DEGRADED);

  info.history.last_epoch_started = osd->osdmap->get_epoch();
  trim_past_intervals();
  
  if (role == 0) {    // primary state
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }

  assert(info.last_complete >= log.bottom || log.backlog);

  need_up_thru = false;

  // clear prior set (and dependency info)... we are done peering!
  clear_prior();

  // if we are building a backlog, cancel it!
  osd->cancel_generate_backlog(this);

  // write pg info, log
  write_info(t);
  write_log(t);

  // clean up stray objects, snaps
  clean_up_local(t); 

  if (!info.snap_trimq.empty())
    queue_snap_trim();

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
    log.last_requested = sobject_t();
    dout(10) << "activate -     complete_to = " << log.complete_to->version << dendl;
    if (is_primary()) {
      dout(10) << "activate - starting recovery" << dendl;
      osd->queue_for_recovery(this);
    }
  }

  // if primary..
  if (is_primary()) {
    // who is clean?
    uptodate_set.clear();
    if (info.is_uptodate()) 
      uptodate_set.insert(osd->whoami);
    
    // start up replicas
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_info.count(peer));
      PG::Info& pi = peer_info[peer];

      MOSDPGLog *m = 0;
      
      if (pi.last_update == info.last_update) {
        // empty log
	if (activator_map) {
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
	if (pi.last_update < log.bottom) {
	  // summary/backlog
	  assert(log.backlog);
	  m->log = log;
	} else {
	  // incremental log
	  assert(pi.last_update < info.last_update);
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
	osd->messenger->send_message(m, osd->osdmap->get_inst(peer));
      }

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd" << peer << " already uptodate, " << pi << dendl;
	assert(pi.is_uptodate());
        uptodate_set.insert(peer);
      } else {
        dout(10) << "activate peer osd" << peer << " " << pi
                 << " missing " << pm << dendl;
      }
            
    }

    // discard unneeded peering state
    //peer_log.clear(); // actually, do this carefully, in case peer() is called again.
    
    // all clean?
    if (is_all_uptodate()) 
      finish_recovery();
    else {
      dout(10) << "activate not all replicas are uptodate, queueing recovery" << dendl;
      osd->queue_for_recovery(this);
    }

    update_stats();
  }

  
  // replay (queue them _before_ other waiting ops!)
  if (!replay_queue.empty()) {
    eversion_t c = info.last_update;
    list<Message*> replay;
    for (map<eversion_t,MOSDOp*>::iterator p = replay_queue.begin();
         p != replay_queue.end();
         p++) {
      if (p->first <= info.last_update) {
        dout(10) << "activate will WRNOOP " << p->first << " " << *p->second << dendl;
        replay.push_back(p->second);
        continue;
      }
      if (p->first.version != c.version+1) {
        dout(10) << "activate replay " << p->first
                 << " skipping " << c.version+1 - p->first.version 
                 << " ops"
                 << dendl;      
      }
      dout(10) << "activate replay " << p->first << " " << *p->second << dendl;
      replay.push_back(p->second);
      c = p->first;
    }
    replay_queue.clear();
    osd->take_waiters(replay);
  }

  // waiters
  osd->take_waiters(waiting_for_active);
}

void PG::queue_snap_trim()
{
  if (osd->snap_trim_wq.queue(this))
    dout(10) << "queue_snap_trim -- queuing" << dendl;
  else
    dout(10) << "queue_snap_trim -- already trimming" << dendl;
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

void PG::finish_recovery()
{
  dout(10) << "finish_recovery" << dendl;
  state_set(PG_STATE_CLEAN);
  assert(info.last_complete == info.last_update);

  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);

  ObjectStore::Transaction t;
  write_info(t);
  osd->store->apply_transaction(t, finish_sync_event);
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
      osd->scrub_wq.queue(this);
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
    } else {
      dout(10) << "not sending PGRemove to down osd" << *p << dendl;
    }
    peer_info.erase(*p);
  }

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_info_requested.clear();
  peer_log_requested.clear();
  peer_summary_requested.clear();
}




void PG::update_stats()
{
  pg_stats_lock.Lock();
  if (is_primary()) {
    // update our stat summary
    info.stats.reported.inc(info.history.same_primary_since);
    info.stats.version = info.last_update;
    pg_stats_valid = true;
    pg_stats_stable = info.stats;
    pg_stats_stable.state = state;
    pg_stats_stable.acting = acting;

    pg_stats_stable.log_size = ondisklog.length();
    pg_stats_stable.ondisk_log_size = ondisklog.length();
    pg_stats_stable.log_start = log.bottom;
    pg_stats_stable.ondisk_log_start = log.bottom;

    pg_stats_stable.num_object_copies = pg_stats_stable.num_objects * osd->osdmap->get_pg_size(info.pgid);
    if (!is_clean() && is_active()) {
      pg_stats_stable.num_objects_missing_on_primary = missing.num_missing();
      int degraded = missing.num_missing();
      for (unsigned i=1; i<acting.size(); i++) {
	assert(peer_missing.count(acting[i]));
	degraded += peer_missing[acting[i]].num_missing();
      }
      pg_stats_stable.num_objects_degraded = degraded;
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
  ::encode(info, infobl);
  dout(20) << "write_info info " << infobl.length() << dendl;
  t.collection_setattr(info.pgid.to_coll(), "info", infobl);
 
  // local state
  bufferlist snapbl;
  ::encode(snap_collections, snapbl);
  dout(20) << "write_info snap " << snapbl.length() << dendl;
  t.collection_setattr(info.pgid.to_coll(), "snap_collections", snapbl);

  bufferlist ki;
  ::encode(past_intervals, ki);
  dout(20) << "write_info pastintervals " << ki.length() << dendl;
  t.collection_setattr(info.pgid.to_coll(), "past_intervals", ki);

  dirty_info = false;
}

void PG::write_log(ObjectStore::Transaction& t)
{
  dout(10) << "write_log" << dendl;

  // assemble buffer
  bufferlist bl;
  
  // build buffer
  ondisklog.bottom = 0;
  ondisklog.block_map.clear();
  for (list<Log::Entry>::iterator p = log.log.begin();
       p != log.log.end();
       p++) {
    __u64 olen = bl.length();
    ::encode(*p, bl);
    if (olen % 4096 < bl.length() % 4096)
      ondisklog.block_map[olen] = p->version;

    /*
    if (g_conf.osd_pad_pg_log) {  // pad to 4k, until i fix ebofs reallocation crap.  FIXME.
      bufferptr bp(4096 - sizeof(*p));
      bl.push_back(bp);
    }
    */
  }
  ondisklog.top = bl.length();
  
  // write it
  t.remove(0, log_oid );
  t.write(0, log_oid , 0, bl.length(), bl);

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(info.pgid.to_coll(), "ondisklog", blb);
  
  dout(10) << "write_log to " << ondisklog.bottom << "~" << ondisklog.length() << dendl;
  dirty_log = false;
}

void PG::trim(ObjectStore::Transaction& t, eversion_t trim_to)
{
  // trim?
  if (trim_to > log.bottom) {
    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(t, trim_to);
    info.log_bottom = log.bottom;
    info.log_backlog = log.backlog;
    trim_ondisklog_to(t, trim_to);
  }
}

void PG::trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v) 
{
  dout(15) << "trim_ondisk_log_to v " << v << dendl;

  map<__u64,eversion_t>::iterator p = ondisklog.block_map.begin();
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
  __u64 trim = p->first;
  dout(10) << "  trimming ondisklog to " << ondisklog.bottom << "~" << ondisklog.length() << dendl;

  assert(trim >= ondisklog.bottom);
  ondisklog.bottom = trim;

  // adjust block_map
  while (p != ondisklog.block_map.begin()) 
    ondisklog.block_map.erase(ondisklog.block_map.begin());
  
  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(info.pgid.to_coll(), "ondisklog", blb);

  if (!g_conf.osd_preserve_trimmed_log)
    t.zero(0, log_oid, 0, ondisklog.bottom & ~4095);
}

void PG::trim_peers()
{
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  if (pg_trim_to != eversion_t()) {
    for (unsigned i=1; i<acting.size(); i++)
      osd->messenger->send_message(new MOSDPGTrim(osd->osdmap->get_epoch(), info.pgid,
						  pg_trim_to),
				   osd->osdmap->get_inst(acting[i]));
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
  ::encode(e, log_bl);
  dout(10) << "add_log_entry " << e << dendl;
}


void PG::append_log(ObjectStore::Transaction &t, bufferlist& bl,
		    eversion_t log_version)
{
  dout(10) << "append_log " << ondisklog.bottom << "~" << ondisklog.length()
	   << " adding " << bl.length() <<  dendl;
 
  // update block map?
  if (ondisklog.top % 4096 < (ondisklog.top + bl.length()) % 4096)
    ondisklog.block_map[ondisklog.top] = log_version;

  t.write(0, log_oid, ondisklog.top, bl.length(), bl );
  
  ondisklog.top += bl.length();

  bufferlist blb(sizeof(ondisklog));
  ::encode(ondisklog, blb);
  t.collection_setattr(info.pgid.to_coll(), "ondisklog", blb);
  dout(10) << "append_log  now " << ondisklog.bottom << "~" << ondisklog.length() << dendl;
}

void PG::read_log(ObjectStore *store)
{
  // load bounds
  ondisklog.bottom = ondisklog.top = 0;

  bufferlist blb;
  store->collection_getattr(info.pgid.to_coll(), "ondisklog", blb);
  bufferlist::iterator p = blb.begin();
  ::decode(ondisklog, p);

  dout(10) << "read_log " << ondisklog.bottom << "~" << ondisklog.length() << dendl;

  log.backlog = info.log_backlog;
  log.bottom = info.log_bottom;
  
  if (ondisklog.top > 0) {
    // read
    bufferlist bl;
    store->read(0, log_oid, ondisklog.bottom, ondisklog.length(), bl);
    if (bl.length() < ondisklog.length()) {
      dout(0) << "read_log got " << bl.length() << " bytes, expected " 
	      << ondisklog.top << "-" << ondisklog.bottom << "="
	      << ondisklog.length()
	      << dendl;
      assert(0);
    }
    
    PG::Log::Entry e;
    bufferlist::iterator p = bl.begin();
    assert(log.log.empty());
    eversion_t last;
    bool reorder = false;
    while (!p.end()) {
      loff_t opos = ondisklog.bottom + p.get_off();
      ::decode(e, p);
      loff_t pos = ondisklog.bottom + p.get_off();
      dout(20) << "read_log " << pos << " " << e << dendl;

      // [repair] in order?
      if (e.version < last) {
	dout(0) << "read_log " << pos << " out of order entry " << e << " follows " << last << dendl;
  	stringstream ss;
	ss << info.pgid << " log has out of order entry " << e << " following " << last;
	osd->get_logclient()->log(LOG_ERROR, ss);
	reorder = true;
      }

      if (e.version <= log.bottom && !log.backlog) {
	dout(20) << "read_log  ignoring entry at " << pos << " below log.bottom" << dendl;
	continue;
      }
      if (last.version == e.version.version) {
	dout(0) << "read_log  got dup " << e.version << " (last was " << last << ", dropping that one)" << dendl;
	log.log.pop_back();
	stringstream ss;
	ss << info.pgid << " read_log got dup " << e.version << " after " << last;
	osd->get_logclient()->log(LOG_ERROR, ss);
      }
      
      if (opos % 4096 < pos % 4096)
	ondisklog.block_map[opos] = e.version;
      log.log.push_back(e);
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	stringstream ss;
	ss << info.pgid << " log has extra data at " << pos << "~" << (ondisklog.top-pos)
	   << " after " << info.last_update;
	osd->get_logclient()->log(LOG_ERROR, ss);
	dout(0) << "read_log " << pos << " *** extra gunk at end of log, adjusting ondisklog.top" << dendl;
	ondisklog.top = opos;
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

  log.top = info.last_update;
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
      int r = osd->store->getattr(info.pgid.to_coll(), i->soid, OI_ATTR, bv);
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



void PG::read_state(ObjectStore *store)
{
  bufferlist bl;
  bufferlist::iterator p;

  // info
  store->collection_getattr(info.pgid.to_coll(), "info", bl);
  p = bl.begin();
  ::decode(info, p);
  
  // snap_collections
  bl.clear();
  store->collection_getattr(info.pgid.to_coll(), "snap_collections", bl);
  p = bl.begin();
  ::decode(snap_collections, p);

  // past_intervals
  bl.clear();
  store->collection_getattr(info.pgid.to_coll(), "past_intervals", bl);
  if (bl.length()) {
    p = bl.begin();
    ::decode(past_intervals, p);
  }

  read_log(store);
}

coll_t PG::make_snap_collection(ObjectStore::Transaction& t, snapid_t s)
{
  coll_t c = info.pgid.to_snap_coll(s);
  if (snap_collections.count(s) == 0) {
    snap_collections.insert(s);
    dout(10) << "create_snap_collection " << c << ", set now " << snap_collections << dendl;
    bufferlist bl;
    ::encode(snap_collections, bl);
    t.collection_setattr(info.pgid.to_coll(), "snap_collections", bl);
    t.create_collection(c);
  }
  return c;
}




// ==============================
// Object locking

//
// If the target object of the operation op is locked for writing by another client, the function puts op to the waiting queue waiting_for_wr_unlock
// returns true if object was locked, otherwise returns false
// 
bool PG::block_if_wrlocked(MOSDOp* op, object_info_t& oi)
{
  sobject_t soid(op->get_oid(), CEPH_NOSNAP);

  if (oi.wrlock_by.tid &&
      oi.wrlock_by.name != op->get_orig_source()) {
    //the object is locked for writing by someone else -- add the op to the waiting queue      
    dout(10) << "blocked on wrlock on " << oi << dendl;
    waiting_for_wr_unlock[soid].push_back(op);
    return true;
  }
  
  return false; //the object wasn't locked, so the operation can be handled right away
}



// ==========================================================================================
// SCRUB

void PG::sub_op_scrub(MOSDSubOp *op)
{
  dout(7) << "sub_op_scrub" << dendl;

  if (op->map_epoch < info.history.same_primary_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << op->map_epoch << " < " << info.history.same_primary_since << dendl;
    delete op;
    return;
  }

  ScrubMap map;
  build_scrub_map(map);

  MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK); 
  ::encode(map, reply->get_data());
  osd->messenger->send_message(reply, op->get_source_inst());

  delete op;
}

void PG::sub_op_scrub_reply(MOSDSubOpReply *op)
{
  dout(7) << "sub_op_scrub_reply" << dendl;

  if (op->map_epoch < info.history.same_primary_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << op->map_epoch << " < " << info.history.same_primary_since << dendl;
    delete op;
    return;
  }

  int from = op->get_source().num();

  if (peer_scrub_map.count(from)) {
    dout(10) << " already had osd" << from << " scrub map" << dendl;
  } else {
    dout(10) << " got osd" << from << " scrub map" << dendl;
    bufferlist::iterator p = op->get_data().begin();
    peer_scrub_map[from].decode(p);
    kick();
  }

  delete op;
}


/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 */ 
void PG::build_scrub_map(ScrubMap &map)
{
  dout(10) << "build_scrub_map" << dendl;
  coll_t c = info.pgid.to_coll();

  // objects
  vector<sobject_t> ls;
  osd->store->collection_list(c, ls);

  // sort
  dout(10) << "sorting " << ls.size() << " objects" << dendl;
  vector< pair<sobject_t,int> > tab(ls.size());
  vector< pair<sobject_t,int> >::iterator q = tab.begin();
  int i = 0;
  for (vector<sobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++, q++) {
    q->first = *p;
    q->second = i;
  }
  sort(tab.begin(), tab.end());
  // tab is now sorted, with ->second indicating object's original position
  vector<int> pos(ls.size());
  i = 0;
  for (vector< pair<sobject_t,int> >::iterator p = tab.begin(); 
       p != tab.end(); 
       p++, i++)
    pos[p->second] = i;
  // now, pos[orig pos] = sorted pos

  dout(10) << " scanning " << ls.size() << " objects" << dendl;
  map.objects.resize(ls.size());
  i = 0;
  for (vector<sobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++) {
    sobject_t poid = *p;

    ScrubMap::object& o = map.objects[pos[i]];
    o.poid = *p;

    struct stat st;
    int r = osd->store->stat(c, poid, &st);
    assert(r == 0);
    o.size = st.st_size;

    osd->store->getattrs(c, poid, o.attrs);    

    dout(25) << "   " << poid << dendl;
  }

  // pg attrs
  osd->store->collection_getattrs(c, map.attrs);

  // log
  osd->store->read(coll_t(), log_oid, 0, 0, map.logbl);
  dout(10) << " done.  pg log is " << map.logbl.length() << " bytes" << dendl;
}



void PG::repair_object(ScrubMap::object *po, int bad_peer, int ok_peer)
{
  eversion_t v;
  bufferlist bv;
  bv.push_back(po->attrs[OI_ATTR]);
  object_info_t oi(bv);
  if (bad_peer != acting[0]) {
    peer_missing[bad_peer].add(po->poid, oi.version, eversion_t());
  } else {
    missing.add(po->poid, oi.version, eversion_t());
    missing_loc[po->poid].insert(ok_peer);
    log.last_requested = sobject_t();
  }
  uptodate_set.erase(bad_peer);
  osd->queue_for_recovery(this);
}

void PG::scrub()
{
  stringstream ss;
  ScrubMap scrubmap;
  int errors = 0, fixed = 0;
  bool repair = state_test(PG_STATE_REPAIR);
  const char *mode = repair ? "repair":"scrub";

  osd->map_lock.get_read();
  lock();
 
  epoch_t epoch = info.history.same_since;

  if (!is_primary()) {
    dout(10) << "scrub -- not primary" << dendl;
    unlock();
    osd->map_lock.put_read();
    return;
  }

  if (!is_active() || !is_clean()) {
    dout(10) << "scrub -- not active or not clean" << dendl;
    unlock();
    osd->map_lock.put_read();
    return;
  }

  dout(10) << "scrub start" << dendl;
  state_set(PG_STATE_SCRUBBING);
  update_stats();

  // request maps from replicas
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub  requesting scrubmap from osd" << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB;
    sobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
				     osd->osdmap->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->messenger->send_message(subop, //new MOSDPGScrub(info.pgid, osd->osdmap->get_epoch()),
				 osd->osdmap->get_inst(acting[i]));
  }
  osd->map_lock.put_read();


  // wait for any ops in progress
  while (is_write_in_progress()) {
    dout(10) << "scrub  write(s) in progress, waiting" << dendl;
    wait();
    if (osd->is_stopping())
      goto out;
  }


  //unlock();

  dout(10) << "scrub  building my map" << dendl;
  build_scrub_map(scrubmap);

  /*
  lock();
  if (epoch != info.history.same_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    goto out;
  }
  */

  while (peer_scrub_map.size() < acting.size() - 1) {
    dout(10) << "scrub  has " << (peer_scrub_map.size()+1) << " / " << acting.size()
	     << " maps, waiting" << dendl;
    wait();

    if (epoch != info.history.same_since ||
	osd->is_stopping()) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      goto out;
    }
  }

  /*
  unlock();
  */

  if (acting.size() > 1) {
    dout(10) << "scrub  comparing replica scrub maps" << dendl;

    // first, compare scrub maps
    vector<ScrubMap*> m(acting.size());
    m[0] = &scrubmap;
    for (unsigned i=1; i<acting.size(); i++)
      m[i] = &peer_scrub_map[acting[i]];
    vector<ScrubMap::object>::iterator p[acting.size()];
    for (unsigned i=0; i<acting.size(); i++)
      p[i] = m[i]->objects.begin();
    
    int num_missing = 0;
    int num_bad = 0;
    
    while (1) {
      ScrubMap::object *po = 0;
      int pi = -1;
      bool anymissing = false;
      for (unsigned i=0; i<acting.size(); i++) {
	if (p[i] == m[i]->objects.end()) {
	  anymissing = true;
	  continue;
	}
	if (!po) {
	  po = &(*p[i]);
	  pi = i;
	}
	else if (po->poid != p[i]->poid) {
	  anymissing = true;
	  if (po->poid > p[i]->poid) {
	    po = &(*p[i]);
	    pi = i;
	  }
	}
      }
      if (!po)
	break;
      if (anymissing) {
	for (unsigned i=0; i<acting.size(); i++) {
	  if (p[i] == m[i]->objects.end() || po->poid != p[i]->poid) {
	    ss << info.pgid << " " << mode << " osd" << acting[i] << " missing " << po->poid;
	    osd->get_logclient()->log(LOG_ERROR, ss);
	    num_missing++;
	    
	    if (repair)
	      repair_object(po, acting[i], acting[pi]);
	  } else
	    p[i]++;
	}
	continue;
      }
      
      // compare
      bool ok = true;
      for (unsigned i=1; i<acting.size(); i++) {
	bool peerok = true;
	if (po->size != p[i]->size) {
	  dout(0) << "scrub osd" << acting[i] << " " << po->poid
		  << " size " << p[i]->size << " != " << po->size << dendl;
	  ss << info.pgid << " " << mode << " osd" << acting[i] << " " << po->poid
	     << " size " << p[i]->size << " != " << po->size;
	  osd->get_logclient()->log(LOG_ERROR, ss);
	  peerok = ok = false;
	  num_bad++;
	}
	if (po->attrs.size() != p[i]->attrs.size()) {
	  dout(0) << "scrub osd" << acting[i] << " " << po->poid
		  << " attr count " << p[i]->attrs.size() << " != " << po->attrs.size() << dendl;
	  ss << info.pgid << " " << mode << " osd" << acting[i] << " " << po->poid
	     << " attr count " << p[i]->attrs.size() << " != " << po->attrs.size();
	  osd->get_logclient()->log(LOG_ERROR, ss);
	  peerok = ok = false;
	  num_bad++;
	}
	for (map<nstring,bufferptr>::iterator q = po->attrs.begin(); q != po->attrs.end(); q++) {
	  if (p[i]->attrs.count(q->first)) {
	    if (q->second.cmp(p[i]->attrs[q->first])) {
	      dout(0) << "scrub osd" << acting[i] << " " << po->poid
		      << " attr " << q->first << " value mismatch" << dendl;
	      ss << info.pgid << " " << mode << " osd" << acting[i] << " " << po->poid
		 << " attr " << q->first << " value mismatch";
	      osd->get_logclient()->log(LOG_ERROR, ss);
	      peerok = ok = false;
	      num_bad++;
	    }
	  } else {
	    dout(0) << "scrub osd" << acting[i] << " " << po->poid
		    << " attr " << q->first << " missing" << dendl;
	    ss << info.pgid << " " << mode << " osd" << acting[i] << " " << po->poid
	       << " attr " << q->first << " missing";
	    osd->get_logclient()->log(LOG_ERROR, ss);
	    peerok = ok = false;
	    num_bad++;
	  }
	}

	if (!peerok && repair)
	  repair_object(po, acting[i], acting[pi]);
      }
      
      if (ok)
	dout(20) << "scrub  " << po->poid << " size " << po->size << " ok" << dendl;
      
      // next
      for (unsigned i=0; i<acting.size(); i++)
	p[i]++;
    }
    
    if (num_missing || num_bad) {
      dout(0) << "scrub " << num_missing << " missing, " << num_bad << " bad objects" << dendl;
      ss << info.pgid << " " << mode << " " << num_missing << " missing, " << num_bad << " bad objects";
      osd->get_logclient()->log(LOG_ERROR, ss);
      state_set(PG_STATE_INCONSISTENT);
      if (repair)
	state_clear(PG_STATE_CLEAN);
    }
  }

  /*
  lock();
  if (epoch != info.history.same_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    goto out;
  }
  */

  // discard peer scrub info.
  peer_scrub_map.clear();

  /*
  unlock();
  */

  // ok, do the pg-type specific scrubbing
  _scrub(scrubmap, errors, fixed);

  /*
  lock();
  if (epoch != info.history.same_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    goto out;
  }
  */

  ss << info.pgid << " " << mode << " " << errors << " errors";
  if (repair)
    ss << ", " << fixed << " fixed";
  osd->get_logclient()->log(errors ? LOG_ERROR:LOG_INFO, ss);

  if (!(errors - fixed) && repair)
    state_clear(PG_STATE_INCONSISTENT);
  state_clear(PG_STATE_REPAIR);

  // finish up
  info.stats.last_scrub = info.last_update;
  info.stats.last_scrub_stamp = g_clock.now();


 out:
  state_clear(PG_STATE_SCRUBBING);
  update_stats();

  dout(10) << "scrub done" << dendl;

  osd->take_waiters(waiting_for_active);

  unlock();
}


