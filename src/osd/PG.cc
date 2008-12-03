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
#include "messages/MOSDPGScrub.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix _prefix(this, osd->whoami, osd->osdmap)
static ostream& _prefix(PG *pg, int whoami, OSDMap *osdmap) {
  return *_dout << dbeginl<< pthread_self() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " " << *pg << " ";
}


/******* PGLog ********/

void PG::Log::copy_after(const Log &other, eversion_t v) 
{
  assert(v >= other.bottom);
  top = bottom = other.top;
  for (list<Entry>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       i++) {
    if (i->version == v) break;
    assert(i->version > v);
    log.push_front(*i);
  }
  bottom = v;
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

  assert(complete_to == log.end() &&
	 requested_to == log.end());

  while (!log.empty()) {
    Entry &e = *log.begin();

    if (e.version > s) break;

    assert(complete_to != log.begin());
    assert(requested_to != log.begin());

    // remove from index,
    unindex(e);

    // from log
    log.pop_front();
  }
  
  // raise bottom?
  if (backlog) backlog = false;
  if (bottom < s) bottom = s;
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

void PG::proc_replica_log(ObjectStore::Transaction& t, Log &olog, Missing& omissing, int from)
{
  dout(10) << "proc_replica_log for osd" << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active());

  if (!have_master_log) {
    // i'm building master log.
    // note peer's missing.
    peer_missing[from] = omissing;
    
    // merge log into our own log
    merge_log(t, olog, omissing, from);
    proc_replica_missing(olog, omissing, from);
  } else {
    // i'm just building missing lists.
    peer_missing[from] = omissing;

    // iterate over peer log. in reverse.
    list<Log::Entry>::reverse_iterator pp = olog.log.rbegin();
    eversion_t lu = peer_info[from].last_update;
    while (pp != olog.log.rend()) {
      if (!log.logged_object(pp->oid)) {
	if (!log.backlog) {
	  dout(10) << " divergent " << *pp << " not in our log, generating backlog" << dendl;
	  //dout(0) << "log" << dendl;
	  //log.print(*_dout);
	  //dout(0) << "olog" << dendl;
	  //olog.print(*_dout);
	  generate_backlog();
	} else {
	  dout(10) << " divergent " << *pp << " not in our log, already have backlog" << dendl;
	}
      }
      
      if (!log.objects.count(pp->oid)) {
        dout(10) << " divergent " << *pp << " dne, must have been new, ignoring" << dendl;
        ++pp;
        continue;
      } 

      if (log.objects[pp->oid]->version == pp->version) {
        break;  // we're no longer divergent.
        //++pp;
        //continue;
      }

      if (log.objects[pp->oid]->version > pp->version) {
        dout(10) << " divergent " << *pp
                 << " superceded by " << log.objects[pp->oid]
                 << ", ignoring" << dendl;
      } else {
        dout(10) << " divergent " << *pp << ", adding to missing" << dendl;
        peer_missing[from].add_event(*pp);
      }

      ++pp;
      if (pp != olog.log.rend())
        lu = pp->version;
      else
        lu = olog.bottom;
    }    

    if (lu < peer_info[from].last_update) {
      dout(10) << " peer osd" << from << " last_update now " << lu << dendl;
      peer_info[from].last_update = lu;
      if (lu < oldest_update) {
        dout(10) << " oldest_update now " << lu << dendl;
        oldest_update = lu;
      }
    }

    proc_replica_missing(olog, peer_missing[from], from);
  }
}


/*
 * merge an old (possibly divergent) log entry into the new log.  this 
 * happens _after_ new log items have been assimilated.  thus, we assume
 * the index already references newer entries (if present), and missing
 * has been updated accordingly.
 */
void PG::merge_old_entry(ObjectStore::Transaction& t, Log::Entry& oe)
{
  if (log.objects.count(oe.oid)) {
    // object still exists.
    Log::Entry &ne = *log.objects[oe.oid];  // new(er?) entry
    
    if (ne.version > oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : older, missing" << dendl;
      assert(missing.is_missing(ne.oid));
      return;
    }
    if (ne.version == oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : same" << dendl;
      return;
    }

    if (oe.is_delete()) {
      if (ne.is_delete()) {
	// old and new are delete
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : both deletes" << dendl;
      } else {
	// old delete, new update.
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : missing" << dendl;
	assert(missing.is_missing(oe.oid));
      }
    } else {
      if (ne.is_delete()) {
	// old update, new delete
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : new delete supercedes" << dendl;
	missing.rm(oe.oid, oe.version);
      } else {
	// old update, new update
	dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : new item supercedes" << dendl;
	missing.rm(oe.oid, oe.version);  // re-add older "new" entry to missing
	missing.add_event(ne);
      }
    }
  } else {
    if (oe.is_delete()) {
      dout(20) << "merge_old_entry  had " << oe << " new dne : ok" << dendl;      
    } else {
      dout(20) << "merge_old_entry  had " << oe << " new dne : deleting" << dendl;
      t.remove(info.pgid.to_coll(), pobject_t(info.pgid.pool(), 0, oe.oid));
      missing.rm(oe.oid, oe.version);
    }
  }
}

void PG::merge_log(ObjectStore::Transaction& t, Log &olog, Missing &omissing, int fromosd)
{
  dout(10) << "merge_log " << olog << " from osd" << fromosd
           << " into " << log << dendl;
  bool changed = false;

  dout(15) << "log";
  log.print(*_dout);
  *_dout << dendl;
  dout(15) << "olog";
  olog.print(*_dout);
  *_dout << dendl;

  if (log.empty() ||
      (olog.bottom > log.top && olog.backlog)) { // e.g. log=(0,20] olog=(40,50]+backlog) 

    // build local backlog, and save old index
    if (!log.empty() && !log.backlog)
      generate_backlog();

    hash_map<object_t,Log::Entry*> old_objects;
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

	p++;       // move past the split point, tho...
	break;
      }
    }
    
    // then add all new items (_after_ split) to missing
    for (; p != log.log.end(); p++) {
      Log::Entry &ne = *p;
      dout(10) << "merge_log merging " << ne << dendl;
      missing.add_event(ne);
      if (ne.is_delete())
	t.remove(info.pgid.to_coll(), pobject_t(info.pgid.pool(), 0, ne.oid));
    }

    // find any divergent or removed items in old log.
    //  skip anything not in the index.
    for (p = olog.log.begin();
	 p != olog.log.end();
	 p++) {
      Log::Entry &oe = *p;                      // old entry
      if (old_objects.count(oe.oid) &&
	  old_objects[oe.oid] == &oe) {
	merge_old_entry(t, oe);
      }
    }

    info.last_update = log.top = olog.top;
    info.log_bottom = log.bottom = olog.bottom;
    info.log_backlog = log.backlog = olog.backlog;
    info.stats = peer_info[fromosd].stats;
    changed = true;
  } 

  else {
    // i can merge the two logs!

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
      list<Log::Entry>::iterator last_kept = olog.log.end();
      while (1) {
        if (from == olog.log.begin())
	  break;
        from--;
        dout(20) << "  ? " << *from << dendl;
        if (from->version <= log.top) {
	  dout(20) << "merge_log last shared is " << *from << dendl;
	  last_kept = from;
          from++;
          break;
        }
      }

      // index, update missing, delete deleted
      for (list<Log::Entry>::iterator p = from; p != to; p++) {
	Log::Entry &ne = *p;
        dout(10) << "merge_log " << ne << dendl;
	log.index(ne);
	missing.add_event(ne);
	if (ne.is_delete())
	  t.remove(info.pgid.to_coll(), pobject_t(info.pgid.pool(), 0, ne.oid));
      }
      
      // move aside divergent items
      list<Log::Entry> divergent;
      if (last_kept != olog.log.end()) {
	while (1) {
	  Log::Entry &oe = *log.log.rbegin();
	  if (oe.version == last_kept->version)
	    break;
	  dout(10) << "merge_log divergent " << oe << dendl;
	  divergent.push_front(oe);
	  log.log.pop_back();
	}
      }

      // splice
      log.log.splice(log.log.end(), 
                     olog.log, from, to);
      
      info.last_update = log.top = olog.top;
      info.stats = peer_info[fromosd].stats;

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
void PG::proc_replica_missing(Log &olog, Missing &omissing, int fromosd)
{
  // found items?
  for (map<object_t,Missing::item>::iterator p = missing.missing.begin();
       p != missing.missing.end();
       p++) {
    eversion_t need = p->second.need;
    eversion_t have = p->second.have;
    if (omissing.is_missing(p->first)) {
      dout(10) << "proc_replica_missing " << p->first << " " << need
	       << " also missing on osd" << fromosd << dendl;
    } 
    else if (need <= olog.top) {
      dout(10) << "proc_replica_missing " << p->first << " " << need
               << " is on osd" << fromosd << dendl;
      missing_loc[p->first].insert(fromosd);
    } else {
      dout(10) << "proc_replica_missing " << p->first << " " << need
               << " > olog.top " << olog.top << ", also missing on osd" << fromosd
               << dendl;
    }
  }

  dout(10) << "proc_missing missing " << missing.missing << dendl;
}



void PG::generate_backlog()
{
  dout(10) << "generate_backlog to " << log << dendl;
  assert(!log.backlog);
  log.backlog = true;

  vector<pobject_t> olist;
  osd->store->collection_list(info.pgid.to_coll(), olist);
  if (olist.size() != info.stats.num_objects)
    dout(10) << " WARNING: " << olist.size() << " != num_objects " << info.stats.num_objects << dendl;

  
  int local = 0;
  map<eversion_t,Log::Entry> add;
  for (vector<pobject_t>::iterator it = olist.begin();
       it != olist.end();
       it++) {
    local++;
    pobject_t poid = pobject_t(info.pgid.pool(), 0, it->oid);
    
    if (log.logged_object(poid.oid)) continue; // already have it logged.
    
    // add entry
    Log::Entry e;
    e.oid = poid.oid;
    if (poid.oid.snap && poid.oid.snap < CEPH_NOSNAP) {
      e.op = Log::Entry::CLONE;
      osd->store->getattr(info.pgid.to_coll(), poid, "snaps", e.snaps);
      osd->store->getattr(info.pgid.to_coll(), poid, "from_version", 
			  &e.prior_version, sizeof(e.prior_version));
    } else {
      e.op = Log::Entry::MODIFY;           // FIXME when we do smarter op codes!
    }
    osd->store->getattr(info.pgid.to_coll(), poid, "version",
                        &e.version, sizeof(e.version));
    add[e.version] = e;
    dout(10) << "generate_backlog found " << e << dendl;
  }

  for (map<eversion_t,Log::Entry>::reverse_iterator i = add.rbegin();
       i != add.rend();
       i++) {
    log.log.push_front(i->second);
    log.index( *log.log.begin() );    // index
  }

  dout(10) << local << " local objects, "
           << add.size() << " objects added to backlog, " 
           << log.objects.size() << " in pg" << dendl;

  //log.print(cout);
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

    dout(15) << "drop_backlog trimming " << e.version << dendl;
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
    out << *p << " " << (logged_object(p->oid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(logged_object(p->oid));
    assert(logged_req(p->reqid));
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
    if (osdmap->is_down(*p)) {
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

  // build prior set.
  prior_set.clear();
  prior_set_down.clear();
  prior_set_up_thru.clear();

  // current nodes, of course.
  for (unsigned i=1; i<acting.size(); i++)
    prior_set.insert(acting[i]);

  // and prior PG mappings.  move backwards in time.
  state_clear(PG_STATE_CRASHED);
  state_clear(PG_STATE_DOWN);
  bool any_up_now = false;
  bool some_down = false;

  must_notify_mon = false;

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
	  prior_set_down.erase(interval.acting[i]);
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

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();
  log.reset_recovery_pointers();

  stat_object_temp_rd.clear();

  peer_scrub_map.clear();
  osd->recovery_wq.dequeue(this);
}

void PG::peer(ObjectStore::Transaction& t, 
              map< int, map<pg_t,Query> >& query_map,
	      map<int, MOSDPGInfo*> *activator_map)
{
  dout(10) << "peer.  acting is " << acting 
           << ", prior_set is " << prior_set << dendl;


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
  if (missing_info) return;

  
  // -- ok, we have all (prior_set) info.  (and maybe others.)

  dout(10) << " have prior_set info.  peers_complete_thru " << peers_complete_thru << dendl;


  /** CREATE THE MASTER PG::Log *********/

  // who (of all priors and active) has the latest PG version?
  eversion_t newest_update = info.last_update;
  int        newest_update_osd = osd->whoami;
  
  oldest_update = info.last_update;  // only of acting (current) osd set.
  peers_complete_thru = info.last_complete;
  
  for (map<int,Info>::iterator it = peer_info.begin();
       it != peer_info.end();
       it++) {
    if (osd->osdmap->is_down(it->first))
      continue;
    if (it->second.last_update > newest_update ||
	(it->second.last_update == newest_update &&    // prefer osds in the prior set
	 prior_set.count(newest_update_osd) == 0)) {
      newest_update = it->second.last_update;
      newest_update_osd = it->first;
    }
    if (is_acting(it->first)) {
      if (it->second.last_update < oldest_update) 
        oldest_update = it->second.last_update;
      if (it->second.last_complete < peers_complete_thru)
        peers_complete_thru = it->second.last_complete;
    }    
  }
  if (newest_update == info.last_update)   // or just me, if nobody better.
    newest_update_osd = osd->whoami;

  // gather log(+missing) from that person!
  if (newest_update_osd != osd->whoami) {
    if (peer_log_requested.count(newest_update_osd) ||
        peer_summary_requested.count(newest_update_osd)) {
      dout(10) << " newest update on osd" << newest_update_osd
               << " v " << newest_update 
               << ", already queried" 
               << dendl;
    } else {
      // we'd like it back to oldest_update, but will settle for log_bottom
      eversion_t since = MAX(peer_info[newest_update_osd].log_bottom,
                             oldest_update);
      if (peer_info[newest_update_osd].log_bottom < log.top) {
        dout(10) << " newest update on osd" << newest_update_osd
                 << " v " << newest_update 
                 << ", querying since " << since
                 << dendl;
        query_map[newest_update_osd][info.pgid] = Query(Query::LOG, log.top, since, info.history);
        peer_log_requested.insert(newest_update_osd);
      } else {
        dout(10) << " newest update on osd" << newest_update_osd
                 << " v " << newest_update 
                 << ", querying entire summary/backlog"
                 << dendl;
        assert((peer_info[newest_update_osd].last_complete >= 
                peer_info[newest_update_osd].log_bottom) ||
               peer_info[newest_update_osd].log_backlog);  // or else we're in trouble.
        query_map[newest_update_osd][info.pgid] = Query(Query::BACKLOG, info.history);
        peer_summary_requested.insert(newest_update_osd);
      }
    }
    return;
  } else {
    dout(10) << " newest_update " << info.last_update << " (me)" << dendl;
  }

  dout(10) << " oldest_update " << oldest_update << dendl;


  if (is_down()) {
    dout(10) << " down.  we wait." << dendl;    
    return;
  }

  have_master_log = true;


  // -- do i need to generate backlog for any of my peers?
  if (oldest_update < log.bottom && !log.backlog) {
    dout(10) << "generating backlog for some peers, bottom " 
             << log.bottom << " > " << oldest_update
             << dendl;
    generate_backlog();
  }


  /** COLLECT MISSING+LOG FROM PEERS **********/
  /*
    we also detect divergent replicas here by pulling the full log
    from everyone.  
  */  

  // gather missing from peers
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_info[peer].is_empty()) continue;
    if (peer_log_requested.count(peer) ||
        peer_summary_requested.count(peer)) continue;

    dout(10) << " pulling log+missing from osd" << peer
             << dendl;
    query_map[peer][info.pgid] = Query(Query::FULLLOG, info.history);
    peer_log_requested.insert(peer);
  }

  // did we get them all?
  bool have_missing = true;
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_info[peer].is_empty()) continue;
    if (peer_missing.count(peer)) continue;
    
    dout(10) << " waiting for log+missing from osd" << peer << dendl;
    have_missing = false;
  }
  if (!have_missing) return;

  dout(10) << " peers_complete_thru " << peers_complete_thru << dendl;

  
  // -- ok.  and have i located all pg contents?
  if (missing.num_missing() > missing_loc.size()) {
    dout(10) << "there are still " << (missing.num_missing() - missing_loc.size()) << " lost objects" << dendl;

    // let's pull info+logs from _everyone_ (strays included, this
    // time) in search of missing objects.

    bool waiting = false;
    for (map<int,Info>::iterator it = peer_info.begin();
         it != peer_info.end();
         it++) {
      int peer = it->first;

      if (osd->osdmap->is_down(peer))
	continue;

      if (peer_summary_requested.count(peer)) {
        dout(10) << " already requested summary/backlog from osd" << peer << dendl;
        waiting = true;
        continue;
      }

      dout(10) << " requesting summary/backlog from osd" << peer << dendl;      
      query_map[peer][info.pgid] = Query(Query::BACKLOG, info.history);
      peer_summary_requested.insert(peer);
      waiting = true;
    }
    
    if (!waiting)
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
    dout(10) << "crashed, allowing op replay for " << g_conf.osd_replay_window << dendl;
    state_set(PG_STATE_REPLAY);
    osd->timer.add_event_after(g_conf.osd_replay_window,
			       new OSD::C_Activate(osd, info.pgid, osd->osdmap->get_epoch()));
  } 
  else if (!is_active()) {
    // -- ok, activate!
    activate(t, activator_map);
  }
  else if (is_all_uptodate()) 
    finish_recovery();

  update_stats(); // update stats
}


void PG::activate(ObjectStore::Transaction& t,
		  map<int, MOSDPGInfo*> *activator_map)
{
  assert(!is_active());

  // twiddle pg state
  state_set(PG_STATE_ACTIVE);
  state_clear(PG_STATE_STRAY);
  state_clear(PG_STATE_DOWN);
  if (is_crashed()) {
    //assert(is_replay());      // HELP.. not on replica?
    state_clear(PG_STATE_CRASHED);
    state_clear(PG_STATE_REPLAY);
  }
  if (is_primary() && 
      info.pgid.size() != acting.size())
    state_set(PG_STATE_DEGRADED);
  else
    state_clear(PG_STATE_DEGRADED);
  
  info.history.last_epoch_started = osd->osdmap->get_epoch();
  trim_past_intervals();
  
  if (role == 0) {    // primary state
    peers_complete_thru = eversion_t(0,0);  // we don't know (yet)!
  }

  assert(info.last_complete >= log.bottom || log.backlog);

  // write pg info, log
  write_info(t);
  write_log(t);

  // clean up stray objects, snaps
  clean_up_local(t); 

  if (!info.dead_snaps.empty())
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
    while (log.complete_to->version < info.last_complete)
      log.complete_to++;
    assert(log.complete_to != log.log.end());
    dout(10) << "activate -     complete_to = " << log.complete_to->version << dendl;

    if (is_primary()) {
      // start recovery
      dout(10) << "activate - starting recovery" << dendl;
      log.requested_to = log.complete_to;
      osd->queue_for_recovery(this);
    }
  }

  // if primary..
  if (role == 0) {
    // who is clean?
    uptodate_set.clear();
    if (info.is_uptodate()) 
      uptodate_set.insert(osd->whoami);
    
    // start up replicas
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_info.count(peer));
      
      MOSDPGLog *m = 0;
      
      if (peer_info[peer].last_update == info.last_update) {
        // empty log
	if (activator_map) {
	  dout(10) << "activate - peer osd" << peer << " is up to date, queueing in pending_activators" << dendl;
	  if (activator_map->count(peer) == 0)
	    (*activator_map)[peer] = new MOSDPGInfo(osd->osdmap->get_epoch());
	  (*activator_map)[peer]->pg_info.push_back(info);
	} else {
	  dout(10) << "activate - peer osd" << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(osd->osdmap->get_epoch(), info);
	}
      } 
      else {
	m = new MOSDPGLog(osd->osdmap->get_epoch(), info);
	if (peer_info[peer].last_update < log.bottom) {
	  // summary/backlog
	  assert(log.backlog);
	  m->log = log;
	} else {
	  // incremental log
	  assert(peer_info[peer].last_update < info.last_update);
	  m->log.copy_after(log, peer_info[peer].last_update);
	}
      }

      // update local version of peer's missing list!
      if (m) {
        eversion_t plu = peer_info[peer].last_update;
        Missing& pm = peer_missing[peer];
        for (list<Log::Entry>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             p++) 
          if (p->version > plu)
            pm.add_event(*p);
      }
      
      if (m) {
	dout(10) << "activate sending " << m->log << " " << m->missing
		 << " to osd" << peer << dendl;
	//m->log.print(cout);
	osd->messenger->send_message(m, osd->osdmap->get_inst(peer));
      }

      // update our missing
      if (peer_missing[peer].num_missing() == 0) {
	peer_info[peer].last_complete = peer_info[peer].last_update;
        dout(10) << "activate peer osd" << peer << " already uptodate, " << peer_info[peer] << dendl;
	assert(peer_info[peer].is_uptodate());
        uptodate_set.insert(peer);
      } else {
        dout(10) << "activate peer osd" << peer << " " << peer_info[peer]
                 << " missing " << peer_missing[peer] << dendl;
      }
            
    }

    // discard unneeded peering state
    //peer_log.clear(); // actually, do this carefully, in case peer() is called again.
    
    // all clean?
    if (is_all_uptodate()) 
      finish_recovery();
    else {
      dout(10) << "activate not all replicas are uptodate, starting recovery" << dendl;
      osd->queue_for_recovery(this);
    }
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

  if (is_primary())
    update_stats(); // update stats

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

  log.reset_recovery_pointers();

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
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  osd->map_lock.put_read();
  osd->finish_recovery_op(this, recovery_ops_active, true);
  unlock();
  put();
}

void PG::defer_recovery()
{
  osd->defer_recovery(this);
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
}




void PG::update_stats()
{
  dout(15) << "update_stats" << dendl;

  pg_stats_lock.Lock();
  if (is_primary()) {
    // update our stat summary
    pg_stats_valid = true;
    pg_stats_stable = info.stats;
    pg_stats_stable.version = info.last_update;
    pg_stats_stable.reported = osd->osdmap->get_epoch();
    pg_stats_stable.state = state;
    pg_stats_stable.acting = acting;
    pg_stats_stable.num_objects_missing_on_primary = missing.num_missing();
  } else {
    pg_stats_valid = false;
  }
  pg_stats_lock.Unlock();

  // put in osd stat_queue
  osd->pg_stat_queue_lock.Lock();
  if (is_primary())
    osd->pg_stat_queue[info.pgid] = info.last_update;    
  osd->osd_stat_updated = true;
  osd->pg_stat_queue_lock.Unlock();
}

void PG::clear_stats()
{
  pg_stats_lock.Lock();
  pg_stats_valid = false;
  pg_stats_lock.Unlock();
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
    if (bl.length() % 4096 == 0)
      ondisklog.block_map[bl.length()] = p->version;
    ::encode(*p, bl);
    /*
    if (g_conf.osd_pad_pg_log) {  // pad to 4k, until i fix ebofs reallocation crap.  FIXME.
      bufferptr bp(4096 - sizeof(*p));
      bl.push_back(bp);
    }
    */
  }
  ondisklog.top = bl.length();
  
  // write it
  t.remove(0, info.pgid.to_log_pobject() );
  t.write(0, info.pgid.to_log_pobject() , 0, bl.length(), bl);
  t.collection_setattr(info.pgid.to_coll(), "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  t.collection_setattr(info.pgid.to_coll(), "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  
  dout(10) << "write_log to [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;
  dirty_log = false;
}

void PG::trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v) 
{
  dout(15) << "trim_ondisk_log_to v " << v << dendl;

  map<loff_t,eversion_t>::iterator p = ondisklog.block_map.begin();
  while (p != ondisklog.block_map.end()) {
    dout(15) << "    " << p->first << " -> " << p->second << dendl;
    p++;
    if (p == ondisklog.block_map.end() ||
        p->second > v) {  // too far!
      p--;                // back up
      break;
    }
  }
  dout(15) << "  * " << p->first << " -> " << p->second << dendl;
  if (p == ondisklog.block_map.begin()) 
    return;  // can't trim anything!
  
  // we can trim!
  loff_t trim = p->first;
  dout(10) << "  trimming ondisklog to [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;

  assert(trim >= ondisklog.bottom);
  ondisklog.bottom = trim;
  
  // adjust block_map
  while (p != ondisklog.block_map.begin()) 
    ondisklog.block_map.erase(ondisklog.block_map.begin());
  
  t.collection_setattr(info.pgid.to_coll(), "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  t.collection_setattr(info.pgid.to_coll(), "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));

  if (!g_conf.osd_preserve_trimmed_log)
    t.zero(0, info.pgid.to_log_pobject(), 0, ondisklog.bottom);
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
		    eversion_t logversion, eversion_t trim_to)
{
  dout(10) << "append_log " << ondisklog.bottom << "," << ondisklog.top << ") adding " << bl.length() <<  dendl;
 
  // update block map?
  if (ondisklog.top % 4096 == 0) 
    ondisklog.block_map[ondisklog.top] = logversion;

  t.write(0, info.pgid.to_log_pobject(), ondisklog.top, bl.length(), bl );
  
  ondisklog.top += bl.length();
  t.collection_setattr(info.pgid.to_coll(), "ondisklog_top",
		       &ondisklog.top, sizeof(ondisklog.top));
  
  // trim?
  if (trim_to > log.bottom &&
      is_clean()) {
    dout(10) << " trimming " << log << " to " << trim_to << dendl;
    log.trim(t, trim_to);
    info.log_bottom = log.bottom;
    info.log_backlog = log.backlog;
    trim_ondisklog_to(t, trim_to);
  }
  dout(10) << " ondisklog [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;
}

void PG::read_log(ObjectStore *store)
{
  int r;
  // load bounds
  ondisklog.bottom = ondisklog.top = 0;
  r = store->collection_getattr(info.pgid.to_coll(), "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  assert(r == sizeof(ondisklog.bottom));
  r = store->collection_getattr(info.pgid.to_coll(), "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  assert(r == sizeof(ondisklog.top));

  dout(10) << "read_log [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;

  log.backlog = info.log_backlog;
  log.bottom = info.log_bottom;
  
  if (ondisklog.top > 0) {
    // read
    bufferlist bl;
    store->read(0, info.pgid.to_log_pobject(), ondisklog.bottom, ondisklog.top-ondisklog.bottom, bl);
    if (bl.length() < ondisklog.top-ondisklog.bottom) {
      dout(0) << "read_log got " << bl.length() << " bytes, expected " 
	      << ondisklog.top << "-" << ondisklog.bottom << "="
	      << (ondisklog.top-ondisklog.bottom)
	      << dendl;
      assert(0);
    }
    
    PG::Log::Entry e;
    bufferlist::iterator p = bl.begin();
    assert(log.log.empty());
    while (!p.end()) {
      ::decode(e, p);
      loff_t pos = ondisklog.bottom + p.get_off();
      dout(10) << "read_log " << pos << " " << e << dendl;

      if (e.version > log.bottom || log.backlog) { // ignore items below log.bottom
        if (pos % 4096 == 0)
	  ondisklog.block_map[pos] = e.version;
        log.log.push_back(e);
      } else {
	dout(10) << "read_log ignoring entry at " << pos << dendl;
      }
    }
  }
  log.top = info.last_update;
  log.index();

  // build missing
  set<object_t> did;
  for (list<Log::Entry>::reverse_iterator i = log.log.rbegin();
       i != log.log.rend();
       i++) {
    if (i->version <= info.last_complete) break;
    if (did.count(i->oid)) continue;
    did.insert(i->oid);

    if (i->is_delete()) continue;

    eversion_t v;
    pobject_t poid(info.pgid.pool(), 0, i->oid);
    int r = osd->store->getattr(info.pgid.to_coll(), poid, "version", &v, sizeof(v));
    if (r < 0 || v < i->version) 
      missing.add_event(*i);
  }
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
bool PG::block_if_wrlocked(MOSDOp* op)
{
  pobject_t poid(info.pgid.pool(), 0, op->get_oid());

  entity_name_t source;
  int len = osd->store->getattr(info.pgid.to_coll(), poid, "wrlock", &source, sizeof(entity_name_t));
  //dout(0) << "getattr returns " << len << " on " << oid << dendl;
  
  if (len == sizeof(source) &&
      source != op->get_orig_source()) {
    //the object is locked for writing by someone else -- add the op to the waiting queue      
    waiting_for_wr_unlock[poid.oid].push_back(op);
    return true;
  }
  
  return false; //the object wasn't locked, so the operation can be handled right away
}



// ==========================================================================================
// SCRUB


/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 */ 
void PG::build_scrub_map(ScrubMap &map)
{
  dout(10) << "build_scrub_map" << dendl;
  coll_t c = info.pgid.to_coll();

  // objects
  vector<pobject_t> ls;
  osd->store->collection_list(c, ls);

  // sort
  dout(10) << "sorting" << dendl;
  vector< pair<pobject_t,int> > tab(ls.size());
  vector< pair<pobject_t,int> >::iterator q = tab.begin();
  int i = 0;
  for (vector<pobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++, q++) {
    q->first = *p;
    q->second = i;
  }
  sort(tab.begin(), tab.end());
  // tab is now sorted, with ->second indicating object's original position
  vector<int> pos(ls.size());
  i = 0;
  for (vector< pair<pobject_t,int> >::iterator p = tab.begin(); 
       p != tab.end(); 
       p++, i++)
    pos[p->second] = i;
  // now, pos[orig pos] = sorted pos

  dout(10) << " " << ls.size() << " objects" << dendl;
  map.objects.resize(ls.size());
  i = 0;
  for (vector<pobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       p++, i++) {
    pobject_t poid = *p;

    ScrubMap::object& o = map.objects[pos[i]];
    o.poid = *p;

    struct stat st;
    int r = osd->store->stat(c, poid, &st);
    assert(r == 0);
    o.size = st.st_size;

    osd->store->getattrs(c, poid, o.attrs);    

    dout(15) << "   " << poid << dendl;
  }

  // pg attrs
  osd->store->collection_getattrs(c, map.attrs);

  // log
  osd->store->read(coll_t(), info.pgid.to_log_pobject(), 0, 0, map.logbl);
  dout(10) << " log " << map.logbl.length() << " bytes" << dendl;
}



void PG::scrub()
{
  osd->map_lock.get_read();

  lock();
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
    dout(10) << " requesting scrubmap from osd" << acting[i] << dendl;
    osd->messenger->send_message(new MOSDPGScrub(info.pgid, osd->osdmap->get_epoch()),
				 osd->osdmap->get_inst(acting[i]));
  }

  osd->map_lock.put_read();

  dout(10) << " building my scrub map" << dendl;
  ScrubMap map;
  build_scrub_map(map);

  while (peer_scrub_map.size() < acting.size() - 1) {
    dout(10) << " have " << peer_scrub_map.size() << " / " << (acting.size()-1)
	     << " scrub maps, waiting" << dendl;
    wait();
  }

  // first, compare scrub maps
  vector<ScrubMap*> m(acting.size());
  m[0] = &map;
  for (unsigned i=1; i<acting.size(); i++)
    m[i] = &peer_scrub_map[acting[i]];
  vector<ScrubMap::object>::iterator p[acting.size()];
  for (unsigned i=0; i<acting.size(); i++)
    p[i] = m[i]->objects.begin();

  while (1) {
    ScrubMap::object *po = 0;
    bool missing = false;
    for (unsigned i=0; i<acting.size(); i++) {
      if (p[i] == m[i]->objects.end())
	continue;
      if (!po)
	po = &(*p[i]);
      else if (po->poid != p[i]->poid) {
	missing = true;
	if (po->poid > p[i]->poid)
	  po = &(*p[i]);
      }
    }
    if (!po)
      break;
    if (missing) {
      for (unsigned i=0; i<acting.size(); i++) {
	if (po->poid != p[i]->poid)
	  dout(0) << " osd" << acting[i] << " missing " << po->poid << dendl;
	else
	  p[i]++;
      }
      continue;
    }

    // compare
    bool ok = true;
    for (unsigned i=1; i<acting.size(); i++) {
      if (po->size != p[i]->size) {
	dout(0) << " osd" << acting[i] << " " << po->poid
		<< " size " << p[i]->size << " != " << po->size << dendl;
	ok = false;
      }
      // fixme: check attrs
    }

    
    if (ok)
      dout(10) << "scrub " << po->poid << " size " << po->size << " ok" << dendl;

    // next
    for (unsigned i=0; i<acting.size(); i++)
      p[i]++;
  }

  // discard peer scrub info.
  peer_scrub_map.clear();

  // ok, do the pg-type specific scrubbing
  _scrub(map);
  
  info.stats.last_scrub = info.last_update;
  info.stats.last_scrub_stamp = g_clock.now();
  state_clear(PG_STATE_SCRUBBING);
  update_stats();

  dout(10) << "scrub done" << dendl;

  unlock();
}


