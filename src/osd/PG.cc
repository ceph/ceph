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
#include "messages/MOSDPGActivateSet.h"

#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_dout << dbeginl << g_clock.now() << " osd" << osd->whoami << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "


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

void PG::proc_replica_log(Log &olog, Missing& omissing, int from)
{
  dout(10) << "proc_replica_log for osd" << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active());

  if (!have_master_log) {
    // i'm building master log.
    // note peer's missing.
    peer_missing[from] = omissing;
    
    // merge log into our own log
    merge_log(olog, omissing, from);
    proc_missing(olog, omissing, from);
  } else {
    // i'm just building missing lists.
    peer_missing[from] = omissing;

    // iterate over peer log. in reverse.
    list<Log::Entry>::reverse_iterator pp = olog.log.rbegin();
    eversion_t lu = peer_info[from].last_update;
    while (pp != olog.log.rend()) {
      if (!log.objects.count(pp->oid)) {
        dout(10) << " divergent " << *pp << " not in our log, generating backlog" << dendl;
        generate_backlog();
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
        peer_missing[from].add(pp->oid, pp->version);
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

    proc_missing(olog, peer_missing[from], from);
  }
}

void PG::merge_log(Log &olog, Missing &omissing, int fromosd)
{
  dout(10) << "merge_log " << olog << " from osd" << fromosd
           << " into " << log << dendl;

  //dout(0) << "log" << dendl;
  //log.print(cout);
  //dout(0) << "olog" << dendl;
  //olog.print(cout);
  
  if (log.empty() ||
      (olog.bottom > log.top && olog.backlog)) { // e.g. log=(0,20] olog=(40,50]+backlog) 

    // swap and index
    log.log.swap(olog.log);
    log.index();

    // find split point (old log.top) in new log
    // add new items to missing along the way.
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (p->version <= log.top) {
        // ok, p is at split point.

        // was our old log divergent?
        if (log.top > p->version) { 
          dout(10) << "merge_log i was (possibly) divergent for (" << p->version << "," << log.top << "]" << dendl;
          if (p->version < oldest_update)
            oldest_update = p->version;
          
          while (!olog.log.empty() && 
                 olog.log.rbegin()->version > p->version) {
            Log::Entry &oe = *olog.log.rbegin();  // old entry (possibly divergent)
            if (log.objects.count(oe.oid)) {
              if (log.objects[oe.oid]->version < oe.version) {
                dout(10) << "merge_log  divergent entry " << oe
                         << " not superceded by " << *log.objects[oe.oid]
                         << ", adding to missing" << dendl;
                missing.add(oe.oid, oe.version);
              } else {
                dout(10) << "merge_log  divergent entry " << oe
                         << " superceded by " << *log.objects[oe.oid] 
                         << ", ignoring" << dendl;
              }
            } else {
              dout(10) << "merge_log  divergent entry " << oe << ", adding to missing" << dendl;
              missing.add(oe.oid, oe.version);
            }
            olog.log.pop_back();  // discard divergent entry
          }
        }
        break;
      }

      if (p->is_delete()) {
        dout(10) << "merge_log merging " << *p << ", not missing" << dendl;
        missing.rm(p->oid, p->version);
      } else {
        dout(10) << "merge_log merging " << *p << ", now missing" << dendl;
        missing.add(p->oid, p->version);
      }
    }

    info.last_update = log.top = olog.top;
    info.log_bottom = log.bottom = olog.bottom;
    info.log_backlog = log.backlog = olog.backlog;
  } 

  else {
    // i can merge the two logs!

    // extend on bottom?
    // FIXME: what if we have backlog, but they have lower bottom?
    if (olog.bottom < log.bottom && olog.top >= log.bottom && !log.backlog) {
      dout(10) << "merge_log extending bottom to " << olog.bottom
               << (olog.backlog ? " +backlog":"")
             << dendl;
      
      // ok
      list<Log::Entry>::iterator from = olog.log.begin();
      list<Log::Entry>::iterator to;
      for (to = from;
           to != olog.log.end();
           to++) {
        if (to->version > log.bottom) break;
        
        // update our index while we're here
        log.index(*to);
        
        dout(15) << *to << dendl;
        
        // new missing object?
        if (to->version > info.last_complete) {
          if (to->is_update()) 
            missing.add(to->oid, to->version);
          else 
          missing.rm(to->oid, to->version);
        }
      }
      assert(to != olog.log.end());
      
      // splice into our log.
      log.log.splice(log.log.begin(),
                     olog.log, from, to);
      
      info.log_bottom = log.bottom = olog.bottom;
      info.log_backlog = log.backlog = olog.backlog;
    }
    
    // extend on top?
    if (olog.top > log.top &&
        olog.bottom <= log.top) {
      dout(10) << "merge_log extending top to " << olog.top << dendl;
      
      list<Log::Entry>::iterator to = olog.log.end();
      list<Log::Entry>::iterator from = olog.log.end();
      while (1) {
        if (from == olog.log.begin()) break;
        from--;
        //dout(0) << "? " << *from << dendl;
        if (from->version < log.top) {
          from++;
          break;
        }
        
        log.index(*from);
        dout(10) << "merge_log " << *from << dendl;
        
        // add to missing
        if (from->is_update()) {
          missing.add(from->oid, from->version);
        } else
          missing.rm(from->oid, from->version);
      }
      
      // remove divergent items
      while (1) {
        Log::Entry *oldtail = &(*log.log.rbegin());
        if (oldtail->version.version+1 == from->version.version) break;

        // divergent!
        assert(oldtail->version.version >= from->version.version);
        
        if (log.objects[oldtail->oid]->version == oldtail->version) {
          // and significant.
          dout(10) << "merge_log had divergent " << *oldtail << ", adding to missing" << dendl;
          //missing.add(oldtail->oid);
          assert(0);
        } else {
          dout(10) << "merge_log had divergent " << *oldtail << ", already missing" << dendl;
          assert(missing.is_missing(oldtail->oid));
        }
        log.log.pop_back();
      }

      // splice
      log.log.splice(log.log.end(), 
                     olog.log, from, to);
      
      info.last_update = log.top = olog.top;
    }
  }
  
  dout(10) << "merge_log result " << log << " " << missing << dendl;
  //log.print(cout);

}

void PG::proc_missing(Log &olog, Missing &omissing, int fromosd)
{
  // found items?
  for (map<object_t,eversion_t>::iterator p = missing.missing.begin();
       p != missing.missing.end();
       p++) {
    if (omissing.is_missing(p->first)) {
      assert(omissing.is_missing(p->first, p->second));
      if (omissing.loc.count(p->first)) {
        dout(10) << "proc_missing missing " << p->first << " " << p->second
                 << " on osd" << omissing.loc[p->first] << dendl;
        missing.loc[p->first] = omissing.loc[p->first];
      } else {
        dout(10) << "proc_missing missing " << p->first << " " << p->second
                 << " also LOST on source, osd" << fromosd << dendl;
      }
    } 
    else if (p->second <= olog.top) {
      dout(10) << "proc_missing missing " << p->first << " " << p->second
               << " on source, osd" << fromosd << dendl;
      missing.loc[p->first] = fromosd;
    } else {
      dout(10) << "proc_missing " << p->first << " " << p->second
               << " > olog.top " << olog.top << ", not found...."
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

  list<pobject_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  int local = 0;
  map<eversion_t,Log::Entry> add;
  for (list<pobject_t>::iterator it = olist.begin();
       it != olist.end();
       it++) {
    local++;
    object_t oid = it->oid;

    if (log.logged_object(oid)) continue; // already have it logged.
    
    // add entry
    Log::Entry e;
    e.op = Log::Entry::MODIFY;           // FIXME when we do smarter op codes!
    e.oid = oid;
    osd->store->getattr(pobject_t(0,0,oid), 
                        "version",
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
  out << *this << dendl;
  for (list<Entry>::const_iterator p = log.begin();
       p != log.end();
       p++) 
    out << *p << dendl;
  return out;
}





/******* PG ***********/
void PG::build_prior()
{
  // build prior set.
  prior_set.clear();
  
  // current
  for (unsigned i=1; i<acting.size(); i++)
    prior_set.insert(acting[i]);

  // and prior map(s), if OSDs are still up
  for (epoch_t epoch = MAX(1, last_epoch_started_any);
       epoch < osd->osdmap->get_epoch();
       epoch++) {
    OSDMap omap;
    osd->get_map(epoch, omap);
    
    vector<int> acting;
    omap.pg_to_acting_osds(get_pgid(), acting);
    
    for (unsigned i=0; i<acting.size(); i++) {
      dout(10) << "build prior considering epoch " << epoch << " osd" << acting[i] << dendl;
      if (osd->osdmap->is_up(acting[i]) &&  // is up now
          acting[i] != osd->whoami)         // and is not me
        prior_set.insert(acting[i]);
    }
  }

  dout(10) << "build_prior built " << prior_set << dendl;
}

void PG::adjust_prior()
{
  assert(!prior_set.empty());

  // raise last_epoch_started_any
  epoch_t max = 0;
  for (map<int,Info>::iterator it = peer_info.begin();
       it != peer_info.end();
       it++) {
    if (it->second.last_epoch_started > max)
      max = it->second.last_epoch_started;
  }

  dout(10) << "adjust_prior last_epoch_started_any " 
           << last_epoch_started_any << " -> " << max << dendl;
  assert(max > last_epoch_started_any);
  last_epoch_started_any = max;

  // rebuild prior set
  build_prior();
}


void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  have_master_log = false;
  prior_set.clear();
  stray_set.clear();
  uptodate_set.clear();
  peer_info_requested.clear();
  peer_log_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  
  stat_object_temp_rd.clear();

  last_epoch_started_any = info.last_epoch_started;
}

void PG::peer(ObjectStore::Transaction& t, 
              map< int, map<pg_t,Query> >& query_map,
	      map<int, MOSDPGActivateSet*> *activator_map)
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
    
    dout(10) << " querying info from osd" << *it << dendl;
    query_map[*it][info.pgid] = Query(Query::INFO, info.history);
    peer_info_requested.insert(*it);
  }
  if (missing_info) return;

  
  // -- ok, we have all (prior_set) info.  (and maybe others.)

  // did we crash?
  dout(10) << " last_epoch_started_any " << last_epoch_started_any << dendl;
  if (last_epoch_started_any) {
    OSDMap omap;
    osd->get_map(last_epoch_started_any, omap);
    
    // start with the last active set of replicas
    set<int> last_started;
    vector<int> acting;
    bool cleanly_down = true;
    omap.pg_to_acting_osds(get_pgid(), acting);
    for (unsigned i=0; i<acting.size(); i++)
      last_started.insert(acting[i]);

    // make sure at least one of them is still up
    for (epoch_t e = last_epoch_started_any+1;
         e <= osd->osdmap->get_epoch();
         e++) {
      OSDMap omap;
      osd->get_map(e, omap);
      
      set<int> still_up;

      for (set<int>::iterator i = last_started.begin();
           i != last_started.end();
           i++) {
        //dout(10) << " down in epoch " << e << " is " << omap.get_down_osds() << dendl;
        if (omap.is_up(*i))
          still_up.insert(*i);
	else if (!omap.is_down_clean(*i))
	  cleanly_down = false;	   
      }

      last_started.swap(still_up);
      //dout(10) << " still active as of epoch " << e << ": " << last_started << dendl;
    }
    
    if (last_started.empty()) {
      if (cleanly_down) {
	dout(10) << " cleanly stopped since epoch " << last_epoch_started_any << dendl;
      } else {
	dout(10) << " crashed since epoch " << last_epoch_started_any << dendl;
	state_set(STATE_CRASHED);
      }
    } else {
      dout(10) << " still active from last started: " << last_started << dendl;
    }
  } else if (osd->osdmap->post_mkfs()) {
    dout(10) << " crashed since epoch " << last_epoch_started_any << dendl;
    state_set(STATE_CRASHED);
  }    

  dout(10) << " peers_complete_thru " << peers_complete_thru << dendl;




  /** CREATE THE MASTER PG::Log *********/

  // who (of all priors and active) has the latest PG version?
  eversion_t newest_update = info.last_update;
  int        newest_update_osd = osd->whoami;
  
  oldest_update = info.last_update;  // only of acting (current) osd set.
  peers_complete_thru = info.last_complete;
  
  for (map<int,Info>::iterator it = peer_info.begin();
       it != peer_info.end();
       it++) {
    if (it->second.last_update > newest_update) {
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
  if (missing.num_lost() > 0) {
    dout(10) << "there are still " << missing.num_lost() << " lost objects" << dendl;

    // *****
    // FIXME: i don't think this actually accomplishes anything!
    // *****

    // ok, let's get more summaries!
    bool waiting = false;
    for (map<int,Info>::iterator it = peer_info.begin();
         it != peer_info.end();
         it++) {
      int peer = it->first;

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
    
    if (!waiting) {
      dout(10) << missing.num_lost() << " objects are still lost, waiting+hoping for a notify from someone else!" << dendl;
    }
    return;
  }

  // sanity check
  assert(missing.num_lost() == 0);
  assert(info.last_complete >= log.bottom || log.backlog);


  // -- crash recovery?
  if (is_crashed()) {
    dout(10) << "crashed, allowing op replay for " << g_conf.osd_replay_window << dendl;
    state_set(STATE_REPLAY);
    osd->timer.add_event_after(g_conf.osd_replay_window,
			       new OSD::C_Activate(osd, info.pgid, osd->osdmap->get_epoch()));
  } 
  else if (!is_active()) {
    // -- ok, activate!
    activate(t, activator_map);
  }
}


void PG::activate(ObjectStore::Transaction& t,
		  map<int, MOSDPGActivateSet*> *activator_map)
{
  assert(!is_active());

  // twiddle pg state
  state_set(STATE_ACTIVE);
  state_clear(STATE_STRAY);
  if (is_crashed()) {
    //assert(is_replay());      // HELP.. not on replica?
    state_clear(STATE_CRASHED);
    state_clear(STATE_REPLAY);
  }
  last_epoch_started_any = info.last_epoch_started = osd->osdmap->get_epoch();
  
  if (role == 0) {    // primary state
    peers_complete_thru = eversion_t(0,0);  // we don't know (yet)!
  }

  assert(info.last_complete >= log.bottom || log.backlog);

  // write pg info
  t.collection_setattr(info.pgid, "info", (char*)&info, sizeof(info));
  
  // write log
  write_log(t);

  // clean up stray objects
  clean_up_local(t); 

  // init complete pointer
  if (info.last_complete == info.last_update) {
    dout(10) << "activate - complete" << dendl;
    log.complete_to == log.log.end();
    log.requested_to = log.log.end();
  } 
  else if (true) {
    dout(10) << "activate - not complete, " << missing << dendl;
    
    // init complete_to
    log.complete_to = log.log.begin();
    while (log.complete_to->version < info.last_complete) {
      log.complete_to++;
      assert(log.complete_to != log.log.end());
    }
    
    if (is_primary()) {
      // start recovery
      dout(10) << "activate - starting recovery" << dendl;    
      log.requested_to = log.complete_to;
      do_recovery();
    }
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
  }

  // if primary..
  if (role == 0 &&
      (!g_conf.osd_hack_fast_startup || osd->osdmap->post_mkfs())) {
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
	    (*activator_map)[peer] = new MOSDPGActivateSet(osd->osdmap->get_epoch());
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
            pm.add(p->oid, p->version);
      }
      
      if (m) {
	dout(10) << "activate sending " << m->log << " " << m->missing
		 << " to osd" << peer << dendl;
	//m->log.print(cout);
	osd->messenger->send_message(m, osd->osdmap->get_inst(peer));
      }

      // update our missing
      if (peer_missing[peer].num_missing() == 0) {
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
      do_recovery();
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


void PG::finish_recovery()
{
  dout(10) << "finish_recovery" << dendl;

  state_set(PG::STATE_CLEAN);
  purge_strays();
  update_stats();
}



void PG::update_stats()
{
  dout(15) << "update_stats" << dendl;

  if (is_primary()) {
    // update our stat summary
    pg_stats_lock.Lock();
    pg_stats.reported = info.last_update;
    pg_stats.state = state;
    pg_stats.num_bytes = stat_num_bytes;
    pg_stats.num_blocks = stat_num_blocks;
    pg_stats_lock.Unlock();
  }

  // put in osd stat_queue
  osd->pg_stat_queue_lock.Lock();
  if (is_primary())
    osd->pg_stat_queue.insert(info.pgid);    
  osd->osd_stat_updated = true;
  osd->pg_stat_queue_lock.Unlock();
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
    bl.append((char*)&(*p), sizeof(*p));
    if (g_conf.osd_pad_pg_log) {  // pad to 4k, until i fix ebofs reallocation crap.  FIXME.
      bufferptr bp(4096 - sizeof(*p));
      bl.push_back(bp);
    }
  }
  ondisklog.top = bl.length();
  
  // write it
  t.remove( info.pgid.to_object() );
  t.write( info.pgid.to_object() , 0, bl.length(), bl);
  t.collection_setattr(info.pgid, "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  t.collection_setattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  
  t.collection_setattr(info.pgid, "info", &info, sizeof(info)); 
}

void PG::trim_ondisklog_to(ObjectStore::Transaction& t, eversion_t v) 
{
  dout(15) << "  trim_ondisk_log_to v " << v << dendl;

  map<off_t,eversion_t>::iterator p = ondisklog.block_map.begin();
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
  off_t trim = p->first;
  dout(10) << "  trimming ondisklog to [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;

  assert(trim >= ondisklog.bottom);
  ondisklog.bottom = trim;
  
  // adjust block_map
  while (p != ondisklog.block_map.begin()) 
    ondisklog.block_map.erase(ondisklog.block_map.begin());
  
  t.collection_setattr(info.pgid, "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  t.collection_setattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  t.zero(info.pgid.to_object(), 0, ondisklog.bottom);
}


void PG::append_log(ObjectStore::Transaction& t, PG::Log::Entry& logentry, 
                    eversion_t trim_to)
{
  dout(10) << "append_log " << ondisklog.top << " " << logentry << dendl;

  // write entry on disk
  bufferlist bl;
  bl.append( (char*)&logentry, sizeof(logentry) );
  if (g_conf.osd_pad_pg_log) {  // pad to 4k, until i fix ebofs reallocation crap.  FIXME.
    bufferptr bp(4096 - sizeof(logentry));
    bl.push_back(bp);
  }
  t.write( info.pgid.to_object(), ondisklog.top, bl.length(), bl );
  
  // update block map?
  if (ondisklog.top % 4096 == 0) 
    ondisklog.block_map[ondisklog.top] = logentry.version;
  
  ondisklog.top += bl.length();
  t.collection_setattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  
  // trim?
  if (trim_to > log.bottom) {
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
  r = store->collection_getattr(info.pgid, "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  assert(r == sizeof(ondisklog.bottom));
  r = store->collection_getattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  assert(r == sizeof(ondisklog.top));

  dout(10) << "read_log [" << ondisklog.bottom << "," << ondisklog.top << ")" << dendl;

  log.backlog = info.log_backlog;
  log.bottom = info.log_bottom;
  
  if (ondisklog.top > 0) {
    // read
    bufferlist bl;
    store->read(info.pgid.to_object(), ondisklog.bottom, ondisklog.top-ondisklog.bottom, bl);
    if (bl.length() < ondisklog.top-ondisklog.bottom) {
      dout(0) << "read_log data doesn't match attrs" << dendl;
      assert(0);
    }
    
    PG::Log::Entry e;
    off_t pos = ondisklog.bottom;
    assert(log.log.empty());
    while (pos < ondisklog.top) {
      bl.copy(pos-ondisklog.bottom, sizeof(e), (char*)&e);
      dout(10) << "read_log " << pos << " " << e << dendl;

      if (e.version > log.bottom || log.backlog) { // ignore items below log.bottom
        if (pos % 4096 == 0)
	  ondisklog.block_map[pos] = e.version;
        log.log.push_back(e);
      } else {
	dout(10) << "read_log ignoring entry at " << pos << dendl;
      }
      
      if (g_conf.osd_pad_pg_log)   // pad to 4k, until i fix ebofs reallocation crap.  FIXME.
	pos += 4096;
      else
	pos += sizeof(e);
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
    int r = osd->store->getattr(i->oid, "version", &v, sizeof(v));
    if (r < 0 || v < i->version) 
      missing.add(i->oid, i->version);
  }
}




// ==============================
// Object locking

//
// If the target object of the operation op is locked for writing by another client, the function puts op to the waiting queue waiting_for_wr_unlock
// returns true if object was locked, otherwise returns false
// 
bool PG::block_if_wrlocked(MOSDOp* op)
{
  object_t oid = op->get_oid();

  entity_name_t source;
  int len = osd->store->getattr(oid, "wrlock", &source, sizeof(entity_name_t));
  //dout(0) << "getattr returns " << len << " on " << oid << dendl;
  
  if (len == sizeof(source) &&
      source != op->get_client()) {
    //the object is locked for writing by someone else -- add the op to the waiting queue      
    waiting_for_wr_unlock[oid].push_back(op);
    return true;
  }
  
  return false; //the object wasn't locked, so the operation can be handled right away
}




// =======================
// revisions


/*
int OSD::list_missing_revs(object_t oid, set<object_t>& revs, PG *pg)
{
  int c = 0;
  oid.rev = 0;
  
  map<object_t,eversion_t>::iterator p = pg->missing.missing.lower_bound(oid);
  if (p == pg->missing.missing.end()) 
    return 0;  // clearly not

  while (p->first.ino == oid.ino &&
	 p->first.bno == oid.bno) {
    revs.insert(p->first);
    c++;
  }
  return c;
}*/

bool PG::pick_missing_object_rev(object_t& oid)
{
  map<object_t,eversion_t>::iterator p = missing.missing.upper_bound(oid);
  if (p == missing.missing.end()) 
    return false;  // clearly no candidate

  if (p->first.ino == oid.ino && p->first.bno == oid.bno) {
    oid = p->first;  // yes!  it's an upper bound revision for me.
    return true;
  }
  return false;
}

bool PG::pick_object_rev(object_t& oid)
{
  pobject_t t = oid;

  if (!osd->store->pick_object_revision_lt(t))
    return false; // we have no revisions of this object!
  
  objectrev_t crev;
  int r = osd->store->getattr(t, "crev", &crev, sizeof(crev));
  assert(r >= 0);
  if (crev <= oid.rev) {
    dout(10) << "pick_object_rev choosing " << t << " crev " << crev << " for " << oid << dendl;
    oid = t.oid;
    return true;
  }

  return false;  
}








