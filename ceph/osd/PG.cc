// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGSummary.h"
#include "messages/MOSDPGRemove.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << osd->whoami << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "


/******* PGLog ********/


void PG::PGLog::trim(version_t s) 
{
  // trim updated
  while (!updated.empty()) {
	map<version_t,object_t>::iterator rit = rupdated.begin();
	if (rit->first > s) break;
	updated.erase(rit->second);
	rupdated.erase(rit);
  }
  
  // trim deleted
  while (!deleted.empty()) {
	map<version_t, object_t>::iterator rit = rdeleted.begin();
	if (rit->first > s) break;
	deleted.erase(rit->second);
	rdeleted.erase(rit);
  }
  
  bottom = s;
}


void PG::PGLog::copy_after(const PGLog &other, version_t v) 
{
  assert(v >= other.bottom);
  bottom = top = v;
  assert(updated.empty() && deleted.empty());
  
  // updated
  for (map<version_t, object_t>::const_iterator it = other.rupdated.upper_bound(v); // first > v
	   it != other.rupdated.end();
	   it++) 
	add_update(it->second, it->first);
  
  // deleted
  for (map<version_t, object_t>::const_iterator it = other.rdeleted.upper_bound(v); // first > v
	   it != other.rdeleted.end();
	   it++) 
	add_delete(it->second, it->first);
  
  assert(top == other.top);
}



void PG::PGLog::merge(const PGLog &other, PGMissing &missing)
{
  if (empty()) {
	// special (easy) case
	updated = other.updated;
	rupdated = other.rupdated;
	deleted = other.deleted;
	rdeleted = other.rdeleted;
	top = other.top;
	bottom = other.bottom;

	// add all to missing
	for (map<object_t,version_t>::const_iterator pu = updated.begin();
		 pu != updated.end();
		 pu++)
	  missing.add(pu->first, pu->second);
	
	return;
  }

  // extend on bottom?
  if (other.bottom < bottom) {
	version_t newbottom = other.bottom;
	
	// updated
	for (map<version_t,object_t>::const_iterator p = other.rupdated.begin();
		 p != other.rupdated.end();
		 p++) {
	  if (p->first > bottom) break;
	  if (updated.count(p->second) || deleted.count(p->second)) continue;
	  updated[p->second] = p->first;
	  rupdated[p->first] = p->second;
	}
	
	// deleted
	for (map<version_t,object_t>::const_iterator p = other.rdeleted.begin();
		 p != other.rdeleted.end();
		 p++) {
	  if (p->first > bottom) break;
	  if (updated.count(p->second) || deleted.count(p->second)) continue;
	  deleted[p->second] = p->first;
	  rdeleted[p->first] = p->second;
	}
	bottom = newbottom;
  }
  
  // extend on top?
  if (other.top > top) {
	map<version_t,object_t>::const_iterator pu = other.rupdated.lower_bound(top);
	map<version_t,object_t>::const_iterator pd = other.rdeleted.lower_bound(top);
	
	// both
	while (pu != other.rupdated.end() && pd != other.rdeleted.end()) {
	  assert(pd->first != pu->first);
	  if (pu->first > pd->first) {
		add_update(pu->second, pu->first);
		missing.add(pu->second, pu->first);
		pu++;
	  } else {
		add_delete(pd->second, pd->first);
		missing.rm(pu->second, pu->first);
		pd++;
	  }
	}
	// tail
	while (pu != other.rupdated.end()) {
	  add_update(pu->second, pu->first);
	  missing.add(pu->second, pu->first);
	  pu++;
	}
	while (pd != other.rdeleted.end()) {
	  add_delete(pd->second, pd->first);
	  missing.rm(pu->second, pu->first);
	  pd++;
	}
	top = other.top;
  }
}

/** assumilate_summary
 * reconstruct "complete" log based on a summary
 * ie we have recent history (as log) and a summary
 * want a log that takes us up to present.
 */
void PG::PGLog::assimilate_summary(const PGSummary &sum, version_t last_complete)
{
  assert(last_complete >= bottom);
  assert(top > last_complete);
  
  // updated
  for (map<object_t,version_t>::const_iterator p = sum.objects.begin();
	   p != sum.objects.end();
	   p++) {
	if (deleted.count(p->first) &&
		deleted[p->first] > p->second) continue;
	if (updated.count(p->first)) continue;
	updated[p->first] = p->second;
	rupdated[p->second] = p->first;
  }

  bottom = 0;
}


ostream& PG::PGLog::print(ostream& out) const 
{
  out << *this << endl;
  
  map<version_t,object_t>::const_iterator pu = rupdated.begin();
  map<version_t,object_t>::const_iterator pd = rdeleted.begin();
  
  // both
  while (pu != rupdated.end() && pd != rdeleted.end()) {
	assert(pd->first != pu->first);
	if (pu->first > pd->first) {
	  out << " " << pu->first << "   " << hex << pu->second << dec << endl;
	  pu++;
	} else {
	  out << " " << pd->first << " - " << hex << pd->second << dec << endl;
	  pd++;
	}
  }
  // tail
  while (pu != rupdated.end()) {
	out << " " << pu->first << "   " << hex << pu->second << dec << endl;
	pu++;
  }
  while (pd != rdeleted.end()) {
	out << " " << pd->first << " - " << hex << pd->second << dec << endl;
	pd++;
  }
  out << " " << top << " top" << endl;
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
  for (version_t epoch = MAX(1, last_epoch_started_any);
	   epoch < osd->osdmap->get_epoch();
	   epoch++) {
	OSDMap *omap = osd->get_osd_map(epoch);
	assert(omap);
	
	vector<int> acting;
	omap->pg_to_acting_osds(get_pgid(), acting);
	
	for (unsigned i=0; i<acting.size(); i++) {
	  //dout(10) << "build prior considering epoch " << epoch << " osd" << acting[i] << endl;
	  if (osd->osdmap->is_up(acting[i]) &&  // is up now
		  omap->is_up(acting[i]) &&         // and was up then
		  acting[i] != osd->whoami)         // and is not me
		prior_set.insert(acting[i]);
	}
  }

  dout(10) << "build_prior built " << prior_set << endl;
}

void PG::adjust_prior()
{
  assert(!prior_set.empty());

  // raise last_epoch_started_any
  epoch_t max;
  for (map<int,PGInfo>::iterator it = peer_info.begin();
	   it != peer_info.end();
	   it++) {
	if (it->second.last_epoch_started > max)
	  max = it->second.last_epoch_started;
  }

  dout(10) << "adjust_prior last_epoch_started_any " 
		   << last_epoch_started_any << " -> " << max << endl;
  assert(max > last_epoch_started_any);
  last_epoch_started_any = max;

  // rebuild prior set
  build_prior();
}


void PG::peer(map< int, map<pg_t,version_t> >& query_map)
{
  dout(10) << "peer.  acting is " << acting 
		   << ", prior_set is " << prior_set << endl;

  // -- query info from everyone in prior_set.
  bool missing_info = false;
  for (set<int>::iterator it = prior_set.begin();
	   it != prior_set.end();
	   it++) {
	if (peer_info.count(*it)) {
	  dout(10) << " have info from osd" << *it 
			   << ": " << peer_info[*it]
			   << endl;	  
	  continue;
	}
	missing_info = true;

	if (peer_info_requested.count(*it)) {
	  dout(10) << " waiting for osd" << *it << endl;
	  continue;
	}
	
	dout(10) << " querying info from osd" << *it << endl;
	query_map[*it][info.pgid] = PG_QUERY_INFO;
	peer_info_requested.insert(*it);
  }
  if (missing_info) return;
  
  
  // -- ok, we have all (prior_set) info.  (and maybe others.)
  // who (of _everyone_ we're heard from) has the latest PG version?
  version_t newest_update = info.last_update;
  int       newest_update_osd = osd->whoami;
  version_t oldest_update_needed = info.last_update;  // only of acting (current) osd set
  
  for (map<int,PGInfo>::iterator it = peer_info.begin();
	   it != peer_info.end();
	   it++) {
	if (it->second.last_update > newest_update) {
	  newest_update = it->second.last_update;
	  newest_update_osd = it->first;
	}
	if (is_acting(it->first) &&
		it->second.last_update < oldest_update_needed) 
	  oldest_update_needed = it->second.last_update;
  }
  
  // get log?
  if (newest_update_osd != osd->whoami) {
	if (peer_log_requested.count(newest_update_osd)) {
	  dout(10) << " newest update on osd" << newest_update_osd
			   << " v " << newest_update 
			   << ", already queried" 
			   << endl;
	} else {
	  dout(10) << " newest update on osd" << newest_update_osd
			   << " v " << newest_update 
			   << ", querying since " << oldest_update_needed
			   << endl;
	  query_map[newest_update_osd][info.pgid] = oldest_update_needed;
	  peer_log_requested[newest_update_osd] = oldest_update_needed;
	}
	return;
  } else {
	dout(10) << " i have the most up-to-date pg v " << info.last_update << endl;
  }

  // -- is that the whole story?  (is my log sufficient?)
  if (info.last_complete < log.bottom) {
	// nope.  fetch a summary from someone.
	if (peer_summary_requested.count(newest_update_osd)) {
	  dout(10) << "i am complete thru " << info.last_complete
			   << ", but my log starts at " << log.bottom 
			   << ".  already waiting for summary from osd" << newest_update_osd
			   << endl;
	} else {
	  dout(10) << "i am complete thru " << info.last_complete
			   << ", but my log starts at " << log.bottom 
			   << ".  fetching summary from osd" << newest_update_osd
			   << endl;
	  assert(newest_update_osd != osd->whoami);  // can't be me!
	  query_map[newest_update_osd][info.pgid] = PG_QUERY_SUMMARY;
	  peer_summary_requested.insert(newest_update_osd);
	}
	return;
  }
  
  // -- ok.  and have i located all pg contents?
  if (missing.num_lost() > 0) {
	dout(10) << "there are still " << missing.num_lost() << " lost objects" << endl;

	// ok, let's get more summaries!
	bool waiting = false;
	for (map<int,PGInfo>::iterator it = peer_info.begin();
		 it != peer_info.end();
		 it++) {
	  int peer = it->first;

	  if (peer_summary_requested.count(peer)) {
		dout(10) << " already requested summary from osd" << peer << endl;
		waiting = true;
		continue;
	  }

	  dout(10) << " requesting summary from osd" << peer << endl;	  
	  query_map[peer][info.pgid] = PG_QUERY_INFO;
	  peer_summary_requested.insert(peer);
	  waiting = true;
	}

	if (!waiting) {
	  dout(10) << missing.num_lost() << " objects are still lost, waiting+hoping for a notify from someone else!" << endl;
	}
	return;
  }


  // -- do i need to generate a larger log for any of my peers?
  PGSummary summary;
  if (oldest_update_needed > log.bottom) {
	dout(10) << "my log isn't long enough for all peers: bottom " 
			 << log.bottom << " > " << oldest_update_needed
			 << endl;
	generate_summary(summary);
  }


  // -- ok, activate!
  info.last_epoch_started = osd->osdmap->get_epoch();
  state_set(PG::STATE_ACTIVE);  // i am active!

  clean_set.clear();
  if (info.is_clean()) 
	clean_set.insert(osd->whoami);

  for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_info.count(peer));
	
	if (peer_info[peer].is_clean()) 
	  clean_set.insert(peer);

	if (peer_info[peer].last_update < log.bottom) {
	  // need full summary
	  dout(10) << "sending complete summary to osd" << peer
			   << ", their last_update was " << peer_info[peer].last_update 
			   << endl;
	  MOSDPGSummary *m = new MOSDPGSummary(osd->osdmap->get_epoch(),
										   info.pgid,
										   summary);
	  osd->messenger->send_message(m, MSG_ADDR_OSD(peer));
	} else {
	  // need incremental (or no) log update.
	  dout(10) << "sending incremental|empty log (" 
			   << peer_info[peer].last_update << "," << info.last_update
			   << "] to osd" << peer << endl;
	  dout(20) << " btw my whole log is " << log.print(cout) << endl;
	  MOSDPGLog *m = new MOSDPGLog(osd->osdmap->get_epoch(), 
								   info.pgid);
	  m->info = info;
	  if (peer_info[peer].last_update < info.last_update) {
		m->log.copy_after(log, peer_info[peer].last_update);
		
		// build missing list for them too
		for (map<object_t, version_t>::iterator p = m->log.updated.begin();
			 p != m->log.updated.end();
			 p++) {
		  const object_t oid = p->first;
		  const version_t v = p->second;
		  m->missing.add(oid, v);
		  if (missing.is_missing(oid, v)) {   // we don't have it?
			assert(missing.loc.count(oid));  // nothing should be lost!
			m->missing.loc[oid] = missing.loc[oid];
		  } else {
			m->missing.loc[oid] = osd->whoami;       // we have it.
		  }
		}

	  }
	  osd->messenger->send_message(m, MSG_ADDR_OSD(peer));
	}
  }

  // all clean?
  if (is_all_clean()) {
	state_set(STATE_CLEAN);
	dout(10) << "all replicas clean" << endl;
  	clean_replicas();	
  }
  
  osd->take_waiters(waiting_for_active);
}


void PG::generate_summary(PGSummary &summary)
{  
  dout(10) << "generating summary" << endl;

  list<object_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  for (list<object_t>::iterator it = olist.begin();
	   it != olist.end();
	   it++) {
	version_t v;
	osd->store->getattr(*it, 
						"version",
						&v, sizeof(v));
	summary.objects[*it] = v;
  }

  dout(10) << summary.objects.size() << " local objects.  " << endl;
}





/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
bool PG::do_recovery()
{
  dout(10) << "do_recovery" << endl;

  // look at log!
  map<version_t,object_t>::iterator p = log.rupdated.upper_bound(requested_thru);
  while (1) {
	if (p == log.rupdated.end()) {
	  dout(10) << "already requested everything in log" << endl;
	  return false;
	}
	
	if (!objects_pulling.count(p->second) &&       // not already pulling
		missing.is_missing(p->second, p->first))  // and missing
	  break;

	p++;
  }
  
  const version_t v = p->first;
  const object_t oid = p->second;
  
  assert(v > requested_thru);
  requested_thru = v;
  
  osd->pull(this, oid, v);
  
  return true;
}


/** clean_up_local
 * remove any objects that we're storing but shouldn't.
 * as determined by log.
 */
void PG::clean_up_local()
{
  dout(10) << "clean_up_local" << endl;

  assert(info.last_update >= log.bottom);  // otherwise we need some help!

  for (map<version_t,object_t>::iterator pd = log.rdeleted.upper_bound(info.last_update);
	   pd != log.rdeleted.end();
	   pd++) {
	const object_t oid = pd->second;
	const version_t when = pd->first;
	if (osd->store->exists(oid)) {
	  version_t ov = 0;
	  osd->store->getattr(oid, "version", &ov, sizeof(ov));
	  if (ov < when) {
		dout(10) << " removing " << hex << oid << dec
				 << " v " << ov << " < " << when << endl;
		osd->store->remove(oid);
	  } else {
		dout(10) << "  keeping " << hex << oid << dec
				 << " v " << ov << " >= " << when << endl;
	  }
	}
  }
}

void PG::clean_replicas()
{
  dout(10) << "clean_replicas.  strays are " << stray_set << endl;
  
  for (set<int>::iterator p = stray_set.begin();
	   p != stray_set.end();
	   p++) {
	dout(10) << "sending PGRemove to osd" << *p << endl;
	set<pg_t> ls;
	ls.insert(info.pgid);
	MOSDPGRemove *m = new MOSDPGRemove(osd->osdmap->get_epoch(), ls);
	osd->messenger->send_message(m, MSG_ADDR_OSD(*p));
  }

  stray_set.clear();
}
