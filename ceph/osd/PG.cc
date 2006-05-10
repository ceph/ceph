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


#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << osd->whoami << " " << *this << " "


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
	   it != other.updated.end();
	   it++) 
	add_update(it->second, it->first);
  
  // deleted
  for (map<version_t, object_t>::const_iterator it = other.rdeleted.upper_bound(v); // first > v
	   it != other.rdeleted.end();
	   it++) 
	add_delete(it->second, it->first);
  
  assert(top == other.top);
}


void PG::PGLog::merge_after(version_t after, const PGLog &other) 
{
  // extend on bottom?
  if (other.bottom < bottom &&
	  after < bottom) {
	version_t newbottom = after;
	if (after > other.bottom) 
	  newbottom = other.bottom;  // skip part of other
	
	// updated
	for (map<version_t,object_t>::const_iterator p = other.rupdated.upper_bound(newbottom); // first > newbottom
		 p != other.rupdated.end();
		 p++) {
	  if (p->first > bottom) break;
	  if (updated.count(p->second) || deleted.count(p->second)) continue;
	  updated[p->second] = p->first;
	  rupdated[p->first] = p->second;
	}
	
	// deleted
	for (map<version_t,object_t>::const_iterator p = other.rdeleted.upper_bound(newbottom);
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
		pu++;
	  } else {
		add_delete(pd->second, pd->first);
		pd++;
	  }
	}
	// tail
	while (pu != other.rupdated.end()) {
	  add_update(pu->second, pu->first);
	  pu++;
	}
	while (pd != other.rdeleted.end()) {
	  add_delete(pd->second, pd->first);
	  pd++;
	}
	top = other.top;
  }
}

void PG::PGLog::print(ostream& out) const 
{
  out << " " << bottom << " - " << top << endl;
  
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
  for (version_t epoch = last_epoch_started_any;
	   epoch < osd->osdmap->get_epoch();
	   epoch++) {
	OSDMap *omap = osd->get_osd_map(epoch);
	assert(omap);
	
	vector<int> acting;
	omap->pg_to_acting_osds(get_pgid(), acting);
	
	for (unsigned i=0; i<acting.size(); i++) 
	  if (osd->osdmap->is_up(acting[i]) &&  // is up now
		  omap->is_up(acting[i]) &&         // and was up then
		  acting[i] != osd->whoami)         // and not me
		prior_set.insert(acting[i]);
  }

  dout(10) << "build_prior " << prior_set << endl;
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
  assert(max > last_epoch_started_any);
  last_epoch_started_any = max;
  
  // rebuild prior set
  build_prior();
}


void PG::peer(map< int, map<pg_t,version_t> >& query_map)
{
  dout(10) << "peer" << endl;

  // -- query info from everyone in prior_set.
  bool missing_info = false;
  for (set<int>::iterator it = prior_set.begin();
	   it != prior_set.end();
	   it++) {
	if (peer_info.count(*it)) {
	  dout(10) << " have info from osd" << *it << endl;	  
	  continue;
	}
	missing_info = true;

	if (peer_info_requested.count(*it)) {
	  dout(10) << " waiting for osd" << *it << endl;
	  continue;
	}
	
	dout(10) << " querying info from osd" << *it << endl;
	query_map[*it][info.pgid] = 0;
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
	  peer_info_requested.insert(newest_update_osd);
	}
	return;
  } else {
	dout(10) << " i have the most up-to-date log " << info.last_update << endl;
  }

  // -- is that the whole story?  (is my log sufficient?)
  if (info.last_complete < log.bottom) {
	// nope.  fetch a summary from someone.
	if (peer_summary.count(newest_update_osd)) {
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
	  query_map[newest_update_osd][info.pgid] = 1;
	  peer_summary_requested.insert(newest_update_osd);
	}
	return;
  }
  
  // -- ok.  and have i located all pg contents?
  if (missing.num_lost()) {
	dout(10) << "there are still " << missing.num_lost() << " lost objects" << endl;

	// ok, let's get more summaries!
	bool waiting = false;
	for (map<int,PGInfo>::iterator it = peer_info.begin();
		 it != peer_info.end();
		 it++) {
	  int peer = it->first;
	  if (peer_summary.count(peer)) {
		dout(10) << " have summary from osd" << peer << endl;
		continue;
	  }
	  if (peer_summary_requested.count(peer)) {
		dout(10) << " already requested summary from osd" << peer << endl;
		waiting = true;
		continue;
	  }

	  dout(10) << " requesting summary from osd" << peer << endl;	  
	  query_map[peer][info.pgid] = 1;
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
  bool allclean = true;
  for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_info.count(peer));
	
	if (peer_info[peer].last_update < log.bottom) {
	  // need full summary
	  dout(10) << "sending complete summary to osd" << peer
			   << ", their last_update was " << peer_info[peer].last_update 
			   << endl;
	  MOSDPGSummary *m = new MOSDPGSummary(osd->osdmap->get_epoch(),
										   info.pgid,
										   summary);
	  allclean = false;
	  osd->messenger->send_message(m, MSG_ADDR_OSD(peer));
	} else {
	  // need incremental (or no) log update.
	  dout(10) << "sending incremental|empty log " 
			   << peer_info[peer].last_update << " - " << info.last_update
			   << " to osd" << peer << endl;
	  MOSDPGLog *m = new MOSDPGLog(osd->osdmap->get_epoch(), 
								   info.pgid);
	  if (peer_info[peer].last_update < info.last_update) {
		m->log.copy_after(log, peer_info[peer].last_update);
		allclean = false;
	  }
	  osd->messenger->send_message(m, MSG_ADDR_OSD(peer));
	}
  }

  // do anything about allclean?
  // ???
  
  // clean up some
  clear_content_recovery_state();

  // i am active!
  state_set(PG::STATE_ACTIVE);
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



void PG::plan_recovery()
{
  dout(10) << "plan_recovery " << endl;  
  
  assert(is_active());
  
  /*
  // load local contents
  list<object_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  // check versions
  map<object_t, version_t> vmap;
  for (list<object_t>::iterator it = olist.begin();
	   it != olist.end();
	   it++) {
	version_t v;
	osd->store->getattr(*it, 
						"version",
						&v, sizeof(v));
	vmap[*it] = v;
  }
  
  // scan summary
  content_summary->remote = 0;
  content_summary->missing = 0;
  for (list<ObjectInfo>::iterator it = content_summary->ls.begin();
		 it != content_summary->ls.end();
		 it++) {
	if (vmap.count(it->oid) &&
		vmap[it->oid] == it->version) {
	  // have latest.
	  vmap.erase(it->oid);
	  continue;
	}

	// need it
	dout(20) << "need " << hex << it->oid << dec 
			 << " v " << it->version << endl;
	objects_missing[it->oid] = it->version;
	recovery_queue[it->version] = *it;
  }

  // hose stray
  for (map<object_t,version_t>::iterator it = vmap.begin();
	   it != vmap.end();
	   it++) {
	dout(20) << "removing stray " << hex << it->first << dec 
			 << " v " << it->second << endl;
	osd->store->remove(it->first);
  }

  */
}

void PG::do_recovery()
{
  dout(0) << "do_recovery - implement me" << endl;
}
