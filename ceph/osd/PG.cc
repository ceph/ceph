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
#include "messages/MOSDPGRemove.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << osd->whoami << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "


/******* PGLog ********/

void PG::PGLog::trim(ObjectStore::Transaction& t, version_t s) 
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
  
  // raise bottom?
  if (bottom < s) {
	bottom = s;
	backlog = false;
  }
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


void PG::merge_log(const PGLog &olog)
{
  dout(10) << "merge_log " << olog << " into " << log << endl;

  if (is_empty() ||
	  (olog.bottom > log.top && olog.backlog)) { // e.g. log=(0,20] olog=(40,50]+backlog
	// special (easy) case
	//assert(info.last_complete == 0);
	//assert(log.empty());

	// add all to missing  (anything above our log.top)
	for (map<object_t,version_t>::const_iterator pu = olog.updated.upper_bound(log.top);
		 pu != olog.updated.end();
		 pu++)
	  missing.add(pu->first, pu->second);

	// copy the log.
	log = olog;
	info.last_update = log.top;
	info.log_floor = log.bottom;

	dout(10) << "merge_log  result " << log << ", " << missing << endl;
	return;
  }

  // extend on bottom?
  if (olog.bottom < log.bottom &&
	  olog.top >= log.bottom) {
	version_t newbottom = olog.bottom;
	dout(10) << "merge_log  extending bottom to " << newbottom << endl;
	
	// updated
	for (map<version_t,object_t>::const_iterator p = olog.rupdated.upper_bound(olog.bottom);
		 p != olog.rupdated.end();
		 p++) {
	  // verify?
	  if (p->first > log.bottom) break;
	  if (log.backlog)
		assert((log.updated.count(p->second) && log.updated[p->second] >= p->first) ||
			   (log.deleted.count(p->second) && log.deleted[p->second] > p->first));

	  if (log.updated.count(p->second) || log.deleted.count(p->second)) continue;
	  log.updated[p->second] = p->first;
	  log.rupdated[p->first] = p->second;
	  if (is_empty())
		missing.add(p->second, p->first);
	}
	
	// deleted
 	for (map<version_t,object_t>::const_iterator p = olog.rdeleted.begin();
		 p != olog.rdeleted.end();
		 p++) {
	  assert(p->first >= olog.bottom);
	  if (p->first > log.bottom) break;
	  if (log.updated.count(p->second) || log.deleted.count(p->second)) continue;
	  log.deleted[p->second] = p->first;
	  log.rdeleted[p->first] = p->second;
	}
	info.log_floor = log.bottom = newbottom;
  }
  
  // backlog?
  if (olog.backlog && !log.backlog &&
	  olog.top >= log.bottom) {
	dout(10) << "merge_log  filling in backlog" << endl;

	for (map<version_t,object_t>::const_iterator p = olog.rupdated.begin();
		 p != olog.rupdated.end();
		 p++) {
	  if (p->first >= olog.bottom) break;
	  if (log.deleted.count(p->second)) {
		assert(log.deleted[p->second] > p->first);   // delete must happen later..
		assert(log.top > olog.top);                  // our log must have more new stuff..
		continue;  // gets deleted later.
	  }
	  log.updated[p->second] = p->first;
	  log.rupdated[p->first] = p->second;
	}
	log.backlog = true;
  }

  // extend on top?
  if (olog.top > log.top &&
	  olog.bottom <= log.top) {
	dout(10) << "merge_log  extending top to " << olog.top << endl;
	map<version_t,object_t>::const_iterator pu = olog.rupdated.upper_bound(log.top);
	map<version_t,object_t>::const_iterator pd = olog.rdeleted.upper_bound(log.top);
	
	// both
	while (pu != olog.rupdated.end() && pd != olog.rdeleted.end()) {
	  assert(pd->first != pu->first);
	  if (pu->first > pd->first) {
		log.add_update(pu->second, pu->first);
		missing.add(pu->second, pu->first);
		pu++;
	  } else {
		log.add_delete(pd->second, pd->first);
		missing.rm(pu->second, pu->first);
		pd++;
	  }
	}
	// tail
	while (pu != olog.rupdated.end()) {
	  log.add_update(pu->second, pu->first);
	  missing.add(pu->second, pu->first);
	  pu++;
	}
	while (pd != olog.rdeleted.end()) {
	  log.add_delete(pd->second, pd->first);
	  missing.rm(pu->second, pu->first);
	  pd++;
	}

	info.last_update = log.top = olog.top;
  }

  dout(10) << "merge_log  result " << log << ", " << missing << endl;
}




void PG::generate_backlog(PGLog& log)
{
  dout(10) << "generate_backlog to " << log << endl;
  
  assert(!log.backlog);
  log.backlog = true;

  list<object_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  int local = 0;
  for (list<object_t>::iterator it = olist.begin();
	   it != olist.end();
	   it++) {
	local++;
	assert(log.deleted.count(*it) == 0);

	if (log.updated.count(*it)) continue;
	
	version_t v;
	osd->store->getattr(*it, 
						"version",
						&v, sizeof(v));
	log.updated[*it] = v;
  }

  dout(10) << local << " local objects, "
		   << log.updated.size() << " in pg" << endl;
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
	OSDMap omap;
	osd->get_map(epoch, omap);
	
	vector<int> acting;
	omap.pg_to_acting_osds(get_pgid(), acting);
	
	for (unsigned i=0; i<acting.size(); i++) {
	  //dout(10) << "build prior considering epoch " << epoch << " osd" << acting[i] << endl;
	  if (osd->osdmap->is_up(acting[i]) &&  // is up now
		  omap.is_up(acting[i]) &&          // and was up then
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
  version_t peers_complete_thru = info.last_complete;
  
  for (map<int,PGInfo>::iterator it = peer_info.begin();
	   it != peer_info.end();
	   it++) {
	if (it->second.last_update > newest_update) {
	  newest_update = it->second.last_update;
	  newest_update_osd = it->first;
	}
	if (is_acting(it->first)) {
	  if (it->second.last_update < oldest_update_needed) 
		oldest_update_needed = it->second.last_update;
	  if (it->second.last_complete < peers_complete_thru)
		peers_complete_thru = it->second.last_complete;
	}	
  }
  
  // get log?
  if (newest_update_osd != osd->whoami) {
	if (peer_log_requested.count(newest_update_osd) ||
		peer_summary_requested.count(newest_update_osd)) {
	  dout(10) << " newest update on osd" << newest_update_osd
			   << " v " << newest_update 
			   << ", already queried" 
			   << endl;
	} else {
	  if (peer_info[newest_update_osd].log_floor < log.top) {
		dout(10) << " newest update on osd" << newest_update_osd
				 << " v " << newest_update 
				 << ", querying since " << oldest_update_needed
				 << endl;
		query_map[newest_update_osd][info.pgid] = oldest_update_needed;
		peer_log_requested[newest_update_osd] = oldest_update_needed;
	  } else {
		assert(peer_info[newest_update_osd].last_complete >= 
			   peer_info[newest_update_osd].log_floor);  // or else we're in trouble.
		dout(10) << " newest update on osd" << newest_update_osd
				 << " v " << newest_update 
				 << ", querying entire summary/backlog"
				 << endl;
		query_map[newest_update_osd][info.pgid] = PG_QUERY_SUMMARY;
		peer_summary_requested.insert(newest_update_osd);
	  }
	}
	return;
  } else {
	dout(10) << " i have the most up-to-date pg v " << info.last_update << endl;
  }
  dout(10) << " peers_complete_thru " << peers_complete_thru << endl;
  dout(10) << " oldest_update_needed " << oldest_update_needed << endl;

  // -- is that the whole story?  (is my log sufficient?)
  if (info.last_complete < log.bottom && !log.backlog) {
	// nope.  fetch backlog from someone.
	if (peer_summary_requested.count(newest_update_osd)) {
	  dout(10) << "i am complete thru " << info.last_complete
			   << ", but my log starts at " << log.bottom 
			   << ".  already waiting for summary/backlog from osd" << newest_update_osd
			   << endl;
	} else {
	  // who to fetch from?
	  int who = newest_update_osd;
	  if (who == osd->whoami) {  // er, pick someone else!
		// pick someone who has (back)log through my log.bottom
		for (map<int,PGInfo>::iterator it = peer_info.begin();
			 it != peer_info.end();
			 it++) {
		  if (it->first == osd->whoami) continue;
		  if (it->second.last_complete >= it->second.log_floor &&  // they store backlog
			  it->second.last_update >= log.bottom) {              // thru my log.bottom
			who = it->first;
			break;
		  }
		}
	  }
	  assert(who != osd->whoami); // can't be me!
	  dout(10) << "i am complete thru " << info.last_complete
			   << ", but my log starts at " << log.bottom 
			   << ".  fetching summary/backlog from osd" << who
			   << endl;
	  query_map[who][info.pgid] = PG_QUERY_SUMMARY;
	  peer_summary_requested.insert(who);
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
		dout(10) << " already requested summary/backlog from osd" << peer << endl;
		waiting = true;
		continue;
	  }

	  dout(10) << " requesting summary/backlog from osd" << peer << endl;	  
	  query_map[peer][info.pgid] = PG_QUERY_INFO;
	  peer_summary_requested.insert(peer);
	  waiting = true;
	}
	
	if (!waiting) {
	  dout(10) << missing.num_lost() << " objects are still lost, waiting+hoping for a notify from someone else!" << endl;
	}
	return;
  }

  // sanity check
  assert(missing.num_lost() == 0);
  assert(info.last_complete >= log.bottom || log.backlog);

  // -- do i need to generate backlog for any of my peers?
  if (oldest_update_needed < log.bottom && !log.backlog) {
	dout(10) << "generating backlog for some peers, bottom " 
			 << log.bottom << " > " << oldest_update_needed
			 << endl;
	generate_backlog(log);
  }


  // -- ok, activate!
  info.last_epoch_started = osd->osdmap->get_epoch();
  state_set(PG::STATE_ACTIVE);  // i am active!
  dout(10) << "marking active" << endl;

  clean_set.clear();
  if (info.is_clean()) 
	clean_set.insert(osd->whoami);

  for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_info.count(peer));
	
	if (peer_info[peer].is_clean()) 
	  clean_set.insert(peer);

	
	MOSDPGLog *m = new MOSDPGLog(osd->osdmap->get_epoch(), 
								 info.pgid);
	m->info = info;

	if (peer_info[peer].last_update == info.last_update) {
	  // empty log
	} 
	else if (peer_info[peer].last_update < log.bottom) {
	  // summary/backlog
	  assert(log.backlog);
	  m->log = log;
	} 
	else {
	  // incremental log
	  assert(peer_info[peer].last_update < info.last_update);
	  m->log.copy_after(log, peer_info[peer].last_update);
	}
		
	// build missing list for them too
	for (map<object_t, version_t>::iterator p = m->log.updated.begin();
		 p != m->log.updated.end();
		 p++) {
	  const object_t oid = p->first;
	  const version_t v = p->second;
	  if (missing.is_missing(oid, v)) {   // we don't have it?
		assert(missing.loc.count(oid));   // nothing should be lost!
		m->missing.add(oid, v);
		m->missing.loc[oid] = missing.loc[oid];
	  } 
	  // note: peer will assume we have it if we don't say otherwise.
	  // else m->missing.loc[oid] = osd->whoami;       // we have it.
	}

	dout(10) << "sending " << m->log << " " << m->missing
			 << " to osd" << peer << endl;

	osd->messenger->send_message(m, MSG_ADDR_OSD(peer));
  }

  // all clean?
  if (is_all_clean()) {
	state_set(STATE_CLEAN);
	dout(10) << "all replicas clean" << endl;
  	clean_replicas();	
  }
  
  osd->take_waiters(waiting_for_active);
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
	
	if (!objects_pulling.count(p->second) &&      // not already pulling
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


void PG::append_log(ObjectStore::Transaction& t, PG::PGLog::Entry& logentry, version_t trim_to)
{
  // write entry
  bufferlist bl;
  bl.append( (char*)&logentry, sizeof(logentry) );
  t.write( info.pgid, ondisklog.top, bl.length(), bl );
  
  t.collection_setattr(info.pgid, "top", &log.top, sizeof(log.top));
  t.collection_setattr(info.pgid, "bottom", &log.bottom, sizeof(log.bottom));

  // update block map?
  if (ondisklog.top % 4096 == 0) 
	ondisklog.block_map[ondisklog.top] = logentry.version;
  
  ondisklog.top += bl.length();
  t.collection_setattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  
  // trim?
  if (trim_to > log.bottom) {
	dout(10) << " trimming " << log << " to " << trim_to << endl;
	log.trim(t, trim_to);
	info.log_floor = log.bottom;
	if (ondisklog.trim_to(trim_to))
	  t.collection_setattr(info.pgid, "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  }
  dout(10) << " ondisklog bytes [" << ondisklog.bottom << "," << ondisklog.top << ")" << endl;
}

void PG::read_log(ObjectStore *store)
{
  // load bounds
  ondisklog.top = ondisklog.bottom = 0;
  store->collection_getattr(info.pgid, "ondisklog_top", &ondisklog.top, sizeof(ondisklog.top));
  store->collection_getattr(info.pgid, "ondisklog_bottom", &ondisklog.bottom, sizeof(ondisklog.bottom));
  
  if (ondisklog.top > ondisklog.bottom) {
	// read
	bufferlist bl;
	store->read(info.pgid, ondisklog.bottom, ondisklog.top-ondisklog.bottom, bl);
	
	off_t pos = ondisklog.bottom;
	while (pos < ondisklog.top) {
	  PG::PGLog::Entry e;
	  bl.copy(pos-ondisklog.bottom, sizeof(e), (char*)&e);
	  
	  if (pos % 4096 == 0)
		ondisklog.block_map[pos] = e.version;
	  
	  if (e.deleted)
		log.add_delete(e.oid, e.version);
	  else
		log.add_update(e.oid, e.version);
	  
	  pos += sizeof(e);
	}
  }

  log.top = log.bottom = 0;
  log.backlog = false;
  store->collection_getattr(info.pgid, "top", &log.top, sizeof(log.top));
  store->collection_getattr(info.pgid, "bottom", &log.bottom, sizeof(log.bottom));
  store->collection_getattr(info.pgid, "backlog", &log.backlog, sizeof(log.backlog));
}

