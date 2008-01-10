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


#include "PGMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "OSDMonitor.h"
#include "MonitorStore.h"

#include "messages/MPGStats.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "common/Timer.h"

#include "osd/osd_types.h"
#include "osd/PG.h"  // yuck

#include "config.h"
#include <sstream>


#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".pg "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".pg "


/*
 Tick function to update the map based on performance every N seconds
*/

/*
void PGMonitor::tick() {

  // magic incantation that Sage told me
  if (!mon->is_leader()) return; 
  if (!paxos->is_active()) return;


  // Is it the nth second? If not, do nothing.
  const int N = 10; //magic number! codify somewhere later
  utime_t now = g_clock.now();
  if (now % N) != 0 return;

  if (mon->osdmon->paxos->is_readable()) {
    // safely use mon->osdmon->osdmap
  }

}

*/
void PGMonitor::create_initial()
{
  dout(1) << "create_initial -- creating initial map" << dendl;
}

bool PGMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv == pg_map.version) return true;
  assert(paxosv >= pg_map.version);

  if (pg_map.version == 0 && paxosv > 1 &&
      mon->store->exists_bl_ss("pgmap","latest")) {
    // starting up: load latest
    dout(7) << "update_from_paxos startup: loading latest full pgmap" << dendl;
    bufferlist bl;
    mon->store->get_bl_ss(bl, "pgmap", "latest");
    int off = 0;
    pg_map._decode(bl, off);
  } 

  // walk through incrementals
  while (paxosv > pg_map.version) {
    bufferlist bl;
    bool success = paxos->read(pg_map.version+1, bl);
    if (success) {
      dout(7) << "update_from_paxos  applying incremental " << pg_map.version+1 << dendl;
      PGMap::Incremental inc;
      int off = 0;
      inc._decode(bl, off);
      pg_map.apply_incremental(inc);
      
      std::stringstream ss;
      for (hash_map<int,int>::iterator p = pg_map.num_pg_by_state.begin();
	   p != pg_map.num_pg_by_state.end();
	   ++p) {
	if (p != pg_map.num_pg_by_state.begin())
	  ss << ", ";
	ss << p->second << " " << PG::get_state_string(p->first) << "(" << p->first << ")";
      }
      string states = ss.str();
      dout(0) << "v" << pg_map.version << " " << states << dendl;

    } else {
      dout(7) << "update_from_paxos  couldn't read incremental " << pg_map.version+1 << dendl;
      return false;
    }
  }

  // save latest
  bufferlist bl;
  pg_map._encode(bl);
  mon->store->put_bl_ss(bl, "pgmap", "latest");

  return true;
}

void PGMonitor::create_pending()
{
  pending_inc = PGMap::Incremental();
  pending_inc.version = pg_map.version + 1;
  dout(10) << "create_pending v " << pending_inc.version << dendl;
}

void PGMonitor::encode_pending(bufferlist &bl)
{
  assert(mon->is_leader());
  dout(10) << "encode_pending v " << pending_inc.version << dendl;
  assert(paxos->get_version() + 1 == pending_inc.version);
  pending_inc._encode(bl);
}

bool PGMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_source_inst() << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    handle_statfs((MStatfs*)m);
    return true;
    
  case MSG_PGSTATS:
    {
      MPGStats *stats = (MPGStats*)m;
      for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
	   p != stats->pg_stat.end();
	   p++) {
	if (pg_map.pg_stat.count(p->first) == 0 ||
	    pg_map.pg_stat[p->first].reported < p->second.reported)
	  return false;
      }
      dout(10) << " message contains no new pg stats" << dendl;
      return true;
    }

  default:
    assert(0);
    delete m;
    return true;
  }
}

bool PGMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return handle_pg_stats((MPGStats*)m);

  default:
    assert(0);
    delete m;
    return false;
  }
}

void PGMonitor::committed()
{

}

void PGMonitor::handle_statfs(MStatfs *statfs)
{
  dout(10) << "handle_statfs " << *statfs << " from " << statfs->get_source() << dendl;

  // fill out stfs
  MStatfsReply *reply = new MStatfsReply(statfs->tid);
  memset(&reply->stfs, 0, sizeof(reply->stfs));

  // these are in KB.
  reply->stfs.f_total = 4*pg_map.total_osd_num_blocks;
  reply->stfs.f_free = 4*pg_map.total_osd_num_blocks_avail;
  reply->stfs.f_avail = 4*pg_map.total_osd_num_blocks_avail;
  reply->stfs.f_objects = pg_map.total_osd_num_objects;

  // reply
  mon->messenger->send_message(reply, statfs->get_source_inst());
  delete statfs;
}

bool PGMonitor::handle_pg_stats(MPGStats *stats) 
{
  dout(10) << "handle_pg_stats " << *stats << " from " << stats->get_source() << dendl;
  int from = stats->get_source().num();
  if (!stats->get_source().is_osd() ||
      !mon->osdmon->osdmap.is_up(from) ||
      stats->get_source_inst() != mon->osdmon->osdmap.get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd" << dendl;
  }
      
  // osd stat
  if (pg_map.osd_stat.count(from))
    pg_map.stat_osd_sub(pg_map.osd_stat[from]);
  pg_map.osd_stat[from] = stats->osd_stat;
  pg_map.stat_osd_add(stats->osd_stat);

  // pg stats
  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       p++) {
    pg_t pgid = p->first;
    if ((pg_map.pg_stat.count(pgid) && 
	 pg_map.pg_stat[pgid].reported > p->second.reported)) {
      dout(15) << " had " << pgid << " from " << pg_map.pg_stat[pgid].reported << dendl;
      continue;
    }
    if (pending_inc.pg_stat_updates.count(pgid) && 
	pending_inc.pg_stat_updates[pgid].reported > p->second.reported) {
      dout(15) << " had " << pgid << " from " << pending_inc.pg_stat_updates[pgid].reported
	       << " (pending)" << dendl;
      continue;
    }

    dout(15) << " got " << pgid << " reported at " << p->second.reported 
	     << " state " << PG::get_state_string(p->second.state)
	     << dendl;
    pending_inc.pg_stat_updates[pgid] = p->second;

    // we don't care about consistency; apply to live map.
    if (pg_map.pg_stat.count(pgid))
      pg_map.stat_pg_sub(pg_map.pg_stat[pgid]);
    pg_map.pg_stat[pgid] = p->second;
    pg_map.stat_pg_add(pg_map.pg_stat[pgid]);
  }
  
  delete stats;
  return true;
}
