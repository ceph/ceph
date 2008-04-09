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
#include "messages/MOSDPGCreate.h"
#include "messages/MMonCommand.h"

#include "common/Timer.h"

#include "osd/osd_types.h"
#include "osd/PG.h"  // yuck

#include "config.h"
#include <sstream>


#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".pg v" << pg_map.version << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".pg v" << pg_map.version << " "

struct kb_t {
  uint64_t v;
  kb_t(uint64_t _v) : v(_v) {}
};
ostream& operator<<(ostream& out, const kb_t& kb)
{
  if (kb.v > 2048*1024*1024*1024ULL)
    return out << (kb.v >> 40) << " PB";    
  if (kb.v > 2048*1024*1024ULL)
    return out << (kb.v >> 30) << " TB";    
  if (kb.v > 2048*1024ULL)
    return out << (kb.v >> 20) << " GB";    
  if (kb.v > 2048ULL)
    return out << (kb.v >> 10) << " MB";
  return out << kb.v << " KB";
}

ostream& operator<<(ostream& out, PGMonitor& pm)
{
  std::stringstream ss;
  for (hash_map<int,int>::iterator p = pm.pg_map.num_pg_by_state.begin();
       p != pm.pg_map.num_pg_by_state.end();
       ++p) {
    if (p != pm.pg_map.num_pg_by_state.begin())
      ss << ", ";
    ss << p->second << " " << pg_state_string(p->first);// << "(" << p->first << ")";
  }
  string states = ss.str();
  return out << "v" << pm.pg_map.version << ": "
	     << pm.pg_map.pg_stat.size() << " pgs: "
	     << states
	     << "; " << kb_t(pm.pg_map.total_used_kb()) << " used, "
	     << kb_t(pm.pg_map.total_avail_kb()) << " / "
	     << kb_t(pm.pg_map.total_kb()) << " free";
}

/*
 Tick function to update the map based on performance every N seconds
*/

void PGMonitor::tick() 
{
  dout(10) << *this << dendl;

  /*
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
  */
}

void PGMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
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
      
      dout(0) << *this << dendl;
    } else {
      dout(7) << "update_from_paxos  couldn't read incremental " << pg_map.version+1 << dendl;
      return false;
    }
  }

  // save latest
  bufferlist bl;
  pg_map._encode(bl);
  mon->store->put_bl_ss(bl, "pgmap", "latest");

  send_pg_creates();

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
      int from = m->get_source().num();
      if (pg_map.osd_stat.count(from) ||
	  memcmp(&pg_map.osd_stat[from], &stats->osd_stat, sizeof(stats->osd_stat)) != 0)
	return false;  // new osd stat
      for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
	   p != stats->pg_stat.end();
	   p++) {
	if (pg_map.pg_stat.count(p->first) == 0 ||
	    memcmp(&pg_map.pg_stat[p->first], &p->second, sizeof(p->second)) != 0)
	  return false; // new pg stat(s)
      }
      dout(10) << " message contains no new osd|pg stats" << dendl;
      return true;
    }

  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

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
    return prepare_pg_stats((MPGStats*)m);

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
  reply->stfs.f_total = cpu_to_le64(pg_map.total_kb());
  reply->stfs.f_free = cpu_to_le64(pg_map.total_avail_kb());
  reply->stfs.f_avail = cpu_to_le64(pg_map.total_avail_kb());
  reply->stfs.f_objects = cpu_to_le64(pg_map.total_osd_num_objects);

  // reply
  mon->messenger->send_message(reply, statfs->get_source_inst());
  delete statfs;
}

bool PGMonitor::prepare_pg_stats(MPGStats *stats) 
{
  dout(10) << "prepare_pg_stats " << *stats << " from " << stats->get_source() << dendl;
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

    if (pg_map.pg_stat.count(pgid) == 0) {
      dout(15) << " got " << pgid << " reported at " << p->second.reported 
	       << " state " << pg_state_string(p->second.state)
	       << " but DNE in pg_map!!"
	       << dendl;
      assert(0);
    }
      
    dout(15) << " got " << pgid
	     << " reported at " << p->second.reported 
	     << " state " << pg_state_string(pg_map.pg_stat[pgid].state)
	     << " -> " << pg_state_string(p->second.state)
	     << dendl;
    pending_inc.pg_stat_updates[pgid] = p->second;

    // we don't care much about consistency, here; apply to live map.
    pg_map.stat_pg_sub(pgid, pg_map.pg_stat[pgid]);
    pg_map.pg_stat[pgid] = p->second;
    pg_map.stat_pg_add(pgid, pg_map.pg_stat[pgid]);
  }
  
  delete stats;
  return true;
}




// ------------------------

struct RetryCheckOSDMap : public Context {
  PGMonitor *pgmon;
  epoch_t epoch;
  RetryCheckOSDMap(PGMonitor *p, epoch_t e) : pgmon(p), epoch(e) {}
  void finish(int r) {
    pgmon->check_osd_map(epoch);
  }
};

void PGMonitor::check_osd_map(epoch_t epoch)
{
  if (mon->is_peon()) 
    return; // whatever.

  if (pg_map.last_osdmap_epoch >= epoch) {
    dout(10) << "check_osd_map already seen " << pg_map.last_osdmap_epoch << " >= " << epoch << dendl;
    return;
  }

  if (!mon->osdmon->paxos->is_readable()) {
    dout(10) << "register_new_pgs -- osdmap not readable, waiting" << dendl;
    mon->osdmon->paxos->wait_for_readable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  if (!paxos->is_writeable()) {
    dout(10) << "register_new_pgs -- pgmap not writeable, waiting" << dendl;
    paxos->wait_for_writeable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  // apply latest map(s)
  for (epoch_t e = pg_map.last_osdmap_epoch+1;
       e <= epoch;
       e++) {
    dout(10) << "check_osd_map applying osdmap e" << e << " to pg_map" << dendl;
    bufferlist bl;
    mon->store->get_bl_sn(bl, "osdmap", e);
    assert(bl.length());
    OSDMap::Incremental inc;
    int off = 0;
    inc.decode(bl, off);
    for (map<int32_t,uint32_t>::iterator p = inc.new_offload.begin();
	 p != inc.new_offload.end();
	 p++)
      if (p->second == 0x10000) {
	dout(10) << "check_osd_map  osd" << p->first << " went OUT" << dendl;
	pending_inc.osd_stat_rm.insert(p->first);
      } else {
	dout(10) << "check_osd_map  osd" << p->first << " is IN" << dendl;
	pending_inc.osd_stat_rm.erase(p->first);
	pending_inc.osd_stat_updates[p->first]; 
      }
  }

  bool propose = false;
  if (pg_map.last_osdmap_epoch < epoch) {
    pending_inc.osdmap_epoch = epoch;
    propose = true;
  }

  // scan pg space?
  if (register_new_pgs())
    propose = true;
  
  if (propose)
    propose_pending();

  send_pg_creates();
}

bool PGMonitor::register_new_pgs()
{


  dout(10) << "osdmap last_pg_change " << mon->osdmon->osdmap.get_last_pg_change()
	   << ", pgmap last_pg_scan " << pg_map.last_pg_scan << dendl;
  if (mon->osdmon->osdmap.get_last_pg_change() <= pg_map.last_pg_scan ||
      mon->osdmon->osdmap.get_last_pg_change() <= pending_inc.pg_scan) {
    dout(10) << "register_new_pgs -- i've already scanned pg space since last significant osdmap update" << dendl;
    return false;
  }

  // iterate over crush mapspace
  dout(10) << "register_new_pgs scanning pgid space defined by crush rule masks" << dendl;

  CrushWrapper *crush = &mon->osdmon->osdmap.crush;
  int pg_num = mon->osdmon->osdmap.get_pg_num();
  epoch_t epoch = mon->osdmon->osdmap.get_epoch();

  bool first = pg_map.pg_stat.empty(); // first pg creation
  int created = 0;
  for (int ruleno=0; ruleno<crush->get_max_rules(); ruleno++) {
    if (!crush->rule_exists(ruleno)) 
      continue;
    int pool = crush->get_rule_mask_pool(ruleno);
    int type = crush->get_rule_mask_type(ruleno);
    int min_size = crush->get_rule_mask_min_size(ruleno);
    int max_size = crush->get_rule_mask_max_size(ruleno);
    for (int size = min_size; size <= max_size; size++) {
      for (ps_t ps = 0; ps < pg_num; ps++) {
	pg_t pgid(type, size, ps, pool, -1);
	if (pg_map.pg_stat.count(pgid)) {
	  dout(20) << "register_new_pgs have " << pgid << dendl;
	  continue;
	}

	pg_t parent;
	int split_bits = 0;
	if (!first) {
	  parent = pgid;
	  while (1) {
	    // remove most significant bit
	    int msb = calc_bits_of(parent.u.pg.ps);
	    if (!msb) break;
	    parent.u.pg.ps &= ~(1<<(msb-1));
	    split_bits++;
	    dout(10) << " is " << pgid << " parent " << parent << " ?" << dendl;
	    //if (parent.u.pg.ps < mon->osdmon->osdmap.get_pgp_num()) {
	    if (pg_map.pg_stat.count(parent) &&
		pg_map.pg_stat[parent].state != PG_STATE_CREATING) {
	      dout(10) << "  parent is " << parent << dendl;
	      break;
	    }
	  }
	}
	
	pending_inc.pg_stat_updates[pgid].state = PG_STATE_CREATING;
	pending_inc.pg_stat_updates[pgid].created = epoch;
	pending_inc.pg_stat_updates[pgid].parent = parent;
	pending_inc.pg_stat_updates[pgid].parent_split_bits = split_bits;
	created++;	

	if (split_bits == 0) {
	  dout(10) << "register_new_pgs will create " << pgid << dendl;
	} else {
	  dout(10) << "register_new_pgs will create " << pgid
		   << " parent " << parent
		   << " by " << split_bits << " bits"
		   << dendl;
	}

      }
    }
  } 
  dout(10) << "register_new_pgs registered " << created << " new pgs" << dendl;
  if (created) {
    last_sent_pg_create.clear();  // reset pg_create throttle timer
    pending_inc.pg_scan = epoch;
    return true;
  }
  return false;
}

void PGMonitor::send_pg_creates()
{
  dout(10) << "send_pg_creates to " << pg_map.creating_pgs.size() << " pgs" << dendl;

  map<int, MOSDPGCreate*> msg;
  utime_t now = g_clock.now();

  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       p++) {
    pg_t pgid = *p;
    pg_t on = pgid;
    if (pg_map.pg_stat[pgid].parent_split_bits)
      on = pg_map.pg_stat[pgid].parent;
    vector<int> acting;
    int nrep = mon->osdmon->osdmap.pg_to_acting_osds(on, acting);
    if (!nrep) 
      continue;  // blarney!
    int osd = acting[0];

    // throttle?
    if (last_sent_pg_create.count(osd) &&
	now - g_conf.mon_pg_create_interval < last_sent_pg_create[osd]) 
      continue;
      
    dout(20) << "send_pg_creates  " << pgid << " -> osd" << osd 
	     << " in epoch " << pg_map.pg_stat[pgid].created << dendl;
    if (msg.count(osd) == 0)
      msg[osd] = new MOSDPGCreate(mon->osdmon->osdmap.get_epoch());
    msg[osd]->mkpg[pgid].created = pg_map.pg_stat[pgid].created;
    msg[osd]->mkpg[pgid].parent = pg_map.pg_stat[pgid].parent;
    msg[osd]->mkpg[pgid].split_bits = pg_map.pg_stat[pgid].parent_split_bits;
  }

  for (map<int, MOSDPGCreate*>::iterator p = msg.begin();
       p != msg.end();
       p++) {
    dout(10) << "sending pg_create to osd" << p->first << dendl;
    mon->messenger->send_message(p->second, mon->osdmon->osdmap.get_inst(p->first));
    last_sent_pg_create[p->first] = g_clock.now();
  }
}


bool PGMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      ss << *this;
      r = 0;
    }
    else if (m->cmd[1] == "getmap") {
      pg_map._encode(rdata);
      ss << "got pgmap version " << pg_map.version;
      r = 0;
    }
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata);
    return true;
  } else
    return false;
}


bool PGMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  // nothing here yet
  ss << "unrecognized command";

  getline(ss, rs);
  mon->reply_command(m, err, rs);
  return false;
}
