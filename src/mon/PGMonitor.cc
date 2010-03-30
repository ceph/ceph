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
#include "messages/MPGStatsAck.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDScrub.h"

#include "common/Timer.h"

#include "osd/osd_types.h"
#include "osd/PG.h"  // yuck

#include "config.h"
#include <sstream>

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, pg_map)
static ostream& _prefix(Monitor *mon, PGMap& pg_map) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".pg v" << pg_map.version << " ";
}


/*
 Tick function to update the map based on performance every N seconds
*/

void PGMonitor::tick() 
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << pg_map << dendl;
}

void PGMonitor::create_initial(bufferlist& bl)
{
  dout(10) << "create_initial -- creating initial map" << dendl;
}

bool PGMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv == pg_map.version) return true;
  assert(paxosv >= pg_map.version);

  if (pg_map.version == 0 && paxosv > 1) {
    // starting up: load latest
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: got latest latest full pgmap v" << v << dendl;
      bufferlist::iterator p = latest.begin();
      pg_map.decode(p);
    }
  } 

  // walk through incrementals
  while (paxosv > pg_map.version) {
    bufferlist bl;
    bool success = paxos->read(pg_map.version+1, bl);
    assert(success);

    dout(7) << "update_from_paxos  applying incremental " << pg_map.version+1 << dendl;
    PGMap::Incremental inc;
    bufferlist::iterator p = bl.begin();
    inc.decode(p);
    pg_map.apply_incremental(inc);
    
    dout(0) << pg_map << dendl;

    if (inc.pg_scan)
      last_sent_pg_create.clear();  // reset pg_create throttle timer
  }

  assert(paxosv == pg_map.version);

  // save latest
  bufferlist bl;
  pg_map.encode(bl);
  paxos->stash_latest(paxosv, bl);
  mon->store->put_int(paxosv, "pgmap", "last_consumed");

  // dump pgmap summaries?  (useful for debugging)
  if (0) {
    stringstream ds;
    pg_map.dump(ds);
    bufferlist d;
    d.append(ds);
    mon->store->put_bl_sn(d, "pgmap_dump", paxosv);
  }

  if (mon->is_leader() &&
      mon->is_full_quorum() &&
      paxosv > 10)
    paxos->trim_to(paxosv - 10);

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
  pending_inc.encode(bl);
}

bool PGMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    handle_statfs((MStatfs*)m);
    return true;
  case MSG_GETPOOLSTATS:
    return preprocess_getpoolstats((MGetPoolStats*)m);
    
  case MSG_PGSTATS:
    return preprocess_pg_stats((MPGStats*)m);

  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);


  default:
    assert(0);
    delete m;
    return true;
  }
}

bool PGMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return prepare_pg_stats((MPGStats*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);

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
  //check caps
  if(!statfs->get_session()->caps.check_privileges(PAXOS_PGMAP, MON_CAP_R)) {
    dout(0) << "MStatfs received from entity with insufficient privileges "
	    << statfs->get_session()->caps << dendl;
    goto out;
  }
  MStatfsReply *reply;

  dout(10) << "handle_statfs " << *statfs << " from " << statfs->get_orig_source() << dendl;

  if (ceph_fsid_compare(&statfs->fsid, &mon->monmap->fsid)) {
    dout(0) << "handle_statfs on fsid " << statfs->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  // fill out stfs
  reply = new MStatfsReply(mon->monmap->fsid, statfs->get_tid(), mon->get_epoch());

  // these are in KB.
  reply->h.st.kb = pg_map.osd_sum.kb;
  reply->h.st.kb_used = pg_map.osd_sum.kb_used;
  reply->h.st.kb_avail = pg_map.osd_sum.kb_avail;
  reply->h.st.num_objects = pg_map.pg_sum.num_objects;

  // reply
  mon->send_reply(statfs, reply);
 out:
  statfs->put();
}

bool PGMonitor::preprocess_getpoolstats(MGetPoolStats *m)
{
  MGetPoolStatsReply *reply;

  if (!m->get_session()->caps.check_privileges(PAXOS_PGMAP, MON_CAP_R)) {
    dout(0) << "MGetPoolStats received from entity with insufficient caps "
	    << m->get_session()->caps << dendl;
    goto out;
  }

  if (ceph_fsid_compare(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "preprocess_getpoolstats on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }
  
  reply = new MGetPoolStatsReply(m->fsid, m->get_tid(), paxos->get_version());

  for (vector<string>::iterator p = m->pools.begin();
       p != m->pools.end();
       p++) {
    int poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(p->c_str());
    if (poolid < 0)
      continue;
    if (pg_map.pg_pool_sum.count(poolid) == 0)
      continue;
    reply->pool_stats[*p] = pg_map.pg_pool_sum[poolid];
  }

  mon->send_reply(m, reply);

 out:
  m->put();

  return true;
}


bool PGMonitor::preprocess_pg_stats(MPGStats *stats)
{
  int from = stats->get_orig_source().num();
  MPGStatsAck *ack;
  //check caps
  if (!stats->get_session()->caps.check_privileges(PAXOS_PGMAP, MON_CAP_R)) {
    dout(0) << "MPGStats received from entity with insufficient privileges "
	    << stats->get_session()->caps << dendl;
    goto out;
  }
  // first, just see if they need a new osdmap.  but 
  // only if they've had the map for a while.
  if (stats->had_map_for > 30.0 && 
      mon->osdmon()->paxos->is_readable() &&
      stats->epoch < mon->osdmon()->osdmap.get_epoch())
    mon->osdmon()->send_latest_now_nodelete(stats, stats->epoch+1);

  // any new osd or pg info?
  if (!pg_map.osd_stat.count(from) ||
      pg_map.osd_stat[from] != stats->osd_stat)
    return false;  // new osd stat

  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       p++) {
    if (pg_map.pg_stat.count(p->first) == 0 ||
	pg_map.pg_stat[p->first].reported != p->second.reported)
      return false; // new pg stat(s)
  }
  
  dout(10) << " message contains no new osd|pg stats" << dendl;
  ack = new MPGStatsAck;
  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       p++)
    ack->pg_stat[p->first] = p->second.reported;
  mon->send_reply(stats, ack);
 out:
  stats->put();
  return true;
}

bool PGMonitor::prepare_pg_stats(MPGStats *stats) 
{
  dout(10) << "prepare_pg_stats " << *stats << " from " << stats->get_orig_source() << dendl;
  int from = stats->get_orig_source().num();

  if (ceph_fsid_compare(&stats->fsid, &mon->monmap->fsid)) {
    dout(0) << "handle_statfs on fsid " << stats->fsid << " != " << mon->monmap->fsid << dendl;
    stats->put();
    return false;
  }
  if (!stats->get_orig_source().is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      stats->get_orig_source_inst() != mon->osdmon()->osdmap.get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd" << dendl;
    stats->put();
    return false;
  }
      
  // osd stat
  pending_inc.osd_stat_updates[from] = stats->osd_stat;
  
  if (pg_map.osd_stat.count(from))
    dout(10) << " got osd" << from << " " << stats->osd_stat << " (was " << pg_map.osd_stat[from] << ")" << dendl;
  else
    dout(10) << " got osd " << from << " " << stats->osd_stat << " (first report)" << dendl;

  // apply to live map too (screw consistency)
  /*
    actually, no, don't.  that screws up our "latest" stash.  and can
    lead to weird output where things appear to jump backwards in
    time... that's just confusing.

  if (pg_map.osd_stat.count(from))
    pg_map.stat_osd_sub(pg_map.osd_stat[from]);
  pg_map.osd_stat[from] = stats->osd_stat;
  pg_map.stat_osd_add(stats->osd_stat);
  */

  // pg stats
  MPGStatsAck *ack = new MPGStatsAck;
  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       p++) {
    pg_t pgid = p->first;
    ack->pg_stat[pgid] = p->second.reported;

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
      stringstream ss;
      ss << "got " << pgid << " pg_stat from osd" << from << " but dne in pg_map";
      mon->logclient.log(LOG_ERROR, ss);
      continue;
    }
      
    dout(15) << " got " << pgid
	     << " reported at " << p->second.reported
	     << " state " << pg_state_string(pg_map.pg_stat[pgid].state)
	     << " -> " << pg_state_string(p->second.state)
	     << dendl;
    pending_inc.pg_stat_updates[pgid] = p->second;

    /*
    // we don't care much about consistency, here; apply to live map.
    pg_map.stat_pg_sub(pgid, pg_map.pg_stat[pgid]);
    pg_map.pg_stat[pgid] = p->second;
    pg_map.stat_pg_add(pgid, pg_map.pg_stat[pgid]);
    */
  }
  
  paxos->wait_for_commit(new C_Stats(this, stats, ack));
  return true;
}

void PGMonitor::_updated_stats(MPGStats *req, MPGStatsAck *ack)
{
  dout(7) << "_updated_stats for " << req->get_orig_source_inst() << dendl;
  mon->send_reply(req, ack);
  req->put();
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

  if (!mon->osdmon()->paxos->is_readable()) {
    dout(10) << "register_new_pgs -- osdmap not readable, waiting" << dendl;
    mon->osdmon()->paxos->wait_for_readable(new RetryCheckOSDMap(this, epoch));
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
    OSDMap::Incremental inc(bl);
    for (map<int32_t,uint32_t>::iterator p = inc.new_weight.begin();
	 p != inc.new_weight.end();
	 p++)
      if (p->second == CEPH_OSD_OUT) {
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

void PGMonitor::register_pg(pg_pool_t& pool, pg_t pgid, epoch_t epoch, bool new_pool)
{
  pg_t parent;
  int split_bits = 0;
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = pool.calc_bits_of(parent.v.ps);
      if (!msb) break;
      parent.v.ps = parent.v.ps & ~(1<<(msb-1));
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
  
  if (split_bits == 0) {
    dout(10) << "register_new_pgs  will create " << pgid << dendl;
  } else {
    dout(10) << "register_new_pgs  will create " << pgid
	     << " parent " << parent
	     << " by " << split_bits << " bits"
	     << dendl;
  }
}

bool PGMonitor::register_new_pgs()
{
  // iterate over crush mapspace
  epoch_t epoch = mon->osdmon()->osdmap.get_epoch();
  dout(10) << "register_new_pgs checking pg pools for osdmap epoch " << epoch
	   << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  OSDMap *osdmap = &mon->osdmon()->osdmap;

  int created = 0;
  for (map<int,pg_pool_t>::iterator p = osdmap->pools.begin();
       p != osdmap->pools.end();
       p++) {
    int poolid = p->first;
    pg_pool_t &pool = p->second;
    int ruleno = pool.get_crush_ruleset();
    if (!osdmap->crush.rule_exists(ruleno)) 
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
	pool.get_last_change() <= pending_inc.pg_scan) {
      dout(10) << " no change in " << pool << dendl;
      continue;
    }

    dout(10) << "register_new_pgs scanning " << pool << dendl;

    bool new_pool = pg_map.pg_pool_sum.count(poolid) == 0;  // first pgs in this pool

    for (ps_t ps = 0; ps < pool.get_pg_num(); ps++) {
      pg_t pgid(ps, poolid, -1);
      if (pg_map.pg_stat.count(pgid)) {
	dout(20) << "register_new_pgs  have " << pgid << dendl;
	continue;
      }
      created++;
      register_pg(pool, pgid, epoch, new_pool);
    }

    for (ps_t ps = 0; ps < pool.get_lpg_num(); ps++) {
      for (int osd = 0; osd < osdmap->get_max_osd(); osd++) {
	pg_t pgid(ps, poolid, osd);
	if (pg_map.pg_stat.count(pgid)) {
	  dout(20) << "register_new_pgs  have " << pgid << dendl;
	  continue;
	}
	created++;
	register_pg(pool, pgid, epoch, new_pool);
      }
    }
  } 

  int max = MIN(osdmap->get_max_osd(), osdmap->crush.get_max_devices());
  int removed = 0;
  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       p++)
    if (p->preferred() >= max) {
      dout(20) << " removing creating_pg " << *p << " because preferred >= max osd or crush device" << dendl;
      pending_inc.pg_remove.insert(*p);
      removed++;
    }

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
	   << removed << " uncreated pgs" << dendl;
  if (created || removed) {
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
  
  OSDMap *osdmap = &mon->osdmon()->osdmap;
  int max = MIN(osdmap->get_max_osd(), osdmap->crush.get_max_devices());

  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       p++) {
    pg_t pgid = *p;
    pg_t on = pgid;
    if (pg_map.pg_stat[pgid].parent_split_bits)
      on = pg_map.pg_stat[pgid].parent;
    vector<int> acting;
    int nrep = mon->osdmon()->osdmap.pg_to_acting_osds(on, acting);
    if (!nrep) {
      dout(20) << "send_pg_creates  " << pgid << " -> no osds in epoch "
	       << mon->osdmon()->osdmap.get_epoch() << ", skipping" << dendl;
      continue;  // blarney!
    }
    int osd = acting[0];

    // don't send creates for non-existant preferred osds!
    if (pgid.preferred() >= max)
      continue;

    // throttle?
    if (last_sent_pg_create.count(osd) &&
	now - g_conf.mon_pg_create_interval < last_sent_pg_create[osd]) 
      continue;
      
    dout(20) << "send_pg_creates  " << pgid << " -> osd" << osd 
	     << " in epoch " << pg_map.pg_stat[pgid].created << dendl;
    if (msg.count(osd) == 0)
      msg[osd] = new MOSDPGCreate(mon->osdmon()->osdmap.get_epoch());
    msg[osd]->mkpg[pgid].created = pg_map.pg_stat[pgid].created;
    msg[osd]->mkpg[pgid].parent = pg_map.pg_stat[pgid].parent;
    msg[osd]->mkpg[pgid].split_bits = pg_map.pg_stat[pgid].parent_split_bits;
  }

  for (map<int, MOSDPGCreate*>::iterator p = msg.begin();
       p != msg.end();
       p++) {
    dout(10) << "sending pg_create to osd" << p->first << dendl;
    mon->messenger->send_message(p->second, mon->osdmon()->osdmap.get_inst(p->first));
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
      ss << pg_map;
      r = 0;
    }
    else if (m->cmd[1] == "getmap") {
      pg_map.encode(rdata);
      ss << "got pgmap version " << pg_map.version;
      r = 0;
    }
    else if (m->cmd[1] == "send_pg_creates") {
      send_pg_creates();
      ss << "sent pg creates ";
      r = 0;
    }
    else if (m->cmd[1] == "dump") {
      ss << "ok";
      r = 0;
      stringstream ds;
      pg_map.dump(ds);
      rdata.append(ds);
    }
    else if (m->cmd[1] == "map" && m->cmd.size() == 3) {
      pg_t pgid;
      r = -EINVAL;
      if (pgid.parse(m->cmd[2].c_str())) {
	vector<int> acting;
	mon->osdmon()->osdmap.pg_to_acting_osds(pgid, acting);
	ss << "osdmap e" << mon->osdmon()->osdmap.get_epoch() << " pg " << pgid << " -> " << acting;
	r = 0;
      } else
	ss << "invalid pgid '" << m->cmd[2] << "'";
    }
    else if ((m->cmd[1] == "scrub" || m->cmd[1] == "repair") && m->cmd.size() == 3) {
      pg_t pgid;
      r = -EINVAL;
      if (pgid.parse(m->cmd[2].c_str())) {
	if (pg_map.pg_stat.count(pgid)) {
	  if (pg_map.pg_stat[pgid].acting.size()) {
	    int osd = pg_map.pg_stat[pgid].acting[0];
	    if (mon->osdmon()->osdmap.is_up(osd)) {
	      vector<pg_t> pgs(1);
	      pgs[0] = pgid;
	      mon->try_send_message(new MOSDScrub(mon->monmap->fsid, pgs,
						  m->cmd[1] == "repair"),
				    mon->osdmon()->osdmap.get_inst(osd));
	      ss << "instructing pg " << pgid << " on osd" << osd << " to " << m->cmd[1];
	      r = 0;
	    } else
	      ss << "pg " << pgid << " primary osd" << osd << " not up";
	  } else
	    ss << "pg " << pgid << " has no primary osd";
	} else
	  ss << "pg " << pgid << " dne";
      } else
	ss << "invalid pgid '" << m->cmd[2] << "'";
    }
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
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
  mon->reply_command(m, err, rs, paxos->get_version());
  return false;
}
