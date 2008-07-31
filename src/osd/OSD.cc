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



#include "include/types.h"

#include "OSD.h"
#include "OSDMap.h"

#include "os/FileStore.h"
#include "ebofs/Ebofs.h"

#ifdef USE_OSBDB
#include "osbdb/OSBDB.h"
#endif // USE_OSBDB


#include "ReplicatedPG.h"
#include "RAID4PG.h"

#include "Ager.h"


#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"

#include "messages/MOSDAlive.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "common/Logger.h"
#include "common/LogType.h"
#include "common/Timer.h"
#include "common/ThreadPool.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_dout << dbeginl << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "
#define  derr(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_derr << dbeginl << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "




const char *osd_base_path = "./osddata";
const char *ebofs_base_path = "./dev";

int OSD::find_osd_dev(char *result, int whoami)
{
  if (!g_conf.ebofs) {// || !g_conf.osbdb) {
    sprintf(result, "%s/osddata/osd%d", osd_base_path, whoami);
    return 0;
  }

  char hostname[100];
  hostname[0] = 0;
  gethostname(hostname,100);

  // try in this order:
  // dev/osd$num
  // dev/osd.$hostname
  // dev/osd.all
  struct stat sta;

  sprintf(result, "%s/osd%d", ebofs_base_path, whoami);
  if (::lstat(result, &sta) == 0) return 0;

  sprintf(result, "%s/osd.%s", ebofs_base_path, hostname);    
  if (::lstat(result, &sta) == 0) return 0;
    
  sprintf(result, "%s/osd.all", ebofs_base_path);
  if (::lstat(result, &sta) == 0) return 0;

  return -ENOENT;
}


ObjectStore *OSD::create_object_store(const char *dev)
{
  struct stat st;
  if (::stat(dev, &st) != 0)
    return 0;

  if (g_conf.ebofs) 
    return new Ebofs(dev);
  if (g_conf.filestore)
    return new FileStore(dev);

  if (S_ISDIR(st.st_mode))
    return new FileStore(dev);
  else
    return new Ebofs(dev);
}


int OSD::mkfs(const char *dev, ceph_fsid fsid, int whoami)
{
  ObjectStore *store = create_object_store(dev);
  if (!store)
    return -ENOENT;
  int err = store->mkfs();    
  if (err < 0) return err;
  err = store->mount();
  if (err < 0) return err;
    
  OSDSuperblock sb;
  sb.fsid = fsid;
  sb.whoami = whoami;

  store->create_collection(0, 0);

  // age?
  if (g_conf.osd_age_time != 0) {
    cout << "aging..." << std::endl;
    Ager ager(store);
    if (g_conf.osd_age_time < 0) 
      ager.load_freelist();
    else 
      ager.age(g_conf.osd_age_time, 
	       g_conf.osd_age, 
	       g_conf.osd_age - .05, 
	       50000, 
	       g_conf.osd_age - .05);
  }

  // benchmark?
  if (g_conf.osd_auto_weight) {
    bufferlist bl;
    bufferptr bp(1048576);
    bp.zero();
    bl.push_back(bp);
    cout << "testing disk bandwidth..." << std::endl;
    utime_t start = g_clock.now();
    for (int i=0; i<1000; i++) 
      store->write(0, pobject_t(0, 0, object_t(999,i)), 0, bl.length(), bl, 0);
    store->sync();
    utime_t end = g_clock.now();
    end -= start;
    cout << "measured " << (1000.0 / (double)end) << " mb/sec" << std::endl;
    for (int i=0; i<1000; i++) 
      store->remove(0, pobject_t(0, 0, object_t(999,i)), 0);
    
    // set osd weight
    sb.weight = (1000.0 / (double)end);
  }

  bufferlist bl;
  ::encode(sb, bl);
  store->write(0, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl, 0);
  store->umount();
  delete store;
  return 0;
}

int OSD::peek_whoami(const char *dev)
{
  ObjectStore *store = create_object_store(dev);
  int err = store->mount();
  if (err < 0) 
    return err;

  OSDSuperblock sb;
  bufferlist bl;
  err = store->read(0, OSD_SUPERBLOCK_POBJECT, 0, sizeof(sb), bl);
  if (err < 0) 
    return -ENOENT;
  bl.copy(0, sizeof(sb), (char*)&sb);
  store->umount();
  delete store;

  return sb.whoami;
}



// <hack> force remount hack for performance testing FileStore
class C_Remount : public Context {
  OSD *osd;
public:
  C_Remount(OSD *o) : osd(o) {}
  void finish(int) {
    osd->force_remount();
  }
};

void OSD::force_remount()
{
  dout(0) << "forcing remount" << dendl;
  osd_lock.Lock();
  {
    store->umount();
    store->mount();
  }
  osd_lock.Unlock();
  dout(0) << "finished remount" << dendl;
}
// </hack>


// cons/des

LogType osd_logtype;

OSD::OSD(int id, Messenger *m, MonMap *mm, const char *dev) : 
  timer(osd_lock),
  whoami(id), dev_name(dev),
  stat_oprate(5.0),
  read_latency_calc(g_conf.osd_max_opq<1 ? 1:g_conf.osd_max_opq),
  qlen_calc(3),
  iat_averager(g_conf.osd_flash_crowd_iat_alpha)
{
  messenger = m;
  monmap = mm;

  osdmap = 0;
  boot_epoch = 0;

  last_tid = 0;
  num_pulling = 0;

  state = STATE_BOOTING;

  memset(&my_stat, 0, sizeof(my_stat));

  booting = boot_pending = false;
  up_thru_wanted = up_thru_pending = 0;
  osd_stat_updated = osd_stat_pending = false;

  stat_ops = 0;
  stat_qlen = 0;
  stat_rd_ops = stat_rd_ops_shed_in = stat_rd_ops_shed_out = 0;
  stat_rd_ops_in_queue = 0;

  pending_ops = 0;
  waiting_for_no_ops = false;

  if (g_conf.osd_remount_at) 
    timer.add_event_after(g_conf.osd_remount_at, new C_Remount(this));
}

OSD::~OSD()
{
  if (threadpool) { delete threadpool; threadpool = 0; }
  if (osdmap) { delete osdmap; osdmap = 0; }
  //if (monitor) { delete monitor; monitor = 0; }
  if (messenger) { delete messenger; messenger = 0; }
  if (logger) { delete logger; logger = 0; }
  if (store) { delete store; store = 0; }
}

bool got_sighup = false;

void handle_sighup(int signal, siginfo_t *info, void *p)
{
  got_sighup = true;
}

int OSD::init()
{
  Mutex::Locker lock(osd_lock);

  char dev_path[100];
  if (dev_name) 
    strcpy(dev_path, dev_name);
  else {
    // search for a suitable dev path, based on our identity
    int r = find_osd_dev(dev_path, whoami);
    if (r < 0) {
      dout(0) << "*** unable to find a dev for osd" << whoami << dendl;
      return r;
    }

    // mkfs?
    if (g_conf.osd_mkfs) {
      dout(2) << "mkfs on local store" << dendl;
      r = mkfs(dev_path, monmap->fsid, whoami);
      if (r < 0) 
	return r;
    }
  }
  
  // mount.
  dout(2) << "mounting " << dev_path << dendl;
  store = create_object_store(dev_path);
  assert(store);
  int r = store->mount();
  if (r < 0) return -1;
  
  dout(2) << "boot" << dendl;
  
  // read superblock
  if (read_superblock() < 0) {
    store->umount();
    delete store;
    return -1;
  }
  
  // load up pgs (as they previously existed)
  load_pgs();
  
  dout(2) << "superblock: i am osd" << superblock.whoami << dendl;
  assert(whoami == superblock.whoami);
    
  // log
  char name[80];
  sprintf(name, "osd%d", whoami);
  logger = new Logger(name, (LogType*)&osd_logtype);
  osd_logtype.add_set("opq");
  osd_logtype.add_inc("op");
  osd_logtype.add_inc("c_rd");
  osd_logtype.add_inc("c_rdb");
  osd_logtype.add_inc("c_wr");
  osd_logtype.add_inc("c_wrb");
  
  osd_logtype.add_inc("r_push");
  osd_logtype.add_inc("r_pushb");
  osd_logtype.add_inc("r_wr");
  osd_logtype.add_inc("r_wrb");
  
  osd_logtype.add_set("qlen");
  osd_logtype.add_set("rqlen");
  osd_logtype.add_set("rdlat");
  osd_logtype.add_set("rdlatm");
  osd_logtype.add_set("fshdin");
  osd_logtype.add_set("fshdout");
  osd_logtype.add_inc("shdout");
  osd_logtype.add_inc("shdin");

  osd_logtype.add_set("loadavg");

  osd_logtype.add_inc("rlsum");
  osd_logtype.add_inc("rlnum");

  osd_logtype.add_set("numpg");
  osd_logtype.add_set("hbto");
  osd_logtype.add_set("hbfrom");
  
  osd_logtype.add_set("buf");
  
  osd_logtype.add_inc("map");
  osd_logtype.add_inc("mapi");
  osd_logtype.add_inc("mapidup");
  osd_logtype.add_inc("mapf");
  osd_logtype.add_inc("mapfdup");
  
  // request thread pool
  {
    char name[80];
    sprintf(name,"osd%d.threadpool", whoami);
    threadpool = new ThreadPool<OSD*, PG*>(name, g_conf.osd_maxthreads, 
					   static_dequeueop,
					   this);
  }
  
  // i'm ready!
  messenger->set_dispatcher(this);
  
  // announce to monitor i exist and have booted.
  booting = true;
  do_mon_report();     // start mon report timer
  
  // start the heartbeat
  timer.add_event_after(g_conf.osd_heartbeat_interval, new C_Heartbeat(this));

  // SIGHUP
  struct sigaction hup;
  memset(&hup, 0, sizeof(hup));
  hup.sa_sigaction = handle_sighup;
  sigaction(SIGHUP, &hup, 0);

  return 0;
}

int OSD::shutdown()
{
  dout(1) << "shutdown" << dendl;

  state = STATE_STOPPING;

  // cancel timers
  timer.cancel_all();
  timer.join();

  // finish ops
  wait_for_no_ops();

  // stop threads
  delete threadpool;
  threadpool = 0;

  // close pgs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    delete p->second;
  }
  pg_map.clear();

  // shut everything else down
  //monitor->shutdown();
  messenger->shutdown();

  osd_lock.Unlock();
  int r = store->umount();
  osd_lock.Lock();
  return r;
}



void OSD::write_superblock(ObjectStore::Transaction& t)
{
  dout(10) << "write_superblock " << superblock << dendl;

  bufferlist bl;
  ::encode(superblock, bl);
  t.write(0, OSD_SUPERBLOCK_POBJECT, 0, sizeof(superblock), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(0, OSD_SUPERBLOCK_POBJECT, 0, sizeof(superblock), bl);
  if (bl.length() != sizeof(superblock)) {
    derr(0) << "read_superblock failed, r = " << r 
	    << ", i got " << bl.length() << " bytes, not " << sizeof(superblock) << dendl;
    return -1;
  }

  bl.copy(0, sizeof(superblock), (char*)&superblock);

  dout(10) << "read_superblock " << superblock << dendl;
  if (whoami != superblock.whoami) {
    derr(0) << "read_superblock superblock says osd" << superblock.whoami
	    << ", but i (think i) am osd" << whoami << dendl;
    return -1;
  }
  
  // load up "current" osdmap
  assert(!osdmap);
  osdmap = new OSDMap;
  if (superblock.current_epoch) {
    bl.clear();
    get_map_bl(superblock.current_epoch, bl);
    osdmap->decode(bl);
  }

  return 0;
}





// ======================================================
// PG's

PG *OSD::_open_lock_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());

  // create
  PG *pg;
  if (pgid.is_rep())
    pg = new ReplicatedPG(this, pgid);
  else if (pgid.is_raid4())
    pg = new RAID4PG(this, pgid);
  else 
    assert(0);

  assert(pg_map.count(pgid) == 0);
  pg_map[pgid] = pg;

  pg->lock(); // always lock.
  pg->get();  // because it's in pg_map
  return pg;
}


PG *OSD::_create_lock_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(10) << "_create_lock_pg " << pgid << dendl;

  if (pg_map.count(pgid)) 
    dout(0) << "_create_lock_pg on " << pgid << ", already have " << *pg_map[pgid] << dendl;

  // open
  PG *pg = _open_lock_pg(pgid);

  // create collection
  assert(!store->collection_exists(pgid));
  t.create_collection(pgid);

  return pg;
}

PG * OSD::_create_lock_new_pg(pg_t pgid, vector<int>& acting, ObjectStore::Transaction& t)
{
  dout(20) << "_create_lock_new_pg pgid " << pgid << " -> " << acting << dendl;
  assert(whoami == acting[0]);
  
  PG *pg = _create_lock_pg(pgid, t);
  pg->set_role(0);
  pg->acting.swap(acting);
  pg->info.history.epoch_created = 
    pg->info.history.last_epoch_started = 
    pg->info.history.same_since = 
    pg->info.history.same_primary_since = 
    pg->info.history.same_acker_since = osdmap->get_epoch();
  pg->write_log(t);
  
  dout(7) << "_create_lock_new_pg " << *pg << dendl;
  return pg;
}


bool OSD::_have_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  return pg_map.count(pgid);
}

PG *OSD::_lookup_lock_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}


void OSD::_remove_unlock_pg(PG *pg) 
{
  assert(osd_lock.is_locked());
  pg_t pgid = pg->info.pgid;

  dout(10) << "_remove_unlock_pg " << pgid << dendl;

  // remove from store
  list<pobject_t> olist;
  store->collection_list(pgid, olist);
  
  ObjectStore::Transaction t;
  {
    for (list<pobject_t>::iterator p = olist.begin();
	 p != olist.end();
	 p++)
      t.remove(pgid, *p);
    t.remove(pgid, pgid.to_pobject());  // log too
    t.remove_collection(pgid);
  }
  store->apply_transaction(t);

  // mark deleted
  pg->mark_deleted();

  // remove from map
  pg_map.erase(pgid);

  // unlock, and probably delete
  pg->put_unlock();     // will delete, if last reference
}

void OSD::load_pgs()
{
  assert(osd_lock.is_locked());
  dout(10) << "load_pgs" << dendl;
  assert(pg_map.empty());

  list<coll_t> ls;
  store->list_collections(ls);

  for (list<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    if (*it == 0)
      continue;
    pg_t pgid = *it;
    PG *pg = _open_lock_pg(pgid);

    // read pg info
    store->collection_getattr(pgid, "info", &pg->info, sizeof(pg->info));
    
    // read pg log
    pg->read_log(store);

    // generate state for current mapping
    int nrep = osdmap->pg_to_acting_osds(pgid, pg->acting);
    int role = osdmap->calc_pg_role(whoami, pg->acting, nrep);
    pg->set_role(role);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << dendl;
    pg->unlock();
  }
}
 


/*
 * calculate prior pg members during an epoch interval [start,end)
 *  - from each epoch, include all osds up then AND now
 *  - if no osds from then are up now, include them all, even tho they're not reachable now
 */
void OSD::calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset)
{
  dout(15) << "calc_priors_during " << pgid << " [" << start << "," << end << ")" << dendl;
  
  for (epoch_t e = start; e < end; e++) {
    OSDMap oldmap;
    get_map(e, oldmap);
    vector<int> acting;
    oldmap.pg_to_acting_osds(pgid, acting);
    dout(20) << "  " << pgid << " in epoch " << e << " was " << acting << dendl;
    int added = 0;
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] != whoami && osdmap->is_up(acting[i])) {
	pset.insert(acting[i]);
	added++;
      }
    if (!added && acting.size()) {
      // sucky.  add down osds, even tho we can't reach them right now.
      for (unsigned i=0; i<acting.size(); i++)
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
    }
  }
  dout(10) << "calc_priors_during " << pgid
	   << " [" << start << "," << end 
	   << ") = " << pset << dendl;
}


/**
 * check epochs starting from start to verify the pg acting set hasn't changed
 * up until now
 */
void OSD::project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from,
			     vector<int>& last)
{
  dout(15) << "project_pg_history " << pgid
           << " from " << from << " to " << osdmap->get_epoch()
           << ", start " << h
           << dendl;

  for (epoch_t e = osdmap->get_epoch();
       e > from;
       e--) {
    // verify during intermediate epoch (e-1)
    OSDMap oldmap;
    get_map(e-1, oldmap);

    vector<int> acting;
    oldmap.pg_to_acting_osds(pgid, acting);

    // acting set change?
    if (acting != last && 
        e > h.same_since) {
      dout(15) << "project_pg_history " << pgid << " changed in " << e 
                << " from " << acting << " -> " << last << dendl;
      h.same_since = e;
    }

    // primary change?
    if (!(!acting.empty() && !last.empty() && acting[0] == last[0]) &&
        e > h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e << dendl;
      h.same_primary_since = e;
    
      if (g_conf.osd_rep == OSD_REP_PRIMARY)
        h.same_acker_since = h.same_primary_since;
    }

    // acker change?
    if (g_conf.osd_rep != OSD_REP_PRIMARY) {
      if (!(!acting.empty() && !last.empty() && acting[acting.size()-1] == last[last.size()-1]) &&
          e > h.same_acker_since) {
        dout(15) << "project_pg_history " << pgid << " acker changed in " << e << dendl;
        h.same_acker_since = e;
      }
    }

    if (h.same_since >= e &&
        h.same_primary_since >= e &&
        h.same_acker_since >= e) break;
  }

  dout(15) << "project_pg_history end " << h << dendl;
}

/*
 * NOTE: this is called from SafeTimer, so caller holds osd_lock
 */
void OSD::activate_pg(pg_t pgid, epoch_t epoch)
{
  assert(osd_lock.is_locked());

  if (pg_map.count(pgid)) {
    PG *pg = _lookup_lock_pg(pgid);
    if (pg->is_crashed() &&
	pg->is_replay() &&
	pg->get_role() == 0 &&
	pg->info.history.same_primary_since <= epoch) {
      ObjectStore::Transaction t;
      pg->activate(t);
      store->apply_transaction(t);
    }
    pg->unlock();
  }

  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();

  // finishers?
  finished_lock.Lock();
  if (finished.empty()) {
    finished_lock.Unlock();
  } else {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    finished_lock.Unlock();
    osd_lock.Unlock();
    
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++) {
      dispatch(*it);
    }

    osd_lock.Lock();
  }
}



// -------------------------------------

void OSD::_refresh_my_stat(utime_t now)
{
  assert(peer_stat_lock.is_locked());

  // refresh?
  if (now - my_stat.stamp > g_conf.osd_stat_refresh_interval ||
      pending_ops > 2*my_stat.qlen) {

    now.encode_timeval(&my_stat.stamp);
    my_stat.oprate = stat_oprate.get(now);

    //read_latency_calc.set_size( 20 );  // hrm.

    // qlen
    my_stat.qlen = 0;
    if (stat_ops) my_stat.qlen = (float)stat_qlen / (float)stat_ops;  //get_average();

    // rd ops shed in
    float frac_rd_ops_shed_in = 0;
    float frac_rd_ops_shed_out = 0;
    if (stat_rd_ops) {
      frac_rd_ops_shed_in = (float)stat_rd_ops_shed_in / (float)stat_rd_ops;
      frac_rd_ops_shed_out = (float)stat_rd_ops_shed_out / (float)stat_rd_ops;
    }
    my_stat.frac_rd_ops_shed_in = (my_stat.frac_rd_ops_shed_in + frac_rd_ops_shed_in) / 2.0;
    my_stat.frac_rd_ops_shed_out = (my_stat.frac_rd_ops_shed_out + frac_rd_ops_shed_out) / 2.0;

    // recent_qlen
    qlen_calc.add(my_stat.qlen);
    my_stat.recent_qlen = qlen_calc.get_average();

    // read latency
    if (stat_rd_ops) {
      my_stat.read_latency = read_latency_calc.get_average();
      if (my_stat.read_latency < 0) my_stat.read_latency = 0;
    } else {
      my_stat.read_latency = 0;
    }

    my_stat.read_latency_mine = my_stat.read_latency * (1.0 - frac_rd_ops_shed_in);

    logger->fset("qlen", my_stat.qlen);
    logger->fset("rqlen", my_stat.recent_qlen);
    logger->fset("rdlat", my_stat.read_latency);
    logger->fset("rdlatm", my_stat.read_latency_mine);
    logger->fset("fshdin", my_stat.frac_rd_ops_shed_in);
    logger->fset("fshdout", my_stat.frac_rd_ops_shed_out);
    dout(12) << "_refresh_my_stat " << my_stat << dendl;

    stat_rd_ops = 0;
    stat_rd_ops_shed_in = 0;
    stat_rd_ops_shed_out = 0;
    stat_ops = 0;
    stat_qlen = 0;
  }
}

osd_peer_stat_t OSD::get_my_stat_for(utime_t now, int peer)
{
  Mutex::Locker lock(peer_stat_lock);
  _refresh_my_stat(now);
  my_stat_on_peer[peer] = my_stat;
  return my_stat;
}

void OSD::take_peer_stat(int peer, const osd_peer_stat_t& stat)
{
  Mutex::Locker lock(peer_stat_lock);
  dout(15) << "take_peer_stat peer osd" << peer << " " << stat << dendl;
  peer_stat[peer] = stat;
}

void OSD::update_heartbeat_peers()
{
  assert(osd_lock.is_locked());

  // filter heartbeat_from_stamp to only include osds that remain in
  // heartbeat_from.
  map<int, utime_t> stamps;
  stamps.swap(heartbeat_from_stamp);

  // build heartbeat to/from set
  heartbeat_to.clear();
  heartbeat_from.clear();
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;

    // replicas ping primary.
    if (pg->get_role() > 0) {
      assert(pg->acting.size() > 1);
      heartbeat_to.insert(pg->acting[0]);
    }
    else if (pg->get_role() == 0) {
      assert(pg->acting[0] == whoami);
      for (unsigned i=1; i<pg->acting.size(); i++) {
	int p = pg->acting[i]; // peer
	assert(p != whoami);
	heartbeat_from.insert(p);
	if (stamps.count(p))
	  heartbeat_from_stamp[p] = stamps[p];
      }
    }
  }
  dout(10) << "hb   to: " << heartbeat_to << dendl;
  dout(10) << "hb from: " << heartbeat_from << dendl;
}

void OSD::heartbeat()
{
  utime_t now = g_clock.now();

  if (got_sighup) {
    dout(0) << "got SIGHUP, shutting down" << dendl;
    messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN), messenger->get_myinst());
    return;
  }
    

  // get CPU load avg
  ifstream in("/proc/loadavg");
  if (in.is_open()) {
    float oneminavg;
    in >> oneminavg;
    logger->fset("loadavg", oneminavg);
    in.close();
  }

  // calc my stats
  Mutex::Locker lock(peer_stat_lock);
  _refresh_my_stat(now);
  my_stat_on_peer.clear();

  dout(5) << "heartbeat: " << my_stat << dendl;

  //load_calc.set_size(stat_ops);
  
  // send heartbeats
  for (set<int>::iterator i = heartbeat_to.begin();
       i != heartbeat_to.end();
       i++) {
    _share_map_outgoing( osdmap->get_inst(*i) );
    my_stat_on_peer[*i] = my_stat;
    messenger->send_message(new MOSDPing(osdmap->get_epoch(), my_stat),
			    osdmap->get_inst(*i));
  }

  // check for incoming heartbeats (move me elsewhere?)
  utime_t grace = now;
  grace -= g_conf.osd_heartbeat_grace;
  for (set<int>::iterator p = heartbeat_from.begin();
       p != heartbeat_from.end();
       p++) {
    if (heartbeat_from_stamp.count(*p)) {
      if (heartbeat_from_stamp[*p] < grace) {
	dout(0) << "no heartbeat from osd" << *p << " since " << heartbeat_from_stamp[*p]
		<< " (cutoff " << grace << ")" << dendl;
	queue_failure(*p);
      }
    } else
      heartbeat_from_stamp[*p] = now;  // fake initial
  }

  if (logger) logger->set("hbto", heartbeat_to.size());
  if (logger) logger->set("hbfrom", heartbeat_from.size());

  
  // hmm.. am i all alone?
  if (heartbeat_from.empty() || heartbeat_to.empty()) {
    dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
    int mon = monmap->pick_mon();
    messenger->send_message(new MOSDGetMap(monmap->fsid, osdmap->get_epoch()+1),
                            monmap->get_inst(mon));
  }


  // hack: fake reorg?
  if (osdmap && g_conf.fake_osdmap_updates) {
    int mon = monmap->pick_mon();
    if ((rand() % g_conf.fake_osdmap_updates) == 0) {
      //if ((rand() % (g_conf.num_osd / g_conf.fake_osdmap_updates)) == whoami / g_conf.fake_osdmap_updates) {
      messenger->send_message(new MOSDIn(osdmap->get_epoch()),
                              monmap->get_inst(mon));
    }
    /*
      if (osdmap->is_out(whoami)) {
      messenger->send_message(new MOSDIn(osdmap->get_epoch()),
                              MSG_ADDR_MON(mon), monmap->get_inst(mon));
      } 
      else if ((rand() % g_conf.fake_osdmap_updates) == 0) {
      //messenger->send_message(new MOSDOut(osdmap->get_epoch()),
      //MSG_ADDR_MON(mon), monmap->get_inst(mon));
      }
    }
    */
  }

  // schedule next!  randomly.
  float wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf.osd_heartbeat_interval;
  timer.add_event_after(wait, new C_Heartbeat(this));
}



// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  last_mon_report = g_clock.now();

  // are prior reports still pending?
  bool retry = false;
  if (boot_pending) {
    dout(10) << "boot still pending" << dendl;
    retry = true;
  }
  if (osdmap->exists(whoami) && 
      up_thru_pending < osdmap->get_up_thru(whoami)) {
    dout(10) << "up_thru_pending " << up_thru_pending << " < " << osdmap->get_up_thru(whoami) 
	     << " -- still pending" << dendl;
    retry = true;
  }
  pg_stat_queue_lock.Lock();
  if (!pg_stat_pending.empty() || osd_stat_pending) {
    dout(10) << "requeueing pg_stat_pending" << dendl;
    retry = true;
    osd_stat_updated = osd_stat_updated || osd_stat_pending;
    osd_stat_pending = false;
    for (map<pg_t,eversion_t>::iterator p = pg_stat_pending.begin(); 
	 p != pg_stat_pending.end(); 
	 p++)
      if (pg_stat_queue.count(p->first) == 0)   // _queue value will always be >= _pending
	pg_stat_queue[p->first] = p->second;
    pg_stat_pending.clear();
  }
  pg_stat_queue_lock.Unlock();

  if (retry) {
    int oldmon = monmap->pick_mon();
    messenger->mark_down(monmap->get_inst(oldmon).addr);
    int mon = monmap->pick_mon(true);
    dout(10) << "marked down old mon" << oldmon << ", chose new mon" << mon << dendl;
  }

  // do any pending reports
  if (booting)
    send_boot();
  send_alive();
  send_failures();
  send_pg_stats();

  // reschedule
  timer.add_event_after(g_conf.osd_mon_report_interval, new C_MonReport(this));
}

void OSD::send_boot()
{
  int mon = monmap->pick_mon();
  dout(10) << "send_boot to mon" << mon << dendl;
  messenger->send_message(new MOSDBoot(superblock), 
			  monmap->get_inst(mon));
}

void OSD::queue_want_up_thru(epoch_t want)
{
  epoch_t cur = osdmap->get_up_thru(whoami);
  if (want > up_thru_wanted) {
    dout(10) << "queue_want_up_thru now " << want << " (was " << up_thru_wanted << ")" 
	     << ", currently " << cur
	     << dendl;
    up_thru_wanted = want;

    // expedite, a bit.  WARNING this will somewhat delay other mon queries.
    last_mon_report = g_clock.now();
    send_alive();
  } else {
    dout(10) << "queue_want_up_thru want " << want << " <= queued " << up_thru_wanted 
	     << ", currently " << cur
	     << dendl;
  }
}

void OSD::send_alive()
{
  if (!osdmap->exists(whoami))
    return;
  epoch_t up_thru = osdmap->get_up_thru(whoami);
  dout(10) << "send_alive up_thru currently " << up_thru << " want " << up_thru_wanted << dendl;
  if (up_thru_wanted > up_thru) {
    up_thru_pending = up_thru_wanted;
    int mon = monmap->pick_mon();
    dout(10) << "send_alive to mon" << mon << " (want " << up_thru_wanted << ")" << dendl;
    messenger->send_message(new MOSDAlive(osdmap->get_epoch()),
			    monmap->get_inst(mon));
  }
}

void OSD::send_failures()
{
  int mon = monmap->pick_mon();
  while (!failure_queue.empty()) {
    int osd = *failure_queue.begin();
    messenger->send_message(new MOSDFailure(monmap->fsid, osdmap->get_inst(osd), osdmap->get_epoch()),
			    monmap->get_inst(mon));
    failure_queue.erase(osd);
  }
}

void OSD::send_pg_stats()
{
  assert(osd_lock.is_locked());

  dout(10) << "send_pg_stats" << dendl;

  // grab queue
  assert(pg_stat_pending.empty());
  pg_stat_queue_lock.Lock();
  pg_stat_pending.swap(pg_stat_queue);
  osd_stat_pending = osd_stat_updated;
  osd_stat_updated = false;
  pg_stat_queue_lock.Unlock();

  if (!pg_stat_pending.empty() || osd_stat_pending) {
    dout(1) << "send_pg_stats - " << pg_stat_pending.size() << " pgs updated" << dendl;
    
    MPGStats *m = new MPGStats;
    for (map<pg_t,eversion_t>::iterator p = pg_stat_pending.begin();
	 p != pg_stat_pending.end();
	 p++) {
      pg_t pgid = p->first;
      
      if (!pg_map.count(pgid)) 
	continue;
      PG *pg = pg_map[pgid];
      pg->pg_stats_lock.Lock();
      m->pg_stat[pgid] = pg->pg_stats;
      dout(20) << " sending " << pgid << " " << pg->pg_stats.state << dendl;
      pg->pg_stats_lock.Unlock();
    }
    
    // fill in osd stats too
    struct statfs stbuf;
    store->statfs(&stbuf);
    m->osd_stat.num_blocks = stbuf.f_blocks;
    m->osd_stat.num_blocks_avail = stbuf.f_bavail;
    m->osd_stat.num_objects = stbuf.f_files;
    dout(20) << " osd_stat " << m->osd_stat << dendl;
    
    int mon = monmap->pick_mon();
    messenger->send_message(m, monmap->get_inst(mon));  
  }
}

void OSD::handle_pgstats_ack(MPGStatsAck *ack)
{
  dout(10) << "handle_pgstats_ack " << dendl;

  for (map<pg_t,eversion_t>::iterator p = ack->pg_stat.begin();
       p != ack->pg_stat.end();
       p++) {
    if (pg_stat_pending.count(p->first) == 0) {
      dout(10) << "ignoring " << p->first << " " << p->second << dendl;
    } else if (pg_stat_pending[p->first] <= p->second) {
      dout(10) << "ack on " << p->first << " " << p->second << dendl;
      pg_stat_pending.erase(p->first);
    } else {
      dout(10) << "still pending " << p->first << " " << pg_stat_pending[p->first]
	       << " > acked " << p->second << dendl;
    }
  }
  
  if (pg_stat_pending.empty()) {
    dout(10) << "clearing osd_stat_pending" << dendl;
    osd_stat_pending = false;
  }

  delete ack;
}




// --------------------------------------
// dispatch

bool OSD::_share_map_incoming(const entity_inst_t& inst, epoch_t epoch)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << inst << " " << epoch << dendl;
  assert(osd_lock.is_locked());

  // does client have old map?
  if (inst.name.is_client()) {
    if (epoch < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst, true);
      shared = true;
    }
  }

  // does peer have old map?
  if (inst.name.is_osd() &&
      osdmap->have_inst(inst.name.num()) &&
      osdmap->get_inst(inst.name.num()) == inst) {
    // remember
    if (peer_map_epoch[inst.name] < epoch) {
      dout(20) << "peer " << inst.name << " has " << epoch << dendl;
      peer_map_epoch[inst.name] = epoch;
    }
    
    // older?
    if (peer_map_epoch[inst.name] < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst, true);
      peer_map_epoch[inst.name] = osdmap->get_epoch();  // so we don't send it again.
      shared = true;
    }
  }

  return shared;
}


void OSD::_share_map_outgoing(const entity_inst_t& inst) 
{
  assert(inst.name.is_osd());

  // send map?
  if (peer_map_epoch.count(inst.name)) {
    epoch_t pe = peer_map_epoch[inst.name];
    if (pe < osdmap->get_epoch()) {
      send_incremental_map(pe, inst, true);
      peer_map_epoch[inst.name] = osdmap->get_epoch();
    }
  } else {
    // no idea about peer's epoch.
    // ??? send recent ???
    // do nothing.
  }
}



void OSD::dispatch(Message *m) 
{
  // lock!
  osd_lock.Lock();
  dout(20) << "dispatch " << m << dendl;

  switch (m->get_type()) {

    // -- don't need lock -- 
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    delete m;
    break;

    // -- don't need OSDMap --

    // map and replication
  case CEPH_MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

    // osd
  case CEPH_MSG_SHUTDOWN:
    shutdown();
    delete m;
    break;

  case MSG_PGSTATSACK:
    handle_pgstats_ack((MPGStatsAck*)m);
    break;
    
    

    // -- need OSDMap --

  default:
    {
      // no map?  starting up?
      if (!osdmap) {
        dout(7) << "no OSDMap, not booted" << dendl;
        waiting_for_osdmap.push_back(m);
        break;
      }
      
      // need OSDMap
      switch (m->get_type()) {

      case MSG_OSD_PING:
        handle_osd_ping((MOSDPing*)m);
        break;

      case MSG_OSD_PG_CREATE:
	handle_pg_create((MOSDPGCreate*)m);
	break;
        
      case MSG_OSD_PG_NOTIFY:
        handle_pg_notify((MOSDPGNotify*)m);
        break;
      case MSG_OSD_PG_QUERY:
        handle_pg_query((MOSDPGQuery*)m);
        break;
      case MSG_OSD_PG_LOG:
        handle_pg_log((MOSDPGLog*)m);
        break;
      case MSG_OSD_PG_REMOVE:
        handle_pg_remove((MOSDPGRemove*)m);
        break;
      case MSG_OSD_PG_INFO:
        handle_pg_info((MOSDPGInfo*)m);
        break;

	// client ops
      case CEPH_MSG_OSD_OP:
        handle_op((MOSDOp*)m);
        break;
        
        // for replication etc.
      case MSG_OSD_SUBOP:
	handle_sub_op((MOSDSubOp*)m);
	break;
      case MSG_OSD_SUBOPREPLY:
        handle_sub_op_reply((MOSDSubOpReply*)m);
        break;
        
        
      default:
        dout(1) << " got unknown message " << m->get_type() << dendl;
        assert(0);
      }
    }
  }

  // finishers?
  finished_lock.Lock();
  if (!finished.empty()) {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    finished_lock.Unlock();
    osd_lock.Unlock();
    
    while (!waiting.empty()) {
      dout(20) << "doing finished " << waiting.front() << dendl;
      dispatch(waiting.front());
      waiting.pop_front();
    }
    return;
  }
  
  finished_lock.Unlock();
  osd_lock.Unlock();
}


void OSD::ms_handle_failure(Message *m, const entity_inst_t& inst)
{
  entity_name_t dest = inst.name;

  if (g_conf.ms_die_on_failure) {
    dout(0) << "ms_handle_failure " << inst << " on " << *m << dendl;
    exit(0);
  }

  if (is_stopping()) {
    delete m;
    return;
  }

  dout(1) << "ms_handle_failure " << inst 
	  << ", dropping " << *m << dendl;
  delete m;
}




void OSD::handle_osd_ping(MOSDPing *m)
{
  dout(20) << "osdping from " << m->get_source() << " got stat " << m->peer_stat << dendl;

  int from = m->get_source().num();
  if (osdmap->have_inst(from) &&
      osdmap->get_inst(from) == m->get_source_inst()) {

    _share_map_incoming(m->get_source_inst(), ((MOSDPing*)m)->map_epoch);
  
    take_peer_stat(from, m->peer_stat);
    heartbeat_from_stamp[from] = g_clock.now(); // don't let _my_ lag interfere... //  m->get_recv_stamp();
  }

  delete m;
}




// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask 
  if (waiting_for_osdmap.empty()) {
    int mon = monmap->pick_mon();
    messenger->send_message(new MOSDGetMap(monmap->fsid, osdmap->get_epoch()+1),
                            monmap->get_inst(mon));
  }
  
  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int osd)
{
  messenger->mark_down(osdmap->get_addr(osd));
  peer_map_epoch.erase(entity_name_t::OSD(osd));
  failure_queue.erase(osd);
  failure_pending.erase(osd);
  heartbeat_from_stamp.erase(osd);
}
void OSD::note_up_osd(int osd)
{
  peer_map_epoch.erase(entity_name_t::OSD(osd));
}

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  if (!ceph_fsid_equal(&m->fsid, &monmap->fsid)) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monmap->fsid << dendl;
    delete m;
    return;
  }

  booting = boot_pending = false;

  wait_for_no_ops();
  
  assert(osd_lock.is_locked());

  ObjectStore::Transaction t;
  
  if (osdmap) {
    dout(3) << "handle_osd_map epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "], i have " << osdmap->get_epoch()
            << dendl;
  } else {
    dout(3) << "handle_osd_map epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "], i have none"
            << dendl;
    osdmap = new OSDMap;
    boot_epoch = m->get_last(); // hrm...?
  }

  logger->inc("mapmsg");

  // store them?
  for (map<epoch_t,bufferlist>::iterator p = m->maps.begin();
       p != m->maps.end();
       p++) {
    pobject_t poid = get_osdmap_pobject_name(p->first);
    if (store->exists(0, poid)) {
      dout(10) << "handle_osd_map already had full map epoch " << p->first << dendl;
      logger->inc("mapfdup");
      bufferlist bl;
      get_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got full map epoch " << p->first << dendl;
    store->write(0, poid, 0, p->second.length(), p->second, 0);  // store _outside_ transaction; activate_map reads it.

    if (p->first > superblock.newest_map)
      superblock.newest_map = p->first;
    if (p->first < superblock.oldest_map ||
        superblock.oldest_map == 0)
      superblock.oldest_map = p->first;

    logger->inc("mapf");
  }
  for (map<epoch_t,bufferlist>::iterator p = m->incremental_maps.begin();
       p != m->incremental_maps.end();
       p++) {
    pobject_t poid = get_inc_osdmap_pobject_name(p->first);
    if (store->exists(0, poid)) {
      dout(10) << "handle_osd_map already had incremental map epoch " << p->first << dendl;
      logger->inc("mapidup");
      bufferlist bl;
      get_inc_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got incremental map epoch " << p->first << dendl;
    store->write(0, poid, 0, p->second.length(), p->second, 0);  // store _outside_ transaction; activate_map reads it.

    if (p->first > superblock.newest_map)
      superblock.newest_map = p->first;
    if (p->first < superblock.oldest_map ||
        superblock.oldest_map == 0)
      superblock.oldest_map = p->first;

    logger->inc("mapi");
  }

  // advance if we can
  bool advanced = false;
  
  epoch_t cur = superblock.current_epoch;
  while (cur < superblock.newest_map) {
    dout(10) << "cur " << cur << " < newest " << superblock.newest_map << dendl;

    if (m->incremental_maps.count(cur+1) ||
        store->exists(0, get_inc_osdmap_pobject_name(cur+1))) {
      dout(10) << "handle_osd_map decoding inc map epoch " << cur+1 << dendl;
      
      bufferlist bl;
      if (m->incremental_maps.count(cur+1)) {
	dout(10) << " using provided inc map" << dendl;
        bl = m->incremental_maps[cur+1];
      } else {
	dout(10) << " using my locally stored inc map" << dendl;
        get_inc_map_bl(cur+1, bl);
      }

      OSDMap::Incremental inc(bl);
      osdmap->apply_incremental(inc);

      // archive the full map
      bl.clear();
      osdmap->encode(bl);
      store->write(0, get_osdmap_pobject_name(cur+1), 0, bl.length(), bl, 0);

      // notify messenger
      for (map<int32_t,uint8_t>::iterator i = inc.new_down.begin();
           i != inc.new_down.end();
           i++) {
        int osd = i->first;
        if (osd == whoami) continue;
	note_down_osd(i->first);
      }
      for (map<int32_t,entity_addr_t>::iterator i = inc.new_up.begin();
           i != inc.new_up.end();
           i++) {
        if (i->first == whoami) continue;
	note_up_osd(i->first);
      }
    }
    else if (m->maps.count(cur+1) ||
             store->exists(0, get_osdmap_pobject_name(cur+1))) {
      dout(10) << "handle_osd_map decoding full map epoch " << cur+1 << dendl;
      bufferlist bl;
      if (m->maps.count(cur+1))
        bl = m->maps[cur+1];
      else
        get_map_bl(cur+1, bl);

      OSDMap *newmap = new OSDMap;
      newmap->decode(bl);

      // kill connections to newly down osds
      set<int> old;
      osdmap->get_all_osds(old);
      for (set<int>::iterator p = old.begin(); p != old.end(); p++)
	if (osdmap->have_inst(*p) && (!newmap->exists(*p) || !newmap->is_up(*p))) 
	  note_down_osd(*p);
      // NOTE: note_up_osd isn't called at all for full maps... FIXME?
      delete osdmap;
      osdmap = newmap;
    }
    else {
      dout(10) << "handle_osd_map missing epoch " << cur+1 << dendl;
      int mon = monmap->pick_mon();
      messenger->send_message(new MOSDGetMap(monmap->fsid, cur+1), monmap->get_inst(mon));
      break;
    }

    cur++;
    superblock.current_epoch = cur;
    advance_map(t);
    advanced = true;
  }

  // all the way?
  if (advanced && cur == superblock.newest_map) {
    if (osdmap->is_up(whoami) &&
	osdmap->get_addr(whoami) == messenger->get_myaddr()) {
      // yay!
      activate_map(t);

      // process waiters
      take_waiters(waiting_for_osdmap);
    }
  }

  // write updated pg state to store
  for (hash_map<pg_t,PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    pg_t pgid = i->first;
    PG *pg = i->second;
    t.collection_setattr( pgid, "info", &pg->info, sizeof(pg->info));
  }

  // superblock and commit
  write_superblock(t);
  store->apply_transaction(t);
  store->sync();

  //if (osdmap->get_epoch() == 1) store->sync();     // in case of early death, blah

  delete m;

  if (osdmap->get_epoch() > 0 &&
      (!osdmap->exists(whoami) || 
       (!osdmap->is_up(whoami) && osdmap->get_addr(whoami) == messenger->get_myaddr()))) {
    dout(0) << "map says i am dead" << dendl;
    shutdown();
  }
}


/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());

  dout(7) << "advance_map epoch " << osdmap->get_epoch() 
          << "  " << pg_map.size() << " pgs"
          << dendl;
  
  // scan pg creations
  hash_map<pg_t, create_pg_info>::iterator n = creating_pgs.begin();
  while (n != creating_pgs.end()) {
    hash_map<pg_t, create_pg_info>::iterator p = n++;
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role != 0) {
      dout(10) << " no longer primary for " << pgid << ", stopping creation" << dendl;
      creating_pgs.erase(p);
    } else {
      /*
       * adding new ppl to our pg has no effect, since we're still primary,
       * and obviously haven't given the new nodes any data.
       */
      p->second.acting.swap(acting);  // keep the latest
    }
  }

  // scan existing pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = it->second;
    
    // get new acting set
    vector<int> tacting;
    int nrep = osdmap->pg_to_acting_osds(pgid, tacting);
    int role = osdmap->calc_pg_role(whoami, tacting, nrep);
    
    // no change?
    if (tacting == pg->acting) 
      continue;
    
    // -- there was a change! --
    pg->lock();
    
    int oldrole = pg->get_role();
    int oldprimary = pg->get_primary();
    int oldacker = pg->get_acker();
    vector<int> oldacting = pg->acting;
    
    // update PG
    pg->acting.swap(tacting);
    pg->set_role(role);
    
    // did primary|acker change?
    pg->info.history.same_since = osdmap->get_epoch();
    if (oldprimary != pg->get_primary()) {
      pg->info.history.same_primary_since = osdmap->get_epoch();
      pg->cancel_recovery();
    }
    if (oldacker != pg->get_acker()) {
      pg->info.history.same_acker_since = osdmap->get_epoch();
    }
    
    // deactivate.
    pg->state_clear(PG_STATE_ACTIVE);
    
    // reset primary state?
    if (oldrole == 0 || pg->get_role() == 0)
      pg->clear_primary_state();

    dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
	     << ", role " << oldrole << " -> " << role << dendl; 
    
    // pg->on_*
    for (unsigned i=0; i<oldacting.size(); i++)
      if (osdmap->is_down(oldacting[i]))
	pg->on_osd_failure(oldacting[i]);
    pg->on_change();
    if (oldacker != pg->get_acker() && oldacker == whoami)
      pg->on_acker_change();
    
    if (role != oldrole) {
      // old primary?
      if (oldrole == 0) {
	pg->state_clear(PG_STATE_CLEAN);
	
	// take replay queue waiters
	list<Message*> ls;
	for (map<eversion_t,MOSDOp*>::iterator it = pg->replay_queue.begin();
	     it != pg->replay_queue.end();
	     it++)
	  ls.push_back(it->second);
	pg->replay_queue.clear();
	take_waiters(ls);
      }

      pg->on_role_change();
      
      // take active waiters
      take_waiters(pg->waiting_for_active);

      // new primary?
      if (role == 0) {
	// i am new primary
	pg->state_clear(PG_STATE_STRAY);
      } else {
	// i am now replica|stray.  we need to send a notify.
	pg->state_set(PG_STATE_STRAY);
	pg->have_master_log = false;

	if (nrep == 0) {
	  // did they all shut down cleanly?
	  bool clean = true;
	  vector<int> inset;
	  osdmap->pg_to_osds(pg->info.pgid, inset);
	  for (unsigned i=0; i<inset.size(); i++)
	    if (!osdmap->is_down_clean(inset[i])) clean = false;
	  if (clean) {
	    dout(1) << *pg << " is cleanly inactive" << dendl;
	  } else {
	    pg->state_set(PG_STATE_CRASHED);
	    dout(1) << *pg << " is crashed" << dendl;
	  }
	}
      }
      
    } else {
      // no role change.
      // did primary change?
      if (pg->get_primary() != oldprimary) {    
	// we need to announce
	pg->state_set(PG_STATE_STRAY);
        
	dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
		 << ", acting primary " 
		 << oldprimary << " -> " << pg->get_primary() 
		 << dendl;
      } else {
	// primary is the same.
	if (role == 0) {
	  // i am (still) primary. but my replica set changed.
	  pg->state_clear(PG_STATE_CLEAN);
	  pg->state_clear(PG_STATE_REPLAY);
	  
	  dout(10) << *pg << " " << oldacting << " -> " << pg->acting
		   << ", replicas changed" << dendl;
	}
      }
    }

    pg->unlock();
  }
}

void OSD::activate_map(ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  map< int, list<PG::Info> >  notify_list;  // primary -> list
  map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since
  map<int,MOSDPGInfo*> info_map;  // peer -> message

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_active()) {
      // update started counter
      pg->info.history.last_epoch_started = osdmap->get_epoch();
    }
    else if (pg->is_primary() && !pg->is_active()) {
      // i am (inactive) primary
      pg->build_prior();
      pg->peer(t, query_map, &info_map);
    }
    else if (pg->is_stray() &&
	     pg->get_primary() >= 0) {
      // i am residual|replica
      notify_list[pg->get_primary()].push_back(pg->info);
    }
    if (pg->is_primary())
      pg->update_stats();
    pg->unlock();
  }  

  last_active_epoch = osdmap->get_epoch();

  do_notifies(notify_list);  // notify? (residual|replica)
  do_queries(query_map);
  do_infos(info_map);

  logger->set("numpg", pg_map.size());

  wake_all_pg_waiters();   // the pg mapping may have shifted

  update_heartbeat_peers();
}


void OSD::send_incremental_map(epoch_t since, const entity_inst_t& inst, bool full, bool lazy)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << inst << dendl;
  
  MOSDMap *m = new MOSDMap(monmap->fsid);
  
  for (epoch_t e = osdmap->get_epoch();
       e > since;
       e--) {
    bufferlist bl;
    if (get_inc_map_bl(e,bl)) {
      m->incremental_maps[e].claim(bl);
    } else if (get_map_bl(e,bl)) {
      m->maps[e].claim(bl);
      if (!full) break;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }

  if (lazy)
    messenger->lazy_send_message(m, inst);  // only if we already have an open connection
  else
    messenger->send_message(m, inst);
}

bool OSD::get_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(0, get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(0, get_inc_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

void OSD::get_map(epoch_t epoch, OSDMap &m)
{
  // find a complete map
  list<OSDMap::Incremental> incs;
  epoch_t e;
  for (e = epoch; e > 0; e--) {
    bufferlist bl;
    if (get_map_bl(e, bl)) {
      //dout(10) << "get_map " << epoch << " full " << e << dendl;
      m.decode(bl);
      break;
    } else {
      OSDMap::Incremental inc;
      bool got = get_inc_map(e, inc);
      assert(got);
      incs.push_front(inc);
    }
  }
  assert(e >= 0);

  // apply incrementals
  for (e++; e <= epoch; e++) {
    //dout(10) << "get_map " << epoch << " inc " << e << dendl;
    m.apply_incremental( incs.front() );
    incs.pop_front();
  }
}


bool OSD::get_inc_map(epoch_t e, OSDMap::Incremental &inc)
{
  bufferlist bl;
  if (!get_inc_map_bl(e, bl)) 
    return false;
  bufferlist::iterator p = bl.begin();
  inc.decode(p);
  return true;
}





bool OSD::require_current_map(Message *m, epoch_t ep) 
{
  // older map?
  if (ep < osdmap->get_epoch()) {
    dout(7) << "require_current_map epoch " << ep << " < " << osdmap->get_epoch() << dendl;
    delete m;   // discard and ignore.
    return false;
  }

  // newer map?
  if (ep > osdmap->get_epoch()) {
    dout(7) << "require_current_map epoch " << ep << " > " << osdmap->get_epoch() << dendl;
    wait_for_new_map(m);
    return false;
  }

  assert(ep == osdmap->get_epoch());
  return true;
}


/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_same_or_newer_map(Message *m, epoch_t epoch)
{
  dout(15) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ") " << m << dendl;

  // do they have a newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << epoch << " > my " << osdmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(m);
    return false;
  }

  if (epoch < boot_epoch) {
    dout(7) << "from pre-boot epoch " << epoch << " < " << boot_epoch << dendl;
    delete m;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_addr(from) != m->get_source_inst().addr) {
      dout(-7) << "from dead osd" << from << ", dropping, sharing map" << dendl;
      send_incremental_map(epoch, m->get_source_inst(), true, true);
      delete m;
      return false;
    }
  }

  return true;
}





// ----------------------------------------
// pg creation


PG *OSD::try_create_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  assert(creating_pgs.count(pgid));

  // priors empty?
  if (!creating_pgs[pgid].prior.empty()) {
    dout(10) << "try_create_pg " << pgid
	     << " - waiting for priors " << creating_pgs[pgid].prior << dendl;
    return 0;
  }

  if (creating_pgs[pgid].split_bits) {
    dout(10) << "try_create_pg " << pgid << " - queueing for split" << dendl;
    pg_split_ready[creating_pgs[pgid].parent].insert(pgid); 
    return 0;
  }

  dout(10) << "try_create_pg " << pgid << " - creating now" << dendl;
  PG *pg = _create_lock_new_pg(pgid, creating_pgs[pgid].acting, t);
  creating_pgs.erase(pgid);
  return pg;
}


void OSD::kick_pg_split_queue()
{
  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  int created = 0;

  dout(10) << "kick_pg_split_queue" << dendl;

  map<pg_t, set<pg_t> >::iterator n = pg_split_ready.begin();
  while (n != pg_split_ready.end()) {
    map<pg_t, set<pg_t> >::iterator p = n++;
    // how many children should this parent have?
    unsigned nchildren = (1 << (creating_pgs[*p->second.begin()].split_bits - 1)) - 1;
    if (p->second.size() < nchildren) {
      dout(15) << " parent " << p->first << " children " << p->second 
	       << " ... waiting for " << nchildren << " children" << dendl;
      continue;
    }

    PG *parent = _lookup_lock_pg(p->first);
    assert(parent);
    if (!parent->is_clean()) {
      dout(10) << "kick_pg_split_queue parent " << p->first << " not clean" << dendl;
      parent->unlock();
      continue;
    }

    dout(15) << " parent " << p->first << " children " << p->second 
	     << " ready" << dendl;
    
    // FIXME: this should be done in a separate thread, eventually

    // create and lock children
    ObjectStore::Transaction t;
    map<pg_t,PG*> children;
    for (set<pg_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      PG *pg = _create_lock_new_pg(*q, creating_pgs[*q].acting, t);
      children[*q] = pg;
    }

    // split
    split_pg(parent, children, t); 

    // unlock parent, children
    parent->unlock();
    for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); q++) {
      PG *pg = q->second;
      // fix up pg metadata
      pg->info.last_complete = pg->info.last_update;
      t.collection_setattr(pg->info.pgid, "info", (char*)&pg->info, sizeof(pg->info));
      pg->write_log(t);

      wake_pg_waiters(pg->info.pgid);

      pg->peer(t, query_map, &info_map);
      pg->update_stats();
      pg->unlock();
      created++;
    }
    store->apply_transaction(t);

    // remove from queue
    pg_split_ready.erase(p);
  }

  do_queries(query_map);
  do_infos(info_map);
  if (created)
    update_heartbeat_peers();

}

void OSD::split_pg(PG *parent, map<pg_t,PG*>& children, ObjectStore::Transaction &t)
{
  dout(10) << "split_pg " << *parent << dendl;
  pg_t parentid = parent->info.pgid;

  list<pobject_t> olist;
  store->collection_list(parent->info.pgid, olist);  

  while (!olist.empty()) {
    pobject_t poid = olist.front();
    olist.pop_front();
    
    ceph_object_layout l = osdmap->make_object_layout(poid.oid, parentid.type(), parentid.size(),
						      parentid.pool(), parentid.preferred());
    if (le64_to_cpu(l.ol_pgid) != parentid.u.pg64) {
      pg_t pgid(le64_to_cpu(l.ol_pgid));
      dout(20) << "  moving " << poid << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      eversion_t v;
      store->getattr(parentid, poid, "version", &v, sizeof(v));
      if (v > child->info.last_update) {
	child->info.last_update = v;
	dout(25) << "        tagging pg with v " << v << "  > " << child->info.last_update << dendl;
      } else {
	dout(25) << "    not tagging pg with v " << v << " <= " << child->info.last_update << dendl;
      }
      t.collection_add(pgid, parentid, poid);
      t.collection_remove(parentid, poid);
    } else {
      dout(20) << " leaving " << poid << "   in " << parentid << dendl;
    }
  }
}  


/*
 * holding osd_lock
 */
void OSD::handle_pg_create(MOSDPGCreate *m)
{
  dout(10) << "handle_pg_create " << *m << dendl;

  if (!require_same_or_newer_map(m, m->epoch)) return;

  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  ObjectStore::Transaction t;
  int created = 0;

  for (map<pg_t,MOSDPGCreate::create_rec>::iterator p = m->mkpg.begin();
       p != m->mkpg.end();
       p++) {
    pg_t pgid = p->first;
    epoch_t created = p->second.created;
    pg_t parent = p->second.parent;
    int split_bits = p->second.split_bits;
    pg_t on = pgid;

    if (split_bits) {
      on = parent;
      dout(20) << "mkpg " << pgid << " e" << created << " from parent " << parent
	       << " split by " << split_bits << " bits" << dendl;
    } else {
      dout(20) << "mkpg " << pgid << " e" << created << dendl;
    }
   
    // is it still ours?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(on, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }

    // does it already exist?
    if (_have_pg(pgid)) {
      dout(10) << "mkpg " << pgid << "  already exists, skipping" << dendl;
      continue;
    }

    // does parent exist?
    if (split_bits && !_have_pg(parent)) {
      dout(10) << "mkpg " << pgid << "  missing parent " << parent << ", skipping" << dendl;
      continue;
    }

    // figure history
    PG::Info::History history;
    project_pg_history(pgid, history, created, acting);
    
    // register.
    creating_pgs[pgid].created = created;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].split_bits = split_bits;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_primary_since, 
		       creating_pgs[pgid].prior);

    // poll priors
    set<int>& pset = creating_pgs[pgid].prior;
    dout(10) << "mkpg " << pgid << " e" << created
	     << " h " << history
	     << " : querying priors " << pset << dendl;
    for (set<int>::iterator p = pset.begin(); p != pset.end(); p++) 
      if (osdmap->is_up(*p))
	query_map[*p][pgid] = PG::Query(PG::Query::INFO, history);
    
    PG *pg = try_create_pg(pgid, t);
    if (pg) {
      created++;
      wake_pg_waiters(pg->info.pgid);
      pg->peer(t, query_map, &info_map);
      pg->update_stats();
      pg->unlock();
    }
  }

  store->apply_transaction(t);

  do_queries(query_map);
  do_infos(info_map);

  kick_pg_split_queue();
  if (created)
    update_heartbeat_peers();
  delete m;
}


// ----------------------------------------
// peering and recovery

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(map< int, list<PG::Info> >& notify_list) 
{
  for (map< int, list<PG::Info> >::iterator it = notify_list.begin();
       it != notify_list.end();
       it++) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd" << it->first << " is self, skipping" << dendl;
      continue;
    }
    dout(7) << "do_notify osd" << it->first << " on " << it->second.size() << " PGs" << dendl;
    MOSDPGNotify *m = new MOSDPGNotify(osdmap->get_epoch(), it->second);
    _share_map_outgoing(osdmap->get_inst(it->first));
    messenger->send_message(m, osdmap->get_inst(it->first));
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,PG::Query> >& query_map)
{
  for (map< int, map<pg_t,PG::Query> >::iterator pit = query_map.begin();
       pit != query_map.end();
       pit++) {
    int who = pit->first;
    dout(7) << "do_queries querying osd" << who
            << " on " << pit->second.size() << " PGs" << dendl;
    MOSDPGQuery *m = new MOSDPGQuery(osdmap->get_epoch(), pit->second);
    _share_map_outgoing(osdmap->get_inst(who));
    messenger->send_message(m, osdmap->get_inst(who));
  }
}


void OSD::do_infos(map<int,MOSDPGInfo*>& info_map)
{
  for (map<int,MOSDPGInfo*>::iterator p = info_map.begin();
       p != info_map.end();
       ++p) 
    messenger->send_message(p->second, osdmap->get_inst(p->first));
  info_map.clear();
}


/** PGNotify
 * from non-primary to primary
 * includes PG::Info.
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  dout(7) << "handle_pg_notify from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  ObjectStore::Transaction t;
  
  // look for unknown PGs i'm primary for
  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  int created = 0;

  for (list<PG::Info>::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       it++) {
    pg_t pgid = it->pgid;
    PG *pg = 0;

    if (!_have_pg(pgid)) {
      // same primary?
      vector<int> acting;
      int nrep = osdmap->pg_to_acting_osds(pgid, acting);
      int role = osdmap->calc_pg_role(whoami, acting, nrep);

      PG::Info::History history = it->history;
      project_pg_history(pgid, history, m->get_epoch(), acting);

      if (m->get_epoch() < history.same_primary_since) {
        dout(10) << "handle_pg_notify pg " << pgid << " dne, and primary changed in "
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << dendl;
        continue;
      }

      assert(role == 0);  // otherwise, probably bug in project_pg_history.
      
      // DNE on source?
      if (it->dne()) {  
	// is there a creation pending on this pg?
	if (creating_pgs.count(pgid)) {
	  creating_pgs[pgid].prior.erase(from);

	  pg = try_create_pg(pgid, t);
	  if (!pg) 
	    continue;
	} else {
	  dout(10) << "handle_pg_notify pg " << pgid
		   << " DNE on source, but creation probe, ignoring" << dendl;
	  continue;
	}
      }
      creating_pgs.erase(pgid);

      // ok, create PG locally using provided Info and History
      if (!pg) {
	pg = _create_lock_pg(pgid, t);
	pg->acting.swap(acting);
	pg->set_role(role);
	pg->info.history = history;
	pg->clear_primary_state();  // yep, notably, set hml=false
	pg->build_prior();      
	pg->write_log(t);
      }
      
      created++;
      dout(10) << *pg << " is new" << dendl;
    
      // kick any waiters
      wake_pg_waiters(pg->info.pgid);
    } else {
      // already had it.  am i (still) the primary?
      pg = _lookup_lock_pg(pgid);
      if (m->get_epoch() < pg->info.history.same_primary_since) {
        dout(10) << *pg << " handle_pg_notify primary changed in "
                 << pg->info.history.same_primary_since
                 << " (msg from " << m->get_epoch() << ")" << dendl;
        pg->unlock();
        continue;
      }
    }

    // ok!
    dout(10) << *pg << " got osd" << from << " info " << *it << dendl;
    pg->info.history.merge(it->history);

    // save info.
    bool had = pg->peer_info.count(from);
    pg->peer_info[from] = *it;

    // stray?
    bool acting = pg->is_acting(from);
    if (!acting) {
      dout(10) << *pg << " osd" << from << " has stray content: " << *it << dendl;
      pg->stray_set.insert(from);
      pg->state_clear(PG_STATE_CLEAN);
    }

    if (had) {
      if (pg->is_active() && 
          (*it).is_uptodate() && 
	  acting) {
        pg->uptodate_set.insert(from);
        dout(10) << *pg << " osd" << from << " now uptodate (" << pg->uptodate_set  
                 << "): " << *it << dendl;
      } else {
        // hmm, maybe keep an eye out for cases where we see this, but peer should happen.
        dout(10) << *pg << " already had notify info from osd" << from << ": " << *it << dendl;
      }
      if (pg->is_all_uptodate()) 
	pg->finish_recovery();
    } else {
      pg->build_prior();
      pg->peer(t, query_map, &info_map);
    }
    pg->update_stats();
    pg->unlock();
  }
  
  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  do_queries(query_map);
  do_infos(info_map);
  
  kick_pg_split_queue();

  if (created)
    update_heartbeat_peers();

  delete m;
}



/** PGLog
 * from non-primary to primary
 *  includes log and info
 * from primary to non-primary
 *  includes log for use in recovery
 * NOTE: called with opqueue active.
 */

void OSD::_process_pg_info(epoch_t epoch, int from,
			   PG::Info &info, 
			   PG::Log &log, 
			   PG::Missing &missing,
			   map<int, MOSDPGInfo*>* info_map,
			   int& created)
{
  ObjectStore::Transaction t;

  PG *pg = 0;
  if (!_have_pg(info.pgid)) {
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(info.pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);

    project_pg_history(info.pgid, info.history, epoch, acting);
    if (epoch < info.history.same_since) {
      dout(10) << "got old info " << info << " on non-existent pg, ignoring" << dendl;
      return;
    }

    // create pg!
    assert(role != 0);
    pg = _create_lock_pg(info.pgid, t);
    dout(10) << " got info on new pg, creating" << dendl;
    pg->acting.swap(acting);
    pg->set_role(role);
    pg->info.history = info.history;
    pg->write_log(t);
    store->apply_transaction(t);
    created++;
  } else {
    pg = _lookup_lock_pg(info.pgid);
    if (epoch < pg->info.history.same_since) {
      dout(10) << *pg << " got old info " << info << ", ignoring" << dendl;
      pg->unlock();
      return;
    }
  }
  assert(pg);

  dout(10) << *pg << " got " << info << " " << log << " " << missing << dendl;
  pg->info.history.merge(info.history);

  //m->log.print(cout);

  if (pg->is_primary()) {
    // i am PRIMARY
    assert(pg->peer_log_requested.count(from) ||
           pg->peer_summary_requested.count(from));
    
    pg->proc_replica_log(log, missing, from);

    // peer
    map< int, map<pg_t,PG::Query> > query_map;
    pg->peer(t, query_map, info_map);
    pg->update_stats();
    do_queries(query_map);

  } else {
    if (!pg->info.dne()) {
      // i am REPLICA
      // merge log
      pg->merge_log(log, missing, from);
      pg->proc_missing(log, missing, from);
      
      // ok activate!
      pg->activate(t, info_map);
    }
  }

  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  pg->unlock();
}


void OSD::handle_pg_log(MOSDPGLog *m) 
{
  dout(7) << "handle_pg_log " << *m << " from " << m->get_source() << dendl;

  int from = m->get_source().num();
  int created = 0;
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  _process_pg_info(m->get_epoch(), from, 
		   m->info, m->log, m->missing, 0,
		   created);
  if (created)
    update_heartbeat_peers();

  delete m;
}

void OSD::handle_pg_info(MOSDPGInfo *m)
{
  dout(7) << "handle_pg_info " << *m << " from " << m->get_source() << dendl;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  PG::Log empty_log;
  PG::Missing empty_missing;
  map<int,MOSDPGInfo*> info_map;
  int created = 0;

  for (list<PG::Info>::iterator p = m->pg_info.begin();
       p != m->pg_info.end();
       ++p) 
    _process_pg_info(m->get_epoch(), from, *p, empty_log, empty_missing, &info_map, created);

  do_infos(info_map);
  if (created)
    update_heartbeat_peers();

  delete m;
}


/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_query(MOSDPGQuery *m) 
{
  assert(osd_lock.is_locked());

  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  int created = 0;
  map< int, list<PG::Info> > notify_list;
  
  for (map<pg_t,PG::Query>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = 0;

    if (pg_map.count(pgid) == 0) {
      // get active crush mapping
      vector<int> acting;
      int nrep = osdmap->pg_to_acting_osds(pgid, acting);
      int role = osdmap->calc_pg_role(whoami, acting, nrep);

      // same primary?
      PG::Info::History history = it->second.history;
      project_pg_history(pgid, history, m->get_epoch(), acting);

      if (m->get_epoch() < history.same_since) {
        dout(10) << " pg " << pgid << " dne, and pg has changed in "
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << dendl;
        continue;
      }

      if (role < 0) {
        dout(10) << " pg " << pgid << " dne, and i am not an active replica" << dendl;
        PG::Info empty(pgid);
        notify_list[from].push_back(empty);
        continue;
      }
      assert(role > 0);

      ObjectStore::Transaction t;
      pg = _create_lock_pg(pgid, t);
      pg->acting.swap( acting );
      pg->set_role(role);
      pg->info.history = history;
      pg->write_log(t);
      store->apply_transaction(t);
      created++;

      dout(10) << *pg << " dne (before), but i am role " << role << dendl;
    } else {
      pg = _lookup_lock_pg(pgid);
      
      // same primary?
      if (m->get_epoch() < pg->info.history.same_since) {
        dout(10) << *pg << " handle_pg_query primary changed in "
                 << pg->info.history.same_since
                 << " (msg from " << m->get_epoch() << ")" << dendl;
	pg->unlock();
        continue;
      }
    }

    pg->info.history.merge(it->second.history);

    // ok, process query!
    assert(!pg->acting.empty());
    assert(from == pg->acting[0]);

    if (it->second.type == PG::Query::INFO) {
      // info
      dout(10) << *pg << " sending info" << dendl;
      notify_list[from].push_back(pg->info);
    } else {
      MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->info);
      m->missing = pg->missing;

      if (it->second.type == PG::Query::LOG) {
        dout(10) << *pg << " sending info+missing+log since split " << it->second.split
                 << " from floor " << it->second.floor 
                 << dendl;
        if (!m->log.copy_after_unless_divergent(pg->log, it->second.split, it->second.floor)) {
          dout(10) << *pg << "  divergent, sending backlog" << dendl;
          it->second.type = PG::Query::BACKLOG;
        }
      }

      if (it->second.type == PG::Query::BACKLOG) {
        dout(10) << *pg << " sending info+missing+backlog" << dendl;
        if (pg->log.backlog) {
          m->log = pg->log;
        } else {
          pg->generate_backlog();
          m->log = pg->log;
          pg->drop_backlog();
        }
      } 
      else if (it->second.type == PG::Query::FULLLOG) {
        dout(10) << *pg << " sending info+missing+full log" << dendl;
        m->log.copy_non_backlog(pg->log);
      }

      dout(10) << *pg << " sending " << m->log << " " << m->missing << dendl;
      //m->log.print(cout);

      _share_map_outgoing(osdmap->get_inst(from));
      messenger->send_message(m, osdmap->get_inst(from));
    }    

    pg->unlock();
  }
  
  do_notifies(notify_list);   

  delete m;

  if (created)
    update_heartbeat_peers();
}


void OSD::handle_pg_remove(MOSDPGRemove *m)
{
  assert(osd_lock.is_locked());

  dout(7) << "handle_pg_remove from " << m->get_source() << dendl;
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  for (set<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = *it;
    PG *pg;

    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }

    pg = _lookup_lock_pg(pgid);
    if (pg->info.history.same_since <= m->get_epoch()) {
      dout(10) << *pg << " removing." << dendl;
      assert(pg->get_role() == -1);
      assert(pg->get_primary() == m->get_source().num());
      _remove_unlock_pg(pg);
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << pg->info.history.same_since << " > " << m->get_epoch() << dendl;
      pg->unlock();
    }
  }

  delete m;
}






// =========================================================
// OPS

void OSD::handle_op(MOSDOp *op)
{
  // throttle?  FIXME PROBABLY!
  while (pending_ops > g_conf.osd_max_opq) {
    dout(10) << "enqueue_op waiting for pending_ops " << pending_ops << " to drop to " << g_conf.osd_max_opq << dendl;
    op_queue_cond.Wait(osd_lock);
  }

  // calc actual pgid
  pg_t pgid = osdmap->raw_pg_to_pg(op->get_pg());

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid):0;

  logger->set("buf", buffer_total_alloc.test());

  utime_t now = g_clock.now();

  // update qlen stats
  stat_oprate.hit(now);
  stat_ops++;
  stat_qlen += pending_ops;
  if (op->get_op() == CEPH_OSD_OP_READ) {
    stat_rd_ops++;
    if (op->get_source().is_osd()) {
      //derr(-10) << "shed in " << stat_rd_ops_shed_in << " / " << stat_rd_ops << dendl;
      stat_rd_ops_shed_in++;
    }
  }

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) {
    if (pg) pg->unlock();
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());


  if (!op->get_source().is_osd()) {
    // REGULAR OP (non-replication)

    // note original source
    op->clear_payload();    // and hose encoded payload (in case we forward)

    // have pg?
    if (!pg) {
      dout(7) << "hit non-existent pg " 
              << pgid 
              << ", waiting" << dendl;
      waiting_for_pg[pgid].push_back(op);
      return;
    }

    // pg must be same-ish...
    if (op->is_read()) {
      // read
      if (!pg->same_for_read_since(op->get_map_epoch())) {
	dout(7) << "handle_rep_op pg changed " << pg->info.history
		<< " after " << op->get_map_epoch() 
		<< ", dropping" << dendl;
	assert(op->get_map_epoch() < osdmap->get_epoch());
	pg->unlock();
	delete op;
	return;
      }

      /*
    if (read && op->get_oid().rev > 0) {
    // versioned read.  hrm.
      // are we missing a revision that we might need?
      object_t moid = op->get_oid();
      if (pick_missing_object_rev(moid, pg)) {
	// is there a local revision we might use instead?
	object_t loid = op->get_oid();
	if (store->pick_object_revision_lt(loid) &&
	    moid <= loid) {
	  // we need moid.  pull it.
	  dout(10) << "handle_op read on " << op->get_oid()
		   << ", have " << loid
		   << ", but need missing " << moid
		   << ", pulling" << dendl;
	  pull(pg, moid);
	  pg->waiting_for_missing_object[moid].push_back(op);
	  return;
	} 
	  
	dout(10) << "handle_op read on " << op->get_oid()
		 << ", have " << loid
		 << ", don't need missing " << moid 
		 << dendl;
      }
    } else {
      // live revision.  easy.
      if (op->get_op() != OSD_OP_PUSH &&
	  waitfor_missing_object(op, pg)) return;
    }
      */

    } else {
      // modify
      if ((pg->get_primary() != whoami ||
	   !pg->same_for_modify_since(op->get_map_epoch()))) {
	dout(7) << "handle_rep_op pg changed " << pg->info.history
		<< " after " << op->get_map_epoch() 
		<< ", dropping" << dendl;
	assert(op->get_map_epoch() < osdmap->get_epoch());
	pg->unlock();
	delete op;
	return;
      }
    }
    
    // pg must be active.
    if (!pg->is_active()) {
      // replay?
      if (op->get_version().version > 0) {
        if (op->get_version() > pg->info.last_update) {
          dout(7) << *pg << " queueing replay at " << op->get_version()
                  << " for " << *op << dendl;
          pg->replay_queue[op->get_version()] = op;
	  pg->unlock();
          return;
        } else {
          dout(7) << *pg << " replay at " << op->get_version() << " <= " << pg->info.last_update 
                  << " for " << *op
                  << ", will queue for WRNOOP" << dendl;
        }
      }
      
      dout(7) << *pg << " not active (yet)" << dendl;
      pg->waiting_for_active.push_back(op);
      pg->unlock();
      return;
    }
    
    // missing object?
    if (pg->is_missing_object(op->get_oid())) {
      pg->wait_for_missing_object(op->get_oid(), op);
      pg->unlock();
      return;
    }

    dout(10) << "handle_op " << *op << " in " << *pg << dendl;

  } else {
    // REPLICATION OP (it's from another OSD)

    // have pg?
    if (!pg) {
      derr(-7) << "handle_rep_op " << *op 
               << " pgid " << pgid << " dne" << dendl;
      delete op;
      //assert(0); // wtf, shouldn't happen.
      return;
    }
    
    // check osd map: same set, or primary+acker?
    if (!pg->same_for_rep_modify_since(op->get_map_epoch())) {
      dout(10) << "handle_rep_op pg changed " << pg->info.history
               << " after " << op->get_map_epoch() 
               << ", dropping" << dendl;
      pg->unlock();
      delete op;
      return;
    }

    assert(pg->get_role() >= 0);
    dout(7) << "handle_rep_op " << op << " in " << *pg << dendl;
  }

  // proprocess op? 
  if (pg->preprocess_op(op, now)) {
    pg->unlock();
    return;
  }

  if (op->get_op() == CEPH_OSD_OP_READ) {
    Mutex::Locker lock(peer_stat_lock);
    stat_rd_ops_in_queue++;
  }

  if (g_conf.osd_maxthreads < 1) {
    // do it now.
    if (op->get_type() == CEPH_MSG_OSD_OP)
      pg->do_op((MOSDOp*)op);
    else if (op->get_type() == MSG_OSD_SUBOP)
      pg->do_sub_op((MOSDSubOp*)op);
    else if (op->get_type() == MSG_OSD_SUBOPREPLY)
      pg->do_sub_op_reply((MOSDSubOpReply*)op);
    else 
      assert(0);
  } else {
    // queue for worker threads
    enqueue_op(pg, op);         
  }
  
  pg->unlock();
}


void OSD::handle_sub_op(MOSDSubOp *op)
{
  dout(10) << "handle_sub_op " << *op << " epoch " << op->get_map_epoch() << dendl;
  if (op->get_map_epoch() < boot_epoch) {
    dout(3) << "replica op from before boot" << dendl;
    delete op;
    return;
  }

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());

  if (!_have_pg(pgid)) {
    // hmm.
    delete op;
    return;
  } 

  PG *pg = _lookup_lock_pg(pgid);
  if (g_conf.osd_maxthreads < 1) {
    pg->do_sub_op(op);    // do it now
  } else {
    enqueue_op(pg, op);     // queue for worker threads
  }
  pg->unlock();
}
void OSD::handle_sub_op_reply(MOSDSubOpReply *op)
{
  if (op->get_map_epoch() < boot_epoch) {
    dout(3) << "replica op reply from before boot" << dendl;
    delete op;
    return;
  }

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());

  if (!_have_pg(pgid)) {
    // hmm.
    delete op;
    return;
  } 

  PG *pg = _lookup_lock_pg(pgid);
  if (g_conf.osd_maxthreads < 1) {
    pg->do_sub_op_reply(op);    // do it now
  } else {
    enqueue_op(pg, op);     // queue for worker threads
  }
  pg->unlock();
}


/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(PG *pg, Message *op)
{
  dout(15) << *pg << " enqueue_op " << op << " " << *op << dendl;
  // add to pg's op_queue
  pg->op_queue.push_back(op);
  pending_ops++;
  logger->set("opq", pending_ops);
  
  // add pg to threadpool queue
  pg->get();   // we're exposing the pointer, here.
  threadpool->put_op(pg);
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(PG *pg)
{
  Message *op = 0;

  osd_lock.Lock();
  {
    // lock pg and get pending op
    pg->lock();

    assert(!pg->op_queue.empty());
    op = pg->op_queue.front();
    pg->op_queue.pop_front();
    
    dout(10) << "dequeue_op " << *op << " pg " << *pg
	     << ", " << (pending_ops-1) << " more pending"
	     << dendl;

    // share map?
    //  do this preemptively while we hold osd_lock and pg->lock
    //  to avoid lock ordering issues later.
    for (unsigned i=1; i<pg->acting.size(); i++) 
      _share_map_outgoing( osdmap->get_inst(pg->acting[i]) ); 
  }
  osd_lock.Unlock();

  // do it
  if (op->get_type() == CEPH_MSG_OSD_OP)
    pg->do_op((MOSDOp*)op); // do it now
  else if (op->get_type() == MSG_OSD_SUBOP)
    pg->do_sub_op((MOSDSubOp*)op);
  else if (op->get_type() == MSG_OSD_SUBOPREPLY)
    pg->do_sub_op_reply((MOSDSubOpReply*)op);
  else 
    assert(0);

  // unlock and put pg
  pg->put_unlock();
  
  // finish
  osd_lock.Lock();
  {
    dout(10) << "dequeue_op " << op << " finish" << dendl;
    assert(pending_ops > 0);
    
    if (pending_ops > g_conf.osd_max_opq) 
      op_queue_cond.Signal();
    
    pending_ops--;
    logger->set("opq", pending_ops);
    if (pending_ops == 0 && waiting_for_no_ops)
      no_pending_ops.Signal();
  }
  osd_lock.Unlock();
}




void OSD::wait_for_no_ops()
{
  if (pending_ops > 0) {
    dout(7) << "wait_for_no_ops - waiting for " << pending_ops << dendl;
    waiting_for_no_ops = true;
    while (pending_ops > 0)
      no_pending_ops.Wait(osd_lock);
    waiting_for_no_ops = false;
    assert(pending_ops == 0);
  } 
  dout(7) << "wait_for_no_ops - none" << dendl;
}




