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

#ifdef USE_OSBDB
#include "osbdb/OSBDB.h"
#endif // USE_OSBDB


#include "ReplicatedPG.h"
//#include "RAID4PG.h"

#include "Ager.h"


#include "msg/Messenger.h"
#include "msg/Message.h"

#include "mon/MonClient.h"

#include "messages/MLog.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"
#include "messages/MOSDPGTemp.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"

#include "messages/MOSDAlive.h"

#include "messages/MOSDScrub.h"

#include "messages/MMonCommand.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MClass.h"

#include "common/Logger.h"
#include "common/LogType.h"
#include "common/Timer.h"
#include "common/LogClient.h"

#include "common/ClassHandler.h"

#include <iostream>
#include <errno.h>
#include <sys/stat.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "objclass/objclass.h"

#include "config.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix _prefix(*_dout, whoami, osdmap)

static ostream& _prefix(ostream& out, int whoami, OSDMap *osdmap) {
  return out << dbeginl << pthread_self()
	     << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " ";
}


ObjectStore *OSD::create_object_store(const char *dev, const char *jdev)
{
  struct stat st;
  if (::stat(dev, &st) != 0)
    return 0;

  //if (g_conf.ebofs) 
  //return new Ebofs(dev, jdev);
  if (g_conf.filestore)
    return new FileStore(dev, jdev);

  if (S_ISDIR(st.st_mode))
    return new FileStore(dev, jdev);
  else
    return 0;
  //return new Ebofs(dev, jdev);
}


int OSD::mkfs(const char *dev, const char *jdev, ceph_fsid_t fsid, int whoami)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  int err = store->mkfs();    
  if (err < 0) return err;
  err = store->mount();
  if (err < 0) return err;
    
  OSDSuperblock sb;
  sb.magic = CEPH_OSD_ONDISK_MAGIC;
  sb.fsid = fsid;
  sb.whoami = whoami;

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
    object_t oid("disk_bw_test");
    for (int i=0; i<1000; i++) {
      ObjectStore::Transaction t;
      t.write(meta_coll, sobject_t(oid, 0), i*bl.length(), bl.length(), bl);
      store->apply_transaction(t);
    }
    store->sync();
    utime_t end = g_clock.now();
    end -= start;
    cout << "measured " << (1000.0 / (double)end) << " mb/sec" << std::endl;
    ObjectStore::Transaction tr;
    tr.remove(meta_coll, sobject_t(oid, 0));
    store->apply_transaction(tr);
    
    // set osd weight
    sb.weight = (1000.0 / (double)end);
  }

  bufferlist bl;
  ::encode(sb, bl);

  ObjectStore::Transaction t;
  t.create_collection(meta_coll);
  t.write(meta_coll, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
  int r = store->apply_transaction(t);
  store->umount();
  delete store;
  return r;
}

int OSD::peek_super(const char *dev, nstring& magic, ceph_fsid_t& fsid, int& whoami)
{
  ObjectStore *store = create_object_store(dev, NULL);
  if (!store)
	return -ENODEV;
  int err = store->mount();
  if (err < 0) 
    return err;

  OSDSuperblock sb;
  bufferlist bl;
  err = store->read(meta_coll, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  store->umount();
  delete store;

  if (err < 0) 
    return -ENOENT;

  bufferlist::iterator p = bl.begin();
  ::decode(sb, p);

  magic = sb.magic;
  fsid = sb.fsid;
  whoami = sb.whoami;
  return 0;
}




// cons/des

OSD::OSD(int id, Messenger *m, Messenger *hbm, MonClient *mc, const char *dev, const char *jdev) : 
  osd_lock("OSD::osd_lock"),
  timer(osd_lock),
  messenger(m),
  monc(mc),
  logger(NULL),
  store(NULL),
  logclient(messenger, &mc->monmap),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  state(STATE_BOOTING), boot_epoch(0), up_epoch(0),
  op_tp("OSD::op_tp", g_conf.osd_maxthreads),
  recovery_tp("OSD::recovery_tp", 1),
  disk_tp("OSD::disk_tp", 2),
  heartbeat_lock("OSD::heartbeat_lock"),
  heartbeat_stop(false), heartbeat_epoch(0),
  heartbeat_messenger(hbm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  stat_oprate(5.0),
  peer_stat_lock("OSD::peer_stat_lock"),
  read_latency_calc(g_conf.osd_max_opq<1 ? 1:g_conf.osd_max_opq),
  qlen_calc(3),
  iat_averager(g_conf.osd_flash_crowd_iat_alpha),
  finished_lock("OSD::finished_lock"),
  op_wq(this, &op_tp),
  osdmap(NULL),
  map_lock("OSD::map_lock"),
  map_cache_lock("OSD::map_cache_lock"),
  class_lock("OSD::class_lock"),
  up_thru_wanted(0), up_thru_pending(0),
  pg_stat_queue_lock("OSD::pg_stat_queue_lock"),
  last_tid(0),
  tid_lock("OSD::tid_lock"),
  backlog_wq(this, &disk_tp),
  recovery_ops_active(0),
  recovery_wq(this, &recovery_tp),
  remove_list_lock("OSD::remove_list_lock"),
  replay_queue_lock("OSD::replay_queue_lock"),
  snap_trim_wq(this, &disk_tp),
  scrub_wq(this, &disk_tp),
  remove_wq(this, &disk_tp)
{
  monc->set_messenger(messenger);
  
  osdmap = 0;

  memset(&my_stat, 0, sizeof(my_stat));

  osd_stat_updated = osd_stat_pending = false;

  stat_ops = 0;
  stat_qlen = 0;
  stat_rd_ops = stat_rd_ops_shed_in = stat_rd_ops_shed_out = 0;
  stat_rd_ops_in_queue = 0;

  pending_ops = 0;
  waiting_for_no_ops = false;
}

OSD::~OSD()
{
  delete class_handler;
  delete osdmap;
  delete logger;
  delete store;
  if (messenger)
    messenger->destroy();
  if (heartbeat_messenger)
    heartbeat_messenger->destroy();
}

bool got_sigterm = false;

void handle_signal(int signal)
{
  switch (signal) {
  case SIGTERM:
  case SIGINT:
    got_sigterm = true;
    break;
  }
}

void cls_initialize(OSD *_osd);

int OSD::init()
{
  Mutex::Locker lock(osd_lock);

  // mount.
  dout(2) << "mounting " << dev_path << " " << (journal_path ? journal_path : "(no journal)") << dendl;
  store = create_object_store(dev_path, journal_path);
  if (!store) {
    dout(0) << " unable to create object store" << dendl;
    return -ENODEV;
  }
  int r = store->mount();
  if (r < 0) {
    dout(0) << " unable to mount object store" << dendl;
    return -1;
  }
  
  dout(2) << "boot" << dendl;
  
  // read superblock
  if (read_superblock() < 0) {
    dout(0) << " unable to read osd superblock" << dendl;
    store->umount();
    delete store;
    return -1;
  }

  class_handler = new ClassHandler(this);
  assert(class_handler);
  cls_initialize(this);

  // load up "current" osdmap
  assert_warn(!osdmap);
  if (osdmap) {
    dout(0) << " unable to read current osdmap" << dendl;
    return -1;
  }
  osdmap = new OSDMap;
  if (superblock.current_epoch) {
    bufferlist bl;
    get_map_bl(superblock.current_epoch, bl);
    osdmap->decode(bl);
  }

  // load up pgs (as they previously existed)
  load_pgs();
  
  dout(2) << "superblock: i am osd" << superblock.whoami << dendl;
  assert_warn(whoami == superblock.whoami);
  if (whoami != superblock.whoami) {
    dout(0) << "wtf, superblock says osd" << superblock.whoami << " but i am osd" << whoami << dendl;
    return -EINVAL;
  }
    
  // log
  static LogType osd_logtype(l_osd_first, l_osd_last);
  char name[80];
  sprintf(name, "osd%d", whoami);
  logger = new Logger(name, (LogType*)&osd_logtype);
  osd_logtype.add_set(l_osd_opq, "opq");
  osd_logtype.add_inc(l_osd_op, "op");
  osd_logtype.add_inc(l_osd_c_rd, "c_rd");
  osd_logtype.add_inc(l_osd_c_rdb, "c_rdb");
  osd_logtype.add_inc(l_osd_c_wr, "c_wr");
  osd_logtype.add_inc(l_osd_c_wrb,"c_wrb");
  
  osd_logtype.add_inc(l_osd_r_wr, "r_wr");
  osd_logtype.add_inc(l_osd_r_wrb, "r_wrb");

  osd_logtype.add_inc(l_osd_subop, "subop");

  osd_logtype.add_inc(l_osd_rop, "rop");
  osd_logtype.add_inc(l_osd_r_push, "r_push");
  osd_logtype.add_inc(l_osd_r_pushb, "r_pushb");
  osd_logtype.add_inc(l_osd_r_pull, "r_pull");
  osd_logtype.add_inc(l_osd_r_pullb, "r_pullb");
  
  osd_logtype.add_set(l_osd_qlen, "qlen");
  osd_logtype.add_set(l_osd_rqlen, "rqlen");
  osd_logtype.add_set(l_osd_rdlat, "rdlat");
  osd_logtype.add_set(l_osd_rdlatm, "rdlatm");
  osd_logtype.add_set(l_osd_fshdin, "fshdin");
  osd_logtype.add_set(l_osd_fshdout, "fshdout");
  osd_logtype.add_inc(l_osd_shdout, "shdout");
  osd_logtype.add_inc(l_osd_shdin, "shdin");

  osd_logtype.add_set(l_osd_loadavg, "loadavg");

  osd_logtype.add_inc(l_osd_rlsum, "rlsum");
  osd_logtype.add_inc(l_osd_rlnum, "rlnum");

  osd_logtype.add_set(l_osd_numpg, "numpg");
  osd_logtype.add_set(l_osd_hbto, "hbto");
  osd_logtype.add_set(l_osd_hbfrom, "hbfrom");
  
  osd_logtype.add_set(l_osd_buf, "buf");
  
  osd_logtype.add_inc(l_osd_map, "map");
  osd_logtype.add_inc(l_osd_mapi, "mapi");
  osd_logtype.add_inc(l_osd_mapidup, "mapidup");
  osd_logtype.add_inc(l_osd_mapf, "mapf");
  osd_logtype.add_inc(l_osd_mapfdup, "mapfdup");

  osd_logtype.validate();

  
  // i'm ready!
  messenger->add_dispatcher_head(this);
  messenger->add_dispatcher_head(&logclient);

  heartbeat_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  
  monc->init();

  monc->sub_want("monmap", 0);
  monc->renew_subs();  

  op_tp.start();
  recovery_tp.start();
  disk_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  timer.add_event_after(g_conf.osd_heartbeat_interval, new C_Tick(this));

  signal(SIGTERM, handle_signal);
  signal(SIGINT, handle_signal);

  return 0;
}

int OSD::shutdown()
{
  g_conf.debug_osd = 100;
  g_conf.debug_journal = 100;
  g_conf.debug_filestore = 100;
  g_conf.debug_ebofs = 100;
  g_conf.debug_ms = 100;
  
  dout(1) << "shutdown" << dendl;

  state = STATE_STOPPING;

  // cancel timers
  timer.cancel_all();
  timer.join();

  heartbeat_lock.Lock();
  heartbeat_stop = true;
  heartbeat_cond.Signal();
  heartbeat_lock.Unlock();
  heartbeat_thread.join();

  // finish ops
  wait_for_no_ops();
  dout(10) << "no ops" << dendl;

  recovery_tp.stop();
  dout(10) << "recovery tp stopped" << dendl;
  op_tp.stop();
  dout(10) << "op tp stopped" << dendl;

  // pause _new_ disk work first (to avoid racing with thread pool),
  disk_tp.pause_new();
  dout(10) << "disk tp paused (new), kicking all pgs" << dendl;

  // then kick all pgs,
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    p->second->kick();
    p->second->unlock();
  }
  dout(20) << " kicked all pgs" << dendl;

  // then stop thread.
  disk_tp.stop();
  dout(10) << "disk tp stopped" << dendl;

  // tell pgs we're shutting down
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    p->second->lock();
    p->second->on_shutdown();
    p->second->unlock();
  }

  // zap waiters (bleh, this is messy)
  finished_lock.Lock();
  finished.clear();
  finished_lock.Unlock();

  // note unmount epoch
  dout(10) << "noting clean unmount in epoch " << osdmap->get_epoch() << dendl;
  superblock.mounted = boot_epoch;
  superblock.clean_thru = osdmap->get_epoch();
  ObjectStore::Transaction t;
  write_superblock(t);
  int r = store->apply_transaction(t);
  if (r) {
    char buf[80];
    dout(0) << "error writing superblock " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
  }

  // flush data to disk
  osd_lock.Unlock();
  dout(10) << "sync" << dendl;
  store->sync();
  r = store->umount();
  delete store;
  store = 0;
  dout(10) << "sync done" << dendl;
  osd_lock.Lock();

  clear_pg_stat_queue();

  // close pgs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    PG *pg = p->second;
    pg->put();
  }
  pg_map.clear();

  messenger->shutdown();
  if (heartbeat_messenger)
    heartbeat_messenger->shutdown();

  monc->shutdown();

  return r;
}



void OSD::write_superblock(ObjectStore::Transaction& t)
{
  dout(10) << "write_superblock " << superblock << dendl;

  bufferlist bl;
  ::encode(superblock, bl);
  t.write(meta_coll, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(meta_coll, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  ::decode(superblock, p);

  dout(10) << "read_superblock " << superblock << dendl;

  if (ceph_fsid_compare(&superblock.fsid, &monc->get_fsid())) {
    derr(0) << "read_superblock fsid " << superblock.fsid << " != monmap " << monc->get_fsid() << dendl;
    return -1;
  }

  if (whoami != superblock.whoami) {
    derr(0) << "read_superblock superblock says osd" << superblock.whoami
	    << ", but i (think i) am osd" << whoami << dendl;
    return -1;
  }
  
  return 0;
}





// ======================================================
// PG's

PGPool *OSD::_lookup_pool(int id)
{
  if (pool_map.count(id))
    return pool_map[id];
  return 0;
}

PGPool* OSD::_get_pool(int id)
{
  PGPool *p = _lookup_pool(id);
  if (!p) {
    p = new PGPool(id);
    pool_map[id] = p;
    p->get();
    
    const pg_pool_t& pi = osdmap->get_pg_pool(id);
    p->info = pi;
    p->snapc = pi.get_snap_context();

    pi.build_removed_snaps(p->removed_snaps);
  }
  dout(10) << "_get_pool " << p->id << " " << p->num_pg << " -> " << (p->num_pg+1) << dendl;
  p->num_pg++;
  return p;
}

void OSD::_put_pool(int id)
{
  PGPool *p = _lookup_pool(id);
  dout(10) << "_put_pool " << id << " " << p->num_pg << " -> " << (p->num_pg-1) << dendl;
  p->num_pg--;
  if (!p->num_pg) {
    pool_map.erase(id);
    p->put();
  }
}


PG *OSD::_open_lock_pg(pg_t pgid, bool no_lockdep_check)
{
  assert(osd_lock.is_locked());

  PGPool *pool = _get_pool(pgid.pool());

  // create
  PG *pg;
  sobject_t logoid = make_pg_log_oid(pgid);
  if (osdmap->get_pg_type(pgid) == CEPH_PG_TYPE_REP)
    pg = new ReplicatedPG(this, pool, pgid, logoid);
  //else if (pgid.is_raid4())
  //pg = new RAID4PG(this, pgid);
  else 
    assert(0);

  assert(pg_map.count(pgid) == 0);
  pg_map[pgid] = pg;

  pg->lock(no_lockdep_check); // always lock.
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
  assert(!store->collection_exists(coll_t::build_pg_coll(pgid)));
  t.create_collection(coll_t::build_pg_coll(pgid));

  pg->write_info(t);
  pg->write_log(t);

  return pg;
}

PG *OSD::_create_lock_new_pg(pg_t pgid, vector<int>& acting, ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(20) << "_create_lock_new_pg pgid " << pgid << " -> " << acting << dendl;
  assert(whoami == acting[0]);
  assert(pg_map.count(pgid) == 0);

  PG *pg = _open_lock_pg(pgid, true);

  assert(!store->collection_exists(coll_t::build_pg_coll(pgid)));
  t.create_collection(coll_t::build_pg_coll(pgid));

  pg->set_role(0);
  pg->acting.swap(acting);
  pg->up = pg->acting;
  pg->info.history.epoch_created = 
    pg->info.history.last_epoch_started =
    pg->info.history.same_up_since =
    pg->info.history.same_acting_since =
    pg->info.history.same_primary_since = osdmap->get_epoch();

  pg->write_info(t);
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

void OSD::load_pgs()
{
  assert(osd_lock.is_locked());
  dout(10) << "load_pgs" << dendl;
  assert(pg_map.empty());

  vector<coll_t> ls;
  store->list_collections(ls);

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    if (*it == meta_coll)
      continue;
    if (it->snap != CEPH_NOSNAP)
      continue;
    pg_t pgid = it->pgid;
    PG *pg = _open_lock_pg(pgid);

    // read pg state, log
    pg->read_state(store);

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
    OSDMap *oldmap = get_map(e);
    vector<int> acting;
    oldmap->pg_to_acting_osds(pgid, acting);
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
			     vector<int>& lastup, vector<int>& lastacting)
{
  dout(15) << "project_pg_history " << pgid
           << " from " << from << " to " << osdmap->get_epoch()
           << ", start " << h
           << dendl;

  for (epoch_t e = osdmap->get_epoch();
       e > from;
       e--) {
    // verify during intermediate epoch (e-1)
    OSDMap *oldmap = get_map(e-1);

    vector<int> up, acting;
    oldmap->pg_to_up_acting_osds(pgid, up, acting);

    // acting set change?
    if (acting != lastacting && e > h.same_acting_since) {
      dout(15) << "project_pg_history " << pgid << " changed in " << e 
                << " from " << acting << " -> " << lastacting << dendl;
      h.same_acting_since = e;
    }
    // up set change?
    if (up != lastup && e > h.same_up_since) {
      dout(15) << "project_pg_history " << pgid << " changed in " << e 
                << " from " << up << " -> " << lastup << dendl;
      h.same_up_since = e;
    }

    // primary change?
    if (!(!acting.empty() && !lastacting.empty() && acting[0] == lastacting[0]) &&
        e > h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e << dendl;
      h.same_primary_since = e;
    }

    if (h.same_acting_since >= e && h.same_up_since >= e && h.same_primary_since >= e)
      break;
  }

  dout(15) << "project_pg_history end " << h << dendl;
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

    logger->fset(l_osd_qlen, my_stat.qlen);
    logger->fset(l_osd_rqlen, my_stat.recent_qlen);
    logger->fset(l_osd_rdlat, my_stat.read_latency);
    logger->fset(l_osd_rdlatm, my_stat.read_latency_mine);
    logger->fset(l_osd_fshdin, my_stat.frac_rd_ops_shed_in);
    logger->fset(l_osd_fshdout, my_stat.frac_rd_ops_shed_out);
    dout(30) << "_refresh_my_stat " << my_stat << dendl;

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
  heartbeat_lock.Lock();

  // filter heartbeat_from_stamp to only include osds that remain in
  // heartbeat_from.
  map<int, utime_t> stamps;
  stamps.swap(heartbeat_from_stamp);

  map<int, epoch_t> old_to, old_from;
  map<int, entity_inst_t> old_inst;
  old_to.swap(heartbeat_to);
  old_from.swap(heartbeat_from);
  old_inst.swap(heartbeat_inst);

  // build heartbeat to/from set
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;

    // replicas ping primary.
    if (pg->get_role() > 0) {
      assert(pg->acting.size() > 1);
      heartbeat_to[pg->acting[0]] = osdmap->get_epoch();
      heartbeat_inst[pg->acting[0]] = osdmap->get_hb_inst(pg->acting[0]);
    }
    else if (pg->get_role() == 0) {
      assert(pg->acting[0] == whoami);
      for (unsigned i=1; i<pg->acting.size(); i++) {
	int p = pg->acting[i]; // peer
	assert(p != whoami);
	heartbeat_from[p] = osdmap->get_epoch();
	heartbeat_inst[p] = osdmap->get_hb_inst(p);
	if (stamps.count(p) && old_from.count(p))  // have a stamp _AND_ i'm not new to the set
	  heartbeat_from_stamp[p] = stamps[p];
      }
    }
  }
  for (map<int,epoch_t>::iterator p = old_to.begin();
       p != old_to.end();
       p++) {
    if (p->second > osdmap->get_epoch()) {
      dout(10) << " keeping newer _to peer " << old_inst[p->first] << " as of " << p->second << dendl;
      heartbeat_to[p->first] = p->second;
      heartbeat_inst[p->first] = old_inst[p->first];
    } else if (p->second < osdmap->get_epoch() &&
	       (!osdmap->is_up(p->first) ||
		osdmap->get_hb_inst(p->first) != old_inst[p->first])) {
      dout(10) << " marking down old _to peer " << old_inst[p->first] << " as of " << p->second << dendl;      
      messenger->mark_down(old_inst[p->first].addr);
    }
  }

  heartbeat_epoch = osdmap->get_epoch();

  dout(10) << "hb   to: " << heartbeat_to << dendl;
  dout(10) << "hb from: " << heartbeat_from << dendl;

  heartbeat_lock.Unlock();
}

void OSD::reset_heartbeat_peers()
{
  dout(10) << "reset_heartbeat_peers" << dendl;
  heartbeat_lock.Lock();
  heartbeat_to.clear();
  heartbeat_from.clear();
  heartbeat_from_stamp.clear();
  heartbeat_inst.clear();
  heartbeat_lock.Unlock();
}

void OSD::handle_osd_ping(MOSDPing *m)
{
  dout(20) << "handle_osd_ping from " << m->get_source() << " got stat " << m->peer_stat << dendl;

  if (!is_active()) {
    dout(10) << "handle_osd_ping - not active" << dendl;
    delete m;
    return;
  }

  if (ceph_fsid_compare(&superblock.fsid, &m->fsid)) {
    dout(20) << "handle_osd_ping from " << m->get_source()
	     << " bad fsid " << m->fsid << " != " << superblock.fsid << dendl;
    delete m;
    return;
  }

  heartbeat_lock.Lock();
  int from = m->get_source().num();

  bool locked = map_lock.try_get_read();

  if (m->ack) {
    dout(5) << " peer " << m->get_source_inst() << " requesting heartbeats" << dendl;
    heartbeat_to[from] = m->peer_as_of_epoch;
    heartbeat_inst[from] = m->get_source_inst();

    if (locked && m->map_epoch)
      _share_map_incoming(m->get_source_inst(), m->map_epoch);
  }

  if (heartbeat_from.count(from) &&
      heartbeat_inst[from] == m->get_source_inst()) {

    // only take peer stat or share map now if map_lock is uncontended
    if (locked) {
      if (m->map_epoch)
	_share_map_incoming(m->get_source_inst(), m->map_epoch);
      take_peer_stat(from, m->peer_stat);  // only with map_lock held!
    }

    heartbeat_from_stamp[from] = g_clock.now();  // don't let _my_ lag interfere.
  } else {
    dout(10) << " ignoring " << m->get_source_inst() << dendl;
  }

  if (locked) 
    map_lock.put_read();

  heartbeat_lock.Unlock();
  delete m;
}

void OSD::heartbeat_entry()
{
  heartbeat_lock.Lock();
  while (!heartbeat_stop) {
    heartbeat();

    double wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf.osd_heartbeat_interval;
    utime_t w;
    w.set_from_double(wait);
    heartbeat_cond.WaitInterval(heartbeat_lock, w);
  }
  heartbeat_lock.Unlock();
}

void OSD::heartbeat()
{
  utime_t now = g_clock.now();

  if (got_sigterm) {
    dout(0) << "got SIGTERM, shutting down" << dendl;
    Message *m = new MGenericMessage(CEPH_MSG_SHUTDOWN);
    m->set_priority(CEPH_MSG_PRIO_HIGHEST);
    messenger->send_message(m, messenger->get_myinst());
    return;
  }

  // get CPU load avg
  try {
    ifstream in("/proc/loadavg");
    if (in.is_open()) {
      float oneminavg;
      in >> oneminavg;
      logger->fset(l_osd_loadavg, oneminavg);
      in.close();
    }
  }
  catch (ios::failure f) {
    dout(0) << "heartbeat: failed to read /proc/loadavg" << dendl;
  }

  // calc my stats
  Mutex::Locker lock(peer_stat_lock);
  _refresh_my_stat(now);
  my_stat_on_peer.clear();

  dout(5) << "heartbeat: " << my_stat << dendl;

  //load_calc.set_size(stat_ops);

  bool map_locked = map_lock.try_get_read();
  
  // send heartbeats
  for (map<int, epoch_t>::iterator i = heartbeat_to.begin();
       i != heartbeat_to.end();
       i++) {
    int peer = i->first;
    if (heartbeat_inst.count(peer)) {
      my_stat_on_peer[peer] = my_stat;
      Message *m = new MOSDPing(osdmap->get_fsid(),
				map_locked ? osdmap->get_epoch():0, 
				i->second,
				my_stat);
      m->set_priority(CEPH_MSG_PRIO_HIGH);
      heartbeat_messenger->send_message(m, heartbeat_inst[peer]);
    }
  }

  // check for incoming heartbeats (move me elsewhere?)
  utime_t grace = now;
  grace -= g_conf.osd_heartbeat_grace;
  for (map<int, epoch_t>::iterator p = heartbeat_from.begin();
       p != heartbeat_from.end();
       p++) {
    if (heartbeat_from_stamp.count(p->first)) {
      if (!osdmap->is_up(p->first)) {
	dout(10) << "not checking timeout on down osd" << p->first << dendl;
      } else if (osdmap->get_hb_inst(p->first) != heartbeat_inst[p->first]) {
	dout(10) << "not checking timeout on osd" << p->first
		 << " hb inst " << heartbeat_inst[p->first]
		 << " != map's " << osdmap->get_hb_inst(p->first)
		 << dendl;
      } else if (heartbeat_from_stamp[p->first] < grace) {
	dout(0) << "no heartbeat from osd" << p->first
		<< " since " << heartbeat_from_stamp[p->first]
		<< " (cutoff " << grace << ")" << dendl;
	queue_failure(p->first);
      }
    } else {
      // fake initial stamp.  and send them a ping so they know we expect it.
      if (heartbeat_inst.count(p->first)) {
	heartbeat_from_stamp[p->first] = now;  
	Message *m = new MOSDPing(osdmap->get_fsid(), 0, heartbeat_epoch, my_stat, true);  // request ack
	m->set_priority(CEPH_MSG_PRIO_HIGH);
	heartbeat_messenger->send_message(m, heartbeat_inst[p->first]);
      }
    }
  }

  if (logger) logger->set(l_osd_hbto, heartbeat_to.size());
  if (logger) logger->set(l_osd_hbfrom, heartbeat_from.size());

  
  // hmm.. am i all alone?
  if (heartbeat_from.empty() || heartbeat_to.empty()) {
    if (now - last_mon_heartbeat > g_conf.osd_mon_heartbeat_interval) {
      last_mon_heartbeat = now;
      dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
      monc->sub_want_onetime("osdmap", osdmap->get_epoch());
      monc->renew_subs();
    }
  }

  if (map_locked)
    map_lock.put_read();
}


// =========================================

void OSD::tick()
{
  assert(osd_lock.is_locked());
  dout(5) << "tick" << dendl;

  _dout_check_log();

  if (got_sigterm) {
    dout(0) << "got SIGTERM, shutting down" << dendl;
    messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN),
			    messenger->get_myinst());
    return;
  }

  // periodically kick recovery work queue
  recovery_tp.kick();
  
  map_lock.get_read();

  check_replay_queue();

  // mon report?
  utime_t now = g_clock.now();
  if (now - last_mon_report > g_conf.osd_mon_report_interval)
    do_mon_report();

  // remove stray pgs?
  remove_list_lock.Lock();
  for (map<epoch_t, map<int, vector<pg_t> > >::iterator p = remove_list.begin();
       p != remove_list.end();
       p++)
    for (map<int, vector<pg_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      if (osdmap->is_up(q->first)) {
	MOSDPGRemove *m = new MOSDPGRemove(p->first, q->second);
	messenger->send_message(m, osdmap->get_inst(q->first));
      }
    }
  remove_list.clear();
  remove_list_lock.Unlock();

  map_lock.put_read();

  timer.add_event_after(1.0, new C_Tick(this));

  do_waiters();
}

// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  last_mon_report = g_clock.now();

  // do any pending reports
  send_alive();
  send_pg_temp();
  send_failures();
  send_pg_stats();
}

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    Mutex::Locker l(osd_lock);
    dout(10) << "ms_handle_connect on mon" << dendl;
    if (is_booting())
      send_boot();
    send_alive();
    send_pg_temp();
    send_failures();
    send_pg_stats();
    class_handler->resend_class_requests();
  }
}



void OSD::send_boot()
{
  dout(10) << "send_boot" << dendl;
  monc->send_mon_message(new MOSDBoot(superblock));
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
    dout(10) << "send_alive want " << up_thru_wanted << dendl;
    monc->send_mon_message(new MOSDAlive(osdmap->get_epoch()));
  }
}

void OSD::queue_want_pg_temp(pg_t pgid, vector<int>& want)
{
  pg_temp_wanted[pgid] = want;
}

void OSD::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return;
  dout(10) << "send_pg_temp " << pg_temp_wanted << dendl;
  MOSDPGTemp *m = new MOSDPGTemp(osdmap->get_epoch());
  m->pg_temp = pg_temp_wanted;
  monc->send_mon_message(m);
}

void OSD::send_failures()
{
  while (!failure_queue.empty()) {
    int osd = *failure_queue.begin();
    monc->send_mon_message(new MOSDFailure(monc->get_fsid(), osdmap->get_inst(osd), osdmap->get_epoch()));
    failure_queue.erase(osd);
  }
}

void OSD::send_pg_stats()
{
  assert(osd_lock.is_locked());

  dout(20) << "send_pg_stats" << dendl;

  pg_stat_queue_lock.Lock();

  if (osd_stat_updated || !pg_stat_queue.empty()) {
    osd_stat_updated = false;
    
    dout(1) << "send_pg_stats - " << pg_stat_queue.size() << " pgs updated" << dendl;
    
    utime_t had_for = g_clock.now();
    had_for -= had_map_since;
    MPGStats *m = new MPGStats(osdmap->get_fsid(), osdmap->get_epoch(), had_for);

    xlist<PG*>::iterator p = pg_stat_queue.begin();
    while (!p.end()) {
      PG *pg = *p;
      ++p;
      if (!pg->is_primary()) {  // we hold map_lock; role is stable.
	pg->stat_queue_item.remove_myself();
	pg->put();
	continue;
      }
      pg->pg_stats_lock.Lock();
      if (pg->pg_stats_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_stable;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << ", not valid" << dendl;
      }
      pg->pg_stats_lock.Unlock();
    }
    
    // fill in osd stats too
    struct statfs stbuf;
    store->statfs(&stbuf);
    m->osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
    m->osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
    m->osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;
    for (map<int,epoch_t>::iterator p = heartbeat_from.begin(); p != heartbeat_from.end(); p++)
      m->osd_stat.hb_in.push_back(p->first);
    for (map<int,epoch_t>::iterator p = heartbeat_to.begin(); p != heartbeat_to.end(); p++)
      m->osd_stat.hb_out.push_back(p->first);
    dout(20) << " osd_stat " << m->osd_stat << dendl;
    
    monc->send_mon_message(m);
  }

  pg_stat_queue_lock.Unlock();
}

void OSD::handle_pg_stats_ack(MPGStatsAck *ack)
{
  dout(10) << "handle_pg_stats_ack " << dendl;

  pg_stat_queue_lock.Lock();

  xlist<PG*>::iterator p = pg_stat_queue.begin();
  while (!p.end()) {
    PG *pg = *p;
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_lock.Lock();
      if (acked == pg->pg_stats_stable.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put();
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_lock.Unlock();
    } else
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
  }
  
  if (pg_stat_queue.empty()) {
    dout(10) << "clearing osd_stat_pending" << dendl;
    osd_stat_pending = false;
  }

  pg_stat_queue_lock.Unlock();

  delete ack;
}




// --------------------------------------
// dispatch

bool OSD::_share_map_incoming(const entity_inst_t& inst, epoch_t epoch)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << inst << " " << epoch << dendl;
  //assert(osd_lock.is_locked());

  // does client have old map?
  if (inst.name.is_client()) {
    if (epoch < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst);
      shared = true;
    }
  }

  // does peer have old map?
  if (inst.name.is_osd() &&
      osdmap->is_up(inst.name.num()) &&
      (osdmap->get_inst(inst.name.num()) == inst ||
       osdmap->get_hb_inst(inst.name.num()) == inst)) {
    // remember
    if (peer_map_epoch[inst.name] < epoch) {
      dout(20) << "peer " << inst.name << " has " << epoch << dendl;
      peer_map_epoch[inst.name] = epoch;
    }
    
    // older?
    if (peer_map_epoch[inst.name] < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      peer_map_epoch[inst.name] = osdmap->get_epoch();  // so we don't send it again.
      send_incremental_map(epoch, osdmap->get_inst(inst.name.num()));
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
      send_incremental_map(pe, inst);
      peer_map_epoch[inst.name] = osdmap->get_epoch();
    }
  } else {
    // no idea about peer's epoch.
    // ??? send recent ???
    // do nothing.
  }
}


bool OSD::heartbeat_dispatch(Message *m)
{
  dout(20) << "heartbeat_dispatch " << m << dendl;

  switch (m->get_type()) {
    
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    delete m;
    break;

  case MSG_OSD_PING:
    handle_osd_ping((MOSDPing*)m);
    break;

  default:
    return false;
  }

  return true;
}

bool OSD::ms_dispatch(Message *m)
{
  // lock!
  osd_lock.Lock();
  _dispatch(m);
  do_waiters();
  osd_lock.Unlock();
  return true;
}

void OSD::do_waiters()
{
  assert(osd_lock.is_locked());
  
  finished_lock.Lock();
  if (finished.empty()) {
    finished_lock.Unlock();
  } else {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    finished_lock.Unlock();
    
    dout(2) << "do_waiters -- start" << dendl;
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++)
      _dispatch(*it);
    dout(2) << "do_waiters -- finish" << dendl;
  }
}


void OSD::_dispatch(Message *m)
{
  assert(osd_lock.is_locked());
  dout(20) << "_dispatch " << m << " " << *m << dendl;

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
    handle_pg_stats_ack((MPGStatsAck*)m);
    break;

  case MSG_MON_COMMAND:
    parse_config_option_string(((MMonCommand*)m)->cmd[0]);
    delete m;
    break;

  case MSG_OSD_SCRUB:
    handle_scrub((MOSDScrub*)m);
    break;    

  case MSG_CLASS:
    handle_class((MClass*)m);
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
      case MSG_OSD_PG_TRIM:
        handle_pg_trim((MOSDPGTrim*)m);
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
        
      }
    }
  }
}


void OSD::handle_scrub(MOSDScrub *m)
{
  dout(10) << "handle_scrub " << *m << dendl;
  
  if (ceph_fsid_compare(&m->fsid, &monc->get_fsid())) {
    dout(0) << "handle_scrub fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    delete m;
    return;
  }

  if (m->scrub_pgs.empty()) {
    for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
	 p != pg_map.end();
	 p++) {
      PG *pg = p->second;
      if (pg->is_primary()) {
	if (m->repair)
	  pg->state_set(PG_STATE_REPAIR);
	if (!pg->is_scrubbing()) {
	  dout(10) << "queueing " << *pg << " for scrub" << dendl;
	  scrub_wq.queue(pg);
	}
      }
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 p++)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	if (pg->is_primary()) {
	  if (m->repair)
	    pg->state_set(PG_STATE_REPAIR);
	  if (!pg->is_scrubbing()) {
	    dout(10) << "queueing " << *pg << " for scrub" << dendl;
	    scrub_wq.queue(pg);
	  }
	}
      }
  }
  
  delete m;
}



// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask?
  if (waiting_for_osdmap.empty()) {
    monc->sub_want_onetime("osdmap", osdmap->get_epoch());
    monc->renew_subs();
  }
  
  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int osd)
{
  messenger->mark_down(osdmap->get_addr(osd));

  heartbeat_lock.Lock();
  if (heartbeat_inst.count(osd)) {
    if (heartbeat_inst[osd] == osdmap->get_hb_inst(osd)) {
      dout(10) << "note_down_osd removing heartbeat_inst " << heartbeat_inst[osd] << dendl;
      heartbeat_inst.erase(osd);
    } else {
      dout(10) << "note_down_osd leaving heartbeat_inst " << heartbeat_inst[osd]
	       << " != " << osdmap->get_hb_inst(osd) << dendl;
    }
  } else
    dout(10) << "note_down_osd no heartbeat_inst for osd" << osd << dendl;

  peer_map_epoch.erase(entity_name_t::OSD(osd));
  failure_queue.erase(osd);
  failure_pending.erase(osd);
  heartbeat_from_stamp.erase(osd);

  heartbeat_lock.Unlock();
}
void OSD::note_up_osd(int osd)
{
  peer_map_epoch.erase(entity_name_t::OSD(osd));
}

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  if (ceph_fsid_compare(&m->fsid, &monc->get_fsid())) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    delete m;
    return;
  }

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
  }

  state = STATE_ACTIVE;

  // pause, requeue op queue
  //wait_for_no_ops();
  
  osd_lock.Unlock();
  op_tp.pause();
  op_wq.lock();
  list<Message*> rq;
  while (!op_queue.empty()) {
    PG *pg = op_queue.back();
    op_queue.pop_back();
    pending_ops--;
    Message *m = pg->op_queue.back();
    pg->op_queue.pop_back();
    pg->put();
    dout(15) << " will requeue " << *m << dendl;
    rq.push_front(m);
  }
  op_wq.unlock();
  push_waiters(rq);
  osd_lock.Lock();

  recovery_tp.pause();
  disk_tp.pause_new();   // _process() may be waiting for a replica message
  map_lock.get_write();

  assert(osd_lock.is_locked());

  ObjectStore::Transaction t;
  C_Contexts *fin = new C_Contexts;
  
  logger->inc(l_osd_map);

  // store them?
  for (map<epoch_t,bufferlist>::iterator p = m->maps.begin();
       p != m->maps.end();
       p++) {
    sobject_t poid = get_osdmap_pobject_name(p->first);
    if (store->exists(meta_coll, poid)) {
      dout(10) << "handle_osd_map already had full map epoch " << p->first << dendl;
      logger->inc(l_osd_mapfdup);
      bufferlist bl;
      get_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got full map epoch " << p->first << dendl;
    ObjectStore::Transaction ft;
    ft.write(meta_coll, poid, 0, p->second.length(), p->second);  // store _outside_ transaction; activate_map reads it.
    int r = store->apply_transaction(ft);
    if (r) {
      char buf[80];
      dout(0) << "error writing map: " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      shutdown();
      return;
    }

    if (p->first > superblock.newest_map)
      superblock.newest_map = p->first;
    if (p->first < superblock.oldest_map ||
        superblock.oldest_map == 0)
      superblock.oldest_map = p->first;

    logger->inc(l_osd_mapf);
  }
  for (map<epoch_t,bufferlist>::iterator p = m->incremental_maps.begin();
       p != m->incremental_maps.end();
       p++) {
    sobject_t poid = get_inc_osdmap_pobject_name(p->first);
    if (store->exists(meta_coll, poid)) {
      dout(10) << "handle_osd_map already had incremental map epoch " << p->first << dendl;
      logger->inc(l_osd_mapidup);
      bufferlist bl;
      get_inc_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got incremental map epoch " << p->first << dendl;
    ObjectStore::Transaction ft;
    ft.write(meta_coll, poid, 0, p->second.length(), p->second);  // store _outside_ transaction; activate_map reads it.
    int r = store->apply_transaction(ft);
    if (r) {
      char buf[80];
      dout(0) << "error writing map: " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      shutdown();
      return;
    }

    if (p->first > superblock.newest_map)
      superblock.newest_map = p->first;
    if (p->first < superblock.oldest_map ||
        superblock.oldest_map == 0)
      superblock.oldest_map = p->first;

    logger->inc(l_osd_mapi);
  }

  // advance if we can
  bool advanced = false;
  
  epoch_t cur = superblock.current_epoch;
  while (cur < superblock.newest_map) {
    dout(10) << "cur " << cur << " < newest " << superblock.newest_map << dendl;

    OSDMap::Incremental inc;

    if (m->incremental_maps.count(cur+1) ||
        store->exists(meta_coll, get_inc_osdmap_pobject_name(cur+1))) {
      dout(10) << "handle_osd_map decoding inc map epoch " << cur+1 << dendl;
      
      bufferlist bl;
      if (m->incremental_maps.count(cur+1)) {
	dout(10) << " using provided inc map" << dendl;
        bl = m->incremental_maps[cur+1];
      } else {
	dout(10) << " using my locally stored inc map" << dendl;
        get_inc_map_bl(cur+1, bl);
      }

      bufferlist::iterator p = bl.begin();
      inc.decode(p);
      osdmap->apply_incremental(inc);

      // archive the full map
      bl.clear();
      osdmap->encode(bl);
      ObjectStore::Transaction ft;
      ft.write(meta_coll, get_osdmap_pobject_name(cur+1), 0, bl.length(), bl);
      int r = store->apply_transaction(ft);
      if (r) {
	char buf[80];
	dout(0) << "error writing map: " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
	shutdown();
	return;
      }

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
             store->exists(meta_coll, get_osdmap_pobject_name(cur+1))) {
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
      monc->sub_want_onetime("osdmap", cur);
      monc->renew_subs();
      break;
    }

    // update pools
    for (map<int, PGPool*>::iterator p = pool_map.begin();
	 p != pool_map.end();
	 p++) {
      const pg_pool_t& pi = osdmap->get_pg_pool(p->first);
      if (pi.get_snap_epoch() == cur+1) {
	PGPool *pool = p->second;
	pi.build_removed_snaps(pool->newly_removed_snaps);
	pool->newly_removed_snaps.subtract(pool->removed_snaps);
	dout(10) << " pool " << p->first << " removed_snaps " << pool->removed_snaps
		 << ", newly so are " << pool->newly_removed_snaps << ")"
		 << dendl;
	pool->info = pi;
	pool->snapc = pi.get_snap_context();
      } else {
	dout(10) << " pool " << p->first << " unchanged (snap_epoch = " << pi.get_snap_epoch() << ")" << dendl;
      }
    }

    cur++;
    superblock.current_epoch = cur;
    advance_map(t);
    advanced = true;
    had_map_since = g_clock.now();
  }

  // all the way?
  if (advanced && cur == superblock.newest_map) {
    if (osdmap->is_up(whoami) &&
	osdmap->get_addr(whoami) == messenger->get_myaddr()) {
      // yay!
      activate_map(t, fin->contexts);

      // process waiters
      take_waiters(waiting_for_osdmap);
    }
  }

  // write updated pg state to store
  for (hash_map<pg_t,PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;
    if (pg->dirty_info)
      pg->write_info(t);
    if (pg->dirty_log)
      pg->write_log(t);
  }

  if (osdmap->get_epoch() > 0 &&
      (!osdmap->exists(whoami) || 
       (!osdmap->is_up(whoami) && osdmap->get_addr(whoami) == messenger->get_myaddr()))) {
    dout(0) << "map says i am down.  switching to boot state." << dendl;
    //shutdown();

    state = STATE_BOOTING;
    up_epoch = 0;

    reset_heartbeat_peers();
  }

  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
    superblock.clean_thru = osdmap->get_epoch();
  }

  // superblock and commit
  write_superblock(t);
  int r = store->apply_transaction(t, fin);
  if (r) {
    char buf[80];
    dout(0) << "error writing map: " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    shutdown();
    return;
  }
  store->sync();

  map_lock.put_write();

  op_tp.unpause();
  recovery_tp.unpause();
  disk_tp.unpause();

  //if (osdmap->get_epoch() == 1) store->sync();     // in case of early death, blah

  delete m;


  if (is_booting())
    send_boot();
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

  if (!up_epoch &&
      osdmap->is_up(whoami) &&
      osdmap->get_inst(whoami) == messenger->get_myinst()) {
    up_epoch = osdmap->get_epoch();
    dout(10) << "up_epoch is " << up_epoch << dendl;
    if (!boot_epoch) {
      boot_epoch = osdmap->get_epoch();
      dout(10) << "boot_epoch is " << boot_epoch << dendl;
    }
  }
  
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

  OSDMap *lastmap = get_map(osdmap->get_epoch() - 1);

  // scan existing pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = it->second;

    // get new acting set
    vector<int> tup, tacting;
    osdmap->pg_to_up_acting_osds(pgid, tup, tacting);
    int role = osdmap->calc_pg_role(whoami, tacting, tacting.size());

    pg->lock();

    // adjust removed_snaps?
    if (!pg->pool->newly_removed_snaps.empty()) {
      for (map<snapid_t,snapid_t>::iterator p = pg->pool->newly_removed_snaps.m.begin();
	   p != pg->pool->newly_removed_snaps.m.end();
	   p++)
	for (snapid_t t = 0; t < p->second; ++t)
	  pg->info.snap_trimq.insert(p->first + t);
      dout(10) << *pg << " snap_trimq now " << pg->info.snap_trimq << dendl;
      pg->dirty_info = true;
    }
    
    // no change?
    if (tacting == pg->acting && tup == pg->up &&
	(pg->is_active() || !pg->prior_set_affected(osdmap))) {
      dout(15) << *pg << " unchanged|active with " << tup << "/" << tacting << " up/acting" << dendl;
      pg->unlock();
      continue;
    }

    // -- there was a change! --
    int oldrole = pg->get_role();
    int oldprimary = pg->get_primary();
    vector<int> oldacting = pg->acting;
    vector<int> oldup = pg->up;
    
    // make sure we clear out any pg_temp change requests
    pg_temp_wanted.erase(pgid);
    
    pg->kick();
    pg->clear_prior();

    // update PG
    pg->acting.swap(tacting);
    pg->up.swap(tup);
    pg->set_role(role);
    
    // did acting, up, primary|acker change?
    if (tacting != pg->acting || tup != pg->up) {
      // remember past interval
      PG::Interval& i = pg->past_intervals[pg->info.history.same_acting_since];
      i.first = pg->info.history.same_acting_since;
      i.last = osdmap->get_epoch() - 1;

      i.acting = oldacting;
      i.up = oldup;
      if (tacting != pg->acting)
	pg->info.history.same_acting_since = osdmap->get_epoch();
      if (tup != pg->up)
	pg->info.history.same_up_since = osdmap->get_epoch();

      if (i.acting.size())
	i.maybe_went_rw = 
	  lastmap->get_up_thru(i.acting[0]) >= i.first &&
	  lastmap->get_up_from(i.acting[0]) <= i.first;
      else
	i.maybe_went_rw = 0;

      if (oldprimary != pg->get_primary())
	pg->info.history.same_primary_since = osdmap->get_epoch();

      dout(10) << *pg << " noting past " << i << dendl;
      pg->dirty_info = true;
    }
    pg->cancel_recovery();

    // deactivate.
    pg->state_clear(PG_STATE_ACTIVE);
    pg->state_clear(PG_STATE_DOWN);
    pg->state_clear(PG_STATE_PEERING);  // we'll need to restart peering
    pg->state_clear(PG_STATE_DEGRADED);

    if (pg->is_primary()) {
      if (osdmap->get_pg_size(pg->info.pgid) != pg->acting.size())
	pg->state_set(PG_STATE_DEGRADED);
    }

    // reset primary state?
    if (oldrole == 0 || pg->get_role() == 0)
      pg->clear_primary_state();

    dout(10) << *pg
	     << " up " << oldup << " -> " << pg->up 
	     << ", acting " << oldacting << " -> " << pg->acting 
	     << ", role " << oldrole << " -> " << role << dendl; 
    
    // pg->on_*
    for (unsigned i=0; i<oldacting.size(); i++)
      if (osdmap->is_down(oldacting[i]))
	pg->on_osd_failure(oldacting[i]);
    pg->on_change();
    
    if (role != oldrole) {
      // old stray?
      if (oldrole < 0 && pg->deleting) {
	dout(10) << *pg << " canceling deletion!" << dendl;
	pg->deleting = false;
      }

      // old primary?
      if (oldrole == 0) {
	pg->state_clear(PG_STATE_CLEAN);
	pg->clear_stats();
	
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

      // interrupt backlog generation
      cancel_generate_backlog(pg);

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

    // sanity check pg_temp
    if (pg->acting.empty() && pg->up.size() && pg->up[0] == whoami) {
      dout(10) << *pg << " acting empty, but i am up[0], clearing pg_temp" << dendl;
      queue_want_pg_temp(pg->info.pgid, pg->acting);
    }

    pg->unlock();
  }
}

void OSD::activate_map(ObjectStore::Transaction& t, list<Context*>& tfin)
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  map< int, vector<PG::Info> >  notify_list;  // primary -> list
  map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since
  map<int,MOSDPGInfo*> info_map;  // peer -> message

  epoch_t up_thru = osdmap->get_up_thru(whoami);

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_active()) {
      // update started counter
      if (!pg->info.snap_trimq.empty())
	pg->queue_snap_trim();
    }
    else if (pg->is_primary() &&
	     !pg->is_active()) {
      // i am (inactive) primary
      if (!pg->is_peering() || 
	  (pg->need_up_thru && up_thru >= pg->info.history.same_acting_since))
	pg->peer(t, tfin, query_map, &info_map);
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

  do_notifies(notify_list);  // notify? (residual|replica)
  do_queries(query_map);
  do_infos(info_map);

  logger->set(l_osd_numpg, pg_map.size());

  wake_all_pg_waiters();   // the pg mapping may have shifted

  clear_map_cache();  // we're done with it
  update_heartbeat_peers();

  send_pg_temp();
}


void OSD::send_incremental_map(epoch_t since, const entity_inst_t& inst, bool lazy)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << inst << dendl;
  
  MOSDMap *m = new MOSDMap(monc->get_fsid());
  
  for (epoch_t e = osdmap->get_epoch();
       e > since;
       e--) {
    bufferlist bl;
    if (get_inc_map_bl(e,bl)) {
      m->incremental_maps[e].claim(bl);
    } else if (get_map_bl(e,bl)) {
      m->maps[e].claim(bl);
      break;
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
  return store->read(meta_coll, get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(meta_coll, get_inc_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

OSDMap *OSD::get_map(epoch_t epoch)
{
  Mutex::Locker l(map_cache_lock);

  if (map_cache.count(epoch)) {
    dout(30) << "get_map " << epoch << " - cached" << dendl;
    return map_cache[epoch];
  }

  dout(25) << "get_map " << epoch << " - loading and decoding" << dendl;
  OSDMap *map = new OSDMap;

  // find a complete map
  list<OSDMap::Incremental> incs;
  epoch_t e;
  for (e = epoch; e > 0; e--) {
    bufferlist bl;
    if (get_map_bl(e, bl)) {
      dout(30) << "get_map " << epoch << " full " << e << dendl;
      map->decode(bl);
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
    dout(30) << "get_map " << epoch << " inc " << e << dendl;
    map->apply_incremental( incs.front() );
    incs.pop_front();
  }

  map_cache[epoch] = map;
  return map;
}

void OSD::clear_map_cache()
{
  Mutex::Locker l(map_cache_lock);
  for (map<epoch_t,OSDMap*>::iterator p = map_cache.begin();
       p != map_cache.end();
       p++)
    delete p->second;
  map_cache.clear();
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

  if (epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << epoch << " < " << up_epoch << dendl;
    delete m;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_addr(from) != m->get_source_inst().addr) {
      dout(-7) << "from dead osd" << from << ", dropping, sharing map" << dendl;
      send_incremental_map(epoch, m->get_source_inst(), true);
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
    C_Contexts *fin = new C_Contexts;
    map<pg_t,PG*> children;
    for (set<pg_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      PG *pg = _create_lock_new_pg(*q, creating_pgs[*q].acting, t);
      children[*q] = pg;
    }

    // split
    split_pg(parent, children, t); 

    parent->update_stats();
    parent->write_info(t);

    // unlock parent, children
    parent->unlock();
    for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); q++) {
      PG *pg = q->second;
      // fix up pg metadata
      pg->info.last_complete = pg->info.last_update;

      pg->write_info(t);
      pg->write_log(t);

      wake_pg_waiters(pg->info.pgid);

      pg->peer(t, fin->contexts, query_map, &info_map);
      pg->update_stats();
      pg->unlock();
      created++;
    }
    int tr = store->apply_transaction(t, fin);
    assert(tr == 0);

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

  vector<sobject_t> olist;
  store->collection_list(coll_t::build_pg_coll(parent->info.pgid), olist);  

  for (vector<sobject_t>::iterator p = olist.begin(); p != olist.end(); p++) {
    sobject_t poid = *p;
    ceph_object_layout l = osdmap->make_object_layout(poid.oid, parentid.pool(), parentid.preferred());
    pg_t pgid = osdmap->raw_pg_to_pg(pg_t(l.ol_pgid));
    if (pgid != parentid) {
      dout(20) << "  moving " << poid << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      bufferlist bv;

      struct stat st;
      store->stat(coll_t::build_pg_coll(parentid), poid, &st);
      store->getattr(coll_t::build_pg_coll(parentid), poid, OI_ATTR, bv);
      object_info_t oi(bv);

      if (oi.version > child->info.last_update) {
	child->info.last_update = oi.version;
	dout(25) << "        tagging pg with v " << oi.version << "  > " << child->info.last_update << dendl;
      } else {
	dout(25) << "    not tagging pg with v " << oi.version << " <= " << child->info.last_update << dendl;
      }

      t.collection_add(coll_t::build_pg_coll(pgid), coll_t::build_pg_coll(parentid), poid);
      t.collection_remove(coll_t::build_pg_coll(parentid), poid);
      if (oi.snaps.size()) {
	snapid_t first = oi.snaps[0];
	t.collection_add(coll_t::build_snap_pg_coll(pgid, first), coll_t::build_pg_coll(parentid), poid);
	t.collection_remove(coll_t::build_snap_pg_coll(parentid, first), poid);
	if (oi.snaps.size() > 1) {
	  snapid_t last = oi.snaps[oi.snaps.size()-1];
	  t.collection_add(coll_t::build_snap_pg_coll(pgid, last), coll_t::build_pg_coll(parentid), poid);
	  t.collection_remove(coll_t::build_snap_pg_coll(parentid, last), poid);
	}
      }

      // add to child stats
      child->info.stats.num_bytes += st.st_size;
      child->info.stats.num_kb += SHIFT_ROUND_UP(st.st_size, 10);
      child->info.stats.num_objects++;
      if (poid.snap && poid.snap != CEPH_NOSNAP)
	child->info.stats.num_object_clones++;
    } else {
      dout(20) << " leaving " << poid << "   in " << parentid << dendl;
    }
  }

  // sub off child stats
  for (map<pg_t,PG*>::iterator p = children.begin();
       p != children.end();
       p++) {
    parent->info.stats.sub(p->second->info.stats);
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
  C_Contexts *fin = new C_Contexts;
  vector<PG*> to_peer;

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
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(on, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }
    if (up != acting) {
      dout(10) << "mkpg " << pgid << "  up " << up << " != acting " << acting << dendl;
      stringstream ss;
      ss << "mkpg " << pgid << " up " << up << " != acting " << acting;
      logclient.log(LOG_ERROR, ss);
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
    project_pg_history(pgid, history, created, up, acting);
    
    // register.
    creating_pgs[pgid].created = created;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].split_bits = split_bits;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_acting_since, 
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
      to_peer.push_back(pg);
      pg->unlock();
    }
  }

  for (vector<PG*>::iterator p = to_peer.begin(); p != to_peer.end(); p++) {
    PG *pg = *p;
    pg->lock();
    wake_pg_waiters(pg->info.pgid);
    pg->peer(t, fin->contexts, query_map, &info_map);
    pg->update_stats();
    pg->unlock();
  }

  int tr = store->apply_transaction(t, fin);
  assert(tr == 0);

  do_queries(query_map);
  do_infos(info_map);

  kick_pg_split_queue();
  if (to_peer.size())
    update_heartbeat_peers();
  delete m;
}


// ----------------------------------------
// peering and recovery

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(map< int, vector<PG::Info> >& notify_list) 
{
  for (map< int, vector<PG::Info> >::iterator it = notify_list.begin();
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
  C_Contexts *fin = new C_Contexts;
  
  // look for unknown PGs i'm primary for
  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  int created = 0;

  for (vector<PG::Info>::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       it++) {
    pg_t pgid = it->pgid;
    PG *pg = 0;

    if (!_have_pg(pgid)) {
      // same primary?
      vector<int> up, acting;
      osdmap->pg_to_up_acting_osds(pgid, up, acting);
      int role = osdmap->calc_pg_role(whoami, acting, acting.size());

      PG::Info::History history = it->history;
      project_pg_history(pgid, history, m->get_epoch(), up, acting);

      if (m->get_epoch() < history.same_acting_since) {
        dout(10) << "handle_pg_notify pg " << pgid << " acting changed in "
                 << history.same_acting_since << " (msg from " << m->get_epoch() << ")" << dendl;
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
	pg->up.swap(up);
	pg->set_role(role);
	pg->info.history = history;
	pg->clear_primary_state();  // yep, notably, set hml=false
	pg->write_info(t);
	pg->write_log(t);
      }
      
      created++;
      dout(10) << *pg << " is new" << dendl;
    
      // kick any waiters
      wake_pg_waiters(pg->info.pgid);
    } else {
      // already had it.  am i (still) the primary?
      pg = _lookup_lock_pg(pgid);
      if (m->get_epoch() < pg->info.history.same_acting_since) {
        dout(10) << *pg << " handle_pg_notify acting changed in "
                 << pg->info.history.same_acting_since
                 << " (msg from " << m->get_epoch() << ")" << dendl;
        pg->unlock();
        continue;
      }
    }

    if (pg->peer_info.count(from) &&
	pg->peer_info[from].last_update == it->last_update) {
      dout(10) << *pg << " got dup osd" << from << " info " << *it << dendl;
    } else {
      dout(10) << *pg << " got osd" << from << " info " << *it << dendl;
      pg->peer_info[from] = *it;
      pg->info.history.merge(it->history);

      // stray?
      if (!pg->is_acting(from)) {
	dout(10) << *pg << " osd" << from << " has stray content: " << *it << dendl;
	pg->stray_set.insert(from);
	pg->state_clear(PG_STATE_CLEAN);
      }
      
      pg->peer(t, fin->contexts, query_map, &info_map);
      pg->update_stats();
    }
    pg->unlock();
  }
  
  int tr = store->apply_transaction(t, fin);
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
  C_Contexts *fin = new C_Contexts;

  PG *pg = 0;
  if (!_have_pg(info.pgid)) {
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    project_pg_history(info.pgid, info.history, epoch, up, acting);
    if (epoch < info.history.same_acting_since) {
      dout(10) << "got old info " << info << " on non-existent pg, ignoring" << dendl;
      return;
    }

    // create pg!
    assert(role != 0);
    pg = _create_lock_pg(info.pgid, t);
    dout(10) << " got info on new pg, creating" << dendl;
    pg->acting.swap(acting);
    pg->up.swap(up);
    pg->set_role(role);
    pg->info.history = info.history;
    pg->write_info(t);
    pg->write_log(t);
    int tr = store->apply_transaction(t);
    assert(tr == 0);
    created++;
  } else {
    pg = _lookup_lock_pg(info.pgid);
    if (epoch < pg->info.history.same_acting_since) {
      dout(10) << *pg << " got old info " << info << ", ignoring" << dendl;
      pg->unlock();
      return;
    }
  }
  assert(pg);

  dout(10) << *pg << " got " << info << " " << log << " " << missing << dendl;
  pg->info.history.merge(info.history);

  // dump log
  dout(15) << *pg << " my log = ";
  pg->log.print(*_dout);
  *_dout << dendl;
  dout(15) << *pg << " osd" << from << " log = ";
  log.print(*_dout);
  *_dout << dendl;

  if (pg->is_primary()) {
    // i am PRIMARY
    if (pg->peer_log_requested.count(from) ||
	pg->peer_summary_requested.count(from)) {
      if (!pg->is_active()) {
	pg->proc_replica_log(t, info, log, missing, from);
	
	// peer
	map< int, map<pg_t,PG::Query> > query_map;
	pg->peer(t, fin->contexts, query_map, info_map);
	pg->update_stats();
	do_queries(query_map);
      } else {
	dout(10) << *pg << " ignoring osd" << from << " log, pg is already active" << dendl;
      }
    } else {
      dout(10) << *pg << " ignoring osd" << from << " log, i didn't ask for it (recently)" << dendl;
    }
  } else {
    if (!pg->info.dne()) {
      // i am REPLICA
      if (!pg->is_active()) {
	pg->merge_log(t, info, log, missing, from);
	pg->activate(t, fin->contexts, info_map);
      } else {
	// just update our stats
	dout(10) << *pg << " writing updated stats" << dendl;
	pg->info.stats = info.stats;
	pg->write_info(t);
      }
    }
  }

  int tr = store->apply_transaction(t, fin);
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

  for (vector<PG::Info>::iterator p = m->pg_info.begin();
       p != m->pg_info.end();
       ++p) 
    _process_pg_info(m->get_epoch(), from, *p, empty_log, empty_missing, &info_map, created);

  do_infos(info_map);
  if (created)
    update_heartbeat_peers();

  delete m;
}

void OSD::handle_pg_trim(MOSDPGTrim *m)
{
  dout(7) << "handle_pg_trim " << *m << " from " << m->get_source() << dendl;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->epoch)) return;

  if (!_have_pg(m->pgid)) {
    dout(10) << " don't have pg " << m->pgid << dendl;
  } else {
    PG *pg = _lookup_lock_pg(m->pgid);
    if (m->epoch < pg->info.history.same_acting_since) {
      dout(10) << *pg << " got old trim to " << m->trim_to << ", ignoring" << dendl;
      pg->unlock();
      goto out;
    }
    assert(pg);

    if (pg->is_primary()) {
      // peer is informing us of their last_complete_ondisk
      dout(10) << *pg << " replica osd" << from << " lcod " << m->trim_to << dendl;
      pg->peer_last_complete_ondisk[from] = m->trim_to;
      if (pg->calc_min_last_complete_ondisk()) {
	dout(10) << *pg << " min lcod now " << pg->min_last_complete_ondisk << dendl;
	pg->trim_peers();
      }
    } else {
      // primary is instructing us to trim
      ObjectStore::Transaction t;
      pg->trim(t, m->trim_to);
      pg->write_info(t);
      int tr = store->apply_transaction(t);
      assert(tr == 0);
    }
    pg->unlock();
  }

 out:
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
  map< int, vector<PG::Info> > notify_list;
  
  for (map<pg_t,PG::Query>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = 0;

    if (pg_map.count(pgid) == 0) {
      // get active crush mapping
      vector<int> up, acting;
      osdmap->pg_to_up_acting_osds(pgid, up, acting);
      int role = osdmap->calc_pg_role(whoami, acting, acting.size());

      // same primary?
      PG::Info::History history = it->second.history;
      project_pg_history(pgid, history, m->get_epoch(), up, acting);

      if (m->get_epoch() < history.same_acting_since) {
        dout(10) << " pg " << pgid << " dne, and pg has changed in "
                 << history.same_acting_since << " (msg from " << m->get_epoch() << ")" << dendl;
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
      pg->up.swap( up );
      pg->set_role(role);
      pg->info.history = history;
      pg->write_info(t);
      pg->write_log(t);
      int tr = store->apply_transaction(t);
      assert(tr == 0);
      created++;

      dout(10) << *pg << " dne (before), but i am role " << role << dendl;
    } else {
      pg = _lookup_lock_pg(pgid);
      if (m->get_epoch() < pg->info.history.same_acting_since) {
        dout(10) << *pg << " handle_pg_query changed in "
                 << pg->info.history.same_acting_since
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
      if (it->second.type == PG::Query::BACKLOG &&
	  !pg->log.backlog) {
	dout(10) << *pg << " requested info+missing+backlog - queueing for backlog" << dendl;
	queue_generate_backlog(pg);
      } else {
	MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->info);
	m->missing = pg->missing;
	
	// primary -> other, when building master log
	if (it->second.type == PG::Query::LOG) {
	  dout(10) << *pg << " sending info+missing+log since " << it->second.since
		   << dendl;
	  m->log.copy_after(pg->log, it->second.since);
	}
	
	if (it->second.type == PG::Query::BACKLOG) {
	  dout(10) << *pg << " sending info+missing+backlog" << dendl;
	  assert(pg->log.backlog);
	  m->log = pg->log;
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

  dout(7) << "handle_pg_remove from " << m->get_source() << " on "
	  << m->pg_list.size() << " pgs" << dendl;
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  for (vector<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = *it;
    PG *pg;

    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }

    pg = _lookup_lock_pg(pgid);
    if (pg->info.history.same_acting_since <= m->get_epoch()) {
      if (pg->deleting) {
	dout(10) << *pg << " already removing." << dendl;
      } else {
	dout(10) << *pg << " removing." << dendl;
	assert(pg->get_role() == -1);
	assert(pg->get_primary() == m->get_source().num());
	pg->deleting = true;
	remove_wq.queue(pg);
      }
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << pg->info.history.same_acting_since << " > " << m->get_epoch() << dendl;
    }
    pg->unlock();
  }

  delete m;
}

void OSD::_remove_pg(PG *pg)
{
  pg_t pgid = pg->info.pgid;
  dout(10) << "_remove_pg " << pgid << dendl;
  
  pg->lock();
  if (!pg->deleting) {
    pg->unlock();
    return;
  }
  
  // reset log, last_complete, in case deletion gets canceled
  pg->info.last_complete = eversion_t();
  pg->log.zero();

  {
    ObjectStore::Transaction t;
    pg->write_info(t);
    t.remove(meta_coll, pg->log_oid);
    int tr = store->apply_transaction(t);
    assert(tr == 0);
  }

  int n = 0;

  // snap collections
  for (set<snapid_t>::iterator p = pg->snap_collections.begin();
       p != pg->snap_collections.end();
       p++) {
    vector<sobject_t> olist;      
    store->collection_list(coll_t::build_snap_pg_coll(pgid, *p), olist);
    dout(10) << "_remove_pg " << pgid << " snap " << *p << " " << olist.size() << " objects" << dendl;
    for (vector<sobject_t>::iterator q = olist.begin();
	 q != olist.end();
	 q++) {
      ObjectStore::Transaction t;
      t.remove(coll_t::build_snap_pg_coll(pgid, *p), *q);
      t.remove(coll_t::build_pg_coll(pgid), *q);          // we may hit this twice, but it's harmless
      int tr = store->apply_transaction(t);
      assert(tr == 0);

      if ((++n & 0xff) == 0) {
	pg->unlock();
	pg->lock();
	if (!pg->deleting) {
	  dout(10) << "_remove_pg aborted on " << *pg << dendl;
	  pg->unlock();
	  return;
	}
      }
    }
    ObjectStore::Transaction t;
    t.remove_collection(coll_t::build_snap_pg_coll(pgid, *p));
    int tr = store->apply_transaction(t);
    assert(tr == 0);
  }

  // (what remains of the) main collection
  vector<sobject_t> olist;
  store->collection_list(coll_t::build_pg_coll(pgid), olist);
  dout(10) << "_remove_pg " << pgid << " " << olist.size() << " objects" << dendl;
  for (vector<sobject_t>::iterator p = olist.begin();
       p != olist.end();
       p++) {
    ObjectStore::Transaction t;
    t.remove(coll_t::build_pg_coll(pgid), *p);
    int tr = store->apply_transaction(t);
    assert(tr == 0);

    if ((++n & 0xff) == 0) {
      pg->unlock();
      pg->lock();
      if (!pg->deleting) {
	dout(10) << "_remove_pg aborted on " << *pg << dendl;
	pg->unlock();
	return;
      }
    }
  }

  pg->unlock();
  dout(10) << "_remove_pg " << pgid << " taking osd_lock" << dendl;
  osd_lock.Lock();
  pg->lock();
  
  if (!pg->deleting) {
    osd_lock.Unlock();
    pg->unlock();
    return;
  }

  dout(10) << "_remove_pg " << pgid << " removing final" << dendl;

  {
    ObjectStore::Transaction t;
    t.remove_collection(coll_t::build_pg_coll(pgid));
    int tr = store->apply_transaction(t);
    assert(tr == 0);
  }
  
  // remove from map
  pg_map.erase(pgid);

  _put_pool(pgid.pool());

  // unlock, and probably delete
  pg->unlock();
  pg->put();  // will delete, if last reference
  osd_lock.Unlock();
  dout(10) << "_remove_pg " << pgid << " all done" << dendl;
}


// =========================================================
// RECOVERY


/*

  there are a few places we need to build a backlog.

  on a primary:
    - during peering 
      - if osd with newest update has log.bottom > our log.top
      - if other peers have log.tops below our log.bottom
        (most common case is they are a fresh osd with no pg info at all)

  on a replica or stray:
    - when queried by the primary (handle_pg_query)

  on a replica:
    - when activated by the primary (handle_pg_log -> merge_log)
    
*/
	
void OSD::queue_generate_backlog(PG *pg)
{
  if (pg->generate_backlog_epoch) {
    dout(10) << *pg << " queue_generate_backlog - already queued epoch " 
	     << pg->generate_backlog_epoch << dendl;
  } else {
    pg->generate_backlog_epoch = osdmap->get_epoch();
    dout(10) << *pg << " queue_generate_backlog epoch " << pg->generate_backlog_epoch << dendl;
    backlog_wq.queue(pg);
  }
}

void OSD::cancel_generate_backlog(PG *pg)
{
  dout(10) << *pg << " cancel_generate_backlog" << dendl;
  pg->generate_backlog_epoch = 0;
  backlog_wq.dequeue(pg);
}

void OSD::generate_backlog(PG *pg)
{
  map<eversion_t,PG::Log::Entry> omap;
  pg->lock();
  dout(10) << *pg << " generate_backlog" << dendl;

  if (!pg->generate_backlog_epoch) {
    dout(10) << *pg << " generate_backlog was canceled" << dendl;
    goto out;
  }

  if (!pg->build_backlog_map(omap))
    goto out;

  pg->assemble_backlog(omap);
  
  // take osd_lock, map_log (read)
  pg->unlock();
  map_lock.get_read();
  pg->lock();

  if (!pg->generate_backlog_epoch) {
    dout(10) << *pg << " generate_backlog aborting" << dendl;
    goto out2;
  }

  if (!pg->is_primary()) {
    dout(10) << *pg << "  sending info+missing+backlog to primary" << dendl;
    assert(!pg->is_active());  // for now
    MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->info);
    m->missing = pg->missing;
    m->log = pg->log;
    _share_map_outgoing(osdmap->get_inst(pg->get_primary()));
    messenger->send_message(m, osdmap->get_inst(pg->get_primary()));
  } else {
    dout(10) << *pg << "  generated backlog, peering" << dendl;

    map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since
    ObjectStore::Transaction t;
    C_Contexts *fin = new C_Contexts;
    pg->peer(t, fin->contexts, query_map, NULL);
    do_queries(query_map);
    if (pg->dirty_info)
      pg->write_info(t);
    if (pg->dirty_log)
      pg->write_log(t);
    int tr = store->apply_transaction(t, fin);
    assert(tr == 0);
  }

 out2:
  map_lock.put_read();

 out:
  pg->generate_backlog_epoch = 0;
  pg->unlock();
  pg->put();
}



void OSD::check_replay_queue()
{
  utime_t now = g_clock.now();
  list< pair<pg_t,utime_t> > pgids;
  replay_queue_lock.Lock();
  while (!replay_queue.empty() &&
	 replay_queue.front().second <= now) {
    pgids.push_back(replay_queue.front());
    replay_queue.pop_front();
  }
  replay_queue_lock.Unlock();

  for (list< pair<pg_t,utime_t> >::iterator p = pgids.begin(); p != pgids.end(); p++)
    activate_pg(p->first, p->second);
}

/*
 * NOTE: this is called from SafeTimer, so caller holds osd_lock
 */
void OSD::activate_pg(pg_t pgid, utime_t activate_at)
{
  assert(osd_lock.is_locked());

  if (pg_map.count(pgid)) {
    PG *pg = _lookup_lock_pg(pgid);
    if (pg->is_crashed() &&
	pg->is_replay() &&
	pg->get_role() == 0 &&
	pg->replay_until == activate_at) {
      ObjectStore::Transaction t;
      C_Contexts *fin = new C_Contexts;
      pg->activate(t, fin->contexts);
      int tr = store->apply_transaction(t, fin);
      assert(tr == 0);
    }
    pg->unlock();
  }

  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();
}


bool OSD::queue_for_recovery(PG *pg)
{
  bool b = recovery_wq.queue(pg);
  if (b)
    dout(10) << "queue_for_recovery queued " << *pg << dendl;
  else
    dout(10) << "queue_for_recovery already queued " << *pg << dendl;
  return b;
}

bool OSD::_recover_now()
{
  if (recovery_ops_active >= g_conf.osd_recovery_max_active) {
    dout(15) << "_recover_now active " << recovery_ops_active
	     << " >= max " << g_conf.osd_recovery_max_active << dendl;
    return false;
  }
  if (g_clock.now() < defer_recovery_until) {
    dout(15) << "_recover_now defer until " << defer_recovery_until << dendl;
    return false;
  }

  return true;
}

void OSD::do_recovery(PG *pg)
{
  pg->lock();

  int max = g_conf.osd_recovery_max_active - recovery_ops_active;
 
  dout(10) << "do_recovery starting " << max
	   << " (" << recovery_ops_active << "/" << g_conf.osd_recovery_max_active << " rops) on "
	   << *pg << dendl;
#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active was " << recovery_oids << dendl;
#endif

  int started = pg->start_recovery_ops(max);

  dout(10) << "do_recovery started " << started
	   << " (" << recovery_ops_active << "/" << g_conf.osd_recovery_max_active << " rops) on "
	   << *pg << dendl;

  if (started < max)
    pg->recovery_item.remove_myself();

  pg->unlock();
  pg->put();
}

void OSD::start_recovery_op(PG *pg, const sobject_t& soid)
{
  recovery_wq.lock();
  dout(10) << "start_recovery_op " << *pg << " " << soid
	   << " (" << recovery_ops_active << "/" << g_conf.osd_recovery_max_active << " rops)"
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active was " << recovery_oids << dendl;
  assert(recovery_oids.count(soid) == 0);
  recovery_oids.insert(soid);
  assert((int)recovery_oids.size() == recovery_ops_active);
#endif

  recovery_wq.unlock();
}

void OSD::finish_recovery_op(PG *pg, const sobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << *pg << " " << soid
	   << " dequeue=" << dequeue
	   << " (" << recovery_ops_active << "/" << g_conf.osd_recovery_max_active << " rops)"
	   << dendl;
  recovery_wq.lock();

  // adjust count
  recovery_ops_active--;
  assert(recovery_ops_active >= 0);

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active oids was " << recovery_oids << dendl;
  assert(recovery_oids.count(soid));
  recovery_oids.erase(soid);
  assert((int)recovery_oids.size() == recovery_ops_active);
#endif

  if (dequeue)
    pg->recovery_item.remove_myself();
  else {
    pg->get();
    recovery_queue.push_front(&pg->recovery_item);  // requeue
  }

  recovery_wq._kick();
  recovery_wq.unlock();
}

void OSD::defer_recovery(PG *pg)
{
  dout(10) << "defer_recovery " << *pg << dendl;

  // move pg to the end of the queue...
  recovery_wq.lock();
  pg->get();
  recovery_queue.push_back(&pg->recovery_item);
  recovery_wq._kick();
  recovery_wq.unlock();
}


// =========================================================
// OPS

void OSD::reply_op_error(MOSDOp *op, int err)
{
  MOSDOpReply *reply = new MOSDOpReply(op, err, osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
  messenger->send_message(reply, op->get_orig_source_inst());
  delete op;
}

void OSD::handle_misdirected_op(PG *pg, MOSDOp *op)
{
  if (op->get_map_epoch() < pg->info.history.same_primary_since) {
    dout(7) << *pg << " changed after " << op->get_map_epoch() << ", dropping" << dendl;
    delete op;
  } else {
    dout(7) << *pg << " misdirected op in " << op->get_map_epoch() << dendl;
    stringstream ss;
    ss << op->get_source_inst() << " misdirected " << op->get_reqid()
       << " " << pg->info.pgid << " to osd" << whoami
       << " not " << pg->acting;
    logclient.log(LOG_WARN, ss);
    reply_op_error(op, -ENXIO);
  }
}

void OSD::handle_op(MOSDOp *op)
{
  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch()))
    return;

  // blacklisted?
  if (osdmap->is_blacklisted(op->get_source_addr())) {
    dout(4) << "handle_op " << op->get_source_addr() << " is blacklisted" << dendl;
    reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());

  throttle_op_queue();

  // calc actual pgid
  pg_t pgid;
  if (op->get_flags() & CEPH_OSD_FLAG_PGOP)
    pgid = op->get_pg();
  else
    pgid = osdmap->raw_pg_to_pg(op->get_pg());

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid):0;


  logger->set(l_osd_buf, buffer_total_alloc.test());

  utime_t now = g_clock.now();

  // set up op flags
  init_op_flags(op);

  // update qlen stats
  stat_oprate.hit(now);
  stat_ops++;
  stat_qlen += pending_ops;
  if (!op->may_write()) {
    stat_rd_ops++;
    if (op->get_source().is_osd()) {
      //derr(-10) << "shed in " << stat_rd_ops_shed_in << " / " << stat_rd_ops << dendl;
      stat_rd_ops_shed_in++;
    }
  }

  // we don't need encoded payload anymore
  op->clear_payload();

  // have pg?
  if (!pg) {
    dout(7) << "hit non-existent pg " 
	    << pgid 
	    << ", waiting" << dendl;
    waiting_for_pg[pgid].push_back(op);
    return;
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

  // pg must be same-ish...
  if (op->may_write()) {
    if (op->get_snapid() != CEPH_NOSNAP) {
      reply_op_error(op, -EINVAL);
      pg->unlock();
      return;
    }

    // modify
    if (!pg->is_primary() ||
	!pg->same_for_modify_since(op->get_map_epoch())) {
      handle_misdirected_op(pg, op);
      pg->unlock();
      return;
    }
    
    // scrubbing?
    if (pg->is_scrubbing()) {
      dout(10) << *pg << " is scrubbing, deferring op " << *op << dendl;
      pg->waiting_for_active.push_back(op);
      pg->unlock();
      return;
    }
  } else {
    // read
    if (!pg->same_for_read_since(op->get_map_epoch())) {
      handle_misdirected_op(pg, op);
      pg->unlock();
      return;
    }

    if (op->get_snapid() > 0) {
      // snap read.  hrm.
      // are we missing a revision that we might need?
      // let's get them all.
      for (unsigned i=0; i<op->get_snaps().size(); i++) {
	sobject_t soid(op->get_oid(), op->get_snaps()[i]);
	if (pg->is_missing_object(soid)) {
	  dout(10) << "handle_op _may_ need missing rev " << soid << ", pulling" << dendl;
	  pg->wait_for_missing_object(soid, op);
	  pg->unlock();
	  return;
	}
      }
    }
  }

  // missing object?
  if ((op->get_flags() & CEPH_OSD_FLAG_PGOP) == 0) {
    sobject_t head(op->get_oid(), CEPH_NOSNAP);
    if (pg->is_missing_object(head)) {
      pg->wait_for_missing_object(head, op);
      pg->unlock();
      return;
    }
  }

  
  dout(10) << "handle_op " << *op << " in " << *pg << dendl;

  /* turn this off for now.
  // proprocess op? 
  if (pg->preprocess_op(op, now)) {
    pg->unlock();
    return;
  }
  */

  if (!op->may_write()) {
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
  dout(10) << "handle_sub_op " << *op << " epoch " << op->map_epoch << dendl;
  if (op->map_epoch < up_epoch) {
    dout(3) << "replica op from before up" << dendl;
    delete op;
    return;
  }

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->pgid;

  // require same or newer map
  if (!require_same_or_newer_map(op, op->map_epoch)) return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->map_epoch);

  if (!_have_pg(pgid)) {
    // hmm.
    delete op;
    return;
  } 

  throttle_op_queue();

  PG *pg = _lookup_lock_pg(pgid);

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (op->map_epoch < pg->info.history.same_acting_since) {
    dout(10) << "handle_sub_op pg changed " << pg->info.history
	     << " after " << op->map_epoch 
	     << ", dropping" << dendl;
    pg->unlock();
    delete op;
    return;
  }

  if (g_conf.osd_maxthreads < 1) {
    pg->do_sub_op(op);    // do it now
  } else {
    enqueue_op(pg, op);     // queue for worker threads
  }
  pg->unlock();
}
void OSD::handle_sub_op_reply(MOSDSubOpReply *op)
{
  if (op->get_map_epoch() < up_epoch) {
    dout(3) << "replica op reply from before up" << dendl;
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
  logger->set(l_osd_opq, pending_ops);
  
  // add pg to threadpool queue
  pg->get();   // we're exposing the pointer, here.
  op_wq.queue(pg);
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
  pg->unlock();
  pg->put();
  
  //#warning foo
  //scrub_wq.queue(pg);

  // finish
  osd_lock.Lock();
  {
    dout(10) << "dequeue_op " << op << " finish" << dendl;
    assert(pending_ops > 0);
    
    if (pending_ops > g_conf.osd_max_opq) 
      op_queue_cond.Signal();
    
    pending_ops--;
    logger->set(l_osd_opq, pending_ops);
    if (pending_ops == 0 && waiting_for_no_ops)
      no_pending_ops.Signal();
  }
  osd_lock.Unlock();
}




void OSD::throttle_op_queue()
{
  // throttle?  FIXME PROBABLY!
  while (pending_ops > g_conf.osd_max_opq) {
    dout(10) << "enqueue_op waiting for pending_ops " << pending_ops 
	     << " to drop to " << g_conf.osd_max_opq << dendl;
    op_queue_cond.Wait(osd_lock);
  }
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


// --------------------------------

int OSD::get_class(const nstring& cname, ClassVersion& version, pg_t pgid, Message *m, ClassHandler::ClassData **cls)
{
  Mutex::Locker l(class_lock);
  dout(10) << "wait_for_missing_class '" << cname << "' by " << pgid << dendl;


  *cls = class_handler->get_class(cname, version);
  if (*cls) {
    switch ((*cls)->status) {
    case ClassHandler::ClassData::CLASS_LOADED:
      return 0;
    case ClassHandler::ClassData::CLASS_INVALID:
      dout(0) << "class not supported" << dendl;
      return -EOPNOTSUPP;
    default:
      assert(0);
    }
  }

  waiting_for_missing_class[cname][pgid].push_back(m);
  return -EAGAIN;
}

void OSD::got_class(const nstring& cname)
{
  // no lock.. this is an upcall from handle_class
  dout(10) << "got_class '" << cname << "'" << dendl;

  if (waiting_for_missing_class.count(cname)) {
    map<pg_t,list<Message*> >& w = waiting_for_missing_class[cname];
    for (map<pg_t,list<Message*> >::iterator p = w.begin(); p != w.end(); p++)
      take_waiters(p->second);
    waiting_for_missing_class.erase(cname);
  }
}

void OSD::handle_class(MClass *m)
{
  Mutex::Locker l(class_lock);
  dout(0) << "handle_class action=" << m->action << dendl;

  switch (m->action) {
  case CLASS_RESPONSE:
    class_handler->handle_class(m);
    break;

  default:
    assert(0);
  }
  delete m;
}

void OSD::send_class_request(const char *cname, ClassVersion& version)
{
  dout(10) << "send_class_request class=" << cname << " version=" << version << dendl;
  MClass *m = new MClass(monc->get_fsid(), 0);
  ClassInfo info;
  info.name = cname;
  info.version = version;
  m->info.push_back(info);
  m->action = CLASS_GET;
  monc->send_mon_message(m);
}


void OSD::init_op_flags(MOSDOp *op)
{
  vector<OSDOp>::iterator iter;

  // did client explicitly set either bit?
  op->rmw_flags = op->get_flags() & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE);

  // implicitly set bits based on op codes, called methods.
  for (iter = op->ops.begin(); iter != op->ops.end(); ++iter) {
    if (iter->op.op & CEPH_OSD_OP_MODE_WR)
      op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
    if (iter->op.op & CEPH_OSD_OP_MODE_RD)
      op->rmw_flags |= CEPH_OSD_FLAG_READ;

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      op->rmw_flags |= CEPH_OSD_FLAG_PGOP;

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = iter->data.begin();
	int is_write, is_read;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);
	is_read = class_handler->get_method_flags(cname, mname) & CLS_METHOD_RD;
	is_write = class_handler->get_method_flags(cname, mname) & CLS_METHOD_WR;
	dout(0) << "class " << cname << " method " << mname
		<< " flags=" << (is_read ? "r" : "") << (is_write ? "w" : "") << dendl;
	if (is_read)
	  op->rmw_flags |= CEPH_OSD_FLAG_READ;
	if (is_write)
	  op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
	break;
      }
      
    default:
      break;
    }
  }
}

