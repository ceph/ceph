// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifdef USE_OBFS
# include "OBFSStore.h"
#else
# include "FakeStore.h"
#endif

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
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"

#include "common/Logger.h"
#include "common/LogType.h"
#include "common/Timer.h"
#include "common/ThreadPool.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << dbeginl << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "
#define  derr(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cerr << dbeginl << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "

char *osd_base_path = "./osddata";
char *ebofs_base_path = "./dev";


object_t SUPERBLOCK_OBJECT(0,0);


// <hack> force remount hack for performance testing FakeStore
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

OSD::OSD(int id, Messenger *m, MonMap *mm, char *dev) : timer(osd_lock)
{
  whoami = id;
  messenger = m;
  monmap = mm;

  osdmap = 0;
  boot_epoch = 0;

  last_tid = 0;
  num_pulling = 0;

  state = STATE_BOOTING;

  hb_stat_ops = 0;
  hb_stat_qlen = 0;

  pending_ops = 0;
  waiting_for_no_ops = false;

  if (g_conf.osd_remount_at) 
    timer.add_event_after(g_conf.osd_remount_at, new C_Remount(this));


  // init object store
  // try in this order:
  // dev/osd$num
  // dev/osd.$hostname
  // dev/osd.all

  if (dev) {
    strcpy(dev_path,dev);
  } else {
    char hostname[100];
    hostname[0] = 0;
    gethostname(hostname,100);
    
    sprintf(dev_path, "%s/osd%d", ebofs_base_path, whoami);

    struct stat sta;
    if (::lstat(dev_path, &sta) != 0)
      sprintf(dev_path, "%s/osd.%s", ebofs_base_path, hostname);    
    
    if (::lstat(dev_path, &sta) != 0)
      sprintf(dev_path, "%s/osd.all", ebofs_base_path);
  }

  if (g_conf.ebofs) {
    store = new Ebofs(dev_path);
    //store->_fake_writes(true);
  }
#ifdef USE_OBFS
  else if (g_conf.uofs) {
    store = new OBFSStore(whoami, NULL, dev_path);
  }
#endif
#ifdef USE_OSBDB
  else if (g_conf.bdbstore) {
    store = new OSBDB(dev_path);
  }
#endif // USE_OSBDB
  else {
    sprintf(dev_path, "osddata/osd%d", whoami);
    store = new FakeStore(dev_path, whoami);
  }

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

int OSD::init()
{
  osd_lock.Lock();
  {
    // mkfs?
    if (g_conf.osd_mkfs) {
      dout(2) << "mkfs" << dendl;
      store->mkfs();

      // make up a superblock
      //superblock.fsid = ???;
      superblock.whoami = whoami;
    }
    
    // mount.
    dout(2) << "mounting " << dev_path << dendl;
    int r = store->mount();
    assert(r>=0);

    if (g_conf.osd_mkfs) {
      // age?
      if (g_conf.osd_age_time != 0) {
        dout(2) << "age" << dendl;
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
    }
    else {
      dout(2) << "boot" << dendl;
      
      // read superblock
      read_superblock();

      // load up pgs (as they previously existed)
      load_pgs();

      dout(2) << "superblock: i am osd" << superblock.whoami << dendl;
      assert(whoami == superblock.whoami);
    }

    
    // log
    char name[80];
    sprintf(name, "osd%02d", whoami);
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
    
    osd_logtype.add_inc("rlnum");

    osd_logtype.add_set("numpg");
    osd_logtype.add_set("pingset");

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
      threadpool = new ThreadPool<OSD*, pg_t>(name, g_conf.osd_maxthreads, 
                                              static_dequeueop,
                                              this);
    }
    
    // i'm ready!
    messenger->set_dispatcher(this);
    
    // announce to monitor i exist and have booted.
    int mon = monmap->pick_mon();
    messenger->send_message(new MOSDBoot(superblock), monmap->get_inst(mon));
    
    // start the heart
    timer.add_event_after(g_conf.osd_heartbeat_interval, new C_Heartbeat(this));
  }
  osd_lock.Unlock();

  //dout(0) << "osd_rep " << g_conf.osd_rep << dendl;

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
  bl.append((char*)&superblock, sizeof(superblock));
  t.write(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
  if (bl.length() != sizeof(superblock)) {
    dout(10) << "read_superblock failed, r = " << r << ", i got " << bl.length() << " bytes, not " << sizeof(superblock) << dendl;
    return -1;
  }

  bl.copy(0, sizeof(superblock), (char*)&superblock);
  
  dout(10) << "read_superblock " << superblock << dendl;

  // load up "current" osdmap
  assert(!osdmap);
  osdmap = new OSDMap;
  bl.clear();
  get_map_bl(superblock.current_epoch, bl);
  osdmap->decode(bl);

  assert(whoami == superblock.whoami);  // fixme!
  return 0;
}





// ======================================================
// PG's

PG *OSD::_create_lock_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  dout(10) << "_create_lock_pg " << pgid << dendl;

  if (pg_map.count(pgid)) 
    dout(0) << "_create_lock_pg on " << pgid << ", already have " << *pg_map[pgid] << dendl;
  
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

  // lock
  pg->lock();
  pg_lock.insert(pgid);

  pg->get(); // because it's in pg_map
  pg->get(); // because we're locking it

  // create collection
  assert(!store->collection_exists(pgid));
  t.create_collection(pgid);

  return pg;
}

bool OSD::_have_pg(pg_t pgid)
{
  return pg_map.count(pgid);
}

PG *OSD::_lock_pg(pg_t pgid)
{
  assert(pg_map.count(pgid));

  // wait?
  if (pg_lock.count(pgid)) {
    Cond c;
    dout(15) << "lock_pg " << pgid << " waiting as " << &c << dendl;
    //cerr << "lock_pg " << pgid << " waiting as " << &c << dendl;

    list<Cond*>& ls = pg_lock_waiters[pgid];   // this is commit, right?
    ls.push_back(&c);
    
    while (pg_lock.count(pgid) ||
           ls.front() != &c)
      c.Wait(osd_lock);

    assert(ls.front() == &c);
    ls.pop_front();
    if (ls.empty())
      pg_lock_waiters.erase(pgid);
  }

  dout(15) << "lock_pg " << pgid << dendl;
  pg_lock.insert(pgid);

  PG *pg = pg_map[pgid];
  pg->lock();
  pg->get();    // because we're "locking" it and returning a pointer copy.
  return pg;
}

void OSD::_unlock_pg(pg_t pgid) 
{
  // unlock
  assert(pg_lock.count(pgid));
  pg_lock.erase(pgid);

  pg_map[pgid]->put_unlock();

  if (pg_lock_waiters.count(pgid)) {
    // someone is in line
    Cond *c = pg_lock_waiters[pgid].front();
    assert(c);
    dout(15) << "unlock_pg " << pgid << " waking up next guy " << c << dendl;
    c->Signal();
  } else {
    // nobody waiting
    dout(15) << "unlock_pg " << pgid << dendl;
  }
}

void OSD::_remove_unlock_pg(PG *pg) 
{
  pg_t pgid = pg->info.pgid;

  dout(10) << "_remove_unlock_pg " << pgid << dendl;

  // there shouldn't be any waiters, since we're a stray, and pg is presumably clean0.
  assert(pg_lock_waiters.count(pgid) == 0);

  // remove from store
  list<object_t> olist;
  store->collection_list(pgid, olist);
  
  ObjectStore::Transaction t;
  {
    for (list<object_t>::iterator p = olist.begin();
	 p != olist.end();
	 p++)
      t.remove(*p);
    t.remove_collection(pgid);
    t.remove(pgid.to_object());  // log too
  }
  store->apply_transaction(t);

  // mark deleted
  pg->mark_deleted();

  // unlock
  pg_lock.erase(pgid);
  pg->put();   

  // remove from map
  pg_map.erase(pgid);
  pg->put_unlock();     // will delete, if last reference
}



void OSD::load_pgs()
{
  dout(10) << "load_pgs" << dendl;
  assert(pg_map.empty());

  list<coll_t> ls;
  store->list_collections(ls);

  for (list<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    pg_t pgid = *it;

    PG *pg = 0;
    if (pgid.is_rep())
      new ReplicatedPG(this, pgid);
    else if (pgid.is_raid4())
      new RAID4PG(this, pgid);
    else 
      assert(0);
    pg_map[pgid] = pg;
    pg->get();

    // read pg info
    store->collection_getattr(pgid, "info", &pg->info, sizeof(pg->info));
    
    // read pg log
    pg->read_log(store);

    // generate state for current mapping
    int nrep = osdmap->pg_to_acting_osds(pgid, pg->acting);
    int role = osdmap->calc_pg_role(whoami, pg->acting, nrep);
    pg->set_role(role);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << dendl;
  }
}
 


/**
 * check epochs starting from start to verify the pg acting set hasn't changed
 * up until now
 */
void OSD::project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from)
{
  dout(15) << "project_pg_history " << pgid
           << " from " << from << " to " << osdmap->get_epoch()
           << ", start " << h
           << dendl;

  vector<int> last;
  osdmap->pg_to_acting_osds(pgid, last);

  for (epoch_t e = osdmap->get_epoch()-1;
       e >= from;
       e--) {
    // verify during intermediate epoch
    OSDMap oldmap;
    get_map(e, oldmap);

    vector<int> acting;
    oldmap.pg_to_acting_osds(pgid, acting);

    // acting set change?
    if (acting != last && 
        e <= h.same_since) {
      dout(15) << "project_pg_history " << pgid << " changed in " << e+1 
                << " from " << acting << " -> " << last << dendl;
      h.same_since = e+1;
    }

    // primary change?
    if (!(!acting.empty() && !last.empty() && acting[0] == last[0]) &&
        e <= h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e+1 << dendl;
      h.same_primary_since = e+1;
    
      if (g_conf.osd_rep == OSD_REP_PRIMARY)
        h.same_acker_since = h.same_primary_since;
    }

    // acker change?
    if (g_conf.osd_rep != OSD_REP_PRIMARY) {
      if (!(!acting.empty() && !last.empty() && acting[acting.size()-1] == last[last.size()-1]) &&
          e <= h.same_acker_since) {
        dout(15) << "project_pg_history " << pgid << " acker changed in " << e+1 << dendl;
        h.same_acker_since = e+1;
      }
    }

    if (h.same_since > e &&
        h.same_primary_since > e &&
        h.same_acker_since > e) break;
  }

  dout(15) << "project_pg_history end " << h << dendl;
}

void OSD::activate_pg(pg_t pgid, epoch_t epoch)
{
  osd_lock.Lock();
  {
    if (pg_map.count(pgid)) {
      PG *pg = _lock_pg(pgid);
      if (pg->is_crashed() &&
          pg->is_replay() &&
          pg->get_role() == 0 &&
          pg->info.history.same_primary_since <= epoch) {
        ObjectStore::Transaction t;
        pg->activate(t);
        store->apply_transaction(t);
      }
      _unlock_pg(pgid);
    }
  }

  // finishers?
  if (finished.empty()) {
    osd_lock.Unlock();
  } else {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    osd_lock.Unlock();
    
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++) {
      dispatch(*it);
    }
  }
}


// -------------------------------------

void OSD::heartbeat()
{
  utime_t now = g_clock.now();
  utime_t since = now;
  since.sec_ref() -= g_conf.osd_heartbeat_interval;

  // calc my stats
  float avg_qlen = 0;
  if (hb_stat_ops) avg_qlen = (float)hb_stat_qlen / (float)hb_stat_ops;

  dout(5) << "heartbeat " << now 
	  << ": ops " << hb_stat_ops
	  << ", avg qlen " << avg_qlen
	  << dendl;
  
  // reset until next time around
  hb_stat_ops = 0;
  hb_stat_qlen = 0;

  // send pings
  set<int> pingset;
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;

    // we want to ping the primary.
    if (pg->get_role() <= 0) continue;   
    if (pg->acting.size() < 1) continue; 

    if (pg->last_heartbeat < since) {
      pg->last_heartbeat = now;
      pingset.insert(pg->acting[0]);
    }
  }
  for (set<int>::iterator i = pingset.begin();
       i != pingset.end();
       i++) {
    _share_map_outgoing( osdmap->get_inst(*i) );
    messenger->send_message(new MOSDPing(osdmap->get_epoch(), avg_qlen), 
                            osdmap->get_inst(*i));
  }

  if (logger) logger->set("pingset", pingset.size());

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



// --------------------------------------
// dispatch

bool OSD::_share_map_incoming(const entity_inst_t& inst, epoch_t epoch)
{
  bool shared = false;

  // does client have old map?
  if (inst.name.is_client()) {
    if (epoch < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst, true);
      shared = true;
    }
  }

  // does peer have old map?
  if (inst.name.is_osd()) {
    // remember
    if (peer_map_epoch[inst.name] < epoch)
      peer_map_epoch[inst.name] = epoch;
    
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

  if (inst.name.is_osd()) {
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
}



void OSD::dispatch(Message *m) 
{
  // lock!
  osd_lock.Lock();

  switch (m->get_type()) {

    // -- don't need lock -- 
  case MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    delete m;
    break;

    // -- don't need OSDMap --

    // map and replication
  case MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

    // osd
  case MSG_SHUTDOWN:
    shutdown();
    delete m;
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
      
      // down?
      if (osdmap->is_down(whoami)) {
        dout(7) << "i am marked down, dropping " << *m << dendl;
        delete m;
        break;
      }


      

      // need OSDMap
      switch (m->get_type()) {

      case MSG_OSD_PING:
        // take note.
        handle_osd_ping((MOSDPing*)m);
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

      case MSG_OSD_OP:
        handle_op((MOSDOp*)m);
        break;
        
        // for replication etc.
      case MSG_OSD_OPREPLY:
        handle_op_reply((MOSDOpReply*)m);
        break;
        
        
      default:
        dout(1) << " got unknown message " << m->get_type() << dendl;
        assert(0);
      }
    }
  }

  // finishers?
  if (!finished.empty()) {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    osd_lock.Unlock();
    
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++) {
      dispatch(*it);
    }
    return;
  }
  
  osd_lock.Unlock();
}


void OSD::ms_handle_failure(Message *m, const entity_inst_t& inst)
{
  entity_name_t dest = inst.name;

  if (g_conf.ms_die_on_failure) {
    dout(0) << "ms_handle_failure " << inst << " on " << *m << dendl;
    exit(0);
  }

  if (dest.is_osd()) {
    // failed osd.  drop message, report to mon.
    int mon = monmap->pick_mon();
    dout(0) << "ms_handle_failure " << inst 
            << ", dropping and reporting to mon" << mon 
	    << " " << *m
            << dendl;
    messenger->send_message(new MOSDFailure(inst, osdmap->get_epoch()),
                            monmap->get_inst(mon));
    delete m;
  } else if (dest.is_mon()) {
    // resend to a different monitor.
    int mon = monmap->pick_mon(true);
    dout(0) << "ms_handle_failure " << inst 
            << ", resending to mon" << mon 
	    << " " << *m
            << dendl;
    messenger->send_message(m, monmap->get_inst(mon));
  }
  else {
    // client?
    dout(0) << "ms_handle_failure " << inst 
            << ", dropping " << *m << dendl;
    delete m;
  }
}




void OSD::handle_osd_ping(MOSDPing *m)
{
  dout(20) << "osdping from " << m->get_source() << dendl;
  _share_map_incoming(m->get_source_inst(), ((MOSDPing*)m)->map_epoch);
  
  int from = m->get_source().num();
  peer_qlen[from] = m->avg_qlen;

  //if (!m->ack)
  //messenger->send_message(new MOSDPing(osdmap->get_epoch(), true),
  //m->get_source());
 
  delete m;
}




// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask 
  if (waiting_for_osdmap.empty()) {
    int mon = monmap->pick_mon();
    messenger->send_message(new MOSDGetMap(osdmap->get_epoch()),
                            monmap->get_inst(mon));
  }
  
  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */
void OSD::handle_osd_map(MOSDMap *m)
{
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
    object_t oid = get_osdmap_object_name(p->first);
    if (store->exists(oid)) {
      dout(10) << "handle_osd_map already had full map epoch " << p->first << dendl;
      logger->inc("mapfdup");
      bufferlist bl;
      get_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got full map epoch " << p->first << dendl;
    //t.write(oid, 0, p->second.length(), p->second);
    store->write(oid, 0, p->second.length(), p->second, 0);

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
    object_t oid = get_inc_osdmap_object_name(p->first);
    if (store->exists(oid)) {
      dout(10) << "handle_osd_map already had incremental map epoch " << p->first << dendl;
      logger->inc("mapidup");
      bufferlist bl;
      get_inc_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << dendl;
      continue;
    }

    dout(10) << "handle_osd_map got incremental map epoch " << p->first << dendl;
    //t.write(oid, 0, p->second.length(), p->second);
    store->write(oid, 0, p->second.length(), p->second, 0);

    if (p->first > superblock.newest_map)
      superblock.newest_map = p->first;
    if (p->first < superblock.oldest_map ||
        superblock.oldest_map == 0)
      superblock.oldest_map = p->first;

    logger->inc("mapi");
  }

  // advance if we can
  bool advanced = false;
  
  if (m->get_source().is_mon() && is_booting()) 
    advanced = true;

  epoch_t cur = superblock.current_epoch;
  while (cur < superblock.newest_map) {
    bufferlist bl;
    if (m->incremental_maps.count(cur+1) ||
        store->exists(get_inc_osdmap_object_name(cur+1))) {
      dout(10) << "handle_osd_map decoding inc map epoch " << cur+1 << dendl;
      
      bufferlist bl;
      if (m->incremental_maps.count(cur+1))
        bl = m->incremental_maps[cur+1];
      else
        get_inc_map_bl(cur+1, bl);

      OSDMap::Incremental inc;
      int off = 0;
      inc.decode(bl, off);

      osdmap->apply_incremental(inc);

      // archive the full map
      bl.clear();
      osdmap->encode(bl);
      t.write( get_osdmap_object_name(cur+1), 0, bl.length(), bl);

      // notify messenger
      for (map<int,entity_inst_t>::iterator i = inc.new_down.begin();
           i != inc.new_down.end();
           i++) {
        int osd = i->first;
        if (osd == whoami) continue;
        messenger->mark_down(i->second.addr);
        peer_map_epoch.erase(MSG_ADDR_OSD(osd));
      
        // kick any replica ops
        for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
             it != pg_map.end();
             it++) {
          PG *pg = it->second;

          _lock_pg(pg->info.pgid);
	  pg->note_failed_osd(osd);
          _unlock_pg(pg->info.pgid);
        }
      }
      for (map<int,entity_inst_t>::iterator i = inc.new_up.begin();
           i != inc.new_up.end();
           i++) {
        if (i->first == whoami) continue;
        peer_map_epoch.erase(MSG_ADDR_OSD(i->first));
      }
    }
    else if (m->maps.count(cur+1) ||
             store->exists(get_osdmap_object_name(cur+1))) {
      dout(10) << "handle_osd_map decoding full map epoch " << cur+1 << dendl;
      bufferlist bl;
      if (m->maps.count(cur+1))
        bl = m->maps[cur+1];
      else
        get_map_bl(cur+1, bl);
      osdmap->decode(bl);

      // FIXME BUG: need to notify messenger of ups/downs!!
    }
    else {
      dout(10) << "handle_osd_map missing epoch " << cur+1 << dendl;
      int mon = monmap->pick_mon();
      messenger->send_message(new MOSDGetMap(cur), monmap->get_inst(mon));
      break;
    }

    cur++;
    superblock.current_epoch = cur;
    advance_map(t);
    advanced = true;
  }

  // all the way?
  if (advanced && cur == superblock.newest_map) {
    // yay!
    activate_map(t);
    
    // process waiters
    take_waiters(waiting_for_osdmap);
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

  //if (osdmap->get_epoch() == 1) store->sync();     // in case of early death, blah

  delete m;
}


/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t)
{
  dout(7) << "advance_map epoch " << osdmap->get_epoch() 
          << "  " << pg_map.size() << " pgs"
          << dendl;
  
  if (osdmap->is_mkfs()) {
    ps_t maxps = 1ULL << osdmap->get_pg_bits();
    ps_t maxlps = 1ULL << osdmap->get_localized_pg_bits();
    dout(1) << "mkfs on " << osdmap->get_pg_bits() << " bits, " << maxps << " pgs" << dendl;
    assert(osdmap->get_epoch() == 1);

    //cerr << "osdmap " << osdmap->get_ctime() << " logger start " << logger->get_start() << dendl;
    logger->set_start( osdmap->get_ctime() );

    assert(g_conf.osd_mkfs);  // make sure we did a mkfs!

    // create PGs
    //  replicated
    for (int nrep = 1; 
         nrep <= MIN(g_conf.num_osd, g_conf.osd_max_rep);    // for low osd counts..  hackish bleh
         nrep++) {
      for (ps_t ps = 0; ps < maxps; ++ps) {
	vector<int> acting;
	pg_t pgid = pg_t(pg_t::TYPE_REP, nrep, ps, -1);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	if (role < 0) continue;
	
	PG *pg = _create_lock_pg(pgid, t);
	pg->set_role(role);
	pg->acting.swap(acting);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_since = 
	  pg->info.history.same_primary_since = 
	    pg->info.history.same_acker_since = osdmap->get_epoch();
	pg->activate(t);

	dout(7) << "created " << *pg << dendl;
	_unlock_pg(pgid);
      }

      for (ps_t ps = 0; ps < maxlps; ++ps) {
	// local PG too
	vector<int> acting;
	pg_t pgid = pg_t(pg_t::TYPE_REP, nrep, ps, whoami);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	
	PG *pg = _create_lock_pg(pgid, t);
	pg->acting.swap(acting);
	pg->set_role(role);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_primary_since = 
	  pg->info.history.same_acker_since = 
	  pg->info.history.same_since = osdmap->get_epoch();
	pg->activate(t);
	
	dout(7) << "created " << *pg << dendl;
	_unlock_pg(pgid);
      }
    }

    // raided
    for (int size = g_conf.osd_min_raid_width;
	 size <= g_conf.osd_max_raid_width;
	 size++) {
      for (ps_t ps = 0; ps < maxps; ++ps) {
	vector<int> acting;
	pg_t pgid = pg_t(pg_t::TYPE_RAID4, size, ps, -1);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	if (role < 0) continue;
	
	PG *pg = _create_lock_pg(pgid, t);
	pg->set_role(role);
	pg->acting.swap(acting);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_since = 
	  pg->info.history.same_primary_since = 
	    pg->info.history.same_acker_since = osdmap->get_epoch();
	pg->activate(t);

	dout(7) << "created " << *pg << dendl;
	_unlock_pg(pgid);
      }

      for (ps_t ps = 0; ps < maxlps; ++ps) {
	// local PG too
	vector<int> acting;
	pg_t pgid = pg_t(pg_t::TYPE_RAID4, size, ps, whoami);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	
	PG *pg = _create_lock_pg(pgid, t);
	pg->acting.swap(acting);
	pg->set_role(role);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_primary_since = 
	  pg->info.history.same_acker_since = 
	  pg->info.history.same_since = osdmap->get_epoch();
	pg->activate(t);
	
	dout(7) << "created " << *pg << dendl;
	_unlock_pg(pgid);
      }
    }
    dout(1) << "mkfs done, created " << pg_map.size() << " pgs" << dendl;

  } else {
    // scan existing pg's
    for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
         it != pg_map.end();
         it++) {
      pg_t pgid = it->first;
      PG *pg = it->second;
      
      // did i finish this epoch?
      if (pg->is_active()) {
        pg->info.last_epoch_finished = osdmap->get_epoch()-1;
      }      

      // get new acting set
      vector<int> tacting;
      int nrep = osdmap->pg_to_acting_osds(pgid, tacting);
      int role = osdmap->calc_pg_role(whoami, tacting, nrep);

      // no change?
      if (tacting == pg->acting) 
        continue;

      // -- there was a change! --
      _lock_pg(pgid);
      
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
      pg->state_clear(PG::STATE_ACTIVE);
      
      // reset primary state?
      if (oldrole == 0 || pg->get_role() == 0)
        pg->clear_primary_state();
      
      // apply any repops in progress.
      if (oldacker == whoami) {
	pg->on_acker_change();
      }

      if (role != oldrole) {
        // old primary?
        if (oldrole == 0) {
          pg->state_clear(PG::STATE_CLEAN);

	  // take replay queue waiters
	  list<Message*> ls;
	  for (map<eversion_t,MOSDOp*>::iterator it = pg->replay_queue.begin();
	       it != pg->replay_queue.end();
	       it++)
	    ls.push_back(it->second);
	  pg->replay_queue.clear();
	  take_waiters(ls);

	  // take active waiters
	  take_waiters(pg->waiting_for_active);
  
	  pg->on_role_change();
        }
        
        // new primary?
        if (role == 0) {
          // i am new primary
          pg->state_clear(PG::STATE_STRAY);
        } else {
          // i am now replica|stray.  we need to send a notify.
          pg->state_set(PG::STATE_STRAY);

          if (nrep == 0) {
            pg->state_set(PG::STATE_CRASHED);
            dout(1) << *pg << " is crashed" << dendl;
          }
        }
        
        // my role changed.
        dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
                 << ", role " << oldrole << " -> " << role << dendl; 
        
      } else {
        // no role change.
        // did primary change?
        if (pg->get_primary() != oldprimary) {    
          // we need to announce
          pg->state_set(PG::STATE_STRAY);
          
          dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
                   << ", acting primary " 
                   << oldprimary << " -> " << pg->get_primary() 
                   << dendl;
        } else {
          // primary is the same.
          if (role == 0) {
            // i am (still) primary. but my replica set changed.
            pg->state_clear(PG::STATE_CLEAN);
            pg->state_clear(PG::STATE_REPLAY);

            dout(10) << *pg << " " << oldacting << " -> " << pg->acting
                     << ", replicas changed" << dendl;
          }
        }
      }
      

      _unlock_pg(pgid);
    }
  }
}

void OSD::activate_map(ObjectStore::Transaction& t)
{
  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  map< int, list<PG::Info> >  notify_list;  // primary -> list
  map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    //pg_t pgid = it->first;
    PG *pg = it->second;

    if (pg->is_active()) {
      // update started counter
      pg->info.last_epoch_started = osdmap->get_epoch();
    } 
    else if (pg->get_role() == 0 && !pg->is_active()) {
      // i am (inactive) primary
      pg->build_prior();
      pg->peer(t, query_map);
    }
    else if (pg->is_stray() &&
             pg->get_primary() >= 0) {
      // i am residual|replica
      notify_list[pg->get_primary()].push_back(pg->info);
    }

  }  

  if (osdmap->is_mkfs())    // hack: skip the queries/summaries if it's a mkfs
    return;

  // notify? (residual|replica)
  do_notifies(notify_list);
  
  // do queries.
  do_queries(query_map);

  logger->set("numpg", pg_map.size());
}


void OSD::send_incremental_map(epoch_t since, const entity_inst_t& inst, bool full)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << inst << dendl;
  
  MOSDMap *m = new MOSDMap;
  
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

  messenger->send_message(m, inst);
}

bool OSD::get_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(get_osdmap_object_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(get_inc_osdmap_object_name(e), 0, 0, bl) >= 0;
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
  assert(e > 0);

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
  int off = 0;
  inc.decode(bl, off);
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
  dout(10) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ")" << dendl;

  // newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "  from newer map epoch " << epoch << " > " << osdmap->get_epoch() << dendl;
    wait_for_new_map(m);
    return false;
  }

  if (epoch < boot_epoch) {
    dout(7) << "  from pre-boot epoch " << epoch << " < " << boot_epoch << dendl;
    delete m;
    return false;
  }

  return true;
}





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

    MOSDPGQuery *m = new MOSDPGQuery(osdmap->get_epoch(),
                                     pit->second);
    _share_map_outgoing(osdmap->get_inst(who));
    messenger->send_message(m, osdmap->get_inst(who));
  }
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

  for (list<PG::Info>::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       it++) {
    pg_t pgid = it->pgid;
    PG *pg;

    if (pg_map.count(pgid) == 0) {
      // same primary?
      PG::Info::History history = it->history;
      project_pg_history(pgid, history, m->get_epoch());

      if (m->get_epoch() < history.same_primary_since) {
        dout(10) << "handle_pg_notify pg " << pgid << " dne, and primary changed in "
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << dendl;
        continue;
      }
      
      // ok, create PG!
      pg = _create_lock_pg(pgid, t);
      osdmap->pg_to_acting_osds(pgid, pg->acting);
      pg->set_role(0);
      pg->info.history = history;

      pg->last_epoch_started_any = it->last_epoch_started;
      pg->build_prior();

      t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
      
      dout(10) << *pg << " is new" << dendl;
    
      // kick any waiters
      if (waiting_for_pg.count(pgid)) {
        take_waiters(waiting_for_pg[pgid]);
        waiting_for_pg.erase(pgid);
      }
    } else {
      // already had it.  am i (still) the primary?
      pg = _lock_pg(pgid);
      if (m->get_epoch() < pg->info.history.same_primary_since) {
        dout(10) << *pg << " handle_pg_notify primary changed in "
                 << pg->info.history.same_primary_since
                 << " (msg from " << m->get_epoch() << ")" << dendl;
        _unlock_pg(pgid);
        continue;
      }
    }

    // ok!
    
    // stray?
    bool acting = pg->is_acting(from);
    if (!acting && (*it).last_epoch_started > 0) {
      dout(10) << *pg << " osd" << from << " has stray content: " << *it << dendl;
      pg->stray_set.insert(from);
      pg->state_clear(PG::STATE_CLEAN);
    }

    // save info.
    bool had = pg->peer_info.count(from);
    pg->peer_info[from] = *it;

    if (had) {
      if (pg->is_active() && 
          (*it).is_clean() && acting) {
        pg->clean_set.insert(from);
        dout(10) << *pg << " osd" << from << " now clean (" << pg->clean_set  
                 << "): " << *it << dendl;
        if (pg->is_all_clean()) {
          dout(10) << *pg << " now clean on all replicas" << dendl;
          pg->state_set(PG::STATE_CLEAN);
          pg->clean_replicas();
        }
      } else {
        // hmm, maybe keep an eye out for cases where we see this, but peer should happen.
        dout(10) << *pg << " already had notify info from osd" << from << ": " << *it << dendl;
      }
    } else {
      // adjust prior?
      if (it->last_epoch_started > pg->last_epoch_started_any) 
        pg->adjust_prior();
      
      // peer
      pg->peer(t, query_map);
    }

    _unlock_pg(pgid);
  }
  
  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  do_queries(query_map);
  
  delete m;
}



/** PGLog
 * from non-primary to primary
 *  includes log and info
 * from primary to non-primary
 *  includes log for use in recovery
 * NOTE: called with opqueue active.
 */

void OSD::handle_pg_log(MOSDPGLog *m) 
{
  int from = m->get_source().num();
  const pg_t pgid = m->get_pgid();

  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  if (pg_map.count(pgid) == 0) {
    dout(10) << "handle_pg_log don't have pg " << pgid << ", dropping" << dendl;
    assert(m->get_epoch() < osdmap->get_epoch());
    delete m;
    return;
  }

  PG *pg = _lock_pg(pgid);
  assert(pg);

  if (m->get_epoch() < pg->info.history.same_since) {
    dout(10) << "handle_pg_log " << *pg 
            << " from " << m->get_source() 
            << " is old, discarding"
            << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_pg_log " << *pg 
          << " got " << m->log << " " << m->missing
          << " from " << m->get_source() << dendl;

  //m->log.print(cout);
  
  ObjectStore::Transaction t;

  if (pg->is_primary()) {
    // i am PRIMARY
    assert(pg->peer_log_requested.count(from) ||
           pg->peer_summary_requested.count(from));
    
    pg->proc_replica_log(m->log, m->missing, from);

    // peer
    map< int, map<pg_t,PG::Query> > query_map;
    pg->peer(t, query_map);
    do_queries(query_map);

  } else {
    // i am REPLICA
    dout(10) << *pg << " got " << m->log << " " << m->missing << dendl;

    // merge log
    pg->merge_log(m->log, m->missing, from);
    pg->proc_missing(m->log, m->missing, from);
    assert(pg->missing.num_lost() == 0);

    // ok activate!
    pg->activate(t);
  }

  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  _unlock_pg(pgid);

  delete m;
}


/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_query(MOSDPGQuery *m) 
{
  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  map< int, list<PG::Info> > notify_list;
  
  for (map<pg_t,PG::Query>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = 0;

    if (pg_map.count(pgid) == 0) {
      // same primary?
      PG::Info::History history = it->second.history;
      project_pg_history(pgid, history, m->get_epoch());

      if (m->get_epoch() < history.same_since) {
        dout(10) << " pg " << pgid << " dne, and pg has changed in "
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << dendl;
        continue;
      }

      // get active crush mapping
      vector<int> acting;
      int nrep = osdmap->pg_to_acting_osds(pgid, acting);
      int role = osdmap->calc_pg_role(whoami, acting, nrep);

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

      t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
      store->apply_transaction(t);

      dout(10) << *pg << " dne (before), but i am role " << role << dendl;
    } else {
      pg = _lock_pg(pgid);
      
      // same primary?
      if (m->get_epoch() < pg->info.history.same_since) {
        dout(10) << *pg << " handle_pg_query primary changed in "
                 << pg->info.history.same_since
                 << " (msg from " << m->get_epoch() << ")" << dendl;
        _unlock_pg(pgid);
        continue;
      }
    }

    // ok, process query!
    assert(!pg->acting.empty());
    assert(from == pg->acting[0]);

    if (it->second.type == PG::Query::INFO) {
      // info
      dout(10) << *pg << " sending info" << dendl;
      notify_list[from].push_back(pg->info);
    } else {
      MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->get_pgid());
      m->info = pg->info;
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

    _unlock_pg(pgid);
  }
  
  do_notifies(notify_list);   

  delete m;
}


void OSD::handle_pg_remove(MOSDPGRemove *m)
{
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

    pg = _lock_pg(pgid);

    dout(10) << *pg << " removing." << dendl;
    assert(pg->get_role() == -1);
    
    _remove_unlock_pg(pg);
  }

  delete m;
}






// =========================================================
// OPS

void OSD::handle_op(MOSDOp *op)
{
  const pg_t pgid = op->get_pg();
  PG *pg = _have_pg(pgid) ? _lock_pg(pgid):0;


  logger->set("buf", buffer_total_alloc);

  // update qlen stats
  hb_stat_ops++;
  hb_stat_qlen += pending_ops;


  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) {
    _unlock_pg(pgid);
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());

  // what kind of op?
  bool read = op->get_op() < 10;   // read, stat.  but not pull.

  if (!op->get_source().is_osd()) {
    // REGULAR OP (non-replication)

    // note original source
    op->set_client_inst( op->get_source_inst() );
    op->clear_payload();    // and hose encoded payload (in case we forward)

    // have pg?
    if (!pg) {
      dout(7) << "hit non-existent pg " 
              << pgid 
              << ", waiting" << dendl;
      waiting_for_pg[pgid].push_back(op);
      _unlock_pg(pgid);
      return;
    }

    // pg must be same-ish...
    if (read && !pg->same_for_read_since(op->get_map_epoch())) {
      dout(7) << "handle_rep_op pg changed " << pg->info.history
	      << " after " << op->get_map_epoch() 
	      << ", dropping" << dendl;
      assert(op->get_map_epoch() < osdmap->get_epoch());
      _unlock_pg(pgid);
      delete op;
      return;
    }
    if (!read && (pg->get_primary() != whoami ||
		  !pg->same_for_modify_since(op->get_map_epoch()))) {
      dout(7) << "handle_rep_op pg changed " << pg->info.history
	      << " after " << op->get_map_epoch() 
	      << ", dropping" << dendl;
      assert(op->get_map_epoch() < osdmap->get_epoch());
      _unlock_pg(pgid);
      delete op;
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
	  _unlock_pg(pgid);
          return;
        } else {
          dout(7) << *pg << " replay at " << op->get_version() << " <= " << pg->info.last_update 
                  << " for " << *op
                  << ", will queue for WRNOOP" << dendl;
        }
      }
      
      dout(7) << *pg << " not active (yet)" << dendl;
      pg->waiting_for_active.push_back(op);
      _unlock_pg(pgid);
      return;
    }
    
    // missing object?
    if (pg->is_missing_object(op->get_oid())) {
      pg->wait_for_missing_object(op->get_oid(), op);
      _unlock_pg(pgid);
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

    dout(7) << "handle_op " << *op << " in " << *pg << dendl;
    
    
    // balance reads?
    if (read &&
	g_conf.osd_balance_reads &&
	pg->get_acker() == whoami) {
      // test
      if (false) {
	if (pg->acting.size() > 1) {
	  int peer = pg->acting[1];
	  dout(-10) << "fwd client read op to osd" << peer << " for " << op->get_client() << " " << op->get_client_inst() << dendl;
	  messenger->send_message(op, osdmap->get_inst(peer));
	  _unlock_pg(pgid);
	  return;
	}
      }
      
      // am i above my average?
      float my_avg = hb_stat_qlen / hb_stat_ops;
      if (pending_ops > my_avg) {
	// is there a peer who is below my average?
	for (unsigned i=1; i<pg->acting.size(); ++i) {
	  int peer = pg->acting[i];
	  if (peer_qlen.count(peer) &&
	      peer_qlen[peer] < my_avg) {
	    // calculate a probability that we should redirect
	    float p = (my_avg - peer_qlen[peer]) / my_avg;             // this is dumb.
	    
	    if (drand48() <= p) {
	      // take the first one
	      dout(-10) << "my qlen " << pending_ops << " > my_avg " << my_avg
			<< ", p=" << p 
			<< ", fwd to peer w/ qlen " << peer_qlen[peer]
			<< " osd" << peer
			<< dendl;
	      messenger->send_message(op, osdmap->get_inst(peer));
	      _unlock_pg(pgid);
	      return;
	    }
	  }
	}
      }
    }

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
      _unlock_pg(pgid);
      delete op;
      return;
    }

    assert(pg->get_role() >= 0);
    dout(7) << "handle_rep_op " << op << " in " << *pg << dendl;
  }
  
  if (g_conf.osd_maxthreads < 1) {

    if (op->get_type() == MSG_OSD_OP)
      pg->do_op((MOSDOp*)op); // do it now
    else if (op->get_type() == MSG_OSD_OPREPLY)
      pg->do_op_reply((MOSDOpReply*)op);
    else 
      assert(0);

    _unlock_pg(pgid);
  } else {
    _unlock_pg(pgid);
    // queue for worker threads
    /*if (read) 
      enqueue_op(0, op);     // no locking needed for reads
    else 
    */
      enqueue_op(pgid, op);     
  }
}

void OSD::handle_op_reply(MOSDOpReply *op)
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

  if (g_conf.osd_maxthreads < 1) {
    PG *pg = _lock_pg(pgid);
    pg->do_op_reply(op); // do it now
    _unlock_pg(pgid);
  } else {
    enqueue_op(pgid, op);     // queue for worker threads
  }
}


/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(pg_t pgid, Message *op)
{
  while (pending_ops > g_conf.osd_max_opq) {
    dout(10) << "enqueue_op waiting for pending_ops " << pending_ops << " to drop to " << g_conf.osd_max_opq << dendl;
    op_queue_cond.Wait(osd_lock);
  }

  op_queue[pgid].push_back(op);
  pending_ops++;
  logger->set("opq", pending_ops);
  
  threadpool->put_op(pgid);
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(pg_t pgid)
{
  Message *op = 0;
  PG *pg = 0;

  osd_lock.Lock();
  {
    if (pgid) {
      // lock pg
      pg = _lock_pg(pgid);  
    }

    // get pending op
    list<Message*> &ls  = op_queue[pgid];
    assert(!ls.empty());
    op = ls.front();
    ls.pop_front();
    
    if (pgid) {
      dout(10) << "dequeue_op " << op << " write pg " << pgid 
               << ls.size() << " / " << (pending_ops-1) << " more pending" << dendl;
    } else {
      dout(10) << "dequeue_op " << op << " read "
               << ls.size() << " / " << (pending_ops-1) << " more pending" << dendl;
    }
    
    if (ls.empty())
      op_queue.erase(pgid);
  }
  osd_lock.Unlock();

  // do it
  if (op->get_type() == MSG_OSD_OP)
    pg->do_op((MOSDOp*)op); // do it now
  else if (op->get_type() == MSG_OSD_OPREPLY)
    pg->do_op_reply((MOSDOpReply*)op);
  else 
    assert(0);

  // finish
  osd_lock.Lock();
  {
    if (pgid) {
      // unlock pg
      _unlock_pg(pgid);
    }
    
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




// ==============================
// Object locking

//
// If the target object of the operation op is locked for writing by another client, the function puts op to the waiting queue waiting_for_wr_unlock
// returns true if object was locked, otherwise returns false
// 
bool OSD::block_if_wrlocked(MOSDOp* op)
{
  object_t oid = op->get_oid();

  entity_name_t source;
  int len = store->getattr(oid, "wrlock", &source, sizeof(entity_name_t));
  //cout << "getattr returns " << len << " on " << oid << dendl;

  if (len == sizeof(source) &&
      source != op->get_client()) {
    //the object is locked for writing by someone else -- add the op to the waiting queue      
    waiting_for_wr_unlock[oid].push_back(op);
    return true;
  }

  return false; //the object wasn't locked, so the operation can be handled right away
}



// ===============================
// OPS
