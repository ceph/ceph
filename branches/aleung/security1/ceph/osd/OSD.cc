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
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "
#define  derr(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cerr << g_clock.now() << " osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "

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
  dout(0) << "forcing remount" << endl;
  osd_lock.Lock();
  {
    store->umount();
    store->mount();
  }
  osd_lock.Unlock();
  dout(0) << "finished remount" << endl;
}
// </hack>


// cons/des

LogType osd_logtype;

OSD::OSD(int id, Messenger *m, MonMap *mm, char *dev) : timer(osd_lock)
{
  whoami = id;
  messenger = m;
  monmap = mm;
  monmap->prepare_mon_key();

  osdmap = 0;
  boot_epoch = 0;

  // create public/private keys
  //myPrivKey = esignPrivKey("crypto/esig1536.dat");
  myPrivKey = esignPrivKey("crypto/esig1023.dat");
  myPubKey = esignPubKey(myPrivKey);
  // write these out to disk

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
      dout(2) << "mkfs" << endl;
      store->mkfs();

      // make up a superblock
      //superblock.fsid = ???;
      superblock.whoami = whoami;
    }
    
    // mount.
    dout(2) << "mounting " << dev_path << endl;
    int r = store->mount();
    assert(r>=0);

    if (g_conf.osd_mkfs) {
      // age?
      if (g_conf.osd_age_time != 0) {
        dout(2) << "age" << endl;
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
      dout(2) << "boot" << endl;
      
      // read superblock
      read_superblock();

      // load up pgs (as they previously existed)
      load_pgs();

      dout(2) << "superblock: i am osd" << superblock.whoami << endl;
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

    // convert public key to string
    string key_str = pubToString(myPubKey);

    // ready cache
    cap_cache = new CapCache();
    
    // i'm ready!
    messenger->set_dispatcher(this);
    
    // announce to monitor i exist and have booted.
    int mon = monmap->pick_mon();
    // new boot message w/ public key
    messenger->send_message(new MOSDBoot(superblock, key_str), monmap->get_inst(mon));
    
    // start the heart
    timer.add_event_after(g_conf.osd_heartbeat_interval, new C_Heartbeat(this));
  }
  osd_lock.Unlock();

  //dout(0) << "osd_rep " << g_conf.osd_rep << endl;

  return 0;
}

int OSD::shutdown()
{
  dout(1) << "shutdown" << endl;

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
  dout(10) << "write_superblock " << superblock << endl;

  bufferlist bl;
  bl.append((char*)&superblock, sizeof(superblock));
  t.write(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
  if (bl.length() != sizeof(superblock)) {
    dout(10) << "read_superblock failed, r = " << r << ", i got " << bl.length() << " bytes, not " << sizeof(superblock) << endl;
    return -1;
  }

  bl.copy(0, sizeof(superblock), (char*)&superblock);
  
  dout(10) << "read_superblock " << superblock << endl;

  // load up "current" osdmap
  assert(!osdmap);
  osdmap = new OSDMap;
  bl.clear();
  get_map_bl(superblock.current_epoch, bl);
  osdmap->decode(bl);

  assert(whoami == superblock.whoami);  // fixme!
  return 0;
}


// security operations

// checks that the access rights in the cap are correct
inline bool OSD::check_request(MOSDOp *op, ExtCap *op_capability) {
  
  if (op_capability->get_type() == UNIX_GROUP ||
      op_capability->get_type() == BATCH) {
    // check if user is in group
    hash_t my_hash = op_capability->get_user_hash();

    // now we should have the group, is the client in it?
    //if (!(user_groups[my_hash].contains(op_capability->get_uid()))) {
    if (!(user_groups[my_hash].contains(op->get_user()))) {
      // do update to get new unix groups
      dout(1) << "User " << op->get_user() << " not in group "
	      << my_hash << endl;
      return false;
    }

  }
  // check users match
  else if (op->get_user() != op_capability->get_uid()) {
    dout(1) << "User did in cap did not match request" << endl;
    return false;
  }
  // check mode matches
  if (op->get_op() == OSD_OP_WRITE &&
      op_capability->mode() & FILE_MODE_W == 0) {
    dout(1) << "Write mode in cap did not match request" << endl;
    return false;
  }
  if (op->get_op() == OSD_OP_READ &&
      op_capability->mode() & FILE_MODE_R == 0) {
    dout(1) << "Read mode in cap did not match request" << endl;
    return false;
  }
  // check object matches
  if (op_capability->get_type() == USER_BATCH) {
    hash_t my_hash = op_capability->get_file_hash();
    if (! user_groups[my_hash].contains_inode(op->get_oid().ino)) {
      dout(1) << "File in request " << op->get_oid().ino
	      << " not in group " << my_hash << " file in cap is "
	      << op_capability->get_ino() << endl;
      return false;
    }
  }
  else if (op->get_oid().ino != op_capability->get_ino()) {
    dout(1) << "File in cap did not match request" << endl;
    return false;
  }
  return true;
}

// gets group information from client (will block!)
void OSD::update_group(entity_inst_t client, hash_t my_hash, MOSDOp *op) {
  // set up reply
  MOSDUpdate *update = new MOSDUpdate(my_hash);
  Cond cond;
  //cout << "OSD Requesting list update" << endl;
  
  // if no one has already requested the ticket
  if (update_waiter_op.count(my_hash) == 0) {
    dout(1) << "update_group requesting update for hash " << my_hash << endl;
      // send it
    messenger->send_message(update, client);
  } else {
    // don't request, someone else already did.  just wait!
    dout(1) << "update_group waiting for update for hash " << my_hash << endl;
  }
  
  // wait for reply
  update_waiter_op[my_hash].push_back( op );
  
  // naively assume we'll get an update FIXME
  //while (user_groups.count(my_hash) == 0) { 
  //  cond.Wait(osd_lock);
  //}
  
}

// gets reply for group and wakes up waiters
void OSD::handle_osd_update_reply(MOSDUpdateReply *m) {

  // store the new list into group
  hash_t my_hash = m->get_user_hash();

  dout(10) << "handle_osd_update_reply for " << my_hash << endl;
  
  // verify
  if (m->verify_list(monmap->get_key()))
    dout(1) << "List verification succeeded" << endl;
  else
    dout(1) << "List verification failed" << endl;

  // add the new list to our cache
  if (g_conf.mds_group == 3) {
    user_groups[my_hash].set_inode_list(m->get_file_list());

    dout(3) << "Received a group update for " << my_hash << endl;
    for (list<inodeno_t>::iterator ii = m->get_file_list().begin();
	 ii != m->get_file_list().end();
	 ii++) {
      dout(3) << my_hash << " contains " << (*ii) << endl;
    }
  }
  else
    user_groups[my_hash].set_list(m->get_list());

  // wait up the waiter(s)
  take_waiters(update_waiter_op[my_hash]);

  update_waiter_op.erase(my_hash);
}

// assumes the request and cap contents has already been checked
inline bool OSD::verify_cap(ExtCap *cap) {

  // have i already verified this cap?
  if (!cap_cache->prev_verified(cap->get_id())) {

    dout(10) << "OSD verifying new cap" << endl;
    // actually verify
    if (cap->verif_extcap(monmap->get_key())) {
      // cache the verification
      cap_cache->insert(cap);
    }
    else
      return false;
  }
  else
    dout(10) << "OSD already cached cap" << endl;
  return true;
}

// object locks

PG *OSD::lock_pg(pg_t pgid) 
{
  osd_lock.Lock();
  PG *pg = _lock_pg(pgid);
  osd_lock.Unlock();
  return pg;
}

PG *OSD::_lock_pg(pg_t pgid)
{
  assert(pg_map.count(pgid));

  if (pg_lock.count(pgid)) {
    Cond c;
    dout(15) << "lock_pg " << pgid << " waiting as " << &c << endl;
    //cerr << "lock_pg " << pgid << " waiting as " << &c << endl;

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

  dout(15) << "lock_pg " << pgid << endl;
  pg_lock.insert(pgid);

  return pg_map[pgid];  
}

void OSD::unlock_pg(pg_t pgid) 
{
  osd_lock.Lock();
  _unlock_pg(pgid);
  osd_lock.Unlock();
}

void OSD::_unlock_pg(pg_t pgid) 
{
  // unlock
  assert(pg_lock.count(pgid));
  pg_lock.erase(pgid);

  if (pg_lock_waiters.count(pgid)) {
    // someone is in line
    Cond *c = pg_lock_waiters[pgid].front();
    assert(c);
    dout(15) << "unlock_pg " << pgid << " waking up next guy " << c << endl;
    c->Signal();
  } else {
    // nobody waiting
    dout(15) << "unlock_pg " << pgid << endl;
  }
}

void OSD::_remove_pg(pg_t pgid) 
{
  dout(10) << "_remove_pg " << pgid << endl;

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
  
  // hose from memory
  delete pg_map[pgid];
  pg_map.erase(pgid);
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
	  << endl;
  
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
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << endl;
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
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << endl;
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
    dout(10) << "ping from " << m->get_source() << endl;
    delete m;
    break;

    // -- don't need OSDMap --

    /*
    // host monitor
  case MSG_PING_ACK:
  case MSG_FAILURE_ACK:
    monitor->proc_message(m);
    break;
    */

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
        dout(7) << "no OSDMap, not booted" << endl;
        waiting_for_osdmap.push_back(m);
        break;
      }
      
      // down?
      if (osdmap->is_down(whoami)) {
        dout(7) << "i am marked down, dropping " << *m << endl;
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
	// for updating security groups
      case MSG_OSD_UPDATE_REPLY:
	handle_osd_update_reply((MOSDUpdateReply*)m);
	break;
        
        
      default:
        dout(1) << " got unknown message " << m->get_type() << endl;
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
    dout(0) << "ms_handle_failure " << inst << " on " << *m << endl;
    exit(0);
  }

  if (dest.is_osd()) {
    // failed osd.  drop message, report to mon.
    int mon = monmap->pick_mon();
    dout(0) << "ms_handle_failure " << inst 
            << ", dropping and reporting to mon" << mon 
	    << " " << *m
            << endl;
    messenger->send_message(new MOSDFailure(inst, osdmap->get_epoch()),
                            monmap->get_inst(mon));
    delete m;
  } else if (dest.is_mon()) {
    // resend to a different monitor.
    int mon = monmap->pick_mon(true);
    dout(0) << "ms_handle_failure " << inst 
            << ", resending to mon" << mon 
	    << " " << *m
            << endl;
    messenger->send_message(m, monmap->get_inst(mon));
  }
  else {
    // client?
    dout(0) << "ms_handle_failure " << inst 
            << ", dropping " << *m << endl;
    delete m;
  }
}




void OSD::handle_osd_ping(MOSDPing *m)
{
  dout(20) << "osdping from " << m->get_source() << endl;
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
 * Takes an OSDMap message
 */
void OSD::handle_osd_map(MOSDMap *m)
{
  wait_for_no_ops();
  
  assert(osd_lock.is_locked());

  ObjectStore::Transaction t;
  
  // checks if I ALREADY had an OSD map
  if (osdmap) {
    dout(3) << "handle_osd_map epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "], i have " << osdmap->get_epoch()
            << endl;
  } else {
    dout(3) << "handle_osd_map epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "], i have none"
            << endl;
    osdmap = new OSDMap;
    boot_epoch = m->get_last(); // hrm...?
  }

  logger->inc("mapmsg");

  // parses all of the available maps to see if
  // ive seen it, if not store it?
  // store them?
  for (map<epoch_t,bufferlist>::iterator p = m->maps.begin();
       p != m->maps.end();
       p++) {
    // checks to see if I have seen this map?
    object_t oid = get_osdmap_object_name(p->first);
    if (store->exists(oid)) {
      dout(10) << "handle_osd_map already had full map epoch " << p->first << endl;
      logger->inc("mapfdup");
      bufferlist bl;
      get_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << endl;
      continue;
    }

    // if I have not already seen the map then store it?
    dout(10) << "handle_osd_map got full map epoch " << p->first << endl;
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
      dout(10) << "handle_osd_map already had incremental map epoch " << p->first << endl;
      logger->inc("mapidup");
      bufferlist bl;
      get_inc_map_bl(p->first, bl);
      dout(10) << " .. it is " << bl.length() << " bytes" << endl;
      continue;
    }

    dout(10) << "handle_osd_map got incremental map epoch " << p->first << endl;
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
  // applies all of the new maps until were up to date?
  while (cur < superblock.newest_map) {
    bufferlist bl;
    // if there is a newer (by 1) inc map OR I have a newer map
    if (m->incremental_maps.count(cur+1) ||
        store->exists(get_inc_osdmap_object_name(cur+1))) {
      dout(10) << "handle_osd_map decoding inc map epoch " << cur+1 << endl;
      
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
          {
            list<PG::RepOpGather*> ls;  // do async; repop_ack() may modify pg->repop_gather
            for (map<tid_t,PG::RepOpGather*>::iterator p = pg->repop_gather.begin();
                 p != pg->repop_gather.end();
                 p++) {
              //dout(-1) << "checking repop tid " << p->first << endl;
              if (p->second->waitfor_ack.count(osd) ||
                  p->second->waitfor_commit.count(osd)) 
                ls.push_back(p->second);
            }
            for (list<PG::RepOpGather*>::iterator p = ls.begin();
                 p != ls.end();
                 p++)
              repop_ack(pg, *p, -1, true, osd);
          }
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
      dout(10) << "handle_osd_map decoding full map epoch " << cur+1 << endl;
      bufferlist bl;
      if (m->maps.count(cur+1))
        bl = m->maps[cur+1];
      else
        get_map_bl(cur+1, bl);
      osdmap->decode(bl);

      // FIXME BUG: need to notify messenger of ups/downs!!
    }
    else {
      dout(10) << "handle_osd_map missing epoch " << cur+1 << endl;
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
          << endl;
  
  if (osdmap->is_mkfs()) {
    ps_t maxps = 1ULL << osdmap->get_pg_bits();
    ps_t maxlps = 1ULL << osdmap->get_localized_pg_bits();
    dout(1) << "mkfs on " << osdmap->get_pg_bits() << " bits, " << maxps << " pgs" << endl;
    assert(osdmap->get_epoch() == 1);

    //cerr << "osdmap " << osdmap->get_ctime() << " logger start " << logger->get_start() << endl;
    logger->set_start( osdmap->get_ctime() );

    assert(g_conf.osd_mkfs);  // make sure we did a mkfs!

    // create PGs
    for (int nrep = 1; 
         nrep <= MIN(g_conf.num_osd, g_conf.osd_max_rep);    // for low osd counts..  hackish bleh
         nrep++) {
      for (ps_t ps = 0; ps < maxps; ++ps) {
	vector<int> acting;
	pg_t pgid = osdmap->ps_nrep_to_pg(ps, nrep);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	if (role < 0) continue;
	
	PG *pg = create_pg(pgid, t);
	pg->set_role(role);
	pg->acting.swap(acting);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_since = 
	  pg->info.history.same_primary_since = 
	    pg->info.history.same_acker_since = osdmap->get_epoch();
	pg->activate(t);
	
	dout(7) << "created " << *pg << endl;
      }

      for (ps_t ps = 0; ps < maxlps; ++ps) {
	// local PG too
	vector<int> acting;
	pg_t pgid = osdmap->ps_osd_nrep_to_pg(ps, whoami, nrep);
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	int role = osdmap->calc_pg_role(whoami, acting, nrep);
	
	PG *pg = create_pg(pgid, t);
	pg->acting.swap(acting);
	pg->set_role(role);
	pg->last_epoch_started_any = 
	  pg->info.last_epoch_started = 
	  pg->info.history.same_primary_since = 
	  pg->info.history.same_acker_since = 
	  pg->info.history.same_since = osdmap->get_epoch();
	pg->activate(t);
	
	dout(7) << "created " << *pg << endl;
      }
    }

    dout(1) << "mkfs done, created " << pg_map.size() << " pgs" << endl;

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
        // apply repops
        for (map<tid_t,PG::RepOpGather*>::iterator p = pg->repop_gather.begin();
             p != pg->repop_gather.end();
             p++) {
          if (!p->second->applied)
            apply_repop(pg, p->second);
          delete p->second->op;
          delete p->second;
        }
        pg->repop_gather.clear();
        
        // and repop waiters
        for (map<tid_t, list<Message*> >::iterator p = pg->waiting_for_repop.begin();
             p != pg->waiting_for_repop.end();
             p++)
          for (list<Message*>::iterator pm = p->second.begin();
               pm != p->second.end();
               pm++)
            delete *pm;
        pg->waiting_for_repop.clear();
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
          
          // take object waiters
          for (hash_map<object_t, list<Message*> >::iterator it = pg->waiting_for_missing_object.begin();
               it != pg->waiting_for_missing_object.end();
               it++)
            take_waiters(it->second);
          pg->waiting_for_missing_object.clear();
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
            dout(1) << *pg << " is crashed" << endl;
          }
        }
        
        // my role changed.
        dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
                 << ", role " << oldrole << " -> " << role << endl; 
        
      } else {
        // no role change.
        // did primary change?
        if (pg->get_primary() != oldprimary) {    
          // we need to announce
          pg->state_set(PG::STATE_STRAY);
          
          dout(10) << *pg << " " << oldacting << " -> " << pg->acting 
                   << ", acting primary " 
                   << oldprimary << " -> " << pg->get_primary() 
                   << endl;
        } else {
          // primary is the same.
          if (role == 0) {
            // i am (still) primary. but my replica set changed.
            pg->state_clear(PG::STATE_CLEAN);
            pg->state_clear(PG::STATE_REPLAY);

            dout(10) << *pg << " " << oldacting << " -> " << pg->acting
                     << ", replicas changed" << endl;
          }
        }
      }
      

      _unlock_pg(pgid);
    }
  }
}

void OSD::activate_map(ObjectStore::Transaction& t)
{
  dout(7) << "activate_map version " << osdmap->get_epoch() << endl;

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
           << " to " << inst << endl;
  
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
      //dout(10) << "get_map " << epoch << " full " << e << endl;
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
    //dout(10) << "get_map " << epoch << " inc " << e << endl;
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
    dout(7) << "require_current_map epoch " << ep << " < " << osdmap->get_epoch() << endl;
    delete m;   // discard and ignore.
    return false;
  }

  // newer map?
  if (ep > osdmap->get_epoch()) {
    dout(7) << "require_current_map epoch " << ep << " > " << osdmap->get_epoch() << endl;
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
  dout(10) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ")" << endl;

  // newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "  from newer map epoch " << epoch << " > " << osdmap->get_epoch() << endl;
    wait_for_new_map(m);
    return false;
  }

  if (epoch < boot_epoch) {
    dout(7) << "  from pre-boot epoch " << epoch << " < " << boot_epoch << endl;
    delete m;
    return false;
  }

  return true;
}




// ======================================================
// REPLICATION

// PG

bool OSD::pg_exists(pg_t pgid) 
{
  return store->collection_exists(pgid);
}

PG *OSD::create_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  if (pg_map.count(pgid)) {
    dout(0) << "create_pg on " << pgid << ", already have " << *pg_map[pgid] << endl;
  }
  assert(pg_map.count(pgid) == 0);
  assert(!pg_exists(pgid));

  PG *pg = new PG(this, pgid);
  pg_map[pgid] = pg;

  t.create_collection(pgid);

  return pg;
}




PG *OSD::get_pg(pg_t pgid)
{
  if (pg_map.count(pgid))
    return pg_map[pgid];
  return 0;
}

void OSD::load_pgs()
{
  dout(10) << "load_pgs" << endl;
  assert(pg_map.empty());

  list<coll_t> ls;
  store->list_collections(ls);

  for (list<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    pg_t pgid = *it;

    PG *pg = new PG(this, pgid);
    pg_map[pgid] = pg;

    // read pg info
    store->collection_getattr(pgid, "info", &pg->info, sizeof(pg->info));
    
    // read pg log
    pg->read_log(store);

    // generate state for current mapping
    int nrep = osdmap->pg_to_acting_osds(pgid, pg->acting);
    int role = osdmap->calc_pg_role(whoami, pg->acting, nrep);
    pg->set_role(role);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << endl;
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
           << endl;

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
                << " from " << acting << " -> " << last << endl;
      h.same_since = e+1;
    }

    // primary change?
    if (!(!acting.empty() && !last.empty() && acting[0] == last[0]) &&
        e <= h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e+1 << endl;
      h.same_primary_since = e+1;
    
      if (g_conf.osd_rep == OSD_REP_PRIMARY)
        h.same_acker_since = h.same_primary_since;
    }

    // acker change?
    if (g_conf.osd_rep != OSD_REP_PRIMARY) {
      if (!(!acting.empty() && !last.empty() && acting[acting.size()-1] == last[last.size()-1]) &&
          e <= h.same_acker_since) {
        dout(15) << "project_pg_history " << pgid << " acker changed in " << e+1 << endl;
        h.same_acker_since = e+1;
      }
    }

    if (h.same_since > e &&
        h.same_primary_since > e &&
        h.same_acker_since > e) break;
  }

  dout(15) << "project_pg_history end " << h << endl;
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
      dout(7) << "do_notify osd" << it->first << " is self, skipping" << endl;
      continue;
    }
    dout(7) << "do_notify osd" << it->first << " on " << it->second.size() << " PGs" << endl;
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
            << " on " << pit->second.size() << " PGs" << endl;

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
  dout(7) << "handle_pg_notify from " << m->get_source() << endl;
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
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << endl;
        continue;
      }
      
      // ok, create PG!
      pg = create_pg(pgid, t);
      osdmap->pg_to_acting_osds(pgid, pg->acting);
      pg->set_role(0);
      pg->info.history = history;

      pg->last_epoch_started_any = it->last_epoch_started;
      pg->build_prior();

      t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
      
      dout(10) << *pg << " is new" << endl;
    
      // kick any waiters
      if (waiting_for_pg.count(pgid)) {
        take_waiters(waiting_for_pg[pgid]);
        waiting_for_pg.erase(pgid);
      }

      _lock_pg(pgid);
    } else {
      // already had it.  am i (still) the primary?
      pg = _lock_pg(pgid);
      if (m->get_epoch() < pg->info.history.same_primary_since) {
        dout(10) << *pg << " handle_pg_notify primary changed in "
                 << pg->info.history.same_primary_since
                 << " (msg from " << m->get_epoch() << ")" << endl;
        _unlock_pg(pgid);
        continue;
      }
    }

    // ok!
    
    // stray?
    bool acting = pg->is_acting(from);
    if (!acting && (*it).last_epoch_started > 0) {
      dout(10) << *pg << " osd" << from << " has stray content: " << *it << endl;
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
                 << "): " << *it << endl;
        if (pg->is_all_clean()) {
          dout(-10) << *pg << " now clean on all replicas" << endl;
          pg->state_set(PG::STATE_CLEAN);
          pg->clean_replicas();
        }
      } else {
        // hmm, maybe keep an eye out for cases where we see this, but peer should happen.
        dout(10) << *pg << " already had notify info from osd" << from << ": " << *it << endl;
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
    dout(10) << "handle_pg_log don't have pg " << pgid << ", dropping" << endl;
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
            << endl;
    delete m;
    return;
  }

  dout(7) << "handle_pg_log " << *pg 
          << " got " << m->log << " " << m->missing
          << " from " << m->get_source() << endl;

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
    dout(10) << *pg << " got " << m->log << " " << m->missing << endl;

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
  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << endl;
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
                 << history.same_primary_since << " (msg from " << m->get_epoch() << ")" << endl;
        continue;
      }

      // get active rush mapping
      vector<int> acting;
      int nrep = osdmap->pg_to_acting_osds(pgid, acting);
      int role = osdmap->calc_pg_role(whoami, acting, nrep);

      if (role < 0) {
        dout(10) << " pg " << pgid << " dne, and i am not an active replica" << endl;
        PG::Info empty(pgid);
        notify_list[from].push_back(empty);
        continue;
      }
      assert(role > 0);

      ObjectStore::Transaction t;
      pg = create_pg(pgid, t);
      pg->acting.swap( acting );
      pg->set_role(role);
      pg->info.history = history;

      t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
      store->apply_transaction(t);

      dout(10) << *pg << " dne (before), but i am role " << role << endl;
      _lock_pg(pgid);
    } else {
      pg = _lock_pg(pgid);
      
      // same primary?
      if (m->get_epoch() < pg->info.history.same_since) {
        dout(10) << *pg << " handle_pg_query primary changed in "
                 << pg->info.history.same_since
                 << " (msg from " << m->get_epoch() << ")" << endl;
        _unlock_pg(pgid);
        continue;
      }
    }

    // ok, process query!
    assert(!pg->acting.empty());
    assert(from == pg->acting[0]);

    if (it->second.type == PG::Query::INFO) {
      // info
      dout(10) << *pg << " sending info" << endl;
      notify_list[from].push_back(pg->info);
    } else {
      MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->get_pgid());
      m->info = pg->info;
      m->missing = pg->missing;

      if (it->second.type == PG::Query::LOG) {
        dout(10) << *pg << " sending info+missing+log since split " << it->second.split
                 << " from floor " << it->second.floor 
                 << endl;
        if (!m->log.copy_after_unless_divergent(pg->log, it->second.split, it->second.floor)) {
          dout(10) << *pg << "  divergent, sending backlog" << endl;
          it->second.type = PG::Query::BACKLOG;
        }
      }

      if (it->second.type == PG::Query::BACKLOG) {
        dout(10) << *pg << " sending info+missing+backlog" << endl;
        if (pg->log.backlog) {
          m->log = pg->log;
        } else {
          pg->generate_backlog();
          m->log = pg->log;
          pg->drop_backlog();
        }
      } 
      else if (it->second.type == PG::Query::FULLLOG) {
        dout(10) << *pg << " sending info+missing+full log" << endl;
        m->log.copy_non_backlog(pg->log);
      }

      dout(10) << *pg << " sending " << m->log << " " << m->missing << endl;
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
  dout(7) << "handle_pg_remove from " << m->get_source() << endl;
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  for (set<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = *it;
    PG *pg;

    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << endl;
      continue;
    }

    pg = _lock_pg(pgid);

    dout(10) << *pg << " removing." << endl;
    assert(pg->get_role() == -1);
    
    _remove_pg(pgid);

    // unlock.  there shouldn't be any waiters, since we're a stray, and pg is presumably clean0.
    assert(pg_lock_waiters.count(pgid) == 0);
    _unlock_pg(pgid);
  }

  delete m;
}






/*** RECOVERY ***/

/** pull - request object from a peer
 */
void OSD::pull(PG *pg, object_t oid)
{
  assert(pg->missing.loc.count(oid));
  eversion_t v = pg->missing.missing[oid];
  int osd = pg->missing.loc[oid];
  
  dout(7) << *pg << " pull " << oid
          << " v " << v 
          << " from osd" << osd
          << endl;

  // send op
  tid_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(messenger->get_myinst(), 0, tid,
                          oid, pg->get_pgid(),
                          osdmap->get_epoch(),
                          OSD_OP_PULL);
  op->set_version(v);
  messenger->send_message(op, osdmap->get_inst(osd));
  
  // take note
  assert(pg->objects_pulling.count(oid) == 0);
  num_pulling++;
  pg->objects_pulling[oid] = v;
}


/** push - send object to a peer
 */
void OSD::push(PG *pg, object_t oid, int dest)
{
  // read data+attrs
  bufferlist bl;
  eversion_t v;
  int vlen = sizeof(v);
  map<string,bufferptr> attrset;
  
  ObjectStore::Transaction t;
  t.read(oid, 0, 0, &bl);
  t.getattr(oid, "version", &v, &vlen);
  t.getattrs(oid, attrset);
  unsigned tr = store->apply_transaction(t);
  
  assert(tr == 0);  // !!!

  // ok
  dout(7) << *pg << " push " << oid << " v " << v 
          << " size " << bl.length()
          << " to osd" << dest
          << endl;

  logger->inc("r_push");
  logger->inc("r_pushb", bl.length());
  
  // send
  MOSDOp *op = new MOSDOp(messenger->get_myinst(), 0, ++last_tid,
                          oid, pg->info.pgid, osdmap->get_epoch(), 
                          OSD_OP_PUSH); 
  op->set_offset(0);
  op->set_length(bl.length());
  op->set_data(bl);   // note: claims bl, set length above here!
  op->set_version(v);
  op->set_attrset(attrset);
  
  messenger->send_message(op, osdmap->get_inst(dest));
}


/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void OSD::op_pull(MOSDOp *op, PG *pg)
{
  const object_t oid = op->get_oid();
  const eversion_t v = op->get_version();
  int from = op->get_source().num();

  dout(7) << *pg << " op_pull " << oid << " v " << op->get_version()
          << " from " << op->get_source()
          << endl;

  // is a replica asking?  are they missing it?
  if (pg->is_primary()) {
    // primary
    assert(pg->peer_missing.count(from));  // we had better know this, from the peering process.

    if (!pg->peer_missing[from].is_missing(oid)) {
      dout(7) << *pg << " op_pull replica isn't actually missing it, we must have already pushed to them" << endl;
      delete op;
      return;
    }

    // do we have it yet?
    if (waitfor_missing_object(op, pg))
      return;
  } else {
    // non-primary
    if (pg->missing.is_missing(oid)) {
      dout(7) << *pg << " op_pull not primary, and missing " << oid << ", ignoring" << endl;
      delete op;
      return;
    }
  }
    
  // push it back!
  push(pg, oid, op->get_source().num());
}


/** op_push
 * NOTE: called from opqueue.
 */
void OSD::op_push(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();
  eversion_t v = op->get_version();

  if (!pg->missing.is_missing(oid)) {
    dout(7) << *pg << " op_push not missing " << oid << endl;
    return;
  }
  
  dout(7) << *pg << " op_push " 
          << oid 
          << " v " << v 
          << " size " << op->get_length() << " " << op->get_data().length()
          << endl;

  assert(op->get_data().length() == op->get_length());
  
  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(oid);  // in case old version exists
  t.write(oid, 0, op->get_length(), op->get_data());
  t.setattrs(oid, op->get_attrset());
  t.collection_add(pg->info.pgid, oid);

  // close out pull op?
  num_pulling--;
  if (pg->objects_pulling.count(oid))
    pg->objects_pulling.erase(oid);
  pg->missing.got(oid, v);


  // raise last_complete?
  assert(pg->log.complete_to != pg->log.log.end());
  while (pg->log.complete_to != pg->log.log.end()) {
    if (pg->missing.missing.count(pg->log.complete_to->oid)) break;
    if (pg->info.last_complete < pg->log.complete_to->version)
      pg->info.last_complete = pg->log.complete_to->version;
    pg->log.complete_to++;
  }
  dout(10) << *pg << " last_complete now " << pg->info.last_complete << endl;
  
  
  // apply to disk!
  t.collection_setattr(pg->info.pgid, "info", &pg->info, sizeof(pg->info));
  unsigned r = store->apply_transaction(t);
  assert(r == 0);



  // am i primary?  are others missing this too?
  if (pg->is_primary()) {
    for (unsigned i=1; i<pg->acting.size(); i++) {
      int peer = pg->acting[i];
      assert(pg->peer_missing.count(peer));
      if (pg->peer_missing[peer].is_missing(oid)) {
        // ok, push it, and they (will) have it now.
        pg->peer_missing[peer].got(oid, v);
        push(pg, oid, peer);
      }
    }
  }

  // continue recovery
  pg->do_recovery();
  
  // kick waiters
  if (pg->waiting_for_missing_object.count(oid)) 
    take_waiters(pg->waiting_for_missing_object[oid]);

  delete op;
}




// op_rep_modify

// commit (to disk) callback
class C_OSD_RepModifyCommit : public Context {
public:
  OSD *osd;
  MOSDOp *op;
  int destosd;

  eversion_t pg_last_complete;

  Mutex lock;
  Cond cond;
  bool acked;
  bool waiting;

  C_OSD_RepModifyCommit(OSD *o, MOSDOp *oo, int dosd, eversion_t lc) : 
    osd(o), op(oo), destosd(dosd), pg_last_complete(lc),
    acked(false), waiting(false) { }
  void finish(int r) {
    lock.Lock();
    assert(!waiting);
    while (!acked) {
      waiting = true;
      cond.Wait(lock);
    }
    assert(acked);
    lock.Unlock();
    osd->op_rep_modify_commit(op, destosd, pg_last_complete);
  }
  void ack() {
    lock.Lock();
    assert(!acked);
    acked = true;
    if (waiting) cond.Signal();

    // discard my reference to buffer
    op->get_data().clear();

    lock.Unlock();
  }
};

void OSD::op_rep_modify_commit(MOSDOp *op, int ackerosd, eversion_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op
           << ", sending commit to osd" << ackerosd
           << endl;
  MOSDOpReply *commit = new MOSDOpReply(op, 0, osdmap->get_epoch(), true);
  commit->set_pg_complete_thru(last_complete);
  messenger->send_message(commit, osdmap->get_inst(ackerosd));
  delete op;
}

// process a modification operation

class C_OSD_WriteCommit : public Context {
public:
  OSD *osd;
  pg_t pgid;
  tid_t rep_tid;
  eversion_t pg_last_complete;
  C_OSD_WriteCommit(OSD *o, pg_t p, tid_t rt, eversion_t lc) : osd(o), pgid(p), rep_tid(rt), pg_last_complete(lc) {}
  void finish(int r) {
    osd->op_modify_commit(pgid, rep_tid, pg_last_complete);
  }
};


/** op_rep_modify
 * process a replicated modify.
 * NOTE: called from opqueue.
 */
void OSD::op_rep_modify(MOSDOp *op, PG *pg)
{ 
  object_t oid = op->get_oid();
  eversion_t nv = op->get_version();

  const char *opname = MOSDOp::get_opname(op->get_op());

  // check crev
  objectrev_t crev = 0;
  store->getattr(oid, "crev", (char*)&crev, sizeof(crev));

  dout(10) << "op_rep_modify " << opname 
           << " " << oid 
           << " v " << nv 
           << " " << op->get_offset() << "~" << op->get_length()
           << " in " << *pg
           << endl;  
  
  // we better not be missing this.
  assert(!pg->missing.is_missing(oid));

  // prepare our transaction
  ObjectStore::Transaction t;

  // am i acker?
  PG::RepOpGather *repop = 0;
  int ackerosd = pg->acting[0];

  if ((g_conf.osd_rep == OSD_REP_CHAIN || g_conf.osd_rep == OSD_REP_SPLAY)) {
    ackerosd = pg->get_acker();
  
    if (pg->is_acker()) {
      // i am tail acker.
      if (pg->repop_gather.count(op->get_rep_tid())) {
        repop = pg->repop_gather[ op->get_rep_tid() ];
      } else {
        repop = new_repop_gather(pg, op);
      }
      
      // infer ack from source
      int fromosd = op->get_source().num();
      get_repop_gather(repop);
      {
        //assert(repop->waitfor_ack.count(fromosd));   // no, we may come thru here twice.
        repop->waitfor_ack.erase(fromosd);
      }
      put_repop_gather(pg, repop);

      // prepare dest socket
      //messenger->prepare_send_message(op->get_client());
    }

    // chain?  forward?
    if (g_conf.osd_rep == OSD_REP_CHAIN && !pg->is_acker()) {
      // chain rep, not at the tail yet.
      int myrank = osdmap->calc_pg_rank(whoami, pg->acting);
      int next = myrank+1;
      if (next == (int)pg->acting.size())
	next = 1;
      issue_repop(pg, op, pg->acting[next]);	
    }
  }

  // do op?
  C_OSD_RepModifyCommit *oncommit = 0;

  logger->inc("r_wr");
  logger->inc("r_wrb", op->get_length());
  
  if (repop) {
    // acker.  we'll apply later.
    if (op->get_op() != OSD_OP_WRNOOP) {
      prepare_log_transaction(repop->t, op, nv, crev, op->get_rev(), pg, op->get_pg_trim_to());
      prepare_op_transaction(repop->t, op, nv, crev, op->get_rev(), pg);
    }
  } else {
    // middle|replica.
    if (op->get_op() != OSD_OP_WRNOOP) {
      prepare_log_transaction(t, op, nv, crev, op->get_rev(), pg, op->get_pg_trim_to());
      prepare_op_transaction(t, op, nv, crev, op->get_rev(), pg);
    }

    oncommit = new C_OSD_RepModifyCommit(this, op, ackerosd, pg->info.last_complete);

    // apply log update. and possibly update itself.
    unsigned tr = store->apply_transaction(t, oncommit);
    if (tr != 0 &&   // no errors
        tr != 2) {   // or error on collection_add
      cerr << "error applying transaction: r = " << tr << endl;
      assert(tr == 0);
    }
  }
  
  // ack?
  if (repop) {
    // (logical) local ack.  this may induce the actual update.
    get_repop_gather(repop);
    {
      assert(repop->waitfor_ack.count(whoami));
      repop->waitfor_ack.erase(whoami);
    }
    put_repop_gather(pg, repop);
  } 
  else {
    // send ack to acker?
    if (g_conf.osd_rep != OSD_REP_CHAIN) {
      MOSDOpReply *ack = new MOSDOpReply(op, 0, osdmap->get_epoch(), false);
      messenger->send_message(ack, osdmap->get_inst(ackerosd));
    }

    // ack myself.
    assert(oncommit);
    oncommit->ack(); 
  }
}


// =========================================================
// OPS

void OSD::handle_op(MOSDOp *op)
{
  const pg_t pgid = op->get_pg();
  PG *pg = get_pg(pgid);


  logger->set("buf", buffer_total_alloc);

  // update qlen stats
  hb_stat_ops++;
  hb_stat_qlen += pending_ops;


  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

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
              << ", waiting" << endl;
      waiting_for_pg[pgid].push_back(op);
      return;
    }
    
    if (read) {
      // read. am i the (same) acker?
      if (//pg->get_acker() != whoami ||
          op->get_map_epoch() < pg->info.history.same_acker_since) {
        dout(7) << "acting acker is osd" << pg->get_acker()
                << " since " << pg->info.history.same_acker_since 
                << ", dropping" << endl;
        assert(op->get_map_epoch() < osdmap->get_epoch());
        delete op;
        return;
      }
    } else {
      // write. am i the (same) primary?
      if (pg->get_primary() != whoami ||
          op->get_map_epoch() < pg->info.history.same_primary_since) {
        dout(7) << "acting primary is osd" << pg->get_primary()
                << " since " << pg->info.history.same_primary_since 
                << ", dropping" << endl;
        assert(op->get_map_epoch() < osdmap->get_epoch());
        delete op;
        return;
      }
    }
    
    // must be active.
    if (!pg->is_active()) {
      // replay?
      if (op->get_version().version > 0) {
        if (op->get_version() > pg->info.last_update) {
          dout(7) << *pg << " queueing replay at " << op->get_version()
                  << " for " << *op << endl;
          pg->replay_queue[op->get_version()] = op;
          return;
        } else {
          dout(7) << *pg << " replay at " << op->get_version() << " <= " << pg->info.last_update 
                  << " for " << *op
                  << ", will queue for WRNOOP" << endl;
        }
      }
      
      dout(7) << *pg << " not active (yet)" << endl;
      pg->waiting_for_active.push_back(op);
      return;
    }
    
    // missing object?
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
		   << ", pulling" << endl;
	  pull(pg, moid);
	  pg->waiting_for_missing_object[moid].push_back(op);
	  return;
	} 
	  
	dout(10) << "handle_op read on " << op->get_oid()
		 << ", have " << loid
		 << ", don't need missing " << moid 
		 << endl;
      }
    } else {
      // live revision.  easy.
      if (op->get_op() != OSD_OP_PUSH &&
	  waitfor_missing_object(op, pg)) return;
    }

    dout(7) << "handle_op " << *op << " in " << *pg << endl;
    
    
    // balance reads?
    if (read &&
	g_conf.osd_balance_reads &&
	pg->get_acker() == whoami) {
      // test
      if (false) {
	if (pg->acting.size() > 1) {
	  int peer = pg->acting[1];
	  dout(-10) << "fwd client read op to osd" << peer << " for " << op->get_client() << " " << op->get_client_inst() << endl;
	  messenger->send_message(op, osdmap->get_inst(peer));
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
			<< endl;
	      messenger->send_message(op, osdmap->get_inst(peer));
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
               << " pgid " << pgid << " dne" << endl;
      delete op;
      //assert(0); // wtf, shouldn't happen.
      return;
    }
    
    // check osd map: same set, or primary+acker?
    if (g_conf.osd_rep == OSD_REP_CHAIN &&
        op->get_map_epoch() < pg->info.history.same_since) {
      dout(10) << "handle_rep_op pg changed " << pg->info.history
               << " after " << op->get_map_epoch() 
               << ", dropping" << endl;
      delete op;
      return;
    }
    if (g_conf.osd_rep != OSD_REP_CHAIN &&
        (op->get_map_epoch() < pg->info.history.same_primary_since ||
         op->get_map_epoch() < pg->info.history.same_acker_since)) {
      dout(10) << "handle_rep_op pg primary|acker changed " << pg->info.history
               << " after " << op->get_map_epoch() 
               << ", dropping" << endl;
      delete op;
      return;
    }

    assert(pg->get_role() >= 0);
    dout(7) << "handle_rep_op " << op << " in " << *pg << endl;
  }
  
  if (g_conf.osd_maxthreads < 1) {
    _lock_pg(pgid);
    do_op(op, pg); // do it now
    _unlock_pg(pgid);
  } else {
    // queue for worker threads
    if (read) 
      enqueue_op(0, op);     // no locking needed for reads
    else 
      enqueue_op(pgid, op);     
  }
}

void OSD::handle_op_reply(MOSDOpReply *op)
{
  if (op->get_map_epoch() < boot_epoch) {
    dout(3) << "replica op reply from before boot" << endl;
    delete op;
    return;
  }

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->get_pg();
  PG *pg = get_pg(pgid);

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch());

  if (!pg) {
    // hmm.
    delete op;
  }

  if (g_conf.osd_maxthreads < 1) {
    _lock_pg(pgid);
    do_op(op, pg); // do it now
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
    dout(10) << "enqueue_op waiting for pending_ops " << pending_ops << " to drop to " << g_conf.osd_max_opq << endl;
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
               << ls.size() << " / " << (pending_ops-1) << " more pending" << endl;
    } else {
      dout(10) << "dequeue_op " << op << " read "
               << ls.size() << " / " << (pending_ops-1) << " more pending" << endl;
    }
    
    if (ls.empty())
      op_queue.erase(pgid);
  }
  osd_lock.Unlock();

  // do it
  do_op(op, pg);

  // finish
  osd_lock.Lock();
  {
    if (pgid) {
      // unlock pg
      _unlock_pg(pgid);
    }
    
    dout(10) << "dequeue_op " << op << " finish" << endl;
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



/** do_op - do an op
 * object lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void OSD::do_op(Message *m, PG *pg) 
{
  //dout(15) << "do_op " << *m << endl;

  if (m->get_type() == MSG_OSD_OP) {
    MOSDOp *op = (MOSDOp*)m;

    logger->inc("op");

    switch (op->get_op()) {
      
      // reads
    case OSD_OP_READ:
      op_read(op);//, pg);
      break;
    case OSD_OP_STAT:
      op_stat(op);//, pg);
      break;
      
      // rep stuff
    case OSD_OP_PULL:
      op_pull(op, pg);
      break;
    case OSD_OP_PUSH:
      op_push(op, pg);
      break;
      
      // writes
    case OSD_OP_WRNOOP:
    case OSD_OP_WRITE:
    case OSD_OP_ZERO:
    case OSD_OP_DELETE:
    case OSD_OP_TRUNCATE:
    case OSD_OP_WRLOCK:
    case OSD_OP_WRUNLOCK:
    case OSD_OP_RDLOCK:
    case OSD_OP_RDUNLOCK:
    case OSD_OP_UPLOCK:
    case OSD_OP_DNLOCK:
      if (op->get_source().is_osd()) 
        op_rep_modify(op, pg);
      else
        op_modify(op, pg);
      break;
      
    default:
      assert(0);
    }
  } 
  else if (m->get_type() == MSG_OSD_OPREPLY) {
    // must be replication.
    MOSDOpReply *r = (MOSDOpReply*)m;
    tid_t rep_tid = r->get_rep_tid();
  
    if (pg->repop_gather.count(rep_tid)) {
      // oh, good.
      int fromosd = r->get_source().num();
      repop_ack(pg, pg->repop_gather[rep_tid], 
                r->get_result(), r->get_commit(), 
                fromosd, 
                r->get_pg_complete_thru());
      delete m;
    } else {
      // early ack.
      pg->waiting_for_repop[rep_tid].push_back(r);
    }

  } else
    assert(0);
}



void OSD::wait_for_no_ops()
{
  if (pending_ops > 0) {
    dout(7) << "wait_for_no_ops - waiting for " << pending_ops << endl;
    waiting_for_no_ops = true;
    while (pending_ops > 0)
      no_pending_ops.Wait(osd_lock);
    waiting_for_no_ops = false;
    assert(pending_ops == 0);
  } 
  dout(7) << "wait_for_no_ops - none" << endl;
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
  //cout << "getattr returns " << len << " on " << oid << endl;

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

bool OSD::pick_missing_object_rev(object_t& oid, PG *pg)
{
  map<object_t,eversion_t>::iterator p = pg->missing.missing.upper_bound(oid);
  if (p == pg->missing.missing.end()) 
    return false;  // clearly no candidate

  if (p->first.ino == oid.ino && p->first.bno == oid.bno) {
    oid = p->first;  // yes!  it's an upper bound revision for me.
    return true;
  }
  return false;
}

bool OSD::pick_object_rev(object_t& oid)
{
  object_t t = oid;

  if (!store->pick_object_revision_lt(t))
    return false; // we have no revisions of this object!
  
  objectrev_t crev;
  int r = store->getattr(t, "crev", &crev, sizeof(crev));
  assert(r >= 0);
  if (crev <= oid.rev) {
    dout(10) << "pick_object_rev choosing " << t << " crev " << crev << " for " << oid << endl;
    oid = t;
    return true;
  }

  return false;  
}

bool OSD::waitfor_missing_object(MOSDOp *op, PG *pg)
{
  const object_t oid = op->get_oid();

  // are we missing the object?
  if (pg->missing.missing.count(oid)) {
    // we don't have it (yet).
    eversion_t v = pg->missing.missing[oid];
    if (pg->objects_pulling.count(oid)) {
      dout(7) << "missing "
              << oid 
              << " v " << v
              << " in " << *pg
              << ", already pulling"
              << endl;
    } else {
      dout(7) << "missing " 
              << oid 
              << " v " << v
              << " in " << *pg
              << ", pulling"
              << endl;
      pull(pg, oid);
    }
    pg->waiting_for_missing_object[oid].push_back(op);
    return true;
  }

  return false;
}




// READ OPS

/** op_read
 * client read op
 * NOTE: called from opqueue.
 */
void OSD::op_read(MOSDOp *op)//, PG *pg)
{
  object_t oid = op->get_oid();
  
  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  // for _any_ op type -- eg only the locker can unlock!
  if (block_if_wrlocked(op)) return; // op will be handled later, after the object unlocks
 
  dout(10) << "op_read " << oid 
           << " " << op->get_offset() << "~" << op->get_length() 
    //<< " in " << *pg 
           << endl;

  utime_t read_time_start;
  if (outstanding_updates.count(op->get_reqid()) != 0)
    read_time_start = outstanding_updates[op->get_reqid()];
  else
    read_time_start = g_clock.now();

  utime_t sec_time_start = g_clock.now();
  if (g_conf.secure_io) {
    // FIXME only verfiy writes from a client
    // i know, i know...not secure but they should all have caps
    if (op->get_source().is_client()) { 
      ExtCap *op_capability = op->get_capability();
      assert(op_capability);
      
      // if using groups...do we know group?
      if (op_capability->get_type() == UNIX_GROUP ||
	  op_capability->get_type() == BATCH) {
	// check if user is in group
	hash_t my_hash = op_capability->get_user_hash();
	
	// do we have group cached? if not, update group
	// we will lose execution control here! re-gain on reply
	if (user_groups.count(my_hash) == 0) {
	  	  outstanding_updates[op->get_reqid()] = read_time_start;
	  update_group(op->get_client_inst(), my_hash, op);
	  return;
	}	
      }
      else if (op_capability->get_type() == USER_BATCH) {
        hash_t my_hash = op_capability->get_file_hash();

        // do we have group cached? if not, update group                                                                                                     
        // we will lose execution control here! re-gain on reply                                                                                             
        if (user_groups.count(my_hash) == 0) {
          outstanding_updates[op->get_reqid()] = read_time_start;
          update_group(op->get_client_inst(), my_hash, op);
          return;
        }
      }
      // check accesses are right
      if (check_request(op, op_capability)) {
	dout(3) << "Access permissions are correct" << endl;
      }
      else
	dout(3) << "Access permissions are incorrect" << endl;
      
      assert(verify_cap(op_capability));
    }
  }
  utime_t sec_time_end = g_clock.now();
  dout(1) << "Read Security time " << sec_time_end - sec_time_start << endl;

  long r = 0;
  bufferlist bl;
  
  if (oid.rev && !pick_object_rev(oid)) {
    // we have no revision for this request.
    r = -EEXIST;
  } else {
    // read into a buffer
    r = store->read(oid, 
		    op->get_offset(), op->get_length(),
		    bl);
  }
  utime_t read_time_end = g_clock.now();
  
  if (op->get_source().is_client())
    dout(1) << "Read time " << read_time_end - read_time_start << endl;

  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap->get_epoch(), true); 
  if (r >= 0) {
    reply->set_result(0);
    reply->set_data(bl);
    reply->set_length(r);
      
    logger->inc("c_rd");
    logger->inc("c_rdb", r);
    
  } else {
    reply->set_result(r);   // error
    reply->set_length(0);
  }
  
  dout(3) << " read got " << r << " / " << op->get_length() << " bytes from obj " << oid << endl;
  
  logger->inc("rd");
  if (r >= 0) logger->inc("rdb", r);
  
  // send it
  messenger->send_message(reply, op->get_client_inst());
  
  delete op;
}


/** op_stat
 * client stat
 * NOTE: called from opqueue
 */
void OSD::op_stat(MOSDOp *op)//, PG *pg)
{
  object_t oid = op->get_oid();

  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  if (block_if_wrlocked(op)) return; //read will be handled later, after the object unlocks

  struct stat st;
  memset(&st, sizeof(st), 0);
  int r = 0;

  if (oid.rev && !pick_object_rev(oid)) {
    // we have no revision for this request.
    r = -EEXIST;
  } else {
    r = store->stat(oid, &st);
  }
  
  dout(3) << "op_stat on " << oid 
          << " r = " << r
          << " size = " << st.st_size
    //<< " in " << *pg
          << endl;
  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap->get_epoch(), true);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_client_inst());
  
  logger->inc("stat");

  delete op;
}



/*********
 * new repops
 */

void OSD::get_repop_gather(PG::RepOpGather *repop)
{
  //repop->lock.Lock();
  dout(10) << "get_repop " << *repop << endl;
}

void OSD::apply_repop(PG *pg, PG::RepOpGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << endl;
  assert(!repop->applied);

  Context *oncommit = new C_OSD_WriteCommit(this, pg->info.pgid, repop->rep_tid, repop->pg_local_last_complete);
  unsigned r = store->apply_transaction(repop->t, oncommit);
  if (r)
    dout(-10) << "apply_repop  apply transaction return " << r << " on " << *repop << endl;
  
  // discard my reference to buffer
  repop->op->get_data().clear();

  repop->applied = true;
}

void OSD::put_repop_gather(PG *pg, PG::RepOpGather *repop)
{
  dout(10) << "put_repop " << *repop << endl;

  // commit?
  if (repop->can_send_commit() &&
      repop->op->wants_commit()) {
    // send commit.
    MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap->get_epoch(), true);
    dout(10) << "put_repop  sending commit on " << *repop << " " << reply << endl;
    messenger->send_message(reply, repop->op->get_client_inst());
    repop->sent_commit = true;
  }

  // ack?
  else if (repop->can_send_ack() &&
           repop->op->wants_ack()) {
    // apply
    apply_repop(pg, repop);

    // send ack
    MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap->get_epoch(), false);
    dout(10) << "put_repop  sending ack on " << *repop << " " << reply << endl;
    messenger->send_message(reply, repop->op->get_client_inst());
    repop->sent_ack = true;

    utime_t now = g_clock.now();
    now -= repop->start;
    logger->finc("rlsum", now);
    logger->inc("rlnum", 1);
  }

  // done.
  if (repop->can_delete()) {
    // adjust peers_complete_thru
    if (!repop->pg_complete_thru.empty()) {
      eversion_t min = pg->info.last_complete;  // hrm....
      for (unsigned i=0; i<pg->acting.size(); i++) {
        if (repop->pg_complete_thru[pg->acting[i]] < min)      // note: if we haven't heard, it'll be zero, which is what we want.
          min = repop->pg_complete_thru[pg->acting[i]];
      }
      
      if (min > pg->peers_complete_thru) {
        dout(10) << "put_repop  peers_complete_thru " << pg->peers_complete_thru << " -> " << min << " in " << *pg << endl;
        pg->peers_complete_thru = min;
      }
    }

    dout(10) << "put_repop  deleting " << *repop << endl;
    //repop->lock.Unlock();  

    assert(pg->repop_gather.count(repop->rep_tid));
    pg->repop_gather.erase(repop->rep_tid);

    delete repop->op;
    delete repop;

  } else {
    //repop->lock.Unlock();
  }
}


void OSD::issue_repop(PG *pg, MOSDOp *op, int osd)
{
  object_t oid = op->get_oid();

  dout(7) << " issue_repop rep_tid " << op->get_rep_tid()
          << " in " << *pg 
          << " o " << oid
          << " to osd" << osd
          << endl;
  
  // forward the write/update/whatever
  MOSDOp *wr = new MOSDOp(op->get_client_inst(), op->get_client_inc(), op->get_reqid().tid,
                          oid,
                          pg->get_pgid(),
                          osdmap->get_epoch(),
                          op->get_op());
  wr->get_data() = op->get_data();   // _copy_ bufferlist
  wr->set_length(op->get_length());
  wr->set_offset(op->get_offset());
  wr->set_version(op->get_version());

  wr->set_rep_tid(op->get_rep_tid());
  wr->set_pg_trim_to(pg->peers_complete_thru);

  messenger->send_message(wr, osdmap->get_inst(osd));
}

PG::RepOpGather *OSD::new_repop_gather(PG *pg, 
                                       MOSDOp *op)
{
  dout(10) << "new_repop_gather rep_tid " << op->get_rep_tid() << " on " << *op << " in " << *pg << endl;

  PG::RepOpGather *repop = new PG::RepOpGather(op, op->get_rep_tid(), 
                                               op->get_version(), 
                                               pg->info.last_complete);

  // osds. commits all come to me.
  for (unsigned i=0; i<pg->acting.size(); i++) {
    int osd = pg->acting[i];
    repop->osds.insert(osd);
    repop->waitfor_commit.insert(osd);
  }

  // acks vary:
  if (g_conf.osd_rep == OSD_REP_CHAIN) {
    // chain rep. 
    // there's my local ack...
    repop->osds.insert(whoami);
    repop->waitfor_ack.insert(whoami);
    repop->waitfor_commit.insert(whoami);

    // also, the previous guy will ack to me
    int myrank = osdmap->calc_pg_rank(whoami, pg->acting);
    if (myrank > 0) {
      int osd = pg->acting[ myrank-1 ];
      repop->osds.insert(osd);
      repop->waitfor_ack.insert(osd);
      repop->waitfor_commit.insert(osd);
    }
  } else {
    // primary, splay.  all osds ack to me.
    for (unsigned i=0; i<pg->acting.size(); i++) {
      int osd = pg->acting[i];
      repop->waitfor_ack.insert(osd);
    }
  }

  repop->start = g_clock.now();

  pg->repop_gather[ repop->rep_tid ] = repop;

  // anyone waiting?  (acks that got here before the op did)
  if (pg->waiting_for_repop.count(repop->rep_tid)) {
    take_waiters(pg->waiting_for_repop[repop->rep_tid]);
    pg->waiting_for_repop.erase(repop->rep_tid);
  }

  return repop;
}
 

void OSD::repop_ack(PG *pg, PG::RepOpGather *repop,
                    int result, bool commit,
                    int fromosd, eversion_t pg_complete_thru)
{
  MOSDOp *op = repop->op;

  dout(7) << "repop_ack rep_tid " << repop->rep_tid << " op " << *op
          << " result " << result << " commit " << commit << " from osd" << fromosd
          << " in " << *pg
          << endl;

  get_repop_gather(repop);
  {
    if (commit) {
      // commit
      assert(repop->waitfor_commit.count(fromosd));      
      repop->waitfor_commit.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      repop->pg_complete_thru[fromosd] = pg_complete_thru;
    } else {
      // ack
      repop->waitfor_ack.erase(fromosd);
    }
  }
  put_repop_gather(pg, repop);
}





/** op_modify_commit
 * transaction commit on the acker.
 */
void OSD::op_modify_commit(pg_t pgid, tid_t rep_tid, eversion_t pg_complete_thru)
{
  PG *pg = lock_pg(pgid);
  if (pg) {
    if (pg->repop_gather.count(rep_tid)) {
      PG::RepOpGather *repop = pg->repop_gather[rep_tid];
      
      dout(10) << "op_modify_commit " << *repop->op << endl;
      get_repop_gather(repop);
      {
        assert(repop->waitfor_commit.count(whoami));
        repop->waitfor_commit.erase(whoami);
        repop->pg_complete_thru[whoami] = pg_complete_thru;
      }
      put_repop_gather(pg, repop);
      dout(10) << "op_modify_commit done on " << repop << endl;
    } else {
      dout(10) << "op_modify_commit pg " << pgid << " rep_tid " << rep_tid << " dne" << endl;
    }

    unlock_pg(pgid);
  } else {
    dout(10) << "op_modify_commit pg " << pgid << " dne" << endl;
  }
}


/** op_modify
 * process client modify op
 * NOTE: called from opqueue.
 */
void OSD::op_modify(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();

  const char *opname = MOSDOp::get_opname(op->get_op());

  utime_t write_time_start;
  if (outstanding_updates.count(op->get_reqid()) != 0) {
    write_time_start = outstanding_updates[op->get_reqid()];
    outstanding_updates.erase(op->get_reqid());
  }
  else
    write_time_start = g_clock.now();

  // are any peers missing this?
  for (unsigned i=1; i<pg->acting.size(); i++) {
    int peer = pg->acting[i];
    if (pg->peer_missing.count(peer) &&
        pg->peer_missing[peer].is_missing(oid)) {
      // push it before this update. 
      // FIXME, this is probably extra much work (eg if we're about to overwrite)
      pg->peer_missing[peer].got(oid);
      push(pg, oid, peer);
    }
  }

  // dup op?
  if (pg->log.logged_req(op->get_reqid())) {
    dout(-3) << "op_modify " << opname << " dup op " << op->get_reqid()
             << ", doing WRNOOP" << endl;
    op->set_op(OSD_OP_WRNOOP);
    opname = MOSDOp::get_opname(op->get_op());
  }

  utime_t sec_time_start = g_clock.now();
  if (g_conf.secure_io) {
    // FIXME only verfiy writes from a client
    // i know, i know...not secure but they should all have caps
    if (op->get_op() == OSD_OP_WRITE
	&& op->get_source().is_client()) {

      ExtCap *op_capability = op->get_capability();
      assert(op_capability);
      // if using groups...do we know group?
      if (op_capability->get_type() == UNIX_GROUP ||
	  op_capability->get_type() == BATCH) {
	// check if user is in group
	hash_t my_hash = op_capability->get_user_hash();
	
	// do we have group cached? if not, update group
	// we will lose execution control here! re-gain on reply
	if (user_groups.count(my_hash) == 0) {
	  outstanding_updates[op->get_reqid()] = write_time_start;
	  update_group(op->get_client_inst(), my_hash, op);
	  return;
	}
      }
      else if (op_capability->get_type() == USER_BATCH) {
	hash_t my_hash = op_capability->get_file_hash();
	
	// do we have group cached? if not, update group
	// we will lose execution control here! re-gain on reply
	if (user_groups.count(my_hash) == 0) {
	  outstanding_updates[op->get_reqid()] = write_time_start;
	  update_group(op->get_client_inst(), my_hash, op);
	  return;
	}
      }

      // check accesses are right
      if (check_request(op, op_capability)) {
	dout(3) << "Access permissions are correct" << endl;
      }
      else
	dout(3) << "Access permissions are incorrect" << endl;
      
      assert(verify_cap(op_capability));
    }

  }
  utime_t sec_time_end = g_clock.now();
  //cout << "Security write time spent " << sec_time_end - sec_time_start << endl;
  
  // locked by someone else?
  // for _any_ op type -- eg only the locker can unlock!
  if (op->get_op() != OSD_OP_WRNOOP &&  // except WRNOOP; we just want to flush
      block_if_wrlocked(op)) 
    return; // op will be handled later, after the object unlocks


  // check crev
  objectrev_t crev = 0;
  store->getattr(oid, "crev", (char*)&crev, sizeof(crev));

  // assign version
  eversion_t clone_version;
  eversion_t nv = pg->log.top;
  if (op->get_op() != OSD_OP_WRNOOP) {
    nv.epoch = osdmap->get_epoch();
    nv.version++;
    assert(nv > pg->info.last_update);
    assert(nv > pg->log.top);

    // will clone?
    if (crev && op->get_rev() && op->get_rev() > crev) {
      clone_version = nv;
      nv.version++;
    }

    if (op->get_version().version) {
      // replay!
      if (nv.version < op->get_version().version) {
        nv.version = op->get_version().version; 

	// clone?
	if (crev && op->get_rev() && op->get_rev() > crev) {
	  // backstep clone
	  clone_version = nv;
	  clone_version.version--;
	}
      }
    }
  }

  // set version in op, for benefit of client and our eventual reply
  op->set_version(nv);
  
  dout(10) << "op_modify " << opname 
           << " " << oid 
           << " v " << nv 
	   << " crev " << crev
	   << " rev " << op->get_rev()
           << " " << op->get_offset() << "~" << op->get_length()
           << endl;  

  if (op->get_op() == OSD_OP_WRITE) {
    logger->inc("c_wr");
    logger->inc("c_wrb", op->get_length());
  }

  // share latest osd map?
  osd_lock.Lock();
  {
    for (unsigned i=1; i<pg->acting.size(); i++) {
      int osd = pg->acting[i];
      _share_map_outgoing( osdmap->get_inst(osd) ); 
    }
  }
  osd_lock.Unlock();

  // issue replica writes
  PG::RepOpGather *repop = 0;
  bool alone = (pg->acting.size() == 1);
  tid_t rep_tid = ++last_tid;
  op->set_rep_tid(rep_tid);

  if (g_conf.osd_rep == OSD_REP_CHAIN && !alone) {
    // chain rep.  send to #2 only.
    int next = pg->acting[1];
    if (pg->acting.size() > 2)
      next = pg->acting[2];
    issue_repop(pg, op, next);
  } 
  else if (g_conf.osd_rep == OSD_REP_SPLAY && !alone) {
    // splay rep.  send to rest.
    for (unsigned i=1; i<pg->acting.size(); ++i)
    //for (unsigned i=pg->acting.size()-1; i>=1; --i)
      issue_repop(pg, op, pg->acting[i]);
  } else {
    // primary rep, or alone.
    repop = new_repop_gather(pg, op);

    // send to rest.
    if (!alone)
      for (unsigned i=1; i<pg->acting.size(); i++)
        issue_repop(pg, op, pg->acting[i]);
  }

  if (repop) {    
    // we are acker.
    if (op->get_op() != OSD_OP_WRNOOP) {
      // log and update later.
      prepare_log_transaction(repop->t, op, nv, crev, op->get_rev(), pg, pg->peers_complete_thru);
      prepare_op_transaction(repop->t, op, nv, crev, op->get_rev(), pg);
    }

    // (logical) local ack.
    // (if alone, this will apply the update.)
    get_repop_gather(repop);
    {
      assert(repop->waitfor_ack.count(whoami));
      repop->waitfor_ack.erase(whoami);
    }
    put_repop_gather(pg, repop);

  } else {
    // chain or splay.  apply.
    ObjectStore::Transaction t;
    prepare_log_transaction(t, op, nv, crev, op->get_rev(), pg, pg->peers_complete_thru);
    prepare_op_transaction(t, op, nv, crev, op->get_rev(), pg);

    C_OSD_RepModifyCommit *oncommit = new C_OSD_RepModifyCommit(this, op, pg->get_acker(), 
                                                                pg->info.last_complete);
    unsigned r = store->apply_transaction(t, oncommit);
    if (r != 0 &&   // no errors
        r != 2) {   // or error on collection_add
      cerr << "error applying transaction: r = " << r << endl;
      assert(r == 0);
    }

    oncommit->ack();
  }
  utime_t write_time_end = g_clock.now();
  if (op->get_op() == OSD_OP_WRITE &&
      op->get_source().is_client())
    dout(1) << "Write time " << write_time_end - write_time_start << endl;
}



void OSD::prepare_log_transaction(ObjectStore::Transaction& t, 
                                  MOSDOp *op, eversion_t& version, 
				  objectrev_t crev, objectrev_t rev,
				  PG *pg,
                                  eversion_t trim_to)
{
  const object_t oid = op->get_oid();

  // clone entry?
  if (crev && rev && rev > crev) {
    eversion_t cv = version;
    cv.version--;
    PG::Log::Entry cloneentry(PG::Log::Entry::CLONE, oid, cv, op->get_reqid());
    pg->log.add(cloneentry);

    dout(10) << "prepare_log_transaction " << op->get_op()
	     << " " << cloneentry
	     << " in " << *pg << endl;
  }

  // actual op
  int opcode = PG::Log::Entry::MODIFY;
  if (op->get_op() == OSD_OP_DELETE) opcode = PG::Log::Entry::DELETE;
  PG::Log::Entry logentry(opcode, oid, version, op->get_reqid());

  dout(10) << "prepare_log_transaction " << op->get_op()
           << " " << logentry
           << " in " << *pg << endl;

  // append to log
  assert(version > pg->log.top);
  pg->log.add(logentry);
  assert(pg->log.top == version);
  dout(10) << "prepare_log_transaction appended to " << *pg << endl;

  // write to pg log on disk
  pg->append_log(t, logentry, trim_to);
}


/** prepare_op_transaction
 * apply an op to the store wrapped in a transaction.
 */
void OSD::prepare_op_transaction(ObjectStore::Transaction& t, 
                                 MOSDOp *op, eversion_t& version, 
				 objectrev_t crev, objectrev_t rev,
				 PG *pg)
{
  const object_t oid = op->get_oid();
  const pg_t pgid = op->get_pg();

  bool did_clone = false;

  dout(10) << "prepare_op_transaction " << MOSDOp::get_opname( op->get_op() )
           << " " << oid 
           << " v " << version
	   << " crev " << crev
	   << " rev " << rev
           << " in " << *pg << endl;
  
  // WRNOOP does nothing.
  if (op->get_op() == OSD_OP_WRNOOP) 
    return;

  // raise last_complete?
  if (pg->info.last_complete == pg->info.last_update)
    pg->info.last_complete = version;
  
  // raise last_update.
  assert(version > pg->info.last_update);
  pg->info.last_update = version;
  
  // write pg info
  t.collection_setattr(pgid, "info", &pg->info, sizeof(pg->info));

  // clone?
  if (crev && rev && rev > crev) {
    object_t noid = oid;
    noid.rev = rev;
    dout(10) << "prepare_op_transaction cloning " << oid << " crev " << crev << " to " << noid << endl;
    t.clone(oid, noid);
    did_clone = true;
  }  

  // apply the op
  switch (op->get_op()) {
  case OSD_OP_WRLOCK:
    { // lock object
      //r = store->setattr(oid, "wrlock", &op->get_asker(), sizeof(msg_addr_t), oncommit);
      t.setattr(oid, "wrlock", &op->get_client(), sizeof(entity_name_t));
    }
    break;  
    
  case OSD_OP_WRUNLOCK:
    { // unlock objects
      //r = store->rmattr(oid, "wrlock", oncommit);
      t.rmattr(oid, "wrlock");
      
      // unblock all operations that were waiting for this object to become unlocked
      if (waiting_for_wr_unlock.count(oid)) {
        take_waiters(waiting_for_wr_unlock[oid]);
        waiting_for_wr_unlock.erase(oid);
      }
    }
    break;
    
  case OSD_OP_WRITE:
    { // write
      assert(op->get_data().length() == op->get_length());
      bufferlist bl;
      bl.claim( op->get_data() );  // give buffers to store; we keep *op in memory for a long time!
      
      //if (oid < 100000000000000ULL)  // hack hack-- don't write client data
      t.write( oid, op->get_offset(), op->get_length(), bl );
    }
    break;
    
  case OSD_OP_ZERO:
    {
      assert(0);  // are you sure this is what you want?
      // zero, remove, or truncate?
      struct stat st;
      int r = store->stat(oid, &st);
      if (r >= 0) {
	if (op->get_offset() + (off_t)op->get_length() >= (off_t)st.st_size) {
	  if (op->get_offset()) 
	    t.truncate(oid, op->get_length() + op->get_offset());
	  else
	    t.remove(oid);
	} else {
	  // zero.  the dumb way.  FIXME.
	  bufferptr bp(op->get_length());
	  bp.zero();
	  bufferlist bl;
	  bl.push_back(bp);
	  t.write(oid, op->get_offset(), op->get_length(), bl);
	}
      } else {
	// noop?
	dout(10) << "apply_transaction zero on " << oid << ", but dne?  stat returns " << r << endl;
      }
    }
    break;

  case OSD_OP_TRUNCATE:
    { // truncate
      //r = store->truncate(oid, op->get_offset());
      t.truncate(oid, op->get_length() );
    }
    break;
    
  case OSD_OP_DELETE:
    { // delete
      //r = store->remove(oid);
      t.remove(oid);
    }
    break;
    
  default:
    assert(0);
  }
  
  // object collection, version
  if (op->get_op() == OSD_OP_DELETE) {
    // remove object from c
    t.collection_remove(pgid, oid);
  } else {
    // add object to c
    t.collection_add(pgid, oid);
    
    // object version
    t.setattr(oid, "version", &version, sizeof(version));

    // set object crev
    if (crev == 0 ||   // new object
	did_clone)     // we cloned
      t.setattr(oid, "crev", &rev, sizeof(rev));
  }
}
