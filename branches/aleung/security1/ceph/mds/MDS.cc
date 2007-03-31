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
#include "common/Clock.h"

#include "msg/Messenger.h"

#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "osdc/Filer.h"

#include "MDSMap.h"

#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "IdAllocator.h"
#include "Migrator.h"
#include "Renamer.h"

#include "AnchorTable.h"
#include "AnchorClient.h"

#include "common/Logger.h"
#include "common/LogType.h"

#include "common/Timer.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "UserBatch.h"


LogType mds_logtype, mds_cache_logtype;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << whoami << " "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << whoami << " "





// cons/des
MDS::MDS(int whoami, Messenger *m, MonMap *mm) : timer(mds_lock) {
  this->whoami = whoami;

  monmap = mm;
  messenger = m;

  mdsmap = new MDSMap;
  osdmap = new OSDMap;

  objecter = new Objecter(messenger, monmap, osdmap);
  filer = new Filer(objecter);

  mdcache = new MDCache(this);
  mdstore = new MDStore(this);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this);

  anchorclient = new AnchorClient(messenger, mdsmap);
  idalloc = new IdAllocator(this);

  anchormgr = new AnchorTable(this);

  server = new Server(this);
  locker = new Locker(this, mdcache);

  // init keys
  //myPrivKey = esignPrivKey("crypto/esig1536.dat");
  myPrivKey = esignPrivKey("crypto/esig1023.dat");
  myPubKey = esignPubKey(myPrivKey);

  cap_cache_hits = 0;
  cap_requests = 0;

  // hard code the unix groups?
  if (g_conf.preload_unix_groups) {
    gid_t group_gid = 1020;
    list<uid_t> uid_list;
    for (int start_uid = 340; start_uid <= 520; start_uid++) {
      uid_list.push_back(start_uid);
      //cout << start_uid << ", ";
    }
    CapGroup cgroup;
    cgroup.set_list(uid_list);
    cgroup.sign_list(myPrivKey);
    unix_groups_byhash[cgroup.get_root_hash()] = cgroup;
    unix_groups_map[group_gid] = cgroup.get_root_hash();
    //cout << endl << "hash " << cgroup.get_root_hash() << endl;
  }

  /*
  // create unix_groups from file?
  if (g_conf.unix_group_file) {
    ifstream from(g_conf.unix_group_file);

    if (from.is_open()) {

      bool seen_gid = false;
      int input;
      gid_t my_gid;
      uid_t my_uid;
      CapGroup *my_group;
      // parse file
      while (! from.eof()) {
	from >> input;
	if (input == -1) {
	  seen_gid = false;
	  // copy hash into map
	  unix_groups_map[my_gid] = my_group->get_root_hash();
	  //cout << " " << my_group->get_root_hash() << endl;
	  delete my_group;
	}
	// first number on line is gid, rest are uids
	else if (!seen_gid) {
	  // make group
	  my_gid = input;
	  //unix_groups[my_gid].set_gid(my_gid);
	  my_group = new CapGroup();	  
	  seen_gid = true;
	  //cout << "Gid: " << my_gid;
	}
	else {
	  my_uid = input;
	  my_group->add_user(my_uid);
	  //unix_groups[my_gid].add_user(my_uid);

	  // sign the hash
	  my_group->sign_list(myPrivKey);
	  
	  // update the map
	  unix_groups_byhash[my_group->get_root_hash()] = (*my_group);
	  
	  //cout << " uid: " << my_uid;
	}
      }
      from.close();
    }
    else {
      cout << "Failed to open the unix_group_file" << endl;
      assert(0);
    }
    cout << "Done doing unix group crap" << endl;
  }
  */

  // do prediction read-in
  if (g_conf.mds_group == 4) {
    map<inodeno_t, deque<inodeno_t> > sequence;

    // read in file from disk
    //int off = 0;
    //bufferlist bl;
    //server->get_bl_ss(bl);
    //::_decode(sequence, bl, off);
    
    // hard code the predictions
    inodeno_t shared1ino(16777216);
    inodeno_t shared2ino(16777217);
    inodeno_t shared3ino(16777218);
    inodeno_t shared4ino(16777219);
    inodeno_t shared5ino(16777220);
    inodeno_t shared6ino(16777221);

    for (int addtimes = 0; addtimes < 6; addtimes++)
      sequence[shared1ino].push_back(shared2ino);
    for (int addtimes = 0; addtimes < 6; addtimes++)
      sequence[shared2ino].push_back(shared3ino);
    for (int addtimes = 0; addtimes < 6; addtimes++)
      sequence[shared3ino].push_back(shared4ino);
    for (int addtimes = 0; addtimes < 6; addtimes++)
      sequence[shared4ino].push_back(shared5ino);
    for (int addtimes = 0; addtimes < 6; addtimes++)
      sequence[shared5ino].push_back(shared6ino);

    // set prediction sequence and parameters
    rp_predicter = RecentPopularity(2, 6, sequence);

    // preload all of the predictions into groups
    for (map<inodeno_t, deque<inodeno_t> >::iterator mi = sequence.begin();
	 mi != sequence.end();
	 mi++) {
      CapGroup inode_list;
      inodeno_t prediction;
      prediction = rp_predicter.predict_successor(mi->first);
      
      cout << "Predictions for " << mi->first << ": ";
      while(prediction != inodeno_t()) {
	cout << prediction << ", ";
	inode_list.add_inode(prediction);
	prediction = rp_predicter.predict_successor(prediction);

      }
      cout << "Cannot make any further predictions" << endl;

      // cache the list
      if (inode_list.num_inodes() != 0) {
	inode_list.sign_list(myPrivKey);
	unix_groups_byhash[inode_list.get_root_hash()] = inode_list;
	precompute_succ[mi->first] = inode_list.get_root_hash();
      }
    }
  }

  // cap identifiers
  cap_id_count = 0;
 
  // beacon
  beacon_last_seq = 0;
  beacon_sender = 0;
  beacon_killer = 0;

  // tick
  tick_event = 0;

  req_rate = 0;

  want_state = state = MDSMap::STATE_DNE;


  logger = logger2 = 0;

  // i'm ready!
  messenger->set_dispatcher(this);
}

MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }
  if (idalloc) { delete idalloc; idalloc = NULL; }
  if (anchormgr) { delete anchormgr; anchormgr = NULL; }
  if (anchorclient) { delete anchorclient; anchorclient = NULL; }
  if (osdmap) { delete osdmap; osdmap = 0; }
  if (mdsmap) { delete mdsmap; mdsmap = 0; }

  if (server) { delete server; server = 0; }
  if (locker) { delete locker; locker = 0; }

  if (filer) { delete filer; filer = 0; }
  if (objecter) { delete objecter; objecter = 0; }
  if (messenger) { delete messenger; messenger = NULL; }

  if (logger) { delete logger; logger = 0; }
  if (logger2) { delete logger2; logger2 = 0; }

}


void MDS::reopen_logger()
{
  // flush+close old log
  if (logger) {
    logger->flush(true);
    delete logger;
  }
  if (logger2) {
    logger2->flush(true);
    delete logger2;
  }


  // log
  string name;
  name = "mds";
  int w = whoami;
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  logger = new Logger(name, (LogType*)&mds_logtype);

  mds_logtype.add_inc("req");
  mds_logtype.add_inc("reply");
  mds_logtype.add_inc("fw");
  mds_logtype.add_inc("cfw");

  mds_logtype.add_set("l");
  mds_logtype.add_set("q");
  mds_logtype.add_set("popanyd");
  mds_logtype.add_set("popnest");

  mds_logtype.add_inc("lih");
  mds_logtype.add_inc("lif");

  mds_logtype.add_set("c");
  mds_logtype.add_set("ctop");
  mds_logtype.add_set("cbot");
  mds_logtype.add_set("cptail");  
  mds_logtype.add_set("cpin");
  mds_logtype.add_inc("cex");
  mds_logtype.add_inc("dis");
  mds_logtype.add_inc("cmiss");

  mds_logtype.add_set("buf");
  mds_logtype.add_inc("cdir");
  mds_logtype.add_inc("fdir");

  mds_logtype.add_inc("iex");
  mds_logtype.add_inc("iim");
  mds_logtype.add_inc("ex");
  mds_logtype.add_inc("im");
  mds_logtype.add_inc("imex");  
  mds_logtype.add_set("nex");
  mds_logtype.add_set("nim");
  mds_logtype.add_set("lsum");
  mds_logtype.add_set("lnum");

  
  char n[80];
  sprintf(n, "mds%d.cache", whoami);
  logger2 = new Logger(n, (LogType*)&mds_cache_logtype);
}

void MDS::send_message_mds(Message *m, int mds, int port, int fromport)
{
  // send mdsmap first?
  if (peer_mdsmap_epoch[mds] < mdsmap->get_epoch()) {
    messenger->send_message(new MMDSMap(mdsmap), 
			    mdsmap->get_inst(mds));
    peer_mdsmap_epoch[mds] = mdsmap->get_epoch();
  }

  // send message
  if (port && !fromport) 
    fromport = port;
  messenger->send_message(m, mdsmap->get_inst(mds), port, fromport);
}


class C_MDS_Tick : public Context {
  MDS *mds;
public:
  C_MDS_Tick(MDS *m) : mds(m) {}
  void finish(int r) {
    mds->tick();
  }
};



int MDS::init(bool standby)
{
  mds_lock.Lock();

  // generate my key pair
  // ..

  if (standby)
    want_state = MDSMap::STATE_STANDBY;
  else
    want_state = MDSMap::STATE_STARTING;
  
  // starting beacon.  this will induce an MDSMap from the monitor
  beacon_start();
  
  // schedule tick
  reset_tick();

  reopen_logger();

  mds_lock.Unlock();
  return 0;
}

void MDS::reset_tick()
{
  // cancel old
  if (tick_event) timer.cancel_event(tick_event);

  // schedule
  tick_event = new C_MDS_Tick(this);
  timer.add_event_after(g_conf.mon_tick_interval, tick_event);
}

void MDS::tick()
{
  // reschedule
  reset_tick();

  // log
  mds_load_t load = balancer->get_load();
  
  if (logger) {
    req_rate = logger->get("req");
    
    logger->set("l", (int)load.mds_load());
    logger->set("q", messenger->get_dispatch_queue_len());
    logger->set("buf", buffer_total_alloc);
    
    mdcache->log_stat(logger);
  }
  
  // booted?
  if (is_active()) {
    
    // balancer
    balancer->tick();
    
    // HACK to test hashing stuff
    if (false) {
      /*
      static map<int,int> didhash;
      if (elapsed.sec() > 15 && !didhash[whoami]) {
	CInode *in = mdcache->get_inode(100000010);
	if (in && in->dir) {
	  if (in->dir->is_auth()) 
	    mdcache->migrator->hash_dir(in->dir);
	  didhash[whoami] = 1;
	}
      }
      if (0 && elapsed.sec() > 25 && didhash[whoami] == 1) {
	CInode *in = mdcache->get_inode(100000010);
	if (in && in->dir) {
	  if (in->dir->is_auth() && in->dir->is_hashed())
	    mdcache->migrator->unhash_dir(in->dir);
	  didhash[whoami] = 2;
	}
      }
      */
    }
  }
}




// -----------------------
// beacons

void MDS::beacon_start()
{
  beacon_send();         // send first beacon
  
  //reset_beacon_killer(); // schedule killer
}
  

class C_MDS_BeaconSender : public Context {
  MDS *mds;
public:
  C_MDS_BeaconSender(MDS *m) : mds(m) {}
  void finish(int r) {
    mds->beacon_send();
  }
};

void MDS::beacon_send()
{
  ++beacon_last_seq;
  dout(10) << "beacon_send " << MDSMap::get_state_name(want_state)
	   << " seq " << beacon_last_seq
	   << " (currently " << MDSMap::get_state_name(state) << ")"
	   << endl;

  beacon_seq_stamp[beacon_last_seq] = g_clock.now();
  
  int mon = monmap->pick_mon();
  messenger->send_message(new MMDSBeacon(want_state, beacon_last_seq),
			  monmap->get_inst(mon));

  // schedule next sender
  if (beacon_sender) timer.cancel_event(beacon_sender);
  beacon_sender = new C_MDS_BeaconSender(this);
  timer.add_event_after(g_conf.mds_beacon_interval, beacon_sender);
}

void MDS::handle_mds_beacon(MMDSBeacon *m)
{
  dout(10) << "handle_mds_beacon " << MDSMap::get_state_name(m->get_state())
	   << " seq " << m->get_seq() << endl;
  version_t seq = m->get_seq();
  
  // update lab
  if (beacon_seq_stamp.count(seq)) {
    assert(beacon_seq_stamp[seq] > beacon_last_acked_stamp);
    beacon_last_acked_stamp = beacon_seq_stamp[seq];
    
    // clean up seq_stamp map
    while (!beacon_seq_stamp.empty() &&
	   beacon_seq_stamp.begin()->first <= seq)
      beacon_seq_stamp.erase(beacon_seq_stamp.begin());
    
    reset_beacon_killer();
  }

  delete m;
}

class C_MDS_BeaconKiller : public Context {
  MDS *mds;
  utime_t lab;
public:
  C_MDS_BeaconKiller(MDS *m, utime_t l) : mds(m), lab(l) {}
  void finish(int r) {
    mds->beacon_kill(lab);
  }
};

void MDS::reset_beacon_killer()
{
  utime_t when = beacon_last_acked_stamp;
  when += g_conf.mds_beacon_grace;
  
  dout(15) << "reset_beacon_killer last_acked_stamp at " << beacon_last_acked_stamp
	   << ", will die at " << when << endl;
  
  if (beacon_killer) timer.cancel_event(beacon_killer);

  beacon_killer = new C_MDS_BeaconKiller(this, beacon_last_acked_stamp);
  timer.add_event_at(when, beacon_killer);
}

void MDS::beacon_kill(utime_t lab)
{
  if (lab == beacon_last_acked_stamp) {
    dout(0) << "beacon_kill last_acked_stamp " << lab 
	    << ", killing myself."
	    << endl;
    exit(0);
  } else {
    dout(20) << "beacon_kill last_acked_stamp " << beacon_last_acked_stamp 
	     << " != my " << lab 
	     << ", doing nothing."
	     << endl;
  }
}



void MDS::handle_mds_map(MMDSMap *m)
{
  version_t epoch = m->get_epoch();
  dout(1) << "handle_mds_map epoch " << epoch << " from " << m->get_source() << endl;

  // note source's map version
  if (m->get_source().is_mds() && 
      peer_mdsmap_epoch[m->get_source().num()] < epoch) {
    dout(15) << " peer " << m->get_source()
	     << " has mdsmap epoch >= " << epoch
	     << endl;
    peer_mdsmap_epoch[m->get_source().num()] = epoch;
  }

  // is it new?
  if (epoch <= mdsmap->get_epoch()) {
    dout(1) << " old map epoch " << epoch << " <= " << mdsmap->get_epoch() 
	    << ", discarding" << endl;
    delete m;
    return;
  }

  // note some old state
  int oldwhoami = whoami;
  int oldstate = state;
  set<int> oldresolve;
  mdsmap->get_mds_set(oldresolve, MDSMap::STATE_RESOLVE);
  bool wasrejoining = mdsmap->is_rejoining();
  set<int> oldfailed;
  mdsmap->get_mds_set(oldfailed, MDSMap::STATE_FAILED);

  // decode and process
  mdsmap->decode(m->get_encoded());

  // see who i am
  whoami = mdsmap->get_inst_rank(messenger->get_myaddr());
  if (oldwhoami != whoami) {
    // update messenger.
    messenger->reset_myname(MSG_ADDR_MDS(whoami));

    reopen_logger();
    dout(1) << "handle_mds_map i am now mds" << whoami
	    << " incarnation " << mdsmap->get_inc(whoami)
	    << endl;

    // do i need an osdmap?
    if (oldwhoami < 0) {
      // we need an osdmap too.
      int mon = monmap->pick_mon();
      messenger->send_message(new MOSDGetMap(0),
			      monmap->get_inst(mon));
    }
  }

  // tell objecter my incarnation
  if (objecter->get_client_incarnation() < 0 &&
      mdsmap->have_inst(whoami)) {
    assert(mdsmap->get_inc(whoami) > 0);
    objecter->set_client_incarnation(mdsmap->get_inc(whoami));
  }

  // update my state
  state = mdsmap->get_state(whoami);
  
  // did it change?
  if (oldstate != state) {
    if (state == want_state) {
      dout(1) << "handle_mds_map new state " << mdsmap->get_state_name(state) << endl;
    } else {
      dout(1) << "handle_mds_map new state " << mdsmap->get_state_name(state)
	      << ", although i wanted " << mdsmap->get_state_name(want_state)
	      << endl;
      want_state = state;
    }

    // now active?
    if (is_active()) {
      dout(1) << "now active" << endl;
      finish_contexts(waitfor_active);  // kick waiters
    }

    else if (is_replay()) {
      // initialize gather sets
      set<int> rs;
      mdsmap->get_recovery_mds_set(rs);
      rs.erase(whoami);
      dout(1) << "now replay.  my recovery peers are " << rs << endl;
      mdcache->set_recovery_set(rs);
    }
    
    // now stopping?
    else if (is_stopping()) {
      assert(oldstate == MDSMap::STATE_ACTIVE);
      dout(1) << "now stopping" << endl;
      
      mdcache->shutdown_start();
      
      // save anchor table
      if (mdsmap->get_anchortable() == whoami) 
	anchormgr->save(0);  // FIXME?  or detect completion via filer?
      
      if (idalloc) 
	idalloc->save(0);    // FIXME?  or detect completion via filer?
      
      // flush log
      mdlog->set_max_events(0);
      mdlog->trim(NULL);
    }

    // now standby?
    else if (is_stopped()) {
      assert(oldstate == MDSMap::STATE_STOPPING);
      dout(1) << "now stopped, sending down:out and exiting" << endl;
      shutdown_final();
    }
  }
  
  
  // is anyone resolving?
  if (is_resolve() || is_rejoin() || is_active() || is_stopping()) {
    set<int> resolve;
    mdsmap->get_mds_set(resolve, MDSMap::STATE_RESOLVE);
    if (oldresolve != resolve) 
      dout(10) << "resolve set is " << resolve << ", was " << oldresolve << endl;
    for (set<int>::iterator p = resolve.begin(); p != resolve.end(); ++p) {
      if (*p == whoami) continue;
      if (oldresolve.count(*p) == 0 ||         // if other guy newly resolve, or
	  oldstate == MDSMap::STATE_REPLAY)    // if i'm newly resolve,
	mdcache->send_import_map(*p);          // share my import map (now or later)
    }
  }
  
  // is everybody finally rejoining?
  if (is_rejoin() || is_active() || is_stopping()) {
    if (!wasrejoining && mdsmap->is_rejoining()) {
      mdcache->send_cache_rejoins();
    }
  }

  // did anyone go down?
  if (is_active() || is_stopping()) {
    set<int> failed;
    mdsmap->get_mds_set(failed, MDSMap::STATE_FAILED);
    for (set<int>::iterator p = failed.begin(); p != failed.end(); ++p) {
      // newly so?
      if (oldfailed.count(*p)) continue;      

      mdcache->migrator->handle_mds_failure(*p);
    }
  }

  delete m;
}

void MDS::handle_osd_map(MOSDMap *m)
{
  version_t had = osdmap->get_epoch();
  
  // process locally
  objecter->handle_osd_map(m);

  if (had == 0) {
    if (is_creating()) 
      boot_create();    // new tables, journal
    else if (is_starting())
      boot_start();     // old tables, empty journal
    else if (is_replay()) 
      boot_replay();    // replay, join
    else 
      assert(is_standby());
  }  
  
  // pass on to clients
  for (set<int>::iterator it = clientmap.get_mount_set().begin();
       it != clientmap.get_mount_set().end();
       it++) {
    MOSDMap *n = new MOSDMap;
    n->maps = m->maps;
    n->incremental_maps = m->incremental_maps;
    messenger->send_message(n, clientmap.get_inst(*it));
  }
}


class C_MDS_BootFinish : public Context {
  MDS *mds;
public:
  C_MDS_BootFinish(MDS *m) : mds(m) {}
  void finish(int r) { mds->boot_finish(); }
};

void MDS::boot_create()
{
  dout(3) << "boot_create" << endl;

  C_Gather *fin = new C_Gather(new C_MDS_BootFinish(this));

  if (whoami == 0) {
    dout(3) << "boot_create since i am also mds0, creating root inode and dir" << endl;

    // create root inode.
    mdcache->open_root(0);
    CInode *root = mdcache->get_root();
    assert(root);
    
    // force empty root dir
    CDir *dir = root->dir;
    dir->mark_complete();
    dir->mark_dirty(dir->pre_dirty());
    
    // save it
    mdstore->commit_dir(dir, fin->new_sub());
  }

  // start with a fresh journal
  dout(10) << "boot_create creating fresh journal" << endl;
  mdlog->reset();
  mdlog->write_head(fin->new_sub());
  
  // write our first importmap
  mdcache->log_import_map(fin->new_sub());

  // fixme: fake out idalloc (reset, pretend loaded)
  dout(10) << "boot_create creating fresh idalloc table" << endl;
  idalloc->reset();
  idalloc->save(fin->new_sub());
  
  // fixme: fake out anchortable
  if (mdsmap->get_anchortable() == whoami) {
    dout(10) << "boot_create creating fresh anchortable" << endl;
    anchormgr->reset();
    anchormgr->save(fin->new_sub());
  }
}

void MDS::boot_start()
{
  dout(2) << "boot_start" << endl;
  
  C_Gather *fin = new C_Gather(new C_MDS_BootFinish(this));
  
  dout(2) << "boot_start opening idalloc" << endl;
  idalloc->load(fin->new_sub());
  
  if (mdsmap->get_anchortable() == whoami) {
    dout(2) << "boot_start opening anchor table" << endl;
    anchormgr->load(fin->new_sub());
  } else {
    dout(2) << "boot_start i have no anchor table" << endl;
  }

  dout(2) << "boot_start opening mds log" << endl;
  mdlog->open(fin->new_sub());

  if (mdsmap->get_root() == whoami) {
    dout(2) << "boot_start opening root directory" << endl;
    mdcache->open_root(fin->new_sub());
  }
}

void MDS::boot_finish()
{
  dout(3) << "boot_finish" << endl;

  if (is_starting()) {
    // make sure mdslog is empty
    assert(mdlog->get_read_pos() == mdlog->get_write_pos());
  }

  set_want_state(MDSMap::STATE_ACTIVE);
}


class C_MDS_BootRecover : public Context {
  MDS *mds;
  int nextstep;
public:
  C_MDS_BootRecover(MDS *m, int n) : mds(m), nextstep(n) {}
  void finish(int r) { mds->boot_replay(nextstep); }
};

void MDS::boot_replay(int step)
{
  switch (step) {
  case 0:
    step = 1;  // fall-thru.

  case 1:
    dout(2) << "boot_replay " << step << ": opening idalloc" << endl;
    idalloc->load(new C_MDS_BootRecover(this, 2));
    break;

  case 2:
    if (mdsmap->get_anchortable() == whoami) {
      dout(2) << "boot_replay " << step << ": opening anchor table" << endl;
      anchormgr->load(new C_MDS_BootRecover(this, 3));
      break;
    }
    dout(2) << "boot_replay " << step << ": i have no anchor table" << endl;
    step++; // fall-thru

  case 3:
    dout(2) << "boot_replay " << step << ": opening mds log" << endl;
    mdlog->open(new C_MDS_BootRecover(this, 4));
    break;
    
  case 4:
    dout(2) << "boot_replay " << step << ": replaying mds log" << endl;
    mdlog->replay(new C_MDS_BootRecover(this, 5));
    break;

  case 5:
    dout(2) << "boot_replay " << step << ": restarting any recovered purges" << endl;
    mdcache->start_recovered_purges();
    
    step++;    // fall-thru
    
  case 6:
    // done with replay!
    if (mdsmap->get_num_mds(MDSMap::STATE_ACTIVE) == 0 &&
	mdsmap->get_num_mds(MDSMap::STATE_STOPPING) == 0 &&
	mdsmap->get_num_mds(MDSMap::STATE_RESOLVE) == 0 &&
	mdsmap->get_num_mds(MDSMap::STATE_REJOIN) == 0 &&
	mdsmap->get_num_mds(MDSMap::STATE_REPLAY) == 1 && // me
	mdsmap->get_num_mds(MDSMap::STATE_FAILED) == 0) {
      dout(2) << "boot_replay " << step << ": i am alone, moving to state active" << endl;      
      set_want_state(MDSMap::STATE_ACTIVE);
    } else {
      dout(2) << "boot_replay " << step << ": i am not alone, moving to state resolve" << endl;
      set_want_state(MDSMap::STATE_RESOLVE);
    }
    break;

  }
}


void MDS::set_want_state(int s)
{
  dout(3) << "set_want_state " << MDSMap::get_state_name(s) << endl;
  want_state = s;
  beacon_send();
}




int MDS::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;
  derr(0) << "mds shutdown start" << endl;

  cout << "Cap cache hits " << cap_cache_hits << endl;
  cout << "Cap requests " << cap_requests << endl;

  // tell everyone to stop.
  set<int> active;
  mdsmap->get_active_mds_set(active);
  for (set<int>::iterator p = active.begin();
       p != active.end();
       p++) {
    if (mdsmap->is_up(*p)) {
      dout(1) << "sending MShutdownStart to mds" << *p << endl;
      send_message_mds(new MGenericMessage(MSG_MDS_SHUTDOWNSTART),
		       *p, MDS_PORT_MAIN);
    }
  }

  // go
  set_want_state(MDSMap::STATE_STOPPING);
  return 0;
}


void MDS::handle_shutdown_start(Message *m)
{
  dout(1) << " handle_shutdown_start" << endl;

  // set flag
  set_want_state(MDSMap::STATE_STOPPING);

  delete m;
}



int MDS::shutdown_final()
{
  dout(1) << "shutdown_final" << endl;

  if (logger) logger->flush(true);
  if (logger2) logger2->flush(true);

  // send final down:out beacon (it doesn't matter if this arrives)
  set_want_state(MDSMap::STATE_OUT);

  // stop timers
  if (beacon_killer) {
    timer.cancel_event(beacon_killer);
    beacon_killer = 0;
  }
  if (beacon_sender) {
    timer.cancel_event(beacon_sender);
    beacon_sender = 0;
  }
  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = 0;
  }
  timer.cancel_all();
  timer.join();
  
  // shut down cache
  mdcache->shutdown();
  
  // shut down messenger
  messenger->shutdown();
  
  return 0;
}





void MDS::dispatch(Message *m)
{
  mds_lock.Lock();
  my_dispatch(m);
  mds_lock.Unlock();
}



void MDS::my_dispatch(Message *m)
{
  // from bad mds?
  if (m->get_source().is_mds()) {
    int from = m->get_source().num();
    if (!mdsmap->have_inst(from) ||
	mdsmap->get_inst(from) != m->get_source_inst()) {
      // bogus mds?
      if (m->get_type() != MSG_MDS_MAP) {
	dout(5) << "got " << *m << " from old/bad/imposter mds " << m->get_source()
		<< ", dropping" << endl;
	delete m;
	return;
      } else {
	dout(5) << "got " << *m << " from old/bad/imposter mds " << m->get_source()
		<< ", but it's an mdsmap, looking at it" << endl;
      }
    }
  }


  switch (m->get_dest_port()) {
    
  case MDS_PORT_ANCHORMGR:
    anchormgr->dispatch(m);
    break;
  case MDS_PORT_ANCHORCLIENT:
    anchorclient->dispatch(m);
    break;
    
  case MDS_PORT_CACHE:
    mdcache->dispatch(m);
    break;
  case MDS_PORT_LOCKER:
    locker->dispatch(m);
    break;

  case MDS_PORT_MIGRATOR:
    mdcache->migrator->dispatch(m);
    break;
  case MDS_PORT_RENAMER:
    mdcache->renamer->dispatch(m);
    break;

  case MDS_PORT_BALANCER:
    balancer->proc_message(m);
    break;
    
  case MDS_PORT_MAIN:
    proc_message(m);
    break;

  case MDS_PORT_SERVER:
    server->dispatch(m);
    break;

  default:
    dout(1) << "MDS dispatch unknown message port" << m->get_dest_port() << endl;
    assert(0);
  }


  // HACK FOR NOW
  if (is_active()) {
    // flush log to disk after every op.  for now.
    mdlog->flush();

    // trim cache
    mdcache->trim();
  }
  
  // finish any triggered contexts
  if (finished_queue.size()) {
    dout(7) << "mds has " << finished_queue.size() << " queued contexts" << endl;
    list<Context*> ls;
    ls.splice(ls.begin(), finished_queue);
    assert(finished_queue.empty());
    finish_contexts(ls);
  }

  

  // hack: force hash root?
  if (false &&
      mdcache->get_root() &&
      mdcache->get_root()->dir &&
      !(mdcache->get_root()->dir->is_hashed() || 
        mdcache->get_root()->dir->is_hashing())) {
    dout(0) << "hashing root" << endl;
    mdcache->migrator->hash_dir(mdcache->get_root()->dir);
  }




  // HACK to force export to test foreign renames
  if (false && whoami == 0) {
    static bool didit = false;
    
    // 7 to 1
    CInode *in = mdcache->get_inode(1001);
    if (in && in->is_dir() && !didit) {
      CDir *dir = in->get_or_open_dir(mdcache);
      if (dir->is_auth()) {
        dout(1) << "FORCING EXPORT" << endl;
        mdcache->migrator->export_dir(dir,1);
        didit = true;
      }
    }
  }



  // shut down?
  if (is_stopping()) {
    if (mdcache->shutdown_pass()) {
      dout(7) << "shutdown_pass=true, finished w/ shutdown, moving to up:stopped" << endl;

      // tell monitor we shut down cleanly.
      set_want_state(MDSMap::STATE_STOPPED);
    }
  }

}


void MDS::proc_message(Message *m)
{
  switch (m->get_type()) {
    // OSD ===============
    /*
  case MSG_OSD_MKFS_ACK:
    handle_osd_mkfs_ack(m);
    return;
    */
  case MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply((class MOSDOpReply*)m);
    return;
  case MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    return;


    // MDS
  case MSG_MDS_MAP:
    handle_mds_map((MMDSMap*)m);
    return;

  case MSG_MDS_BEACON:
    handle_mds_beacon((MMDSBeacon*)m);
    return;

  case MSG_MDS_SHUTDOWNSTART:    // mds0 -> mds1+
    handle_shutdown_start(m);
    return;

  case MSG_PING:
    handle_ping((MPing*)m);
    return;

  default:
    assert(0);
  }

}






void MDS::handle_ping(MPing *m)
{
  dout(10) << " received ping from " << m->get_source() << " with seq " << m->seq << endl;

  messenger->send_message(new MPingAck(m),
                          m->get_source_inst());
  
  delete m;
}

