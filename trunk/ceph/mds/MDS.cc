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
#include "MDLog.h"
#include "MDBalancer.h"
#include "IdAllocator.h"
#include "Migrator.h"
//#include "Renamer.h"

#include "AnchorTable.h"
#include "AnchorClient.h"

#include "common/Logger.h"
#include "common/LogType.h"

#include "common/Timer.h"

#include "events/ESession.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"


#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << whoami << " "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " mds" << whoami << " "





// cons/des
MDS::MDS(int whoami, Messenger *m, MonMap *mm) : 
  timer(mds_lock), 
  clientmap(this) {
  this->whoami = whoami;

  monmap = mm;
  messenger = m;

  mdsmap = new MDSMap;
  osdmap = new OSDMap;

  objecter = new Objecter(messenger, monmap, osdmap);
  filer = new Filer(objecter);

  mdcache = new MDCache(this);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this);

  anchorclient = new AnchorClient(this);
  idalloc = new IdAllocator(this);

  anchortable = new AnchorTable(this);

  server = new Server(this);
  locker = new Locker(this, mdcache);

  // clients
  last_client_mdsmap_bcast = 0;
  
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
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }
  if (idalloc) { delete idalloc; idalloc = NULL; }
  if (anchortable) { delete anchortable; anchortable = NULL; }
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
  static LogType mds_logtype, mds_cache_logtype;
  static bool didit = false;
  if (!didit) {
    didit = true;
    
    mds_logtype.add_inc("req");
    mds_logtype.add_inc("reply");
    mds_logtype.add_inc("fw");
    mds_logtype.add_inc("cfw");
    
    mds_logtype.add_inc("dir_f");
    mds_logtype.add_inc("dir_c");
    mds_logtype.add_inc("mkdir");
    
    mds_logtype.add_set("c");
    mds_logtype.add_set("ctop");
    mds_logtype.add_set("cbot");
    mds_logtype.add_set("cptail");  
    mds_logtype.add_set("cpin");
    mds_logtype.add_inc("cex");
    mds_logtype.add_inc("dis");
    mds_logtype.add_inc("cmiss");
    
    mds_logtype.add_set("l");
    mds_logtype.add_set("q");
    mds_logtype.add_set("popanyd");
    mds_logtype.add_set("popnest");
    
    mds_logtype.add_set("buf");
    
    mds_logtype.add_inc("iex");
    mds_logtype.add_inc("iim");
    mds_logtype.add_inc("ex");
    mds_logtype.add_inc("im");
    mds_logtype.add_inc("imex");  
    mds_logtype.add_set("nex");
    mds_logtype.add_set("nim");
  }
 
  if (whoami < 0) return;

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

  char n[80];
  sprintf(n, "mds%d.cache", whoami);
  logger2 = new Logger(n, (LogType*)&mds_cache_logtype);

  server->reopen_logger();
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

void MDS::forward_message_mds(Message *req, int mds, int port)
{
  // client request?
  if (req->get_type() == MSG_CLIENT_REQUEST) {
    MClientRequest *creq = (MClientRequest*)req;
    creq->inc_num_fwd();    // inc forward counter

    // tell the client where it should go
    messenger->send_message(new MClientRequestForward(creq->get_tid(), mds, creq->get_num_fwd()),
			    creq->get_client_inst());
    
    if (!creq->is_idempotent()) {
      delete req;
      return;  // don't actually forward if non-idempotent!  client has to do it.
    }
  }
  
  // forward
  send_message_mds(req, mds, port);
}



void MDS::send_message_client(Message *m, int client)
{
  version_t seq = clientmap.inc_push_seq(client);
  dout(10) << "send_message_client client" << client << " seq " << seq << " " << *m << dendl;
  messenger->send_message(m, clientmap.get_inst(client));
}

void MDS::send_message_client(Message *m, entity_inst_t clientinst)
{
  version_t seq = clientmap.inc_push_seq(clientinst.name.num());
  dout(10) << "send_message_client client" << clientinst.name.num() << " seq " << seq << " " << *m << dendl;
  messenger->send_message(m, clientinst);
}


class C_MDS_SendMessageClientSession : public Context {
  MDS *mds;
  Message *msg;
  entity_inst_t clientinst;
public:
  C_MDS_SendMessageClientSession(MDS *md, Message *ms, entity_inst_t& ci) :
    mds(md), msg(ms), clientinst(ci) {}
  void finish(int r) {
    mds->clientmap.open_session(clientinst);
    mds->send_message_client(msg, clientinst.name.num());
  }
};

void MDS::send_message_client_maybe_open(Message *m, entity_inst_t clientinst)
{
  int client = clientinst.name.num();
  if (!clientmap.have_session(client)) {
    // no session!
    dout(10) << "send_message_client opening session with " << clientinst << dendl;
    clientmap.add_opening(client);
    mdlog->submit_entry(new ESession(clientinst, true, clientmap.inc_projected()),
			new C_MDS_SendMessageClientSession(this, m, clientinst));
  } else {
    // we have a session.
    send_message_client(m, clientinst);
  }
}



int MDS::init(bool standby)
{
  mds_lock.Lock();
  
  want_state = MDSMap::STATE_BOOT;
  
  // starting beacon.  this will induce an MDSMap from the monitor
  beacon_start();
  
  // schedule tick
  reset_tick();

  // init logger
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
  tick_event = 0;

  // reschedule
  reset_tick();

  // log
  mds_load_t load = balancer->get_load();
  
  if (logger) {
    req_rate = logger->get("req");
    
    logger->set("l", (int)load.mds_load(g_clock.now()));
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
  


void MDS::beacon_send()
{
  ++beacon_last_seq;
  dout(10) << "beacon_send " << MDSMap::get_state_name(want_state)
	   << " seq " << beacon_last_seq
	   << " (currently " << MDSMap::get_state_name(state) << ")"
	   << dendl;

  beacon_seq_stamp[beacon_last_seq] = g_clock.now();
  
  int mon = monmap->pick_mon();
  messenger->send_message(new MMDSBeacon(messenger->get_myinst(), mdsmap->get_epoch(), 
					 want_state, beacon_last_seq),
			  monmap->get_inst(mon));

  // schedule next sender
  if (beacon_sender) timer.cancel_event(beacon_sender);
  beacon_sender = new C_MDS_BeaconSender(this);
  timer.add_event_after(g_conf.mds_beacon_interval, beacon_sender);
}

void MDS::handle_mds_beacon(MMDSBeacon *m)
{
  dout(10) << "handle_mds_beacon " << MDSMap::get_state_name(m->get_state())
	   << " seq " << m->get_seq() << dendl;
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

void MDS::reset_beacon_killer()
{
  utime_t when = beacon_last_acked_stamp;
  when += g_conf.mds_beacon_grace;
  
  dout(15) << "reset_beacon_killer last_acked_stamp at " << beacon_last_acked_stamp
	   << ", will die at " << when << dendl;
  
  if (beacon_killer) timer.cancel_event(beacon_killer);

  beacon_killer = new C_MDS_BeaconKiller(this, beacon_last_acked_stamp);
  timer.add_event_at(when, beacon_killer);
}

void MDS::beacon_kill(utime_t lab)
{
  if (lab == beacon_last_acked_stamp) {
    dout(0) << "beacon_kill last_acked_stamp " << lab 
	    << ", killing myself."
	    << dendl;
    suicide();
  } else {
    dout(20) << "beacon_kill last_acked_stamp " << beacon_last_acked_stamp 
	     << " != my " << lab 
	     << ", doing nothing."
	     << dendl;
  }
}



void MDS::handle_mds_map(MMDSMap *m)
{
  version_t hadepoch = mdsmap->get_epoch();
  version_t epoch = m->get_epoch();
  dout(5) << "handle_mds_map epoch " << epoch << " from " << m->get_source() << dendl;

  // note source's map version
  if (m->get_source().is_mds() && 
      peer_mdsmap_epoch[m->get_source().num()] < epoch) {
    dout(15) << " peer " << m->get_source()
	     << " has mdsmap epoch >= " << epoch
	     << dendl;
    peer_mdsmap_epoch[m->get_source().num()] = epoch;
  }

  // is it new?
  if (epoch <= mdsmap->get_epoch()) {
    dout(5) << " old map epoch " << epoch << " <= " << mdsmap->get_epoch() 
	    << ", discarding" << dendl;
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
  set<int> oldactive;
  mdsmap->get_mds_set(oldactive, MDSMap::STATE_ACTIVE);
  set<int> oldcreating;
  mdsmap->get_mds_set(oldcreating, MDSMap::STATE_CREATING);
  set<int> oldstopped;
  mdsmap->get_mds_set(oldstopped, MDSMap::STATE_STOPPED);

  // decode and process
  mdsmap->decode(m->get_encoded());
  
  // see who i am
  whoami = mdsmap->get_addr_rank(messenger->get_myaddr());
  if (whoami < 0) {
    dout(1) << "handle_mds_map i'm not in the mdsmap, killing myself" << dendl;
    suicide();
    return;
  }
  if (oldwhoami != whoami) {
    // update messenger.
    reopen_logger();
    dout(1) << "handle_mds_map i am now mds" << whoami
	    << " incarnation " << mdsmap->get_inc(whoami)
	    << dendl;

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

  // for debug
  if (g_conf.mds_dump_cache_on_map)
    mdcache->dump_cache();

  // update my state
  state = mdsmap->get_state(whoami);
  
  // did it change?
  if (oldstate != state) {
    if (state == want_state) {
      dout(1) << "handle_mds_map new state " << mdsmap->get_state_name(state) << dendl;
    } else {
      dout(1) << "handle_mds_map new state " << mdsmap->get_state_name(state)
	//	      << ", although i wanted " << mdsmap->get_state_name(want_state)
	      << dendl;
      want_state = state;
    }    

    // contemplate suicide
    if (mdsmap->get_inst(whoami) != messenger->get_myinst()) {
      dout(1) << "apparently i've been replaced by " << mdsmap->get_inst(whoami) << ", committing suicide." << dendl;
      suicide();
      return;
    }

    // now active?
    if (is_active()) {
      // did i just recover?
      if (oldstate == MDSMap::STATE_REJOIN ||
	  oldstate == MDSMap::STATE_RECONNECT) 
	recovery_done();
      finish_contexts(waiting_for_active);  // kick waiters
    } else if (is_replay()) {
      replay_start();
    } else if (is_resolve()) {
      resolve_start();
    } else if (is_reconnect()) {
      reconnect_start();
    } else if (is_stopping()) {
      assert(oldstate == MDSMap::STATE_ACTIVE);
      stopping_start();
    } else if (is_stopped()) {
      assert(oldstate == MDSMap::STATE_STOPPING);
      suicide();
      return;
    }
  }
  
  
  // RESOLVE
  // is someone else newly resolving?
  if (is_resolve() || is_rejoin() || is_active() || is_stopping()) {
    set<int> resolve;
    mdsmap->get_mds_set(resolve, MDSMap::STATE_RESOLVE);
    if (oldresolve != resolve) {
      dout(10) << "resolve set is " << resolve << ", was " << oldresolve << dendl;
      for (set<int>::iterator p = resolve.begin(); p != resolve.end(); ++p) {
	if (*p == whoami) continue;
	if (oldresolve.count(*p)) continue;
	mdcache->send_resolve(*p);  // now or later.
      }
    }
  }
  
  // REJOIN
  // is everybody finally rejoining?
  if (is_rejoin() || is_active() || is_stopping()) {
    // did we start?
    if (!wasrejoining && mdsmap->is_rejoining())
      rejoin_joint_start();

    // did we finish?
    if (g_conf.mds_dump_cache_after_rejoin &&
	wasrejoining && !mdsmap->is_rejoining()) 
      mdcache->dump_cache();      // for DEBUG only
  }

  // did someone go active?
  if (is_active() || is_stopping()) {
    set<int> active;
    mdsmap->get_mds_set(active, MDSMap::STATE_ACTIVE);
    for (set<int>::iterator p = active.begin(); p != active.end(); ++p) {
      if (*p == whoami) continue;         // not me
      if (oldactive.count(*p)) continue;  // newly so?
      handle_mds_recovery(*p);
    }
  }

  if (is_active() || is_stopping()) {
    // did anyone go down?
    set<int> failed;
    mdsmap->get_mds_set(failed, MDSMap::STATE_FAILED);
    for (set<int>::iterator p = failed.begin(); p != failed.end(); ++p) {
      if (oldfailed.count(*p)) continue;       // newly so?
      mdcache->handle_mds_failure(*p);
    }

    // did anyone stop?
    set<int> stopped;
    mdsmap->get_mds_set(stopped, MDSMap::STATE_STOPPED);
    for (set<int>::iterator p = stopped.begin(); p != stopped.end(); ++p) {
      if (oldstopped.count(*p)) continue;       // newly so?
      mdcache->migrator->handle_mds_failure_or_stop(*p);
    }
  }


  // in set set changed?
  /*
  if (state >= MDSMap::STATE_ACTIVE &&   // only if i'm active+.  otherwise they'll get map during reconnect.
      mdsmap->get_same_in_set_since() > last_client_mdsmap_bcast) {
    bcast_mds_map();
  }
  */

  // just got mdsmap+osdmap?
  if (hadepoch == 0 && 
      mdsmap->get_epoch() > 0 &&
      osdmap->get_epoch() > 0) {
    boot();
  } else if (want_state != state) {
    // resend beacon.
    beacon_send();
  }

  delete m;
}

void MDS::bcast_mds_map()
{
  dout(7) << "bcast_mds_map " << mdsmap->get_epoch() << dendl;

  // share the map with mounted clients
  for (set<int>::const_iterator p = clientmap.get_session_set().begin();
       p != clientmap.get_session_set().end();
       ++p) {
    messenger->send_message(new MMDSMap(mdsmap),
			    clientmap.get_inst(*p));
  }
  last_client_mdsmap_bcast = mdsmap->get_epoch();
}


void MDS::handle_osd_map(MOSDMap *m)
{
  version_t hadepoch = osdmap->get_epoch();
  dout(10) << "handle_osd_map had " << hadepoch << dendl;
  
  // process
  objecter->handle_osd_map(m);

  // just got mdsmap+osdmap?
  if (hadepoch == 0 && 
      osdmap->get_epoch() > 0 &&
      mdsmap->get_epoch() > 0) 
    boot();
}


void MDS::set_want_state(int s)
{
  dout(3) << "set_want_state " << MDSMap::get_state_name(s) << dendl;
  want_state = s;
  beacon_send();
}

void MDS::boot()
{   
  if (is_creating()) 
    boot_create();    // new tables, journal
  else if (is_starting())
    boot_start();     // old tables, empty journal
  else if (is_replay()) 
    boot_replay();    // replay, join
  else 
    assert(is_standby());
}


class C_MDS_BootFinish : public Context {
  MDS *mds;
public:
  C_MDS_BootFinish(MDS *m) : mds(m) {}
  void finish(int r) { mds->boot_finish(); }
};

void MDS::boot_create()
{
  dout(3) << "boot_create" << dendl;

  C_Gather *fin = new C_Gather(new C_MDS_BootFinish(this));

  if (whoami == 0) {
    dout(3) << "boot_create since i am also mds0, creating root inode and dir" << dendl;

    // create root inode.
    mdcache->open_root(0);
    CInode *root = mdcache->get_root();
    assert(root);
    
    // force empty root dir
    CDir *dir = root->get_dirfrag(frag_t());
    dir->mark_complete();
    dir->mark_dirty(dir->pre_dirty());
    
    // save it
    dir->commit(0, fin->new_sub());
  }

  // create my stray dir
  {
    dout(10) << "boot_create creating local stray dir" << dendl;
    mdcache->open_local_stray();
    CInode *stray = mdcache->get_stray();
    CDir *dir = stray->get_dirfrag(frag_t());
    dir->mark_complete();
    dir->mark_dirty(dir->pre_dirty());
    dir->commit(0, fin->new_sub());
  }

  // start with a fresh journal
  dout(10) << "boot_create creating fresh journal" << dendl;
  mdlog->reset();
  mdlog->write_head(fin->new_sub());
  
  // write our first subtreemap
  mdcache->log_subtree_map(fin->new_sub());

  // fixme: fake out idalloc (reset, pretend loaded)
  dout(10) << "boot_create creating fresh idalloc table" << dendl;
  idalloc->reset();
  idalloc->save(fin->new_sub());

  // write empty clientmap
  clientmap.save(fin->new_sub());
  
  // fixme: fake out anchortable
  if (mdsmap->get_anchortable() == whoami) {
    dout(10) << "boot_create creating fresh anchortable" << dendl;
    anchortable->create_fresh();
    anchortable->save(fin->new_sub());
  }
}

void MDS::boot_start()
{
  dout(2) << "boot_start" << dendl;
  
  C_Gather *fin = new C_Gather(new C_MDS_BootFinish(this));
  
  dout(2) << "boot_start opening idalloc" << dendl;
  idalloc->load(fin->new_sub());

  dout(2) << "boot_start opening clientmap" << dendl;
  clientmap.load(fin->new_sub());
  
  if (mdsmap->get_anchortable() == whoami) {
    dout(2) << "boot_start opening anchor table" << dendl;
    anchortable->load(fin->new_sub());
  } else {
    dout(2) << "boot_start i have no anchor table" << dendl;
  }

  dout(2) << "boot_start opening mds log" << dendl;
  mdlog->open(fin->new_sub());

  if (mdsmap->get_root() == whoami) {
    dout(2) << "boot_start opening root directory" << dendl;
    mdcache->open_root(fin->new_sub());
  }

  dout(2) << "boot_start opening local stray directory" << dendl;
  mdcache->open_local_stray();
}

void MDS::boot_finish()
{
  dout(3) << "boot_finish" << dendl;

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
    {
      C_Gather *gather = new C_Gather(new C_MDS_BootRecover(this, 2));
      dout(2) << "boot_replay " << step << ": opening idalloc" << dendl;
      idalloc->load(gather->new_sub());

      dout(2) << "boot_replay " << step << ": opening clientmap" << dendl;
      clientmap.load(gather->new_sub());

      if (mdsmap->get_anchortable() == whoami) {
	dout(2) << "boot_replay " << step << ": opening anchor table" << dendl;
	anchortable->load(gather->new_sub());
      }
    }
    break;

  case 2:
    dout(2) << "boot_replay " << step << ": opening mds log" << dendl;
    mdlog->open(new C_MDS_BootRecover(this, 3));
    break;
    
  case 3:
    dout(2) << "boot_replay " << step << ": replaying mds log" << dendl;
    mdlog->replay(new C_MDS_BootRecover(this, 4));
    break;

  case 4:
    replay_done();
    break;

  }
}


void MDS::replay_start()
{
  dout(1) << "replay_start" << dendl;

  // initialize gather sets
  set<int> rs;
  mdsmap->get_recovery_mds_set(rs);
  rs.erase(whoami);
  dout(1) << "now replay.  my recovery peers are " << rs << dendl;
  mdcache->set_recovery_set(rs);

  // note: don't actually start yet.  boot() will get called once we have 
  // an mdsmap AND osdmap.
}

void MDS::replay_done()
{
  dout(1) << "replay_done" << dendl;

  if (mdsmap->get_num_in_mds() == 1 &&
      mdsmap->get_num_mds(MDSMap::STATE_FAILED) == 0) { // just me!
    dout(2) << "i am alone, moving to state reconnect" << dendl;      
    set_want_state(MDSMap::STATE_RECONNECT);
  } else {
    dout(2) << "i am not alone, moving to state resolve" << dendl;
    set_want_state(MDSMap::STATE_RESOLVE);
  }
}


void MDS::resolve_start()
{
  dout(1) << "resolve_start" << dendl;

  set<int> who;
  mdsmap->get_mds_set(who, MDSMap::STATE_RESOLVE);
  mdsmap->get_mds_set(who, MDSMap::STATE_REJOIN);
  mdsmap->get_mds_set(who, MDSMap::STATE_ACTIVE);
  mdsmap->get_mds_set(who, MDSMap::STATE_STOPPING);
  for (set<int>::iterator p = who.begin(); p != who.end(); ++p) {
    if (*p == whoami) continue;
    mdcache->send_resolve(*p);  // now.
  }
}
void MDS::resolve_done()
{
  dout(1) << "resolve_done" << dendl;
  set_want_state(MDSMap::STATE_RECONNECT);
}

void MDS::reconnect_start()
{
  dout(1) << "reconnect_start" << dendl;
  server->reconnect_clients();
}
void MDS::reconnect_done()
{
  dout(1) << "reconnect_done" << dendl;
  set_want_state(MDSMap::STATE_REJOIN);    // move to rejoin state

  /*
  if (mdsmap->get_num_in_mds() == 1 &&
      mdsmap->get_num_mds(MDSMap::STATE_FAILED) == 0) { // just me!

    // finish processing caps (normally, this happens during rejoin, but we're skipping that...)
    mdcache->rejoin_gather_finish();

    set_want_state(MDSMap::STATE_ACTIVE);    // go active
  } else {
    set_want_state(MDSMap::STATE_REJOIN);    // move to rejoin state
  }
  */
}

void MDS::rejoin_joint_start()
{
  dout(1) << "rejoin_joint_start" << dendl;
  mdcache->rejoin_send_rejoins();
}
void MDS::rejoin_done()
{
  dout(1) << "rejoin_done" << dendl;
  mdcache->show_subtrees();
  mdcache->show_cache();
  set_want_state(MDSMap::STATE_ACTIVE);
}


void MDS::recovery_done()
{
  dout(1) << "recovery_done -- successful recovery!" << dendl;
  assert(is_active());
  
  // kick anchortable (resent AGREEs)
  if (mdsmap->get_anchortable() == whoami) 
    anchortable->finish_recovery();
  
  // kick anchorclient (resent COMMITs)
  anchorclient->finish_recovery();
  
  mdcache->start_recovered_purges();
  
  // tell connected clients
  bcast_mds_map();  
}

void MDS::handle_mds_recovery(int who) 
{
  dout(5) << "handle_mds_recovery mds" << who << dendl;
  
  mdcache->handle_mds_recovery(who);

  if (anchortable)
    anchortable->handle_mds_recovery(who);
  anchorclient->handle_mds_recovery(who);
  
  queue_waiters(waiting_for_active_peer[who]);
  waiting_for_active_peer.erase(who);
}

void MDS::stopping_start()
{
  dout(2) << "stopping_start" << dendl;

  // start cache shutdown
  mdcache->shutdown_start();
  
  // terminate client sessions
  server->terminate_sessions();
  
  // flush log
  mdlog->set_max_events(0);
  mdlog->trim(NULL);
}
void MDS::stopping_done()
{
  dout(2) << "stopping_done" << dendl;

  // tell monitor we shut down cleanly.
  set_want_state(MDSMap::STATE_STOPPED);
}

  

void MDS::suicide()
{
  dout(1) << "suicide" << dendl;

  // flush loggers
  if (logger) logger->flush(true);
  if (logger2) logger2->flush(true);
  mdlog->flush_logger();
  
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
	mdsmap->get_inst(from) != m->get_source_inst() ||
	mdsmap->is_down(from)) {
      // bogus mds?
      if (m->get_type() != MSG_MDS_MAP) {
	dout(5) << "got " << *m << " from down/old/bad/imposter mds " << m->get_source()
		<< ", dropping" << dendl;
	delete m;
	return;
      } else {
	dout(5) << "got " << *m << " from old/bad/imposter mds " << m->get_source()
		<< ", but it's an mdsmap, looking at it" << dendl;
      }
    }
  }


  switch (m->get_dest_port()) {
    
  case MDS_PORT_ANCHORTABLE:
    anchortable->dispatch(m);
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
    //mdcache->renamer->dispatch(m);
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
    dout(1) << "MDS dispatch unknown message port" << m->get_dest_port() << dendl;
    assert(0);
  }
  
  // finish any triggered contexts
  if (finished_queue.size()) {
    dout(7) << "mds has " << finished_queue.size() << " queued contexts" << dendl;
    dout(10) << finished_queue << dendl;
    list<Context*> ls;
    ls.splice(ls.begin(), finished_queue);
    assert(finished_queue.empty());
    finish_contexts(ls);
  }


  // HACK FOR NOW
  if (is_active()) {
    // flush log to disk after every op.  for now.
    mdlog->flush();

    // trim cache
    mdcache->trim();
  }

  
  // hack: thrash exports
  for (int i=0; i<g_conf.mds_thrash_exports; i++) {
    set<int> s;
    if (!is_active()) break;
    mdsmap->get_mds_set(s, MDSMap::STATE_ACTIVE);
    if (s.size() < 2 || mdcache->get_num_inodes() < 10) 
      break;  // need peers for this to work.

    dout(7) << "mds thrashing exports pass " << (i+1) << "/" << g_conf.mds_thrash_exports << dendl;
    
    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (ls.empty()) continue;                // must be an open dir.
    CDir *dir = ls.front();
    if (!dir->get_parent_dir()) continue;    // must be linked.
    if (!dir->is_auth()) continue;           // must be auth.

    int dest;
    do {
      int k = rand() % s.size();
      set<int>::iterator p = s.begin();
      while (k--) p++;
      dest = *p;
    } while (dest == whoami);
    mdcache->migrator->export_dir(dir,dest);
  }
  // hack: thrash exports
  for (int i=0; i<g_conf.mds_thrash_fragments; i++) {
    if (!is_active()) break;
    dout(7) << "mds thrashing fragments pass " << (i+1) << "/" << g_conf.mds_thrash_fragments << dendl;
    
    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (ls.empty()) continue;                // must be an open dir.
    CDir *dir = ls.front();
    if (!dir->get_parent_dir()) continue;    // must be linked.
    if (!dir->is_auth()) continue;           // must be auth.
    mdcache->split_dir(dir, 1);// + (rand() % 3));
  }

  // hack: force hash root?
  /*
  if (false &&
      mdcache->get_root() &&
      mdcache->get_root()->dir &&
      !(mdcache->get_root()->dir->is_hashed() || 
        mdcache->get_root()->dir->is_hashing())) {
    dout(0) << "hashing root" << dendl;
    mdcache->migrator->hash_dir(mdcache->get_root()->dir);
  }
  */



  // shut down?
  if (is_stopping()) {
    if (mdcache->shutdown_pass()) {
      dout(7) << "shutdown_pass=true, finished w/ shutdown, moving to down:stopped" << dendl;
      stopping_done();
    }
  }

}


void MDS::proc_message(Message *m)
{
  switch (m->get_type()) {

    // OSD
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

  default:
    assert(0);
  }

}



void MDS::ms_handle_failure(Message *m, const entity_inst_t& inst) 
{
  mds_lock.Lock();
  dout(10) << "handle_ms_failure to " << inst << " on " << *m << dendl;
  
  if (m->get_type() == MSG_MDS_MAP && m->get_dest().is_client()) 
    server->client_reconnect_failure(m->get_dest().num());

  delete m;
  mds_lock.Unlock();
}

