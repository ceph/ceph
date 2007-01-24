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
#include "messages/MMDSBoot.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"


LogType mds_logtype, mds_cache_logtype;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << whoami << " "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << whoami << " "





// cons/des
MDS::MDS(int whoami, Messenger *m, MonMap *mm) {
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


  req_rate = 0;

  state = MDSMap::STATE_DOWN;  // booting


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

  if (filer) { delete filer; filer = 0; }
  if (objecter) { delete objecter; objecter = 0; }
  if (messenger) { delete messenger; messenger = NULL; }

  if (logger) { delete logger; logger = 0; }
  if (logger2) { delete logger2; logger2 = 0; }

}


void MDS::reopen_log()
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

  
  char n[80];
  sprintf(n, "mds%d.cache", whoami);
  logger2 = new Logger(n, (LogType*)&mds_cache_logtype);
}

void MDS::send_message_mds(Message *m, int mds, int port, int fromport)
{
  if (port && !fromport) 
    fromport = port;
  messenger->send_message(m, MSG_ADDR_MDS(mds), mdsmap->get_inst(mds), port, fromport);
}


class C_MDS_Tick : public Context {
  MDS *mds;
public:
  C_MDS_Tick(MDS *m) : mds(m) {}
  void finish(int r) {
    mds->tick();
  }
};



int MDS::init()
{
  // request osd map
  dout(5) << "requesting mds and osd maps from mon" << endl;
  int mon = monmap->pick_mon();
  messenger->send_message(new MMDSBoot, MSG_ADDR_MON(mon), monmap->get_inst(mon));

  // schedule tick
  g_timer.add_event_after(1.0, new C_MDS_Tick(this));
  return 0;
}



void MDS::tick()
{
  mds_lock.Lock();

  // reschedule
  g_timer.add_event_after(1.0, new C_MDS_Tick(this));

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
  if (!is_booting()) {
    
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


  mds_lock.Unlock();
}

void MDS::handle_mds_map(MMDSMap *m)
{
  map<epoch_t, bufferlist>::reverse_iterator p = m->maps.rbegin();

  dout(1) << "handle_mds_map epoch " << p->first << endl;
  mdsmap->decode(p->second);

  delete m;
  
  // see who i am
  int w = mdsmap->get_inst_rank(messenger->get_myinst());
  if (w != whoami) {
    whoami = w;
    messenger->reset_myaddr(MSG_ADDR_MDS(w));
    reopen_log();
  }
  dout(1) << "map says i am " << w << endl;

  if (is_booting()) {
    // we need an osdmap too.
    int mon = monmap->pick_mon();
    messenger->send_message(new MOSDGetMap(0),
			    MSG_ADDR_MON(mon), monmap->get_inst(mon));
  }
}

void MDS::handle_osd_map(MOSDMap *m)
{
  // process locally
  objecter->handle_osd_map(m);
  
  if (is_booting()) {
    // we got our maps.  mkfs for recovery?
    if (g_conf.mkfs)
      boot_mkfs();
    else 
      boot_recover();
  }
  
  // pass on to clients
  for (set<int>::iterator it = clientmap.get_mount_set().begin();
       it != clientmap.get_mount_set().end();
       it++) {
    MOSDMap *n = new MOSDMap;
    n->maps = m->maps;
    n->incremental_maps = m->incremental_maps;
    messenger->send_message(n, MSG_ADDR_CLIENT(*it), clientmap.get_inst(*it));
  }
}


class C_MDS_MkfsFinish : public Context {
  MDS *mds;
public:
  C_MDS_MkfsFinish(MDS *m) : mds(m) {}
  void finish(int r) { mds->boot_mkfs_finish(); }
};

void MDS::boot_mkfs()
{
  dout(3) << "boot_mkfs" << endl;

  C_Gather *fin = new C_Gather(new C_MDS_MkfsFinish(this));
  
  if (whoami == 0) {
    dout(3) << "boot_mkfs creating root inode and dir" << endl;

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
  dout(10) << "boot_mkfs creating fresh journal" << endl;
  mdlog->reset();
  mdlog->write_head(fin->new_sub());

  // write our empty importmap
  mdcache->log_import_map(fin->new_sub());

  // fixme: fake out idalloc (reset, pretend loaded)
  dout(10) << "boot_mkfs creating fresh idalloc table" << endl;
  idalloc->reset();
  idalloc->save(fin->new_sub());
  
  // fixme: fake out anchortable
  if (mdsmap->get_anchortable() == whoami) {
    dout(10) << "boot_mkfs creating fresh anchortable" << endl;
    anchormgr->reset();
    anchormgr->save(fin->new_sub());
  }
}

void MDS::boot_mkfs_finish()
{
  dout(3) << "boot_mkfs_finish" << endl;
  mark_active();
}


class C_MDS_BootRecover : public Context {
  MDS *mds;
  int nextstep;
public:
  C_MDS_BootRecover(MDS *m, int n) : mds(m), nextstep(n) {}
  void finish(int r) { mds->boot_recover(nextstep); }
};

void MDS::boot_recover(int step)
{
  if (is_booting()) 
    state = MDSMap::STATE_STARTING;

  switch (step) {
  case 0:
    /* no, EImportMap takes care of all this.
    if (whoami == 0) {
      dout(2) << "boot_recover " << step << ": creating root inode" << endl;
      mdcache->open_root(0);
      step = 1;
      // fall-thru
    } else {
      // FIXME
      assert(0);
      }*/
    step = 1;

  case 1:
    dout(2) << "boot_recover " << step << ": opening idalloc" << endl;
    idalloc->load(new C_MDS_BootRecover(this, 2));
    break;

  case 2:
    if (mdsmap->get_anchortable() == whoami) {
      dout(2) << "boot_recover " << step << ": opening anchor table" << endl;
      anchormgr->load(new C_MDS_BootRecover(this, 3));
      break;
    } else {
      dout(2) << "boot_recover " << step << ": i have no anchor table" << endl;
      step++;
    }
    // fall-thru

  case 3:
    dout(2) << "boot_recover " << step << ": opening mds log" << endl;
    mdlog->open(new C_MDS_BootRecover(this, 4));
    break;
    
  case 4:
    dout(2) << "boot_recover " << step << ": replaying mds log" << endl;
    mdlog->replay(new C_MDS_BootRecover(this, 5));
    break;

  case 5:
    dout(2) << "boot_recover " << step << ": restarting any recovered purges" << endl;
    mdcache->start_recovered_purges();
    step++;
    // fall-thru

  case 6:
    dout(2) << "boot_recover " << step << ": done." << endl;
    mark_active();
  }
}



void MDS::mark_active()
{
  dout(3) << "mark_active" << endl;
  state = MDSMap::STATE_ACTIVE;
  finish_contexts(waitfor_active);  // kick waiters
}





int MDS::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;
  derr(0) << "mds shutdown start" << endl;

  for (set<int>::iterator p = mdsmap->get_mds_set().begin();
       p != mdsmap->get_mds_set().end();
       p++) {
    if (mdsmap->is_starting(*p) || mdsmap->is_active(*p)) {
      dout(1) << "sending MShutdownStart to mds" << *p << endl;
      send_message_mds(new MGenericMessage(MSG_MDS_SHUTDOWNSTART),
		       *p, MDS_PORT_MAIN);
    }
  }

  if (idalloc) idalloc->shutdown();
  
  handle_shutdown_start(NULL);
  return 0;
}


void MDS::handle_shutdown_start(Message *m)
{
  dout(1) << " handle_shutdown_start" << endl;

  // set flag
  state = MDSMap::STATE_STOPPING;

  mdcache->shutdown_start();
  
  // save anchor table
  if (mdsmap->get_anchortable() == whoami) 
    anchormgr->save(0);  // FIXME FIXME

  // flush log
  mdlog->set_max_events(0);
  mdlog->trim(NULL);

  if (m) delete m;

  //g_conf.debug_mds = 10;
}



int MDS::shutdown_final()
{
  dout(1) << "shutdown" << endl;
  
  state = MDSMap::STATE_DOWN;
  
  // shut down cache
  mdcache->shutdown();

  // tell monitor
  messenger->send_message(new MGenericMessage(MSG_SHUTDOWN),
			  MSG_ADDR_MON(0), monmap->get_inst(0));

  // shut down messenger
  messenger->shutdown();

  return 0;
}





void MDS::dispatch(Message *m)
{
  // make sure we advacne the clock
  g_clock.now();

  // process
  mds_lock.Lock();
  my_dispatch(m);
  mds_lock.Unlock();
}



void MDS::my_dispatch(Message *m)
{

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
  /*
  static bool did_heartbeat_hack = false;
  if (!shutting_down && !shut_down &&
      false && 
      !did_heartbeat_hack) {
    osdmonitor->initiate_heartbeat();
    did_heartbeat_hack = true;
  }
  */


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
      dout(7) << "shutdown_pass=true, finished w/ shutdown" << endl;
      shutdown_final();      
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

  case MSG_MDS_SHUTDOWNSTART:    // mds0 -> mds1+
    handle_shutdown_start(m);
    return;



  case MSG_PING:
    handle_ping((MPing*)m);
    return;
  }

}






void MDS::handle_ping(MPing *m)
{
  dout(10) << " received ping from " << m->get_source() << " with seq " << m->seq << endl;

  messenger->send_message(new MPingAck(m),
                          m->get_source(), m->get_source_inst());
  
  delete m;
}

