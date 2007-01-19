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



#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <vector>
#include <set>
#include <map>
#include <ostream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "msg/Dispatcher.h"
#include "include/types.h"
#include "include/Context.h"
#include "common/DecayCounter.h"
#include "common/Logger.h"
#include "common/Mutex.h"

#include "mon/MonMap.h"

#include "ClientMap.h"


#define MDS_PORT_MAIN     0
#define MDS_PORT_SERVER   1
#define MDS_PORT_CACHE    2
#define MDS_PORT_LOCKER   3
#define MDS_PORT_STORE    4
#define MDS_PORT_BALANCER 5
#define MDS_PORT_MIGRATOR 6
#define MDS_PORT_RENAMER  7

#define MDS_PORT_ANCHORCLIENT 10
#define MDS_PORT_ANCHORMGR    11


#define MDS_INO_ROOT              1
#define MDS_INO_PGTABLE           2
#define MDS_INO_LOG_OFFSET        0x100
#define MDS_INO_IDS_OFFSET        0x200
#define MDS_INO_INODEFILE_OFFSET  0x300
#define MDS_INO_ANCHORTABLE       0x400
#define MDS_INO_BASE              0x1000

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.
#define MDS_TRAVERSE_FAIL          4


class filepath;

class MDSMap;
class OSDMap;
class Objecter;
class Filer;

class Server;
class Locker;
class AnchorTable;
class AnchorClient;
class MDCache;
class MDStore;
class MDLog;
class MDBalancer;
class IdAllocator;

class CInode;
class CDir;
class CDentry;

class Messenger;
class Message;

class MClientRequest;
class MClientReply;
class MHashReaddir;
class MHashReaddirReply;




class MDS : public Dispatcher {
 public:
  Mutex        mds_lock;

 protected:
  int          whoami;

 public:
  Messenger    *messenger;
  MDSMap       *mdsmap;
  MonMap       *monmap;
  OSDMap       *osdmap;
  Objecter     *objecter;
  Filer        *filer;       // for reading/writing to/from osds

  ClientMap    clientmap;

  // sub systems
  Server       *server;
  MDCache      *mdcache;
  Locker       *locker;
  MDStore      *mdstore;
  MDLog        *mdlog;
  MDBalancer   *balancer;

  IdAllocator  *idalloc;

  AnchorTable  *anchormgr;
  AnchorClient *anchorclient;

  Logger       *logger, *logger2;



 protected:
  // -- MDS state --
  static const int STATE_BOOTING       = 1;  // fetching mds and osd maps
  static const int STATE_MKFS          = 2;  // creating a file system
  static const int STATE_RECOVERING    = 3;  // recovering mds log
  static const int STATE_ACTIVE        = 4;  // up and active!
  static const int STATE_STOPPING      = 5;
  static const int STATE_STOPPED       = 6;

  int state;
  list<Context*> waitfor_active;

public:
  void queue_waitfor_active(Context *c) { waitfor_active.push_back(c); }

  bool is_booting() { return state == STATE_BOOTING; }
  bool is_recovering() { return state == STATE_RECOVERING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }
  bool is_stopped() { return state == STATE_STOPPED; }

  void mark_active();


  // -- waiters --
  list<Context*> finished_queue;

  void queue_finished(Context *c) {
    finished_queue.push_back(c);
  }
  void queue_finished(list<Context*>& ls) {
    finished_queue.splice( finished_queue.end(), ls );
  }
  


  // shutdown crap
  int req_rate;

  // ino's and fh's
 public:

  int get_req_rate() { return req_rate; }

 protected:

  friend class MDStore;

  
 public:
  MDS(int whoami, Messenger *m, MonMap *mm);
  ~MDS();

  // who am i etc
  int get_nodeid() { return whoami; }
  MDSMap *get_mds_map() { return mdsmap; }
  OSDMap *get_osd_map() { return osdmap; }

  void send_message_mds(Message *m, int mds, int port=0, int fromport=0);

  // start up, shutdown
  int init();

  void boot_mkfs();      
  void boot_mkfs_finish();
  void boot_recover(int step=0);   

  int shutdown_start();
  int shutdown_final();

  void tick();

  // messages
  void proc_message(Message *m);
  virtual void dispatch(Message *m);
  void my_dispatch(Message *m);

  // special message types
  void handle_ping(class MPing *m);

  void handle_mds_map(class MMDSMap *m);

  void handle_shutdown_start(Message *m);

  // osds
  void handle_osd_getmap(Message *m);
  void handle_osd_map(class MOSDMap *m);

};



class C_MDS_RetryMessage : public Context {
  Message *m;
  MDS *mds;
public:
  C_MDS_RetryMessage(MDS *mds, Message *m) {
    assert(m);
    this->m = m;
    this->mds = mds;
  }
  virtual void finish(int r) {
    mds->my_dispatch(m);
  }
};


ostream& operator<<(ostream& out, MDS& mds);


#endif
