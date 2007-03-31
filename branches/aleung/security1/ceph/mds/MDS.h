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
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "mdstypes.h"

#include "msg/Dispatcher.h"
#include "include/types.h"
#include "include/Context.h"
#include "common/DecayCounter.h"
#include "common/Logger.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Timer.h"

#include "mon/MonMap.h"
#include "MDSMap.h"

#include "ClientMap.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#include "crypto/ExtCap.h"
#include "crypto/CapGroup.h"
#include "crypto/MerkleTree.h"
#include "crypto/Renewal.h"
#include "crypto/RecentPopularity.h"

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

class MMDSBeacon;

class UserBatch;


class MDS : public Dispatcher {
 public:
  Mutex        mds_lock;

  SafeTimer    timer;

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
  int state;         // my confirmed state
  int want_state;    // the state i want
  list<Context*> waitfor_active;

  // mds pub/priv keys
  esignPriv myPrivKey;
  esignPub myPubKey;

public:
  map<int,version_t> peer_mdsmap_epoch;

  
  int cap_cache_hits;
  int cap_requests;
  // logical user group
  //map<gid_t, CapGroup> unix_groups;
  map<gid_t, hash_t> unix_groups_map;
  // hash to group map
  map<hash_t, CapGroup> unix_groups_byhash;

  // recent capabilities to renew
  set<cap_id_t> recent_caps;
  Renewal token;

  // count of capability ids used
  int cap_id_count;

  // batched file groups by user
  map<uid_t, UserBatch*> user_batch;

  // successors
  RecentPopularity rp_predicter;
  inodeno_t last_access;
  map<inodeno_t, hash_t> precompute_succ;
  map<inodeno_t , map<uid_t, ExtCap> > predict_cap_cache;

  void queue_waitfor_active(Context *c) { waitfor_active.push_back(c); }

  bool is_dne()      { return state == MDSMap::STATE_DNE; }
  bool is_out()      { return state == MDSMap::STATE_OUT; }
  bool is_failed()   { return state == MDSMap::STATE_FAILED; }
  bool is_creating() { return state == MDSMap::STATE_CREATING; }
  bool is_starting() { return state == MDSMap::STATE_STARTING; }
  bool is_standby()  { return state == MDSMap::STATE_STANDBY; }
  bool is_replay()   { return state == MDSMap::STATE_REPLAY; }
  bool is_resolve()  { return state == MDSMap::STATE_RESOLVE; }
  bool is_rejoin()   { return state == MDSMap::STATE_REJOIN; }
  bool is_active()   { return state == MDSMap::STATE_ACTIVE; }
  bool is_stopping() { return state == MDSMap::STATE_STOPPING; }
  bool is_stopped()  { return state == MDSMap::STATE_STOPPED; }

  void set_want_state(int s);


  // -- waiters --
  list<Context*> finished_queue;

  void queue_finished(Context *c) {
    finished_queue.push_back(c);
  }
  void queue_finished(list<Context*>& ls) {
    finished_queue.splice( finished_queue.end(), ls );
  }
  
  // -- keepalive beacon --
  version_t               beacon_last_seq;          // last seq sent to monitor
  map<version_t,utime_t>  beacon_seq_stamp;         // seq # -> time sent
  utime_t                 beacon_last_acked_stamp;  // last time we sent a beacon that got acked
  Context *beacon_sender;
  Context *beacon_killer;                           // next scheduled time of death

  // tick and other timer fun
  Context *tick_event;
  void     reset_tick();

  

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
  int init(bool standby=false);
  void reopen_logger();

  void boot_create();             // i am new mds.
  void boot_start();              // i am old but empty (was down:out) mds.
  void boot_replay(int step=0);   // i am recovering existing (down:failed) mds.
  void boot_finish();

  int shutdown_start();
  int shutdown_final();

  esignPub& getPubKey() { return myPubKey; }
  esignPriv& getPrvKey() { return myPrivKey; }

  int hash_dentry(inodeno_t ino, const string& s) {
    return 0; // fixme
  }

  void tick();
  
  void beacon_start();
  void beacon_send();
  void beacon_kill(utime_t lab);
  void handle_mds_beacon(MMDSBeacon *m);
  void reset_beacon_killer();

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



#endif
