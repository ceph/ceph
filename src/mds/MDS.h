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



#ifndef __MDS_H
#define __MDS_H

#include "mdstypes.h"

#include "msg/Dispatcher.h"
#include "include/types.h"
#include "include/Context.h"
#include "common/DecayCounter.h"
#include "common/Logger.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Timer.h"
#include "common/LogClient.h"

#include "mon/MonMap.h"
#include "MDSMap.h"

#include "SessionMap.h"


class filepath;

class OSDMap;
class Objecter;
class Filer;

class Server;
class Locker;
class MDCache;
class MDLog;
class MDBalancer;

class CInode;
class CDir;
class CDentry;

class Messenger;
class Message;

class MClientRequest;
class MClientReply;

class MMDSBeacon;

class InoTable;
class SnapServer;
class SnapClient;
class AnchorServer;
class AnchorClient;

class MDSTableServer;
class MDSTableClient;

class MDS : public Dispatcher {
 public:
  Mutex        mds_lock;
  SafeTimer    timer;

 protected:
  int          whoami;

 public:
  Messenger    *messenger;
  MonMap       *monmap;
  MDSMap       *mdsmap;
  OSDMap       *osdmap;
  Objecter     *objecter;
  Filer        *filer;       // for reading/writing to/from osds
  LogClient    logclient;

  // sub systems
  Server       *server;
  MDCache      *mdcache;
  Locker       *locker;
  MDLog        *mdlog;
  MDBalancer   *balancer;

  InoTable     *inotable;

  AnchorServer *anchorserver;
  AnchorClient *anchorclient;

  SnapServer   *snapserver;
  SnapClient   *snapclient;

  MDSTableClient *get_table_client(int t);
  MDSTableServer *get_table_server(int t);

  Logger       *logger, *logger2;


 protected:
  // -- MDS state --
  int state;         // my confirmed state
  int want_state;    // the state i want
  int want_rank;     // the mds rank i want

  list<Context*> waiting_for_active;
  map<int, list<Context*> > waiting_for_active_peer;
  list<Context*> waiting_for_nolaggy;

  map<int,version_t> peer_mdsmap_epoch;

  tid_t last_tid;    // for mds-initiated requests (e.g. stray rename)

 public:
  void wait_for_active(Context *c) { 
    waiting_for_active.push_back(c); 
  }
  void wait_for_active_peer(int who, Context *c) { 
    waiting_for_active_peer[who].push_back(c);
  }

  int get_state() { return state; } 
  bool is_dne()      { return state == MDSMap::STATE_DNE; }
  bool is_failed()   { return state == MDSMap::STATE_FAILED; }
  bool is_creating() { return state == MDSMap::STATE_CREATING; }
  bool is_starting() { return state == MDSMap::STATE_STARTING; }
  bool is_standby()  { return state == MDSMap::STATE_STANDBY; }
  bool is_replay()   { return state == MDSMap::STATE_REPLAY; }
  bool is_resolve()  { return state == MDSMap::STATE_RESOLVE; }
  bool is_reconnect() { return state == MDSMap::STATE_RECONNECT; }
  bool is_rejoin()   { return state == MDSMap::STATE_REJOIN; }
  bool is_active()   { return state == MDSMap::STATE_ACTIVE; }
  bool is_stopping() { return state == MDSMap::STATE_STOPPING; }
  bool is_stopped()  { return state == MDSMap::STATE_STOPPED; }

  void request_state(int s);

  tid_t issue_tid() { return ++last_tid; }
    

  // -- waiters --
  list<Context*> finished_queue;

  void queue_waiter(Context *c) {
    finished_queue.push_back(c);
  }
  void queue_waiters(list<Context*>& ls) {
    finished_queue.splice( finished_queue.end(), ls );
  }
  
  // -- keepalive beacon --
  version_t               beacon_last_seq;          // last seq sent to monitor
  map<version_t,utime_t>  beacon_seq_stamp;         // seq # -> time sent
  utime_t                 beacon_last_acked_stamp;  // last time we sent a beacon that got acked
  bool laggy;

  bool is_laggy() { return laggy; }

  class C_MDS_BeaconSender : public Context {
    MDS *mds;
  public:
    C_MDS_BeaconSender(MDS *m) : mds(m) {}
    void finish(int r) {
      mds->beacon_sender = 0;
      mds->beacon_send();
    }
  } *beacon_sender;
  class C_MDS_BeaconKiller : public Context {
    MDS *mds;
    utime_t lab;
  public:
    C_MDS_BeaconKiller(MDS *m, utime_t l) : mds(m), lab(l) {}
    void finish(int r) {
      if (mds->beacon_killer) {
	mds->beacon_killer = 0;
	mds->beacon_kill(lab);
      } 
      // else mds is pbly already shutting down
    }
  } *beacon_killer;

  // tick and other timer fun
  class C_MDS_Tick : public Context {
    MDS *mds;
  public:
    C_MDS_Tick(MDS *m) : mds(m) {}
    void finish(int r) {
      mds->tick_event = 0;
      mds->tick();
    }
  } *tick_event;
  void     reset_tick();

  // -- client map --
  SessionMap   sessionmap;
  epoch_t      last_client_mdsmap_bcast;
  //void log_clientmap(Context *c);


  // shutdown crap
  int req_rate;

  // ino's and fh's
 public:

  int get_req_rate() { return req_rate; }

 private:
  virtual bool dispatch_impl(Message *m);
 public:
  MDS(int whoami, Messenger *m, MonMap *mm);
  ~MDS();

  // who am i etc
  int get_nodeid() { return whoami; }
  MDSMap *get_mds_map() { return mdsmap; }
  OSDMap *get_osd_map() { return osdmap; }

  void send_message_mds(Message *m, int mds);
  void forward_message_mds(Message *req, int mds);

  void send_message_client(Message *m, int client);
  void send_message_client(Message *m, entity_inst_t clientinst);


  // start up, shutdown
  int init(bool standby=false);
  void reopen_logger(utime_t start);

  void bcast_mds_map();  // to mounted clients

  void boot_create();             // i am new mds.
  void boot_start(int step=0, int r=0);    // starting|replay

  void replay_start();
  void creating_done();
  void starting_done();
  void replay_done();

  void resolve_start();
  void resolve_done();
  void reconnect_start();
  void reconnect_done();
  void rejoin_joint_start();
  void rejoin_done();
  void recovery_done();
  void handle_mds_recovery(int who);

  void stopping_start();
  void stopping_done();
  void suicide();

  void tick();
  
  void beacon_start();
  void beacon_send();
  void beacon_kill(utime_t lab);
  void handle_mds_beacon(MMDSBeacon *m);
  void reset_beacon_killer();

  // messages
  bool _dispatch(Message *m);
  
  void ms_handle_failure(Message *m, const entity_inst_t& inst);
  void ms_handle_reset(const entity_addr_t& addr, entity_name_t last);
  void ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t last);

  // special message types
  void handle_mds_map(class MMDSMap *m);
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
    mds->_dispatch(m);
  }
};



#endif
