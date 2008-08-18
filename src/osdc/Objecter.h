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

#ifndef __OBJECTER_H
#define __OBJECTER_H

#include "include/types.h"
#include "include/buffer.h"

#include "osd/OSDMap.h"
#include "messages/MOSDOp.h"

#include "common/Timer.h"

#include <list>
#include <map>
#include <ext/hash_map>
using namespace std;
using namespace __gnu_cxx;

class Context;
class Messenger;
class OSDMap;
class MonMap;
class Message;

class Objecter {
 public:  
  Messenger *messenger;
  MonMap    *monmap;
  OSDMap    *osdmap;
  
 private:
  tid_t last_tid;
  int client_inc;
  int inc_lock;       // optional
  int num_unacked;
  int num_uncommitted;

  epoch_t last_epoch_requested;
  utime_t last_epoch_requested_stamp;

  void maybe_request_map();

  Mutex &client_lock;
  SafeTimer timer;
  
  class C_Tick : public Context {
    Objecter *ob;
  public:
    C_Tick(Objecter *o) : ob(o) {}
    void finish(int r) { ob->tick(); }
  };
  void tick();


  /*** track pending operations ***/
  // read
 public:
  class OSDOp {
  public:
    list<ObjectExtent> extents;
    int inc_lock;
    OSDOp() : inc_lock(0) {}
    virtual ~OSDOp() {}
  };

  class OSDRead : public OSDOp {
  public:
    bufferlist *bl;
    Context *onfinish;
    map<tid_t, ObjectExtent> ops;
    map<object_t, bufferlist*> read_data;  // bits of data as they come back
    int flags;

    OSDRead(bufferlist *b, int f) : OSDOp(), bl(b), onfinish(0), flags(f) {
      if (bl)
	bl->clear();
    }
  };

  OSDRead *prepare_read(bufferlist *b, int f) {
    return new OSDRead(b, f);
  }

  class OSDStat : public OSDOp {
  public:
    tid_t tid;
    __u64 *size;  // where the size goes.
    int flags;
    Context *onfinish;
    OSDStat(__u64 *s, int f) : OSDOp(), tid(0), size(s), flags(f), onfinish(0) { }
  };

  OSDStat *prepare_stat(__u64 *s, int f) {
    return new OSDStat(s, f);
  }

  // generic modify
  class OSDModify : public OSDOp {
  public:
    SnapContext snapc;
    int op;
    list<ObjectExtent> extents;
    int flags;
    Context *onack;
    Context *oncommit;
    map<tid_t, ObjectExtent> waitfor_ack;
    map<tid_t, eversion_t>   tid_version;
    map<tid_t, ObjectExtent> waitfor_commit;

    OSDModify(const SnapContext& sc, int o, int f) : OSDOp(), snapc(sc), op(o), flags(f), onack(0), oncommit(0) {}
  };

  OSDModify *prepare_modify(const SnapContext& sc, int o, int f) { 
    return new OSDModify(sc, o, f); 
  }
  
  // write (includes the bufferlist)
  class OSDWrite : public OSDModify {
  public:
    bufferlist bl;
    OSDWrite(const SnapContext& sc, bufferlist &b, int f) : OSDModify(sc, CEPH_OSD_OP_WRITE, f), bl(b) {}
  };

  OSDWrite *prepare_write(const SnapContext& sc, bufferlist &b, int f) { 
    return new OSDWrite(sc, b, f); 
  }

  

 private:
  // pending ops
  hash_map<tid_t,OSDStat*>   op_stat;
  hash_map<tid_t,OSDRead*>   op_read;
  hash_map<tid_t,OSDModify*> op_modify;

  /**
   * track pending ops by pg
   *  ...so we can cope with failures, map changes
   */
  class PG {
  public:
    vector<int> acting;
    set<tid_t>  active_tids; // active ops
    utime_t last;

    PG() {}
    
    // primary - where i write
    int primary() {
      if (acting.empty()) return -1;
      return acting[0];
    }
    // acker - where i read, and receive acks from
    int acker() {
      if (acting.empty()) return -1;
      if (g_conf.osd_rep == OSD_REP_PRIMARY)
        return acting[0];
      else
        return acting[acting.size() > 1 ? 1:0];
    }
  };

  hash_map<pg_t,PG> pg_map;
  
  
  PG &get_pg(pg_t pgid);
  void close_pg(pg_t pgid) {
    assert(pg_map.count(pgid));
    assert(pg_map[pgid].active_tids.empty());
    pg_map.erase(pgid);
  }
  void scan_pgs(set<pg_t>& chnaged_pgs);
  void kick_requests(set<pg_t>& changed_pgs);
    

 public:
  Objecter(Messenger *m, MonMap *mm, OSDMap *om, Mutex& l) : 
    messenger(m), monmap(mm), osdmap(om), 
    last_tid(0), client_inc(-1), inc_lock(0),
    num_unacked(0), num_uncommitted(0),
    last_epoch_requested(0),
    client_lock(l), timer(l)
  { }
  ~Objecter() { }

  void init();
  void shutdown();

  // messages
 public:
  void dispatch(Message *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_stat_reply(class MOSDOpReply *m);
  void handle_osd_read_reply(class MOSDOpReply *m);
  void handle_osd_modify_reply(class MOSDOpReply *m);
  void handle_osd_lock_reply(class MOSDOpReply *m);
  void handle_osd_map(class MOSDMap *m);

 private:
  tid_t readx_submit(OSDRead *rd, ObjectExtent& ex, bool retry=false);
  tid_t modifyx_submit(OSDModify *wr, ObjectExtent& ex, tid_t tid=0);
  tid_t stat_submit(OSDStat *st);

  // public interface
 public:
  bool is_active() {
    return !(op_read.empty() && op_modify.empty());
  }
  void dump_active();

  int get_client_incarnation() const { return client_inc; }
  void set_client_incarnation(int inc) { client_inc = inc; }

  //int get_inc_lock() const { return inc_lock; }
  void set_inc_lock(int l) { inc_lock = l; }
    

  // med level
  tid_t readx(OSDRead *read, Context *onfinish);
  tid_t modifyx(OSDModify *wr, Context *onack, Context *oncommit);

  // even lazier
  tid_t read(object_t oid, __u64 off, size_t len, ceph_object_layout ol, bufferlist *bl, int flags,
             Context *onfinish);
  tid_t stat(object_t oid, __u64 *size, ceph_object_layout ol, int flags, Context *onfinish);

  tid_t write(object_t oid, __u64 off, size_t len, ceph_object_layout ol, const SnapContext& snapc, bufferlist &bl, int flags,
              Context *onack, Context *oncommit);
  tid_t zero(object_t oid, __u64 off, size_t len, ceph_object_layout ol, const SnapContext& snapc, int flags,
             Context *onack, Context *oncommit);

  // no snapc for lock ops
  tid_t lock(int op, object_t oid, int flags, ceph_object_layout ol, Context *onack, Context *oncommit);


  void ms_handle_failure(Message *m, entity_name_t dest, const entity_inst_t& inst);

};

#endif
