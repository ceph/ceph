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
  struct ReadOp {
    object_t oid;
    ceph_object_layout layout;
    vector<ceph_osd_op> ops;
    bufferlist *pbl;
    __u64 *psize;
    int flags;
    Context *onfinish;

    tid_t tid;
    int attempts;
    int inc_lock;

    ReadOp(object_t o, ceph_object_layout& ol, vector<ceph_osd_op>& op, int f, Context *of) :
      oid(o), layout(ol), 
      pbl(0), psize(0), flags(f), onfinish(of), 
      tid(0), attempts(0), inc_lock(-1) {
      ops.swap(op);
    }
  };


  struct ModifyOp {
    object_t oid;
    ceph_object_layout layout;
    SnapContext snapc;
    vector<ceph_osd_op> ops;
    bufferlist bl;
    int flags;
    Context *onack, *oncommit;

    tid_t tid;
    int attempts;
    int inc_lock;
    eversion_t version;

    ModifyOp(object_t o, ceph_object_layout& l, vector<ceph_osd_op>& op,
	     const SnapContext& sc, int f, Context *ac, Context *co) :
      oid(o), layout(l), snapc(sc), flags(f), onack(ac), oncommit(co), 
      tid(0), attempts(0), inc_lock(-1) {
      ops.swap(op);
    }
  };


 private:
  // pending ops
  hash_map<tid_t,ReadOp* >  op_read;
  hash_map<tid_t,ModifyOp*> op_modify;

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
      return acting[0];
    }
  };

  hash_map<pg_t,PG> pg_map;
  
  
  PG &get_pg(pg_t pgid);
  void close_pg(pg_t pgid) {
    assert(pg_map.count(pgid));
    assert(pg_map[pgid].active_tids.empty());
    pg_map.erase(pgid);
  }
  void scan_pgs(set<pg_t>& changed_pgs);
  void scan_pgs_for(set<pg_t>& changed_pgs, int osd);
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
  void handle_osd_read_reply(class MOSDOpReply *m);
  void handle_osd_modify_reply(class MOSDOpReply *m);
  void handle_osd_lock_reply(class MOSDOpReply *m);
  void handle_osd_map(class MOSDMap *m);

 private:
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
    
  // low-level
  tid_t read_submit(ReadOp *rd);
  tid_t modify_submit(ModifyOp *wr);

  tid_t read(object_t oid, ceph_object_layout ol, vector<ceph_osd_op>& ops,
	     bufferlist *pbl, __u64 *psize, int flags, 
	     Context *onfinish) {
    ReadOp *rd = new ReadOp(oid, ol, ops, flags, onfinish);
    rd->pbl = pbl;
    rd->psize = psize;
    return read_submit(rd);
  }

  tid_t modify(object_t oid, ceph_object_layout ol, vector<ceph_osd_op>& ops,
	       const SnapContext& snapc, bufferlist &bl, int flags,
	       Context *onack, Context *oncommit) {
    ModifyOp *wr = new ModifyOp(oid, ol, ops, snapc, flags, onack, oncommit);
    wr->bl = bl;
    return modify_submit(wr);
  }

  // high-level helpers
  tid_t stat(object_t oid, ceph_object_layout ol,
	     __u64 *psize, int flags, 
	     Context *onfinish) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_STAT;
    return read(oid, ol, ops, 0, psize, flags, onfinish);
  }

  tid_t read(object_t oid, ceph_object_layout ol,
	     __u64 off, size_t len, bufferlist *pbl, int flags,
	     Context *onfinish) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_READ;
    ops[0].offset = off;
    ops[0].length = len;
    return read(oid, ol, ops, pbl, 0, flags, onfinish);
  }

  tid_t mutate(object_t oid, ceph_object_layout ol, 
	       ObjectMutation& mutation,
	       const SnapContext& snapc, int flags,
	       Context *onack, Context *oncommit) {
    return modify(oid, ol, mutation.ops, snapc, mutation.data, flags, onack, oncommit);
  }
  tid_t write(object_t oid, ceph_object_layout ol,
	      __u64 off, size_t len, const SnapContext& snapc, bufferlist &bl, int flags,
              Context *onack, Context *oncommit) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_WRITE;
    ops[0].offset = off;
    ops[0].length = len;
    return modify(oid, ol, ops, snapc, bl, flags, onack, oncommit);
  }
  tid_t write_full(object_t oid, ceph_object_layout ol,
		   const SnapContext& snapc, bufferlist &bl, int flags,
		   Context *onack, Context *oncommit) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_WRITEFULL;
    ops[0].offset = 0;
    ops[0].length = bl.length();
    return modify(oid, ol, ops, snapc, bl, flags, onack, oncommit);
  }
  tid_t zero(object_t oid, ceph_object_layout ol, 
	     __u64 off, size_t len, const SnapContext& snapc, int flags,
             Context *onack, Context *oncommit) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_ZERO;
    ops[0].offset = off;
    ops[0].length = len;
    return modify_submit(new ModifyOp(oid, ol, ops, snapc, flags, onack, oncommit));
  }
  tid_t remove(object_t oid, ceph_object_layout ol, 
	       const SnapContext& snapc, int flags,
	       Context *onack, Context *oncommit) {
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = CEPH_OSD_OP_DELETE;
    return modify_submit(new ModifyOp(oid, ol, ops, snapc, flags, onack, oncommit));
  }

  tid_t lock(object_t oid, ceph_object_layout ol, int op, int flags, Context *onack, Context *oncommit) {
    SnapContext snapc;  // no snapc for lock ops
    vector<ceph_osd_op> ops(1);
    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = op;
    return modify_submit(new ModifyOp(oid, ol, ops, snapc, flags, onack, oncommit));
  }



  // ---------------------------
  // some scatter/gather hackery

  void _sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
		       bufferlist *bl, Context *onfinish);

  struct C_SGRead : public Context {
    Objecter *objecter;
    vector<ObjectExtent> extents;
    vector<bufferlist> resultbl;
    bufferlist *bl;
    Context *onfinish;
    C_SGRead(Objecter *ob, 
	     vector<ObjectExtent>& e, vector<bufferlist>& r, bufferlist *b, Context *c) :
      objecter(ob), bl(b), onfinish(c) {
      extents.swap(e);
      resultbl.swap(r);
    }
    void finish(int r) {
      objecter->_sg_read_finish(extents, resultbl, bl, onfinish);
    }      
  };

  void sg_read(vector<ObjectExtent>& extents, bufferlist *bl, int flags, Context *onfinish) {
    if (extents.size() == 1) {
      read(extents[0].oid, extents[0].layout, extents[0].offset, extents[0].length,
	   bl, flags, onfinish);
    } else {
      C_Gather *g = new C_Gather;
      vector<bufferlist> resultbl(extents.size());
      int i=0;
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	read(p->oid, p->layout, p->offset, p->length,
	     &resultbl[i++], flags, g->new_sub());
      }
      g->set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
    }
  }


  void sg_write(vector<ObjectExtent>& extents, const SnapContext& snapc, bufferlist bl,
		int flags, Context *onack, Context *oncommit) {
    if (extents.size() == 1) {
      write(extents[0].oid, extents[0].layout, extents[0].offset, extents[0].length,
	    snapc, bl, flags, onack, oncommit);
    } else {
      C_Gather *gack = 0, *gcom = 0;
      if (onack)
	gack = new C_Gather(onack);
      if (oncommit)
	gcom = new C_Gather(oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	bufferlist cur;
	for (map<__u32,__u32>::iterator bit = p->buffer_extents.begin();
	     bit != p->buffer_extents.end();
	     bit++)
	  bl.copy(bit->first, bit->second, cur);
	assert(cur.length() == p->length);
	write(p->oid, p->layout, p->offset, p->length, 
	      snapc, cur, flags,
	      gack ? gack->new_sub():0,
	      gcom ? gcom->new_sub():0);
      }
    }
  }

  void ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t dest);

};

#endif
