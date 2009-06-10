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

class MGetPoolStatsReply;

// -----------------------------------------

struct ObjectOperation {
  vector<OSDOp> ops;
  int flags;

  void add_data(int op, __u64 off, __u64 len, bufferlist& bl) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    ops[s].op.offset = off;
    ops[s].op.length = len;
    ops[s].data.claim_append(bl);
  }
  void add_xattr(int op, const char *name, const bufferlist& data) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    ops[s].op.name_len = (name ? strlen(name) : 0);
    ops[s].op.value_len = data.length();
    if (name)
      ops[s].data.append(name);
    ops[s].data.append(data);
  }
  void add_call(int op, const char *cname, const char *method, bufferlist &indata) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    ops[s].op.class_len = strlen(cname);
    ops[s].op.method_len = strlen(method);
    ops[s].op.indata_len = indata.length();
    ops[s].data.append(cname, ops[s].op.class_len);
    ops[s].data.append(method, ops[s].op.method_len);
    ops[s].data.append(indata);
  }
  void add_pgls(int op, __u64 count, __u64 cookie) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    ops[s].op.count = count;
    ops[s].op.pgls_cookie = cookie;
  }

  ObjectOperation() : flags(0) {}
};

struct ObjectRead : public ObjectOperation {
  void read(__u64 off, __u64 len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
  }
  void getxattr(const char *name) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
  }
  void getxattrs() {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTRS, 0, bl);
  }

  void call(const char *cname, const char *method, bufferlist &indata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata);
  }

  void pg_ls(__u64 count, __u64 cookie) {
    add_pgls(CEPH_OSD_OP_PGLS, count, cookie);
    flags |= CEPH_OSD_FLAG_PGOP;
  }
};

struct ObjectMutation : public ObjectOperation {
  utime_t mtime;
  
  // object data
  void write(__u64 off, __u64 len, bufferlist& bl) {
    add_data(CEPH_OSD_OP_WRITE, off, len, bl);
  }
  void write_full(bufferlist& bl) {
    add_data(CEPH_OSD_OP_WRITEFULL, 0, bl.length(), bl);
  }
  void zero(__u64 off, __u64 len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_ZERO, off, len, bl);
  }
  void remove() {
    bufferlist bl;
    add_data(CEPH_OSD_OP_DELETE, 0, 0, bl);
  }

  // object attrs
  void setxattr(const char *name, const bufferlist& bl) {
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void setxattr(const char *name, const string& s) {
    bufferlist bl;
    bl.append(s);
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void rmxattr(const char *name) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_RMXATTR, name, bl);
  }
  void setxattrs(map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, 0, bl.length());
  }
  void resetxattrs(const char *prefix, map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, prefix, bl);
  }
};


// ----------------


class Objecter {
 public:  
  Messenger *messenger;
  MonMap    *monmap;
  OSDMap    *osdmap;

  bufferlist signed_ticket;

 
 private:
  tid_t last_tid;
  int client_inc;
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
    vector<OSDOp> ops;
    snapid_t snap;
    bufferlist *pbl;

    int flags;
    Context *onfinish;

    tid_t tid;
    int attempts;

    bool paused;

    ReadOp(const object_t& o, ceph_object_layout& ol, vector<OSDOp>& op, snapid_t s, int f, Context *of) :
      oid(o), layout(ol), snap(s),
      pbl(0), flags(f), onfinish(of), 
      tid(0), attempts(0),
      paused(false) {
      ops.swap(op);
    }
  };


  struct ModifyOp {
    object_t oid;
    ceph_object_layout layout;
    SnapContext snapc;
    vector<OSDOp> ops;
    utime_t mtime;
    bufferlist bl;
    int flags;
    Context *onack, *oncommit;

    tid_t tid;
    int attempts;
    eversion_t version;

    bool paused;

    ModifyOp(const object_t& o, ceph_object_layout& l, vector<OSDOp>& op, utime_t mt,
	     const SnapContext& sc, int f, Context *ac, Context *co) :
      oid(o), layout(l), snapc(sc), mtime(mt), flags(f), onack(ac), oncommit(co), 
      tid(0), attempts(0),
      paused(false) {
      ops.swap(op);
    }
  };

  struct C_Stat : public Context {
    bufferlist bl;
    __u64 *psize;
    utime_t *pmtime;
    Context *fin;
    C_Stat(__u64 *ps, utime_t *pm, Context *c) :
      psize(ps), pmtime(pm), fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	__u64 s;
	utime_t m;
	::decode(s, p);
	::decode(m, p);
	if (psize)
	  *psize = s;
	if (pmtime)
	  *pmtime = m;
      }
      fin->finish(r);
      delete fin;
    }
  };

  
  // 
  struct PoolStatOp {
    tid_t tid;
    vector<string> pools;

    map<string,pool_stat_t> *pool_stats;
    Context *onfinish;
  };
  

 private:
  // pending ops
  hash_map<tid_t,ReadOp*>   op_read;
  hash_map<tid_t,ModifyOp*> op_modify;
  map<tid_t,PoolStatOp*>    op_poolstat;

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
    last_tid(0), client_inc(-1),
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

  // low-level
  tid_t read_submit(ReadOp *rd);
  tid_t modify_submit(ModifyOp *wr);

  tid_t read(const object_t& oid, ceph_object_layout ol, vector<OSDOp>& ops,
	     snapid_t snap, bufferlist *pbl, int flags, 
	     Context *onfinish) {
    ReadOp *rd = new ReadOp(oid, ol, ops, snap, flags, onfinish);
    rd->pbl = pbl;
    return read_submit(rd);
  }
  tid_t read(const object_t& oid, ceph_object_layout ol, 
	     ObjectRead& read, snapid_t snap, bufferlist *pbl, int flags, Context *onfinish) {
    ReadOp *rd = new ReadOp(oid, ol, read.ops, snap, read.flags | flags, onfinish);
    rd->pbl = pbl;
    return read_submit(rd);
  }

  tid_t modify(const object_t& oid, ceph_object_layout ol, vector<OSDOp>& ops,
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit) {
    ModifyOp *wr = new ModifyOp(oid, ol, ops, mtime, snapc, flags, onack, oncommit);
    return modify_submit(wr);
  }

  // high-level helpers
  tid_t stat(const object_t& oid, ceph_object_layout ol, snapid_t snap,
	     __u64 *psize, utime_t *pmtime, int flags, 
	     Context *onfinish) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_STAT;
    C_Stat *fin = new C_Stat(psize, pmtime, onfinish);
    return read(oid, ol, ops, snap, &fin->bl, flags, fin);
  }

  tid_t read(const object_t& oid, ceph_object_layout ol, 
	     __u64 off, size_t len, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    return read(oid, ol, ops, snap, pbl, flags, onfinish);
  }
  tid_t read_full(const object_t& oid, ceph_object_layout ol,
		  snapid_t snap, bufferlist *pbl, int flags,
		  Context *onfinish) {
    return read(oid, ol, 0, 0, snap, pbl, flags, onfinish);
  }
     
  tid_t mutate(const object_t& oid, ceph_object_layout ol, 
	       ObjectMutation& mutation,
	       const SnapContext& snapc, int flags,
	       Context *onack, Context *oncommit) {
    return modify(oid, ol, mutation.ops, snapc, mutation.mtime, flags, onack, oncommit);
  }
  tid_t write(const object_t& oid, ceph_object_layout ol,
	      __u64 off, size_t len, const SnapContext& snapc, const bufferlist &bl, utime_t mtime, int flags,
              Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITE;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    ops[0].data = bl;
    return modify(oid, ol, ops, snapc, mtime, flags, onack, oncommit);
  }
  tid_t write_full(const object_t& oid, ceph_object_layout ol,
		   const SnapContext& snapc, bufferlist &bl, utime_t mtime, int flags,
		   Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[0].op.offset = 0;
    ops[0].op.length = bl.length();
    ops[0].data = bl;
    return modify(oid, ol, ops, snapc, mtime, flags, onack, oncommit);
  }
  tid_t zero(const object_t& oid, ceph_object_layout ol, 
	     __u64 off, size_t len, const SnapContext& snapc, utime_t mtime, int flags,
             Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_ZERO;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    return modify_submit(new ModifyOp(oid, ol, ops, mtime, snapc, flags, onack, oncommit));
  }
  tid_t remove(const object_t& oid, ceph_object_layout ol, 
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_DELETE;
    return modify_submit(new ModifyOp(oid, ol, ops, mtime, snapc, flags, onack, oncommit));
  }

  tid_t lock(const object_t& oid, ceph_object_layout ol, int op, int flags, Context *onack, Context *oncommit) {
    SnapContext snapc;  // no snapc for lock ops
    vector<OSDOp> ops(1);
    ops[0].op.op = op;
    return modify_submit(new ModifyOp(oid, ol, ops, utime_t(), snapc, flags, onack, oncommit));
  }


  // --------------------------
  // pool stats
private:
  void poolstat_submit(PoolStatOp *op);
  void handle_get_pool_stats_reply(MGetPoolStatsReply *m);
public:
  void get_pool_stats(vector<string>& pools, map<string,pool_stat_t> *result,
		      Context *onfinish);


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

  void sg_read(vector<ObjectExtent>& extents, snapid_t snap, bufferlist *bl, int flags, Context *onfinish) {
    if (extents.size() == 1) {
      read(extents[0].oid, extents[0].layout, extents[0].offset, extents[0].length,
	   snap, bl, flags, onfinish);
    } else {
      C_Gather *g = new C_Gather;
      vector<bufferlist> resultbl(extents.size());
      int i=0;
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	read(p->oid, p->layout, p->offset, p->length,
	     snap, &resultbl[i++], flags, g->new_sub());
      }
      g->set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
    }
  }


  void sg_write(vector<ObjectExtent>& extents, const SnapContext& snapc, const bufferlist& bl, utime_t mtime,
		int flags, Context *onack, Context *oncommit) {
    if (extents.size() == 1) {
      write(extents[0].oid, extents[0].layout, extents[0].offset, extents[0].length,
	    snapc, bl, mtime, flags, onack, oncommit);
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
	      snapc, cur, mtime, flags,
	      gack ? gack->new_sub():0,
	      gcom ? gcom->new_sub():0);
      }
    }
  }

  void ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t dest);

};

#endif
