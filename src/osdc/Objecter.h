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
class MonClient;
class Message;

class MPoolSnapReply;

class MGetPoolStatsReply;
class MStatfsReply;

// -----------------------------------------

struct ObjectOperation {
  vector<OSDOp> ops;
  int flags;

  void add_op(int op) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
  }
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

  // ------

  // pg
  void pg_ls(__u64 count, __u64 cookie) {
    add_pgls(CEPH_OSD_OP_PGLS, count, cookie);
    flags |= CEPH_OSD_FLAG_PGOP;
  }

  // object data
  void read(__u64 off, __u64 len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
  }
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
  void getxattr(const char *name) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
  }
  void getxattrs() {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTRS, 0, bl);
  }
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
  
  // trivialmap
  void tmap_update(bufferlist& bl) {
    add_data(CEPH_OSD_OP_TMAPUP, 0, 0, bl);
  }
  void tmap_put(bufferlist& bl) {
    add_data(CEPH_OSD_OP_TMAPPUT, 0, bl.length(), bl);
  }
  void tmap_get() {
    add_op(CEPH_OSD_OP_TMAPGET);
  }

  // object classes
  void call(const char *cname, const char *method, bufferlist &indata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata);
  }
 
  ObjectOperation() : flags(0) {}
};


// ----------------


class Objecter {
 public:  
  Messenger *messenger;
  MonClient *monc;
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

  version_t last_seen_version;
  Mutex &client_lock;
  SafeTimer timer;
  
  class C_Tick : public Context {
    Objecter *ob;
  public:
    C_Tick(Objecter *o) : ob(o) {}
    void finish(int r) { ob->tick(); }
  };
  void tick();
  void resend_slow_ops();

  /*** track pending operations ***/
  // read
 public:

  struct Op {
    object_t oid;
    ceph_object_layout layout;
    vector<OSDOp> ops;

    snapid_t snapid;
    SnapContext snapc;
    utime_t mtime;

    bufferlist inbl;
    bufferlist *outbl;

    int flags;
    Context *onack, *oncommit;

    tid_t tid;
    eversion_t version;        // for op replay
    int attempts;

    bool paused;

    Op(const object_t& o, ceph_object_layout& l, vector<OSDOp>& op,
       int f, Context *ac, Context *co) :
      oid(o), layout(l), 
      snapid(CEPH_NOSNAP), outbl(0), flags(f), onack(ac), oncommit(co), 
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

  // Pools and statistics 
  struct ListContext {
    int current_pg;
    __u64 cookie;
    int starting_pg_num;
    bool at_end;

    int pool_id;
    int pool_snap_seq;
    int max_entries;
    std::list<object_t> *entries;

    //silly list for the C interface
    std::list<object_t> list;

    ListContext() : current_pg(0), cookie(0), starting_pg_num(0),
		    at_end(false), pool_id(0),
		    pool_snap_seq(0), max_entries(0),
		    entries(NULL) {}
  };

  struct C_List : public Context {
    ListContext *list_context;
    Context *final_finish;
    bufferlist *bl;
    Objecter *objecter;
    C_List(ListContext *lc, Context * finish, bufferlist *b, Objecter *ob) :
      list_context(lc), final_finish(finish), bl(b), objecter(ob) {}
    void finish(int r) {
      objecter->_list_reply(list_context, bl, final_finish);
    }
  };
  
  struct PoolStatOp {
    tid_t tid;
    vector<string> pools;

    map<string,pool_stat_t> *pool_stats;
    Context *onfinish;

    utime_t last_submit;
  };

  struct StatfsOp {
    tid_t tid;
    ceph_statfs *stats;
    Context *onfinish;

    utime_t last_submit;
  };

  struct SnapOp {
    tid_t tid;
    int pool;
    string name;
    Context *onfinish;
    bool create;
    int* replyCode;

    utime_t last_submit;
  };

 private:
  // pending ops
  hash_map<tid_t,Op*>       op_osd;
  map<tid_t,PoolStatOp*>    op_poolstat;
  map<tid_t,StatfsOp*>      op_statfs;
  map<tid_t,SnapOp*>        op_snap;

  map<epoch_t,list<Context*> > waiting_for_map;

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

  void _list_reply(ListContext *list_context, bufferlist *bl, Context *final_finish);


 public:
  Objecter(Messenger *m, MonClient *mc, OSDMap *om, Mutex& l) : 
    messenger(m), monc(mc), osdmap(om),
    last_tid(0), client_inc(-1),
    num_unacked(0), num_uncommitted(0),
    last_epoch_requested(0),
    last_seen_version(0),
    client_lock(l), timer(l)
  { }
  ~Objecter() { }

  void init();
  void shutdown();

  // messages
 public:
  void dispatch(Message *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_map(class MOSDMap *m);

private:
  // low-level
  tid_t op_submit(Op *op);

  // public interface
 public:
  bool is_active() {
    return !(op_osd.empty() && op_poolstat.empty() && op_statfs.empty());
  }
  void dump_active();

  int get_client_incarnation() const { return client_inc; }
  void set_client_incarnation(int inc) { client_inc = inc; }

  void wait_for_new_map(Context *c, epoch_t epoch) {
    waiting_for_map[epoch].push_back(c);
  }

  // mid-level helpers
  tid_t mutate(const object_t& oid, ceph_object_layout ol, 
	       ObjectOperation& op,
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit) {
    Op *o = new Op(oid, ol, op.ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t read(const object_t& oid, ceph_object_layout ol, 
	     ObjectOperation& op,
	     snapid_t snapid, bufferlist *pbl, int flags,
	     Context *onack) {
    Op *o = new Op(oid, ol, op.ops, flags, onack, NULL);
    o->snapid = snapid;
    o->outbl = pbl;
    return op_submit(o);
  }

  // high-level helpers
  tid_t stat(const object_t& oid, ceph_object_layout ol, snapid_t snap,
	     __u64 *psize, utime_t *pmtime, int flags, 
	     Context *onfinish) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_STAT;
    C_Stat *fin = new C_Stat(psize, pmtime, onfinish);
    Op *o = new Op(oid, ol, ops, flags, fin, 0);
    o->snapid = snap;
    o->outbl = &fin->bl;
    return op_submit(o);
  }

  tid_t read(const object_t& oid, ceph_object_layout ol, 
	     __u64 off, size_t len, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    Op *o = new Op(oid, ol, ops, flags, onfinish, 0);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }

  tid_t getxattr(const object_t& oid, ceph_object_layout ol,
	     const char *name, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_GETXATTR;
    ops[0].op.name_len = (name ? strlen(name) : 0);
    ops[0].op.value_len = 0;
    if (name)
      ops[0].data.append(name);
    Op *o = new Op(oid, ol, ops, flags, onfinish, 0);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }

  tid_t read_full(const object_t& oid, ceph_object_layout ol,
		  snapid_t snap, bufferlist *pbl, int flags,
		  Context *onfinish) {
    return read(oid, ol, 0, 0, snap, pbl, flags, onfinish);
  }
     
  // writes
  tid_t _modify(const object_t& oid, ceph_object_layout ol, 
		vector<OSDOp>& ops, utime_t mtime,
		const SnapContext& snapc, int flags,
	       Context *onack, Context *oncommit) {
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t write(const object_t& oid, ceph_object_layout ol,
	      __u64 off, size_t len, const SnapContext& snapc, const bufferlist &bl,
	      utime_t mtime, int flags,
              Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITE;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    ops[0].data = bl;
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t write_full(const object_t& oid, ceph_object_layout ol,
		   const SnapContext& snapc, const bufferlist &bl, utime_t mtime, int flags,
		   Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[0].op.offset = 0;
    ops[0].op.length = bl.length();
    ops[0].data = bl;
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t zero(const object_t& oid, ceph_object_layout ol, 
	     __u64 off, size_t len, const SnapContext& snapc, utime_t mtime, int flags,
             Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_ZERO;
    ops[0].op.offset = off;
    ops[0].op.length = len;
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t remove(const object_t& oid, ceph_object_layout ol, 
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_DELETE;
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }

  tid_t lock(const object_t& oid, ceph_object_layout ol, int op, int flags,
	     Context *onack, Context *oncommit) {
    SnapContext snapc;  // no snapc for lock ops
    vector<OSDOp> ops(1);
    ops[0].op.op = op;
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t setxattr(const object_t& oid, ceph_object_layout ol,
	      const char *name, const SnapContext& snapc, const bufferlist &bl,
	      utime_t mtime, int flags,
              Context *onack, Context *oncommit) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_SETXATTR;
    ops[0].op.name_len = (name ? strlen(name) : 0);
    ops[0].op.value_len = bl.length();
    if (name)
      ops[0].data.append(name);
   ops[0].data.append(bl);
    Op *o = new Op(oid, ol, ops, flags, onack, oncommit);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }


  void list_objects(ListContext *p, Context *onfinish);

  // -------------------------
  // snapshots
private:
  void pool_snap_submit(SnapOp *op);
public:
  void create_pool_snap(int *reply, int pool, string& snapName, Context *onfinish);
  void delete_pool_snap(int *reply, int pool, string& snapName, Context *onfinish);
  void handle_pool_snap_reply(MPoolSnapReply *m);

  // --------------------------
  // pool stats
private:
  void poolstat_submit(PoolStatOp *op);
public:
  void handle_get_pool_stats_reply(MGetPoolStatsReply *m);
  void get_pool_stats(vector<string>& pools, map<string,pool_stat_t> *result,
		      Context *onfinish);

  // ---------------------------
  // df stats
private:
  void fs_stats_submit(StatfsOp *op);
public:
  void handle_fs_stats_reply(MStatfsReply *m);
  void get_fs_stats(ceph_statfs& result, Context *onfinish);

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
