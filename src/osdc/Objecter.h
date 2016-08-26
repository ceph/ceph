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

#ifndef CEPH_OBJECTER_H
#define CEPH_OBJECTER_H

#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <memory>
#include <sstream>
#include <type_traits>

#include <boost/thread/shared_mutex.hpp>

#include "include/assert.h"
#include "include/buffer.h"
#include "include/types.h"
#include "include/rados/rados_types.hpp"

#include "common/admin_socket.h"
#include "common/ceph_time.h"
#include "common/ceph_timer.h"
#include "common/Finisher.h"
#include "common/shunique_lock.h"

#include "messages/MOSDOp.h"
#include "osd/OSDMap.h"


using namespace std;

class Context;
class Messenger;
class OSDMap;
class MonClient;
class Message;
class Finisher;

class MPoolOpReply;

class MGetPoolStatsReply;
class MStatfsReply;
class MCommandReply;
class MWatchNotify;

class PerfCounters;

// -----------------------------------------

struct ObjectOperation {
  vector<OSDOp> ops;
  int flags;
  int priority;

  vector<bufferlist*> out_bl;
  vector<Context*> out_handler;
  vector<int*> out_rval;

  ObjectOperation() : flags(0), priority(0) {}
  ~ObjectOperation() {
    while (!out_handler.empty()) {
      delete out_handler.back();
      out_handler.pop_back();
    }
  }

  size_t size() {
    return ops.size();
  }

  void set_last_op_flags(int flags) {
    assert(!ops.empty());
    ops.rbegin()->op.flags = flags;
  }

  class C_TwoContexts;
  /**
   * Add a callback to run when this operation completes,
   * after any other callbacks for it.
   */
  void add_handler(Context *extra);

  OSDOp& add_op(int op) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    out_bl.resize(s+1);
    out_bl[s] = NULL;
    out_handler.resize(s+1);
    out_handler[s] = NULL;
    out_rval.resize(s+1);
    out_rval[s] = NULL;
    return ops[s];
  }
  void add_data(int op, uint64_t off, uint64_t len, bufferlist& bl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    osd_op.indata.claim_append(bl);
  }
  void add_writesame(int op, uint64_t off, uint64_t write_len,
		     bufferlist& bl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.writesame.offset = off;
    osd_op.op.writesame.length = write_len;
    osd_op.op.writesame.data_length = bl.length();
    osd_op.indata.claim_append(bl);
  }
  void add_clone_range(int op, uint64_t off, uint64_t len,
		       const object_t& srcoid, uint64_t srcoff,
		       snapid_t srcsnapid) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.clonerange.offset = off;
    osd_op.op.clonerange.length = len;
    osd_op.op.clonerange.src_offset = srcoff;
    osd_op.soid = sobject_t(srcoid, srcsnapid);
  }
  void add_xattr(int op, const char *name, const bufferlist& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    if (name)
      osd_op.indata.append(name);
    osd_op.indata.append(data);
  }
  void add_xattr_cmp(int op, const char *name, uint8_t cmp_op,
		     uint8_t cmp_mode, const bufferlist& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    osd_op.op.xattr.cmp_op = cmp_op;
    osd_op.op.xattr.cmp_mode = cmp_mode;
    if (name)
      osd_op.indata.append(name);
    osd_op.indata.append(data);
  }
  void add_call(int op, const char *cname, const char *method,
		bufferlist &indata,
		bufferlist *outbl, Context *ctx, int *prval) {
    OSDOp& osd_op = add_op(op);

    unsigned p = ops.size() - 1;
    out_handler[p] = ctx;
    out_bl[p] = outbl;
    out_rval[p] = prval;

    osd_op.op.cls.class_len = strlen(cname);
    osd_op.op.cls.method_len = strlen(method);
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname, osd_op.op.cls.class_len);
    osd_op.indata.append(method, osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
  }
  void add_pgls(int op, uint64_t count, collection_list_handle_t cookie,
		epoch_t start_epoch) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    ::encode(cookie, osd_op.indata);
  }
  void add_pgls_filter(int op, uint64_t count, const bufferlist& filter,
		       collection_list_handle_t cookie, epoch_t start_epoch) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    string cname = "pg";
    string mname = "filter";
    ::encode(cname, osd_op.indata);
    ::encode(mname, osd_op.indata);
    osd_op.indata.append(filter);
    ::encode(cookie, osd_op.indata);
  }
  void add_alloc_hint(int op, uint64_t expected_object_size,
                      uint64_t expected_write_size,
		      uint32_t flags) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.alloc_hint.expected_object_size = expected_object_size;
    osd_op.op.alloc_hint.expected_write_size = expected_write_size;
    osd_op.op.alloc_hint.flags = flags;
  }

  // ------

  // pg
  void pg_ls(uint64_t count, bufferlist& filter,
	     collection_list_handle_t cookie, epoch_t start_epoch) {
    if (filter.length() == 0)
      add_pgls(CEPH_OSD_OP_PGLS, count, cookie, start_epoch);
    else
      add_pgls_filter(CEPH_OSD_OP_PGLS_FILTER, count, filter, cookie,
		      start_epoch);
    flags |= CEPH_OSD_FLAG_PGOP;
  }

  void pg_nls(uint64_t count, const bufferlist& filter,
	      collection_list_handle_t cookie, epoch_t start_epoch) {
    if (filter.length() == 0)
      add_pgls(CEPH_OSD_OP_PGNLS, count, cookie, start_epoch);
    else
      add_pgls_filter(CEPH_OSD_OP_PGNLS_FILTER, count, filter, cookie,
		      start_epoch);
    flags |= CEPH_OSD_FLAG_PGOP;
  }

  void scrub_ls(const librados::object_id_t& start_after,
		uint64_t max_to_get,
		std::vector<librados::inconsistent_obj_t> *objects,
		uint32_t *interval,
		int *rval);
  void scrub_ls(const librados::object_id_t& start_after,
		uint64_t max_to_get,
		std::vector<librados::inconsistent_snapset_t> *objects,
		uint32_t *interval,
		int *rval);

  void create(bool excl) {
    OSDOp& o = add_op(CEPH_OSD_OP_CREATE);
    o.op.flags = (excl ? CEPH_OSD_OP_FLAG_EXCL : 0);
  }

  struct C_ObjectOperation_stat : public Context {
    bufferlist bl;
    uint64_t *psize;
    ceph::real_time *pmtime;
    time_t *ptime;
    struct timespec *pts;
    int *prval;
    C_ObjectOperation_stat(uint64_t *ps, ceph::real_time *pm, time_t *pt, struct timespec *_pts,
			   int *prval)
      : psize(ps), pmtime(pm), ptime(pt), pts(_pts), prval(prval) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  uint64_t size;
	  ceph::real_time mtime;
	  ::decode(size, p);
	  ::decode(mtime, p);
	  if (psize)
	    *psize = size;
	  if (pmtime)
	    *pmtime = mtime;
	  if (ptime)
	    *ptime = ceph::real_clock::to_time_t(mtime);
	  if (pts)
	    *pts = ceph::real_clock::to_timespec(mtime);
	} catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  void stat(uint64_t *psize, ceph::real_time *pmtime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_stat *h = new C_ObjectOperation_stat(psize, pmtime, NULL, NULL,
							   prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }
  void stat(uint64_t *psize, time_t *ptime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_stat *h = new C_ObjectOperation_stat(psize, NULL, ptime, NULL,
							   prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }
  void stat(uint64_t *psize, struct timespec *pts, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_stat *h = new C_ObjectOperation_stat(psize, NULL, NULL, pts,
							   prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }
  // object data
  void read(uint64_t off, uint64_t len, bufferlist *pbl, int *prval,
	    Context* ctx) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
    out_handler[p] = ctx;
  }

  struct C_ObjectOperation_sparse_read : public Context {
    bufferlist bl;
    bufferlist *data_bl;
    std::map<uint64_t, uint64_t> *extents;
    int *prval;
    C_ObjectOperation_sparse_read(bufferlist *data_bl,
				  std::map<uint64_t, uint64_t> *extents,
				  int *prval)
      : data_bl(data_bl), extents(extents), prval(prval) {}
    void finish(int r) {
      bufferlist::iterator iter = bl.begin();
      if (r >= 0) {
	try {
	  ::decode(*extents, iter);
	  ::decode(*data_bl, iter);
	} catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
		   bufferlist *data_bl, int *prval) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_sparse_read *h =
      new C_ObjectOperation_sparse_read(data_bl, m, prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }
  void write(uint64_t off, bufferlist& bl,
	     uint64_t truncate_size,
	     uint32_t truncate_seq) {
    add_data(CEPH_OSD_OP_WRITE, off, bl.length(), bl);
    OSDOp& o = *ops.rbegin();
    o.op.extent.truncate_size = truncate_size;
    o.op.extent.truncate_seq = truncate_seq;
  }
  void write(uint64_t off, bufferlist& bl) {
    write(off, bl, 0, 0);
  }
  void write_full(bufferlist& bl) {
    add_data(CEPH_OSD_OP_WRITEFULL, 0, bl.length(), bl);
  }
  void writesame(uint64_t off, uint64_t write_len, bufferlist& bl) {
    add_writesame(CEPH_OSD_OP_WRITESAME, off, write_len, bl);
  }
  void append(bufferlist& bl) {
    add_data(CEPH_OSD_OP_APPEND, 0, bl.length(), bl);
  }
  void zero(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_ZERO, off, len, bl);
  }
  void truncate(uint64_t off) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_TRUNCATE, off, 0, bl);
  }
  void remove() {
    bufferlist bl;
    add_data(CEPH_OSD_OP_DELETE, 0, 0, bl);
  }
  void mapext(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_MAPEXT, off, len, bl);
  }
  void sparse_read(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
  }

  void clone_range(const object_t& src_oid, uint64_t src_offset, uint64_t len,
		   uint64_t dst_offset) {
    add_clone_range(CEPH_OSD_OP_CLONERANGE, dst_offset, len, src_oid,
		    src_offset, CEPH_NOSNAP);
  }

  // object attrs
  void getxattr(const char *name, bufferlist *pbl, int *prval) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  struct C_ObjectOperation_decodevals : public Context {
    bufferlist bl;
    std::map<std::string,bufferlist> *pattrs;
    int *prval;
    C_ObjectOperation_decodevals(std::map<std::string,bufferlist> *pa, int *pr)
      : pattrs(pa), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  if (pattrs)
	    ::decode(*pattrs, p);
	}
	catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodekeys : public Context {
    bufferlist bl;
    std::set<std::string> *pattrs;
    int *prval;
    C_ObjectOperation_decodekeys(std::set<std::string> *pa, int *pr)
      : pattrs(pa), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  if (pattrs)
	    ::decode(*pattrs, p);
	}
	catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodewatchers : public Context {
    bufferlist bl;
    list<obj_watch_t> *pwatchers;
    int *prval;
    C_ObjectOperation_decodewatchers(list<obj_watch_t> *pw, int *pr)
      : pwatchers(pw), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  obj_list_watch_response_t resp;
	  ::decode(resp, p);
	  if (pwatchers) {
	    for (list<watch_item_t>::iterator i = resp.entries.begin() ;
		 i != resp.entries.end() ; ++i) {
	      obj_watch_t ow;
	      ostringstream sa;
	      sa << i->addr;
	      strncpy(ow.addr, sa.str().c_str(), 256);
	      ow.watcher_id = i->name.num();
	      ow.cookie = i->cookie;
	      ow.timeout_seconds = i->timeout_seconds;
	      pwatchers->push_back(ow);
	    }
	  }
	}
	catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodesnaps : public Context {
    bufferlist bl;
    librados::snap_set_t *psnaps;
    int *prval;
    C_ObjectOperation_decodesnaps(librados::snap_set_t *ps, int *pr)
      : psnaps(ps), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  obj_list_snap_response_t resp;
	  ::decode(resp, p);
	  if (psnaps) {
	    psnaps->clones.clear();
	    for (vector<clone_info>::iterator ci = resp.clones.begin();
		 ci != resp.clones.end();
		 ++ci) {
	      librados::clone_info_t clone;

	      clone.cloneid = ci->cloneid;
	      clone.snaps.reserve(ci->snaps.size());
	      clone.snaps.insert(clone.snaps.end(), ci->snaps.begin(),
				 ci->snaps.end());
	      clone.overlap = ci->overlap;
	      clone.size = ci->size;

	      psnaps->clones.push_back(clone);
	    }
	    psnaps->seq = resp.seq;
	  }
	} catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  void getxattrs(std::map<std::string,bufferlist> *pattrs, int *prval) {
    add_op(CEPH_OSD_OP_GETXATTRS);
    if (pattrs || prval) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h
	= new C_ObjectOperation_decodevals(pattrs, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }
  void setxattr(const char *name, const bufferlist& bl) {
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void setxattr(const char *name, const string& s) {
    bufferlist bl;
    bl.append(s);
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void cmpxattr(const char *name, uint8_t cmp_op, uint8_t cmp_mode,
		const bufferlist& bl) {
    add_xattr_cmp(CEPH_OSD_OP_CMPXATTR, name, cmp_op, cmp_mode, bl);
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
  void tmap_get(bufferlist *pbl, int *prval) {
    add_op(CEPH_OSD_OP_TMAPGET);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  void tmap_get() {
    add_op(CEPH_OSD_OP_TMAPGET);
  }
  void tmap_to_omap(bool nullok=false) {
     OSDOp& osd_op = add_op(CEPH_OSD_OP_TMAP2OMAP);
     if (nullok)
       osd_op.op.tmap2omap.flags = CEPH_OSD_TMAP2OMAP_NULLOK;
  }

  // objectmap
  void omap_get_keys(const string &start_after,
		     uint64_t max_to_get,
		     std::set<std::string> *out_set,
		     int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETKEYS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodekeys *h =
	new C_ObjectOperation_decodekeys(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_get_vals(const string &start_after,
		     const string &filter_prefix,
		     uint64_t max_to_get,
		     std::map<std::string, bufferlist> *out_set,
		     int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    ::encode(filter_prefix, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h =
	new C_ObjectOperation_decodevals(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_get_vals_by_keys(const std::set<std::string> &to_get,
			    std::map<std::string, bufferlist> *out_set,
			    int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
    bufferlist bl;
    ::encode(to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h =
	new C_ObjectOperation_decodevals(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_cmp(const std::map<std::string, pair<bufferlist,int> > &assertions,
		int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAP_CMP);
    bufferlist bl;
    ::encode(assertions, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval) {
      unsigned p = ops.size() - 1;
      out_rval[p] = prval;
    }
  }

  struct C_ObjectOperation_copyget : public Context {
    bufferlist bl;
    object_copy_cursor_t *cursor;
    uint64_t *out_size;
    ceph::real_time *out_mtime;
    std::map<std::string,bufferlist> *out_attrs;
    bufferlist *out_data, *out_omap_header, *out_omap_data;
    vector<snapid_t> *out_snaps;
    snapid_t *out_snap_seq;
    uint32_t *out_flags;
    uint32_t *out_data_digest;
    uint32_t *out_omap_digest;
    vector<pair<osd_reqid_t, version_t> > *out_reqids;
    uint64_t *out_truncate_seq;
    uint64_t *out_truncate_size;
    int *prval;
    C_ObjectOperation_copyget(object_copy_cursor_t *c,
			      uint64_t *s,
			      ceph::real_time *m,
			      std::map<std::string,bufferlist> *a,
			      bufferlist *d, bufferlist *oh,
			      bufferlist *o,
			      std::vector<snapid_t> *osnaps,
			      snapid_t *osnap_seq,
			      uint32_t *flags,
			      uint32_t *dd,
			      uint32_t *od,
			      vector<pair<osd_reqid_t, version_t> > *oreqids,
			      uint64_t *otseq,
			      uint64_t *otsize,
			      int *r)
      : cursor(c),
	out_size(s), out_mtime(m),
	out_attrs(a), out_data(d), out_omap_header(oh),
	out_omap_data(o), out_snaps(osnaps), out_snap_seq(osnap_seq),
	out_flags(flags), out_data_digest(dd), out_omap_digest(od),
	out_reqids(oreqids),
	out_truncate_seq(otseq),
	out_truncate_size(otsize),
	prval(r) {}
    void finish(int r) {
      // reqids are copied on ENOENT
      if (r < 0 && r != -ENOENT)
	return;
      try {
	bufferlist::iterator p = bl.begin();
	object_copy_data_t copy_reply;
	::decode(copy_reply, p);
	if (r == -ENOENT) {
	  if (out_reqids)
	    *out_reqids = copy_reply.reqids;
	  return;
	}
	if (out_size)
	  *out_size = copy_reply.size;
	if (out_mtime)
	  *out_mtime = ceph::real_clock::from_ceph_timespec(copy_reply.mtime);
	if (out_attrs)
	  *out_attrs = copy_reply.attrs;
	if (out_data)
	  out_data->claim_append(copy_reply.data);
	if (out_omap_header)
	  out_omap_header->claim_append(copy_reply.omap_header);
	if (out_omap_data)
	  *out_omap_data = copy_reply.omap_data;
	if (out_snaps)
	  *out_snaps = copy_reply.snaps;
	if (out_snap_seq)
	  *out_snap_seq = copy_reply.snap_seq;
	if (out_flags)
	  *out_flags = copy_reply.flags;
	if (out_data_digest)
	  *out_data_digest = copy_reply.data_digest;
	if (out_omap_digest)
	  *out_omap_digest = copy_reply.omap_digest;
	if (out_reqids)
	  *out_reqids = copy_reply.reqids;
	if (out_truncate_seq)
	  *out_truncate_seq = copy_reply.truncate_seq;
	if (out_truncate_size)
	  *out_truncate_size = copy_reply.truncate_size;
	*cursor = copy_reply.cursor;
      } catch (buffer::error& e) {
	if (prval)
	  *prval = -EIO;
      }
    }
  };

  void copy_get(object_copy_cursor_t *cursor,
		uint64_t max,
		uint64_t *out_size,
		ceph::real_time *out_mtime,
		std::map<std::string,bufferlist> *out_attrs,
		bufferlist *out_data,
		bufferlist *out_omap_header,
		bufferlist *out_omap_data,
		vector<snapid_t> *out_snaps,
		snapid_t *out_snap_seq,
		uint32_t *out_flags,
		uint32_t *out_data_digest,
		uint32_t *out_omap_digest,
		vector<pair<osd_reqid_t, version_t> > *out_reqids,
		uint64_t *truncate_seq,
		uint64_t *truncate_size,
		int *prval) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_COPY_GET);
    osd_op.op.copy_get.max = max;
    ::encode(*cursor, osd_op.indata);
    ::encode(max, osd_op.indata);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_copyget *h =
      new C_ObjectOperation_copyget(cursor, out_size, out_mtime,
				    out_attrs, out_data, out_omap_header,
				    out_omap_data, out_snaps, out_snap_seq,
				    out_flags, out_data_digest,
				    out_omap_digest, out_reqids, truncate_seq,
				    truncate_size, prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
  }

  void undirty() {
    add_op(CEPH_OSD_OP_UNDIRTY);
  }

  struct C_ObjectOperation_isdirty : public Context {
    bufferlist bl;
    bool *pisdirty;
    int *prval;
    C_ObjectOperation_isdirty(bool *p, int *r)
      : pisdirty(p), prval(r) {}
    void finish(int r) {
      if (r < 0)
	return;
      try {
	bufferlist::iterator p = bl.begin();
	bool isdirty;
	::decode(isdirty, p);
	if (pisdirty)
	  *pisdirty = isdirty;
      } catch (buffer::error& e) {
	if (prval)
	  *prval = -EIO;
      }
    }
  };

  void is_dirty(bool *pisdirty, int *prval) {
    add_op(CEPH_OSD_OP_ISDIRTY);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_isdirty *h =
      new C_ObjectOperation_isdirty(pisdirty, prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
  }

  struct C_ObjectOperation_hit_set_ls : public Context {
    bufferlist bl;
    std::list< std::pair<time_t, time_t> > *ptls;
    std::list< std::pair<ceph::real_time, ceph::real_time> > *putls;
    int *prval;
    C_ObjectOperation_hit_set_ls(std::list< std::pair<time_t, time_t> > *t,
				 std::list< std::pair<ceph::real_time,
						      ceph::real_time> > *ut,
				 int *r)
      : ptls(t), putls(ut), prval(r) {}
    void finish(int r) {
      if (r < 0)
	return;
      try {
	bufferlist::iterator p = bl.begin();
	std::list< std::pair<ceph::real_time, ceph::real_time> > ls;
	::decode(ls, p);
	if (ptls) {
	  ptls->clear();
	  for (auto p = ls.begin(); p != ls.end(); ++p)
	    // round initial timestamp up to the next full second to
	    // keep this a valid interval.
	    ptls->push_back(
	      make_pair(ceph::real_clock::to_time_t(
			  ceph::ceil(p->first,
				     // Sadly, no time literals until C++14.
				     std::chrono::seconds(1))),
			ceph::real_clock::to_time_t(p->second)));
	}
	if (putls)
	  putls->swap(ls);
      } catch (buffer::error& e) {
	r = -EIO;
      }
      if (prval)
	*prval = r;
    }
  };

  /**
   * list available HitSets.
   *
   * We will get back a list of time intervals.  Note that the most
   * recent range may have an empty end timestamp if it is still
   * accumulating.
   *
   * @param pls [out] list of time intervals
   * @param prval [out] return value
   */
  void hit_set_ls(std::list< std::pair<time_t, time_t> > *pls, int *prval) {
    add_op(CEPH_OSD_OP_PG_HITSET_LS);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_hit_set_ls *h =
      new C_ObjectOperation_hit_set_ls(pls, NULL, prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
  }
  void hit_set_ls(std::list<std::pair<ceph::real_time, ceph::real_time> > *pls,
		  int *prval) {
    add_op(CEPH_OSD_OP_PG_HITSET_LS);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_hit_set_ls *h =
      new C_ObjectOperation_hit_set_ls(NULL, pls, prval);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
  }

  /**
   * get HitSet
   *
   * Return an encoded HitSet that includes the provided time
   * interval.
   *
   * @param stamp [in] timestamp
   * @param pbl [out] target buffer for encoded HitSet
   * @param prval [out] return value
   */
  void hit_set_get(ceph::real_time stamp, bufferlist *pbl, int *prval) {
    OSDOp& op = add_op(CEPH_OSD_OP_PG_HITSET_GET);
    op.op.hit_set_get.stamp = ceph::real_clock::to_ceph_timespec(stamp);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    out_bl[p] = pbl;
  }

  void omap_get_header(bufferlist *bl, int *prval) {
    add_op(CEPH_OSD_OP_OMAPGETHEADER);
    unsigned p = ops.size() - 1;
    out_bl[p] = bl;
    out_rval[p] = prval;
  }

  void omap_set(const map<string, bufferlist> &map) {
    bufferlist bl;
    ::encode(map, bl);
    add_data(CEPH_OSD_OP_OMAPSETVALS, 0, bl.length(), bl);
  }

  void omap_set_header(bufferlist &bl) {
    add_data(CEPH_OSD_OP_OMAPSETHEADER, 0, bl.length(), bl);
  }

  void omap_clear() {
    add_op(CEPH_OSD_OP_OMAPCLEAR);
  }

  void omap_rm_keys(const std::set<std::string> &to_remove) {
    bufferlist bl;
    ::encode(to_remove, bl);
    add_data(CEPH_OSD_OP_OMAPRMKEYS, 0, bl.length(), bl);
  }

  // object classes
  void call(const char *cname, const char *method, bufferlist &indata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, NULL, NULL, NULL);
  }

  void call(const char *cname, const char *method, bufferlist &indata,
	    bufferlist *outdata, Context *ctx, int *prval) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, outdata, ctx, prval);
  }

  // watch/notify
  void watch(uint64_t cookie, __u8 op) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_WATCH);
    osd_op.op.watch.cookie = cookie;
    osd_op.op.watch.op = op;
  }

  void notify(uint64_t cookie, uint32_t prot_ver, uint32_t timeout,
              bufferlist &bl, bufferlist *inbl) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_NOTIFY);
    osd_op.op.notify.cookie = cookie;
    ::encode(prot_ver, *inbl);
    ::encode(timeout, *inbl);
    ::encode(bl, *inbl);
    osd_op.indata.append(*inbl);
  }

  void notify_ack(uint64_t notify_id, uint64_t cookie,
		  bufferlist& reply_bl) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_NOTIFY_ACK);
    bufferlist bl;
    ::encode(notify_id, bl);
    ::encode(cookie, bl);
    ::encode(reply_bl, bl);
    osd_op.indata.append(bl);
  }

  void list_watchers(list<obj_watch_t> *out,
		     int *prval) {
    (void)add_op(CEPH_OSD_OP_LIST_WATCHERS);
    if (prval || out) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodewatchers *h =
	new C_ObjectOperation_decodewatchers(out, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void list_snaps(librados::snap_set_t *out, int *prval) {
    (void)add_op(CEPH_OSD_OP_LIST_SNAPS);
    if (prval || out) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodesnaps *h =
	new C_ObjectOperation_decodesnaps(out, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void assert_version(uint64_t ver) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_ASSERT_VER);
    osd_op.op.assert_ver.ver = ver;
  }
  void assert_src_version(const object_t& srcoid, snapid_t srcsnapid,
			  uint64_t ver) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_ASSERT_SRC_VERSION);
    osd_op.op.assert_ver.ver = ver;
    ops.rbegin()->soid = sobject_t(srcoid, srcsnapid);
  }

  void cmpxattr(const char *name, const bufferlist& val,
		int op, int mode) {
    add_xattr(CEPH_OSD_OP_CMPXATTR, name, val);
    OSDOp& o = *ops.rbegin();
    o.op.xattr.cmp_op = op;
    o.op.xattr.cmp_mode = mode;
  }
  void src_cmpxattr(const object_t& srcoid, snapid_t srcsnapid,
		    const char *name, const bufferlist& val,
		    int op, int mode) {
    add_xattr(CEPH_OSD_OP_SRC_CMPXATTR, name, val);
    OSDOp& o = *ops.rbegin();
    o.soid = sobject_t(srcoid, srcsnapid);
    o.op.xattr.cmp_op = op;
    o.op.xattr.cmp_mode = mode;
  }

  void rollback(uint64_t snapid) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_ROLLBACK);
    osd_op.op.snap.snapid = snapid;
  }

  void copy_from(object_t src, snapid_t snapid, object_locator_t src_oloc,
		 version_t src_version, unsigned flags,
		 unsigned src_fadvise_flags) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_COPY_FROM);
    osd_op.op.copy_from.snapid = snapid;
    osd_op.op.copy_from.src_version = src_version;
    osd_op.op.copy_from.flags = flags;
    osd_op.op.copy_from.src_fadvise_flags = src_fadvise_flags;
    ::encode(src, osd_op.indata);
    ::encode(src_oloc, osd_op.indata);
  }

  /**
   * writeback content to backing tier
   *
   * If object is marked dirty in the cache tier, write back content
   * to backing tier. If the object is clean this is a no-op.
   *
   * If writeback races with an update, the update will block.
   *
   * use with IGNORE_CACHE to avoid triggering promote.
   */
  void cache_flush() {
    add_op(CEPH_OSD_OP_CACHE_FLUSH);
  }

  /**
   * writeback content to backing tier
   *
   * If object is marked dirty in the cache tier, write back content
   * to backing tier. If the object is clean this is a no-op.
   *
   * If writeback races with an update, return EAGAIN.  Requires that
   * the SKIPRWLOCKS flag be set.
   *
   * use with IGNORE_CACHE to avoid triggering promote.
   */
  void cache_try_flush() {
    add_op(CEPH_OSD_OP_CACHE_TRY_FLUSH);
  }

  /**
   * evict object from cache tier
   *
   * If object is marked clean, remove the object from the cache tier.
   * Otherwise, return EBUSY.
   *
   * use with IGNORE_CACHE to avoid triggering promote.
   */
  void cache_evict() {
    add_op(CEPH_OSD_OP_CACHE_EVICT);
  }

  void set_alloc_hint(uint64_t expected_object_size,
                      uint64_t expected_write_size,
		      uint32_t flags) {
    add_alloc_hint(CEPH_OSD_OP_SETALLOCHINT, expected_object_size,
		   expected_write_size, flags);

    // CEPH_OSD_OP_SETALLOCHINT op is advisory and therefore deemed
    // not worth a feature bit.  Set FAILOK per-op flag to make
    // sure older osds don't trip over an unsupported opcode.
    set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
  }

  void dup(vector<OSDOp>& sops) {
    ops = sops;
    out_bl.resize(sops.size());
    out_handler.resize(sops.size());
    out_rval.resize(sops.size());
    for (uint32_t i = 0; i < sops.size(); i++) {
      out_bl[i] = &sops[i].outdata;
      out_handler[i] = NULL;
      out_rval[i] = &sops[i].rval;
    }
  }

  /**
   * Pin/unpin an object in cache tier
   */
  void cache_pin() {
    add_op(CEPH_OSD_OP_CACHE_PIN);
  }

  void cache_unpin() {
    add_op(CEPH_OSD_OP_CACHE_UNPIN);
  }
};


// ----------------


class Objecter : public md_config_obs_t, public Dispatcher {
public:
  // config observer bits
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

public:
  Messenger *messenger;
  MonClient *monc;
  Finisher *finisher;
private:
  OSDMap    *osdmap;
public:
  using Dispatcher::cct;
  std::multimap<string,string> crush_location;

  atomic_t initialized;

private:
  atomic64_t last_tid;
  atomic_t inflight_ops;
  atomic_t client_inc;
  uint64_t max_linger_id;
  atomic_t num_unacked;
  atomic_t num_uncommitted;
  atomic_t global_op_flags; // flags which are applied to each IO op
  bool keep_balanced_budget;
  bool honor_osdmap_full;

public:
  void maybe_request_map();
private:

  void _maybe_request_map();

  version_t last_seen_osdmap_version;
  version_t last_seen_pgmap_version;

  mutable boost::shared_mutex rwlock;
  using lock_guard = std::unique_lock<decltype(rwlock)>;
  using unique_lock = std::unique_lock<decltype(rwlock)>;
  using shared_lock = boost::shared_lock<decltype(rwlock)>;
  using shunique_lock = ceph::shunique_lock<decltype(rwlock)>;
  ceph::timer<ceph::mono_clock> timer;

  PerfCounters *logger;

  uint64_t tick_event;

  void start_tick();
  void tick();
  void update_crush_location();

  class RequestStateHook : public AdminSocketHook {
    Objecter *m_objecter;
  public:
    explicit RequestStateHook(Objecter *objecter);
    bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	      bufferlist& out);
  };

  RequestStateHook *m_request_state_hook;

public:
  /*** track pending operations ***/
  // read
 public:

  struct OSDSession;

  struct op_target_t {
    int flags;
    object_t base_oid;
    object_locator_t base_oloc;
    object_t target_oid;
    object_locator_t target_oloc;

    bool precalc_pgid; ///< true if we are directed at base_pgid, not base_oid
    pg_t base_pgid; ///< explciti pg target, if any

    pg_t pgid; ///< last pg we mapped to
    unsigned pg_num; ///< last pg_num we mapped to
    unsigned pg_num_mask; ///< last pg_num_mask we mapped to
    vector<int> up; ///< set of up osds for last pg we mapped to
    vector<int> acting; ///< set of acting osds for last pg we mapped to
    int up_primary; ///< primary for last pg we mapped to based on the up set
    int acting_primary;  ///< primary for last pg we mapped to based on the
			 ///  acting set
    int size; ///< the size of the pool when were were last mapped
    int min_size; ///< the min size of the pool when were were last mapped
    bool sort_bitwise; ///< whether the hobject_t sort order is bitwise

    bool used_replica;
    bool paused;

    int osd;      ///< the final target osd, or -1

    op_target_t(object_t oid, object_locator_t oloc, int flags)
      : flags(flags),
	base_oid(oid),
	base_oloc(oloc),
	precalc_pgid(false),
	pg_num(0),
	pg_num_mask(0),
	up_primary(-1),
	acting_primary(-1),
	size(-1),
	min_size(-1),
	sort_bitwise(false),
	used_replica(false),
	paused(false),
	osd(-1)
    {}

    void dump(Formatter *f) const;
  };

  struct Op : public RefCountedObject {
    OSDSession *session;
    int incarnation;

    op_target_t target;

    ConnectionRef con;  // for rx buffer only
    uint64_t features;  // explicitly specified op features

    vector<OSDOp> ops;

    snapid_t snapid;
    SnapContext snapc;
    ceph::real_time mtime;

    bufferlist *outbl;
    vector<bufferlist*> out_bl;
    vector<Context*> out_handler;
    vector<int*> out_rval;

    int priority;
    Context *onack, *oncommit;
    uint64_t ontimeout;
    Context *oncommit_sync; // used internally by watch/notify

    ceph_tid_t tid;
    eversion_t replay_version; // for op replay
    int attempts;

    version_t *objver;
    epoch_t *reply_epoch;

    ceph::mono_time stamp;

    epoch_t map_dne_bound;

    bool budgeted;

    /// true if we should resend this message on failure
    bool should_resend;

    /// true if the throttle budget is get/put on a series of OPs,
    /// instead of per OP basis, when this flag is set, the budget is
    /// acquired before sending the very first OP of the series and
    /// released upon receiving the last OP reply.
    bool ctx_budgeted;

    int *data_offset;

    epoch_t last_force_resend;

    osd_reqid_t reqid; // explicitly setting reqid

    Op(const object_t& o, const object_locator_t& ol, vector<OSDOp>& op,
       int f, Context *ac, Context *co, version_t *ov, int *offset = NULL) :
      session(NULL), incarnation(0),
      target(o, ol, f),
      con(NULL),
      features(CEPH_FEATURES_SUPPORTED_DEFAULT),
      snapid(CEPH_NOSNAP),
      outbl(NULL),
      priority(0),
      onack(ac),
      oncommit(co),
      ontimeout(0),
      oncommit_sync(NULL),
      tid(0),
      attempts(0),
      objver(ov),
      reply_epoch(NULL),
      map_dne_bound(0),
      budgeted(false),
      should_resend(true),
      ctx_budgeted(false),
      data_offset(offset),
      last_force_resend(0) {
      ops.swap(op);

      /* initialize out_* to match op vector */
      out_bl.resize(ops.size());
      out_rval.resize(ops.size());
      out_handler.resize(ops.size());
      for (unsigned i = 0; i < ops.size(); i++) {
	out_bl[i] = NULL;
	out_handler[i] = NULL;
	out_rval[i] = NULL;
      }

      if (target.base_oloc.key == o)
	target.base_oloc.key.clear();
    }

    bool operator<(const Op& other) const {
      return tid < other.tid;
    }

  private:
    ~Op() {
      while (!out_handler.empty()) {
	delete out_handler.back();
	out_handler.pop_back();
      }
    }
  };

  struct C_Op_Map_Latest : public Context {
    Objecter *objecter;
    ceph_tid_t tid;
    version_t latest;
    C_Op_Map_Latest(Objecter *o, ceph_tid_t t) : objecter(o), tid(t),
						 latest(0) {}
    void finish(int r);
  };

  struct C_Command_Map_Latest : public Context {
    Objecter *objecter;
    uint64_t tid;
    version_t latest;
    C_Command_Map_Latest(Objecter *o, ceph_tid_t t) :  objecter(o), tid(t),
						       latest(0) {}
    void finish(int r);
  };

  struct C_Stat : public Context {
    bufferlist bl;
    uint64_t *psize;
    ceph::real_time *pmtime;
    Context *fin;
    C_Stat(uint64_t *ps, ceph::real_time *pm, Context *c) :
      psize(ps), pmtime(pm), fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	uint64_t s;
	ceph::real_time m;
	::decode(s, p);
	::decode(m, p);
	if (psize)
	  *psize = s;
	if (pmtime)
	  *pmtime = m;
      }
      fin->complete(r);
    }
  };

  struct C_GetAttrs : public Context {
    bufferlist bl;
    map<string,bufferlist>& attrset;
    Context *fin;
    C_GetAttrs(map<string, bufferlist>& set, Context *c) : attrset(set),
							   fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	::decode(attrset, p);
      }
      fin->complete(r);
    }
  };


  // Pools and statistics
  struct NListContext {
    int current_pg;
    collection_list_handle_t cookie;
    epoch_t current_pg_epoch;
    int starting_pg_num;
    bool at_end_of_pool;
    bool at_end_of_pg;
    bool sort_bitwise;

    int64_t pool_id;
    int pool_snap_seq;
    int max_entries;
    string nspace;

    bufferlist bl;   // raw data read to here
    std::list<librados::ListObjectImpl> list;

    bufferlist filter;

    bufferlist extra_info;

    // The budget associated with this context, once it is set (>= 0),
    // the budget is not get/released on OP basis, instead the budget
    // is acquired before sending the first OP and released upon receiving
    // the last op reply.
    int ctx_budget;

    NListContext() : current_pg(0),
		     current_pg_epoch(0),
		     starting_pg_num(0),
		     at_end_of_pool(false),
		     at_end_of_pg(false),
		     sort_bitwise(false),
		     pool_id(0),
		     pool_snap_seq(0),
		     max_entries(0),
		     nspace(),
		     bl(),
		     list(),
		     filter(),
		     extra_info(),
		     ctx_budget(-1) {}

    bool at_end() const {
      return at_end_of_pool;
    }

    uint32_t get_pg_hash_position() const {
      return current_pg;
    }
  };

  struct C_NList : public Context {
    NListContext *list_context;
    Context *final_finish;
    Objecter *objecter;
    epoch_t epoch;
    C_NList(NListContext *lc, Context * finish, Objecter *ob) :
      list_context(lc), final_finish(finish), objecter(ob), epoch(0) {}
    void finish(int r) {
      if (r >= 0) {
	objecter->_nlist_reply(list_context, r, final_finish, epoch);
      } else {
	final_finish->complete(r);
      }
    }
  };

  // Old pgls context we still use for talking to older OSDs
  struct ListContext {
    int current_pg;
    collection_list_handle_t cookie;
    epoch_t current_pg_epoch;
    int starting_pg_num;
    bool at_end_of_pool;
    bool at_end_of_pg;
    bool sort_bitwise;

    int64_t pool_id;
    int pool_snap_seq;
    int max_entries;
    string nspace;

    bufferlist bl;   // raw data read to here
    std::list<pair<object_t, string> > list;

    bufferlist filter;

    bufferlist extra_info;

    // The budget associated with this context, once it is set (>= 0),
    // the budget is not get/released on OP basis, instead the budget
    // is acquired before sending the first OP and released upon receiving
    // the last op reply.
    int ctx_budget;

    ListContext() : current_pg(0), current_pg_epoch(0), starting_pg_num(0),
		    at_end_of_pool(false),
		    at_end_of_pg(false),
		    sort_bitwise(false),
		    pool_id(0),
		    pool_snap_seq(0),
		    max_entries(0),
		    nspace(),
		    bl(),
		    list(),
		    filter(),
		    extra_info(),
		    ctx_budget(-1) {}

    bool at_end() const {
      return at_end_of_pool;
    }

    uint32_t get_pg_hash_position() const {
      return current_pg;
    }
  };

  struct C_List : public Context {
    ListContext *list_context;
    Context *final_finish;
    Objecter *objecter;
    epoch_t epoch;
    C_List(ListContext *lc, Context * finish, Objecter *ob) :
      list_context(lc), final_finish(finish), objecter(ob), epoch(0) {}
    void finish(int r) {
      if (r >= 0) {
	objecter->_list_reply(list_context, r, final_finish, epoch);
      } else {
	final_finish->complete(r);
      }
    }
  };

  struct PoolStatOp {
    ceph_tid_t tid;
    list<string> pools;

    map<string,pool_stat_t> *pool_stats;
    Context *onfinish;
    uint64_t ontimeout;

    ceph::mono_time last_submit;
  };

  struct StatfsOp {
    ceph_tid_t tid;
    struct ceph_statfs *stats;
    Context *onfinish;
    uint64_t ontimeout;

    ceph::mono_time last_submit;
  };

  struct PoolOp {
    ceph_tid_t tid;
    int64_t pool;
    string name;
    Context *onfinish;
    uint64_t ontimeout;
    int pool_op;
    uint64_t auid;
    int16_t crush_rule;
    snapid_t snapid;
    bufferlist *blp;

    ceph::mono_time last_submit;
    PoolOp() : tid(0), pool(0), onfinish(NULL), ontimeout(0), pool_op(0),
	       auid(0), crush_rule(0), snapid(0), blp(NULL) {}
  };

  // -- osd commands --
  struct CommandOp : public RefCountedObject {
    OSDSession *session;
    ceph_tid_t tid;
    vector<string> cmd;
    bufferlist inbl;
    bufferlist *poutbl;
    string *prs;
    int target_osd;
    pg_t target_pg;
    int osd; /* calculated osd for sending request */
    epoch_t map_dne_bound;
    int map_check_error; // error to return if map check fails
    const char *map_check_error_str;
    Context *onfinish;
    uint64_t ontimeout;
    ceph::mono_time last_submit;

    CommandOp()
      : session(NULL),
	tid(0), poutbl(NULL), prs(NULL), target_osd(-1), osd(-1),
	map_dne_bound(0),
	map_check_error(0),
	map_check_error_str(NULL),
	onfinish(NULL), ontimeout(0) {}
  };

  int submit_command(CommandOp *c, ceph_tid_t *ptid);
  int _calc_command_target(CommandOp *c, shunique_lock &sul);
  void _assign_command_session(CommandOp *c, shunique_lock &sul);
  void _send_command(CommandOp *c);
  int command_op_cancel(OSDSession *s, ceph_tid_t tid, int r);
  void _finish_command(CommandOp *c, int r, string rs);
  void handle_command_reply(MCommandReply *m);


  // -- lingering ops --

  struct WatchContext {
    // this simply mirrors librados WatchCtx2
    virtual void handle_notify(uint64_t notify_id,
			       uint64_t cookie,
			       uint64_t notifier_id,
			       bufferlist& bl) = 0;
    virtual void handle_error(uint64_t cookie, int err) = 0;
    virtual ~WatchContext() {}
  };

  struct LingerOp : public RefCountedObject {
    uint64_t linger_id;

    op_target_t target;

    snapid_t snap;
    SnapContext snapc;
    ceph::real_time mtime;

    vector<OSDOp> ops;
    bufferlist inbl;
    bufferlist *poutbl;
    version_t *pobjver;

    bool is_watch;
    ceph::mono_time watch_valid_thru; ///< send time for last acked ping
    int last_error;  ///< error from last failed ping|reconnect, if any
    boost::shared_mutex watch_lock;
    using lock_guard = std::unique_lock<decltype(watch_lock)>;
    using unique_lock = std::unique_lock<decltype(watch_lock)>;
    using shared_lock = boost::shared_lock<decltype(watch_lock)>;
    using shunique_lock = ceph::shunique_lock<decltype(watch_lock)>;

    // queue of pending async operations, with the timestamp of
    // when they were queued.
    list<ceph::mono_time> watch_pending_async;

    uint32_t register_gen;
    bool registered;
    bool canceled;
    Context *on_reg_commit;

    // we trigger these from an async finisher
    Context *on_notify_finish;
    bufferlist *notify_result_bl;
    uint64_t notify_id;

    WatchContext *watch_context;

    OSDSession *session;

    ceph_tid_t register_tid;
    ceph_tid_t ping_tid;
    epoch_t map_dne_bound;

    epoch_t last_force_resend;

    void _queued_async() {
      // watch_lock ust be locked unique
      watch_pending_async.push_back(ceph::mono_clock::now());
    }
    void finished_async() {
      unique_lock l(watch_lock);
      assert(!watch_pending_async.empty());
      watch_pending_async.pop_front();
    }

    LingerOp() : linger_id(0),
		 target(object_t(), object_locator_t(), 0),
		 snap(CEPH_NOSNAP), poutbl(NULL), pobjver(NULL),
		 is_watch(false), last_error(0),
		 register_gen(0),
		 registered(false),
		 canceled(false),
		 on_reg_commit(NULL),
		 on_notify_finish(NULL),
		 notify_result_bl(NULL),
		 notify_id(0),
		 watch_context(NULL),
		 session(NULL),
		 register_tid(0),
		 ping_tid(0),
		 map_dne_bound(0),
		 last_force_resend(0) {}

    // no copy!
    const LingerOp &operator=(const LingerOp& r);
    LingerOp(const LingerOp& o);

    uint64_t get_cookie() {
      return reinterpret_cast<uint64_t>(this);
    }

  private:
    ~LingerOp() {
      delete watch_context;
    }
  };

  struct C_Linger_Commit : public Context {
    Objecter *objecter;
    LingerOp *info;
    bufferlist outbl;  // used for notify only
    C_Linger_Commit(Objecter *o, LingerOp *l) : objecter(o), info(l) {
      info->get();
    }
    ~C_Linger_Commit() {
      info->put();
    }
    void finish(int r) {
      objecter->_linger_commit(info, r, outbl);
    }
  };

  struct C_Linger_Reconnect : public Context {
    Objecter *objecter;
    LingerOp *info;
    C_Linger_Reconnect(Objecter *o, LingerOp *l) : objecter(o), info(l) {
      info->get();
    }
    ~C_Linger_Reconnect() {
      info->put();
    }
    void finish(int r) {
      objecter->_linger_reconnect(info, r);
    }
  };

  struct C_Linger_Ping : public Context {
    Objecter *objecter;
    LingerOp *info;
    ceph::mono_time sent;
    uint32_t register_gen;
    C_Linger_Ping(Objecter *o, LingerOp *l)
      : objecter(o), info(l), register_gen(info->register_gen) {
      info->get();
    }
    ~C_Linger_Ping() {
      info->put();
    }
    void finish(int r) {
      objecter->_linger_ping(info, r, sent, register_gen);
    }
  };

  struct C_Linger_Map_Latest : public Context {
    Objecter *objecter;
    uint64_t linger_id;
    version_t latest;
    C_Linger_Map_Latest(Objecter *o, uint64_t id) :
      objecter(o), linger_id(id), latest(0) {}
    void finish(int r);
  };

  // -- osd sessions --
  struct OSDSession : public RefCountedObject {
    boost::shared_mutex lock;
    using lock_guard = std::lock_guard<decltype(lock)>;
    using unique_lock = std::unique_lock<decltype(lock)>;
    using shared_lock = boost::shared_lock<decltype(lock)>;
    using shunique_lcok = ceph::shunique_lock<decltype(lock)>;

    // pending ops
    map<ceph_tid_t,Op*> ops;
    map<uint64_t, LingerOp*> linger_ops;
    map<ceph_tid_t,CommandOp*> command_ops;

    int osd;
    int incarnation;
    ConnectionRef con;
    int num_locks;
    std::unique_ptr<std::mutex[]> completion_locks;
    using unique_completion_lock = std::unique_lock<
      decltype(completion_locks)::element_type>;


    OSDSession(CephContext *cct, int o) :
      osd(o), incarnation(0), con(NULL),
      num_locks(cct->_conf->objecter_completion_locks_per_session),
      completion_locks(new std::mutex[num_locks]) {}

    ~OSDSession();

    bool is_homeless() { return (osd == -1); }

    unique_completion_lock get_lock(object_t& oid);
  };
  map<int,OSDSession*> osd_sessions;

  bool osdmap_full_flag() const;
  bool osdmap_pool_full(const int64_t pool_id) const;

 private:

  /**
   * Test pg_pool_t::FLAG_FULL on a pool
   *
   * @return true if the pool exists and has the flag set, or
   *         the global full flag is set, else false
   */
  bool _osdmap_pool_full(const int64_t pool_id) const;
  bool _osdmap_pool_full(const pg_pool_t &p) const;
  void update_pool_full_map(map<int64_t, bool>& pool_full_map);

  map<uint64_t, LingerOp*> linger_ops;
  // we use this just to confirm a cookie is valid before dereferencing the ptr
  set<LingerOp*> linger_ops_set;

  map<ceph_tid_t,PoolStatOp*> poolstat_ops;
  map<ceph_tid_t,StatfsOp*> statfs_ops;
  map<ceph_tid_t,PoolOp*> pool_ops;
  atomic_t num_homeless_ops;

  OSDSession *homeless_session;

  // ops waiting for an osdmap with a new pool or confirmation that
  // the pool does not exist (may be expanded to other uses later)
  map<uint64_t, LingerOp*> check_latest_map_lingers;
  map<ceph_tid_t, Op*> check_latest_map_ops;
  map<ceph_tid_t, CommandOp*> check_latest_map_commands;

  map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

  ceph::timespan mon_timeout;
  ceph::timespan osd_timeout;

  MOSDOp *_prepare_osd_op(Op *op);
  void _send_op(Op *op, MOSDOp *m = NULL);
  void _send_op_account(Op *op);
  void _cancel_linger_op(Op *op);
  void finish_op(OSDSession *session, ceph_tid_t tid);
  void _finish_op(Op *op, int r);
  static bool is_pg_changed(
    int oldprimary,
    const vector<int>& oldacting,
    int newprimary,
    const vector<int>& newacting,
    bool any_change=false);
  enum recalc_op_target_result {
    RECALC_OP_TARGET_NO_ACTION = 0,
    RECALC_OP_TARGET_NEED_RESEND,
    RECALC_OP_TARGET_POOL_DNE,
    RECALC_OP_TARGET_OSD_DNE,
    RECALC_OP_TARGET_OSD_DOWN,
  };
  bool _osdmap_full_flag() const;
  bool _osdmap_has_pool_full() const;

  bool target_should_be_paused(op_target_t *op);
  int _calc_target(op_target_t *t, epoch_t *last_force_resend = 0,
		   bool any_change = false);
  int _map_session(op_target_t *op, OSDSession **s,
		   shunique_lock& lc);

  void _session_op_assign(OSDSession *s, Op *op);
  void _session_op_remove(OSDSession *s, Op *op);
  void _session_linger_op_assign(OSDSession *to, LingerOp *op);
  void _session_linger_op_remove(OSDSession *from, LingerOp *op);
  void _session_command_op_assign(OSDSession *to, CommandOp *op);
  void _session_command_op_remove(OSDSession *from, CommandOp *op);

  int _assign_op_target_session(Op *op, shunique_lock& lc,
				bool src_session_locked,
				bool dst_session_locked);
  int _recalc_linger_op_target(LingerOp *op, shunique_lock& lc);

  void _linger_submit(LingerOp *info, shunique_lock& sul);
  void _send_linger(LingerOp *info, shunique_lock& sul);
  void _linger_commit(LingerOp *info, int r, bufferlist& outbl);
  void _linger_reconnect(LingerOp *info, int r);
  void _send_linger_ping(LingerOp *info);
  void _linger_ping(LingerOp *info, int r, ceph::mono_time sent,
		    uint32_t register_gen);
  int _normalize_watch_error(int r);

  friend class C_DoWatchError;
public:
  void linger_callback_flush(Context *ctx) {
    finisher->queue(ctx);
  }

private:
  void _check_op_pool_dne(Op *op, unique_lock& sl);
  void _send_op_map_check(Op *op);
  void _op_cancel_map_check(Op *op);
  void _check_linger_pool_dne(LingerOp *op, bool *need_unregister);
  void _send_linger_map_check(LingerOp *op);
  void _linger_cancel_map_check(LingerOp *op);
  void _check_command_map_dne(CommandOp *op);
  void _send_command_map_check(CommandOp *op);
  void _command_cancel_map_check(CommandOp *op);

  void kick_requests(OSDSession *session);
  void _kick_requests(OSDSession *session, map<uint64_t, LingerOp *>& lresend);
  void _linger_ops_resend(map<uint64_t, LingerOp *>& lresend, unique_lock& ul);

  int _get_session(int osd, OSDSession **session, shunique_lock& sul);
  void put_session(OSDSession *s);
  void get_session(OSDSession *s);
  void _reopen_session(OSDSession *session);
  void close_session(OSDSession *session);

  void _nlist_reply(NListContext *list_context, int r, Context *final_finish,
		   epoch_t reply_epoch);
  void _list_reply(ListContext *list_context, int r, Context *final_finish,
		   epoch_t reply_epoch);

  void resend_mon_ops();

  /**
   * handle a budget for in-flight ops
   * budget is taken whenever an op goes into the ops map
   * and returned whenever an op is removed from the map
   * If throttle_op needs to throttle it will unlock client_lock.
   */
  int calc_op_budget(Op *op);
  void _throttle_op(Op *op, shunique_lock& sul, int op_size = 0);
  int _take_op_budget(Op *op, shunique_lock& sul) {
    assert(sul && sul.mutex() == &rwlock);
    int op_budget = calc_op_budget(op);
    if (keep_balanced_budget) {
      _throttle_op(op, sul, op_budget);
    } else {
      op_throttle_bytes.take(op_budget);
      op_throttle_ops.take(1);
    }
    op->budgeted = true;
    return op_budget;
  }
  void put_op_budget_bytes(int op_budget) {
    assert(op_budget >= 0);
    op_throttle_bytes.put(op_budget);
    op_throttle_ops.put(1);
  }
  void put_op_budget(Op *op) {
    assert(op->budgeted);
    int op_budget = calc_op_budget(op);
    put_op_budget_bytes(op_budget);
  }
  void put_list_context_budget(ListContext *list_context);
  void put_nlist_context_budget(NListContext *list_context);
  Throttle op_throttle_bytes, op_throttle_ops;

 public:
  Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	   Finisher *fin,
	   double mon_timeout,
	   double osd_timeout) :
    Dispatcher(cct_), messenger(m), monc(mc), finisher(fin),
    osdmap(new OSDMap), initialized(0), last_tid(0), client_inc(-1),
    max_linger_id(0), num_unacked(0), num_uncommitted(0), global_op_flags(0),
    keep_balanced_budget(false), honor_osdmap_full(true),
    last_seen_osdmap_version(0), last_seen_pgmap_version(0),
    logger(NULL), tick_event(0), m_request_state_hook(NULL),
    num_homeless_ops(0),
    homeless_session(new OSDSession(cct, -1)),
    mon_timeout(ceph::make_timespan(mon_timeout)),
    osd_timeout(ceph::make_timespan(osd_timeout)),
    op_throttle_bytes(cct, "objecter_bytes",
		      cct->_conf->objecter_inflight_op_bytes),
    op_throttle_ops(cct, "objecter_ops", cct->_conf->objecter_inflight_ops),
    epoch_barrier(0),
    retry_writes_after_first_reply(cct->_conf->objecter_retry_writes_after_first_reply)
  { }
  ~Objecter();

  void init();
  void start(const OSDMap *o = nullptr);
  void shutdown();

  // These two templates replace osdmap_(get)|(put)_read. Simply wrap
  // whatever functionality you want to use the OSDMap in a lambda like:
  //
  // with_osdmap([](const OSDMap& o) { o.do_stuff(); });
  //
  // or
  //
  // auto t = with_osdmap([&](const OSDMap& o) { return o.lookup_stuff(x); });
  //
  // Do not call into something that will try to lock the OSDMap from
  // here or you will have great woe and misery.

  template<typename Callback, typename...Args>
  auto with_osdmap(Callback&& cb, Args&&...args) ->
    typename std::enable_if<
      std::is_void<
    decltype(cb(const_cast<const OSDMap&>(*osdmap),
		std::forward<Args>(args)...))>::value,
      void>::type {
    shared_lock l(rwlock);
    std::forward<Callback>(cb)(const_cast<const OSDMap&>(*osdmap),
			       std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_osdmap(Callback&& cb, Args&&... args) ->
    typename std::enable_if<
      !std::is_void<
	decltype(cb(const_cast<const OSDMap&>(*osdmap),
		    std::forward<Args>(args)...))>::value,
      decltype(cb(const_cast<const OSDMap&>(*osdmap),
		  std::forward<Args>(args)...))>::type {
    shared_lock l(rwlock);
    return std::forward<Callback>(cb)(const_cast<const OSDMap&>(*osdmap),
				      std::forward<Args>(args)...);
  }


  /**
   * Tell the objecter to throttle outgoing ops according to its
   * budget (in _conf). If you do this, ops can block, in
   * which case it will unlock client_lock and sleep until
   * incoming messages reduce the used budget low enough for
   * the ops to continue going; then it will lock client_lock again.
   */
  void set_balanced_budget() { keep_balanced_budget = true; }
  void unset_balanced_budget() { keep_balanced_budget = false; }

  void set_honor_osdmap_full() { honor_osdmap_full = true; }
  void unset_honor_osdmap_full() { honor_osdmap_full = false; }

  void _scan_requests(OSDSession *s,
		      bool force_resend,
		      bool cluster_full,
		      map<int64_t, bool> *pool_full_map,
		      map<ceph_tid_t, Op*>& need_resend,
		      list<LingerOp*>& need_resend_linger,
		      map<ceph_tid_t, CommandOp*>& need_resend_command,
		      shunique_lock& sul);

  int64_t get_object_hash_position(int64_t pool, const string& key,
				   const string& ns);
  int64_t get_object_pg_hash_position(int64_t pool, const string& key,
				      const string& ns);

  // messages
 public:
  bool ms_dispatch(Message *m);
  bool ms_can_fast_dispatch_any() const {
    return true;
  }
  bool ms_can_fast_dispatch(Message *m) const {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OPREPLY:
    case CEPH_MSG_WATCH_NOTIFY:
      return true;
    default:
      return false;
    }
  }
  void ms_fast_dispatch(Message *m) {
    ms_dispatch(m);
  }

  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_watch_notify(class MWatchNotify *m);
  void handle_osd_map(class MOSDMap *m);
  void wait_for_osd_map();

  int pool_snap_by_name(int64_t poolid, const char *snap_name, snapid_t *snap);
  int pool_snap_get_info(int64_t poolid, snapid_t snap,
			 pool_snap_info_t *info);
  int pool_snap_list(int64_t poolid, vector<uint64_t> *snaps);
private:

  // low-level
  void _op_submit(Op *op, shunique_lock& lc, ceph_tid_t *ptid);
  void _op_submit_with_budget(Op *op, shunique_lock& lc,
			      ceph_tid_t *ptid,
			      int *ctx_budget = NULL);
  inline void unregister_op(Op *op);

  // public interface
public:
  void op_submit(Op *op, ceph_tid_t *ptid = NULL, int *ctx_budget = NULL);
  bool is_active() {
    shared_lock l(rwlock);
    return !((!inflight_ops.read()) && linger_ops.empty() &&
	     poolstat_ops.empty() && statfs_ops.empty());
  }

  /**
   * Output in-flight requests
   */
  void _dump_active(OSDSession *s);
  void _dump_active();
  void dump_active();
  void dump_requests(Formatter *fmt);
  void _dump_ops(const OSDSession *s, Formatter *fmt);
  void dump_ops(Formatter *fmt);
  void _dump_linger_ops(const OSDSession *s, Formatter *fmt);
  void dump_linger_ops(Formatter *fmt);
  void _dump_command_ops(const OSDSession *s, Formatter *fmt);
  void dump_command_ops(Formatter *fmt);
  void dump_pool_ops(Formatter *fmt) const;
  void dump_pool_stat_ops(Formatter *fmt) const;
  void dump_statfs_ops(Formatter *fmt) const;

  int get_client_incarnation() const { return client_inc.read(); }
  void set_client_incarnation(int inc) { client_inc.set(inc); }

  bool have_map(epoch_t epoch);
  /// wait for epoch; true if we already have it
  bool wait_for_map(epoch_t epoch, Context *c, int err=0);
  void _wait_for_new_map(Context *c, epoch_t epoch, int err=0);
  void wait_for_latest_osdmap(Context *fin);
  void get_latest_version(epoch_t oldest, epoch_t neweset, Context *fin);
  void _get_latest_version(epoch_t oldest, epoch_t neweset, Context *fin);

  /** Get the current set of global op flags */
  int get_global_op_flags() { return global_op_flags.read(); }
  /** Add a flag to the global op flags, not really atomic operation */
  void add_global_op_flags(int flag) {
    global_op_flags.set(global_op_flags.read() | flag);
  }
  /** Clear the passed flags from the global op flag set, not really
      atomic operation */
  void clear_global_op_flag(int flags) {
    global_op_flags.set(global_op_flags.read() & ~flags);
  }

  /// cancel an in-progress request with the given return code
private:
  int op_cancel(OSDSession *s, ceph_tid_t tid, int r);
  int _op_cancel(ceph_tid_t tid, int r);
public:
  int op_cancel(ceph_tid_t tid, int r);

  /**
   * Any write op which is in progress at the start of this call shall no
   * longer be in progress when this call ends.  Operations started after the
   * start of this call may still be in progress when this call ends.
   *
   * @return the latest possible epoch in which a cancelled op could have
   *         existed, or -1 if nothing was cancelled.
   */
  epoch_t op_cancel_writes(int r, int64_t pool=-1);

  // commands
  int osd_command(int osd, vector<string>& cmd,
		  const bufferlist& inbl, ceph_tid_t *ptid,
		  bufferlist *poutbl, string *prs, Context *onfinish) {
    assert(osd >= 0);
    CommandOp *c = new CommandOp;
    c->cmd = cmd;
    c->inbl = inbl;
    c->poutbl = poutbl;
    c->prs = prs;
    c->onfinish = onfinish;
    c->target_osd = osd;
    return submit_command(c, ptid);
  }
  int pg_command(pg_t pgid, vector<string>& cmd,
		 const bufferlist& inbl, ceph_tid_t *ptid,
		 bufferlist *poutbl, string *prs, Context *onfinish) {
    CommandOp *c = new CommandOp;
    c->cmd = cmd;
    c->inbl = inbl;
    c->poutbl = poutbl;
    c->prs = prs;
    c->onfinish = onfinish;
    c->target_pg = pgid;
    return submit_command(c, ptid);
  }

  // mid-level helpers
  Op *prepare_mutate_op(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op, const SnapContext& snapc,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    osd_reqid_t reqid = osd_reqid_t()) {
    Op *o = new Op(oid, oloc, op.ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->priority = op.priority;
    o->mtime = mtime;
    o->snapc = snapc;
    o->out_rval.swap(op.out_rval);
    o->reqid = reqid;
    return o;
  }
  ceph_tid_t mutate(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op, const SnapContext& snapc,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    osd_reqid_t reqid = osd_reqid_t()) {
    Op *o = prepare_mutate_op(oid, oloc, op, snapc, mtime, flags, onack,
			      oncommit, objver, reqid);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_read_op(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op,
    snapid_t snapid, bufferlist *pbl, int flags,
    Context *onack, version_t *objver = NULL,
    int *data_offset = NULL,
    uint64_t features = 0) {
    Op *o = new Op(oid, oloc, op.ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, onack, NULL, objver, data_offset);
    o->priority = op.priority;
    o->snapid = snapid;
    o->outbl = pbl;
    if (!o->outbl && op.size() == 1 && op.out_bl[0]->length())
	o->outbl = op.out_bl[0];
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    return o;
  }
  ceph_tid_t read(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op,
    snapid_t snapid, bufferlist *pbl, int flags,
    Context *onack, version_t *objver = NULL,
    int *data_offset = NULL,
    uint64_t features = 0) {
    Op *o = prepare_read_op(oid, oloc, op, snapid, pbl, flags, onack, objver,
			    data_offset);
    if (features)
      o->features = features;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_pg_read_op(
    uint32_t hash, object_locator_t oloc,
    ObjectOperation& op, bufferlist *pbl, int flags,
    Context *onack, epoch_t *reply_epoch,
    int *ctx_budget) {
    Op *o = new Op(object_t(), oloc,
		   op.ops, flags | global_op_flags.read() | CEPH_OSD_FLAG_READ,
		   onack, NULL, NULL);
    o->target.precalc_pgid = true;
    o->target.base_pgid = pg_t(hash, oloc.pool);
    o->priority = op.priority;
    o->snapid = CEPH_NOSNAP;
    o->outbl = pbl;
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->reply_epoch = reply_epoch;
    if (ctx_budget) {
      // budget is tracked by listing context
      o->ctx_budgeted = true;
    }
    return o;
  }
  ceph_tid_t pg_read(
    uint32_t hash, object_locator_t oloc,
    ObjectOperation& op, bufferlist *pbl, int flags,
    Context *onack, epoch_t *reply_epoch,
    int *ctx_budget) {
    Op *o = prepare_pg_read_op(hash, oloc, op, pbl, flags,
			       onack, reply_epoch, ctx_budget);
    ceph_tid_t tid;
    op_submit(o, &tid, ctx_budget);
    return tid;
  }

  // caller owns a ref
  LingerOp *linger_register(const object_t& oid, const object_locator_t& oloc,
			    int flags);
  ceph_tid_t linger_watch(LingerOp *info,
			  ObjectOperation& op,
			  const SnapContext& snapc, ceph::real_time mtime,
			  bufferlist& inbl,
			  Context *onfinish,
			  version_t *objver);
  ceph_tid_t linger_notify(LingerOp *info,
			   ObjectOperation& op,
			   snapid_t snap, bufferlist& inbl,
			   bufferlist *poutbl,
			   Context *onack,
			   version_t *objver);
  int linger_check(LingerOp *info);
  void linger_cancel(LingerOp *info);  // releases a reference
  void _linger_cancel(LingerOp *info);

  void _do_watch_notify(LingerOp *info, MWatchNotify *m);

  /**
   * set up initial ops in the op vector, and allocate a final op slot.
   *
   * The caller is responsible for filling in the final ops_count ops.
   *
   * @param ops op vector
   * @param ops_count number of final ops the caller will fill in
   * @param extra_ops pointer to [array of] initial op[s]
   * @return index of final op (for caller to fill in)
   */
  int init_ops(vector<OSDOp>& ops, int ops_count, ObjectOperation *extra_ops) {
    int i;
    int extra = 0;

    if (extra_ops)
      extra = extra_ops->ops.size();

    ops.resize(ops_count + extra);

    for (i=0; i<extra; i++) {
      ops[i] = extra_ops->ops[i];
    }

    return i;
  }


  // high-level helpers
  Op *prepare_stat_op(
    const object_t& oid, const object_locator_t& oloc,
    snapid_t snap, uint64_t *psize, ceph::real_time *pmtime,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_STAT;
    C_Stat *fin = new C_Stat(psize, pmtime, onfinish);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, fin, 0, objver);
    o->snapid = snap;
    o->outbl = &fin->bl;
    return o;
  }
  ceph_tid_t stat(
    const object_t& oid, const object_locator_t& oloc,
    snapid_t snap, uint64_t *psize, ceph::real_time *pmtime,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    Op *o = prepare_stat_op(oid, oloc, snap, psize, pmtime, flags,
			    onfinish, objver, extra_ops);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  Op *prepare_read_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return o;
  }
  ceph_tid_t read(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_read_op(oid, oloc, off, len, snap, pbl, flags,
			    onfinish, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t read_trunc(const object_t& oid, const object_locator_t& oloc,
			uint64_t off, uint64_t len, snapid_t snap,
			bufferlist *pbl, int flags, uint64_t trunc_size,
			__u32 trunc_seq, Context *onfinish,
			version_t *objver = NULL,
			ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t mapext(const object_t& oid, const object_locator_t& oloc,
		    uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl,
		    int flags, Context *onfinish, version_t *objver = NULL,
		    ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_MAPEXT;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t getxattr(const object_t& oid, const object_locator_t& oloc,
	     const char *name, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish,
	     version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t getxattrs(const object_t& oid, const object_locator_t& oloc,
		       snapid_t snap, map<string,bufferlist>& attrset,
		       int flags, Context *onfinish, version_t *objver = NULL,
		       ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTRS;
    C_GetAttrs *fin = new C_GetAttrs(attrset, onfinish);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_READ, fin, 0, objver);
    o->snapid = snap;
    o->outbl = &fin->bl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t read_full(const object_t& oid, const object_locator_t& oloc,
		       snapid_t snap, bufferlist *pbl, int flags,
		       Context *onfinish, version_t *objver = NULL,
		       ObjectOperation *extra_ops = NULL) {
    return read(oid, oloc, 0, 0, snap, pbl, flags | global_op_flags.read() |
		CEPH_OSD_FLAG_READ, onfinish, objver, extra_ops);
  }


  // writes
  ceph_tid_t _modify(const object_t& oid, const object_locator_t& oloc,
		     vector<OSDOp>& ops, ceph::real_time mtime,
		     const SnapContext& snapc, int flags,
		     Context *onack, Context *oncommit,
		     version_t *objver = NULL) {
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_write_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, const SnapContext& snapc,
    const bufferlist &bl, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t write(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, const SnapContext& snapc,
    const bufferlist &bl, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_write_op(oid, oloc, off, len, snapc, bl, mtime, flags,
			     onack, oncommit, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_append_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t len, const SnapContext& snapc,
    const bufferlist &bl, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit,
    version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_APPEND;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t append(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t len, const SnapContext& snapc,
    const bufferlist &bl, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit,
    version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    Op *o = prepare_append_op(oid, oloc, len, snapc, bl, mtime, flags,
			      onack, oncommit, objver, extra_ops);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t write_trunc(const object_t& oid, const object_locator_t& oloc,
			 uint64_t off, uint64_t len, const SnapContext& snapc,
			 const bufferlist &bl, ceph::real_time mtime, int flags,
			 uint64_t trunc_size, __u32 trunc_seq,
			 Context *onack, Context *oncommit,
			 version_t *objver = NULL,
			 ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_write_full_op(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, const bufferlist &bl,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = bl.length();
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t write_full(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, const bufferlist &bl,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_write_full_op(oid, oloc, snapc, bl, mtime, flags,
				  onack, oncommit, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_writesame_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t write_len, uint64_t off,
    const SnapContext& snapc, const bufferlist &bl,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {

    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITESAME;
    ops[i].op.writesame.offset = off;
    ops[i].op.writesame.length = write_len;
    ops[i].op.writesame.data_length = bl.length();
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t writesame(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t write_len, uint64_t off,
    const SnapContext& snapc, const bufferlist &bl,
    ceph::real_time mtime, int flags, Context *onack,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {

    Op *o = prepare_writesame_op(oid, oloc, write_len, off, snapc, bl,
				 mtime, flags, onack, oncommit, objver,
				 extra_ops, op_flags);

    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t trunc(const object_t& oid, const object_locator_t& oloc,
		   const SnapContext& snapc, ceph::real_time mtime, int flags,
		   uint64_t trunc_size, __u32 trunc_seq, Context *onack,
		   Context *oncommit, version_t *objver = NULL,
		   ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_TRUNCATE;
    ops[i].op.extent.offset = trunc_size;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t zero(const object_t& oid, const object_locator_t& oloc,
		  uint64_t off, uint64_t len, const SnapContext& snapc,
		  ceph::real_time mtime, int flags, Context *onack, Context *oncommit,
	     version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ZERO;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t rollback_object(const object_t& oid, const object_locator_t& oloc,
			     const SnapContext& snapc, snapid_t snapid,
			     ceph::real_time mtime, Context *onack, Context *oncommit,
			     version_t *objver = NULL,
			     ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ROLLBACK;
    ops[i].op.snap.snapid = snapid;
    Op *o = new Op(oid, oloc, ops, CEPH_OSD_FLAG_WRITE, onack, oncommit,
		   objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t create(const object_t& oid, const object_locator_t& oloc,
		    const SnapContext& snapc, ceph::real_time mtime, int global_flags,
		    int create_flags, Context *onack, Context *oncommit,
		    version_t *objver = NULL,
		    ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_CREATE;
    ops[i].op.flags = create_flags;
    Op *o = new Op(oid, oloc, ops, global_flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_remove_op(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit,
    version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_DELETE;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t remove(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, ceph::real_time mtime, int flags,
    Context *onack, Context *oncommit,
    version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    Op *o = prepare_remove_op(oid, oloc, snapc, mtime, flags,
			      onack, oncommit, objver, extra_ops);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t setxattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc, const bufferlist &bl,
	      ceph::real_time mtime, int flags,
	      Context *onack, Context *oncommit,
	      version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_SETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = bl.length();
    if (name)
      ops[i].indata.append(name);
    ops[i].indata.append(bl);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t removexattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc,
	      ceph::real_time mtime, int flags,
	      Context *onack, Context *oncommit,
	      version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_RMXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags.read() |
		   CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  void list_nobjects(NListContext *p, Context *onfinish);
  uint32_t list_nobjects_seek(NListContext *p, uint32_t pos);
  void list_objects(ListContext *p, Context *onfinish);
  uint32_t list_objects_seek(ListContext *p, uint32_t pos);

  hobject_t enumerate_objects_begin();
  hobject_t enumerate_objects_end();
  //hobject_t enumerate_objects_begin(int n, int m);
  void enumerate_objects(
    int64_t pool_id,
    const std::string &ns,
    const hobject_t &start,
    const hobject_t &end,
    const uint32_t max,
    const bufferlist &filter_bl,
    std::list<librados::ListObjectImpl> *result, 
    hobject_t *next,
    Context *on_finish);

  void _enumerate_reply(
      bufferlist &bl,
      int r,
      const hobject_t &end,
      const int64_t pool_id,
      int budget,
      epoch_t reply_epoch,
      std::list<librados::ListObjectImpl> *result, 
      hobject_t *next,
      Context *on_finish);
  friend class C_EnumerateReply;

  // -------------------------
  // pool ops
private:
  void pool_op_submit(PoolOp *op);
  void _pool_op_submit(PoolOp *op);
  void _finish_pool_op(PoolOp *op, int r);
  void _do_delete_pool(int64_t pool, Context *onfinish);
public:
  int create_pool_snap(int64_t pool, string& snapName, Context *onfinish);
  int allocate_selfmanaged_snap(int64_t pool, snapid_t *psnapid,
				Context *onfinish);
  int delete_pool_snap(int64_t pool, string& snapName, Context *onfinish);
  int delete_selfmanaged_snap(int64_t pool, snapid_t snap, Context *onfinish);

  int create_pool(string& name, Context *onfinish, uint64_t auid=0,
		  int crush_rule=-1);
  int delete_pool(int64_t pool, Context *onfinish);
  int delete_pool(const string& name, Context *onfinish);
  int change_pool_auid(int64_t pool, Context *onfinish, uint64_t auid);

  void handle_pool_op_reply(MPoolOpReply *m);
  int pool_op_cancel(ceph_tid_t tid, int r);

  // --------------------------
  // pool stats
private:
  void _poolstat_submit(PoolStatOp *op);
public:
  void handle_get_pool_stats_reply(MGetPoolStatsReply *m);
  void get_pool_stats(list<string>& pools, map<string,pool_stat_t> *result,
		      Context *onfinish);
  int pool_stat_op_cancel(ceph_tid_t tid, int r);
  void _finish_pool_stat_op(PoolStatOp *op, int r);

  // ---------------------------
  // df stats
private:
  void _fs_stats_submit(StatfsOp *op);
public:
  void handle_fs_stats_reply(MStatfsReply *m);
  void get_fs_stats(struct ceph_statfs& result, Context *onfinish);
  int statfs_op_cancel(ceph_tid_t tid, int r);
  void _finish_statfs_op(StatfsOp *op, int r);

  // ---------------------------
  // some scatter/gather hackery

  void _sg_read_finish(vector<ObjectExtent>& extents,
		       vector<bufferlist>& resultbl,
		       bufferlist *bl, Context *onfinish);

  struct C_SGRead : public Context {
    Objecter *objecter;
    vector<ObjectExtent> extents;
    vector<bufferlist> resultbl;
    bufferlist *bl;
    Context *onfinish;
    C_SGRead(Objecter *ob,
	     vector<ObjectExtent>& e, vector<bufferlist>& r, bufferlist *b,
	     Context *c) :
      objecter(ob), bl(b), onfinish(c) {
      extents.swap(e);
      resultbl.swap(r);
    }
    void finish(int r) {
      objecter->_sg_read_finish(extents, resultbl, bl, onfinish);
    }
  };

  void sg_read_trunc(vector<ObjectExtent>& extents, snapid_t snap,
		     bufferlist *bl, int flags, uint64_t trunc_size,
		     __u32 trunc_seq, Context *onfinish, int op_flags = 0) {
    if (extents.size() == 1) {
      read_trunc(extents[0].oid, extents[0].oloc, extents[0].offset,
		 extents[0].length, snap, bl, flags, extents[0].truncate_size,
		 trunc_seq, onfinish, 0, 0, op_flags);
    } else {
      C_GatherBuilder gather(cct);
      vector<bufferlist> resultbl(extents.size());
      int i=0;
      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end();
	   ++p) {
	read_trunc(p->oid, p->oloc, p->offset, p->length, snap, &resultbl[i++],
		   flags, p->truncate_size, trunc_seq, gather.new_sub(),
		   0, 0, op_flags);
      }
      gather.set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
      gather.activate();
    }
  }

  void sg_read(vector<ObjectExtent>& extents, snapid_t snap, bufferlist *bl,
	       int flags, Context *onfinish, int op_flags = 0) {
    sg_read_trunc(extents, snap, bl, flags, 0, 0, onfinish, op_flags);
  }

  void sg_write_trunc(vector<ObjectExtent>& extents, const SnapContext& snapc,
		      const bufferlist& bl, ceph::real_time mtime, int flags,
		      uint64_t trunc_size, __u32 trunc_seq, Context *onack,
		      Context *oncommit, int op_flags = 0) {
    if (extents.size() == 1) {
      write_trunc(extents[0].oid, extents[0].oloc, extents[0].offset,
		  extents[0].length, snapc, bl, mtime, flags,
		  extents[0].truncate_size, trunc_seq, onack, oncommit,
		  0, 0, op_flags);
    } else {
      C_GatherBuilder gack(cct, onack);
      C_GatherBuilder gcom(cct, oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end();
	   ++p) {
	bufferlist cur;
	for (vector<pair<uint64_t,uint64_t> >::iterator bit
	       = p->buffer_extents.begin();
	     bit != p->buffer_extents.end();
	     ++bit)
	  bl.copy(bit->first, bit->second, cur);
	assert(cur.length() == p->length);
	write_trunc(p->oid, p->oloc, p->offset, p->length,
	      snapc, cur, mtime, flags, p->truncate_size, trunc_seq,
	      onack ? gack.new_sub():0,
	      oncommit ? gcom.new_sub():0,
	      0, 0, op_flags);
      }
      gack.activate();
      gcom.activate();
    }
  }

  void sg_write(vector<ObjectExtent>& extents, const SnapContext& snapc,
		const bufferlist& bl, ceph::real_time mtime, int flags,
		Context *onack, Context *oncommit, int op_flags = 0) {
    sg_write_trunc(extents, snapc, bl, mtime, flags, 0, 0, onack, oncommit,
		   op_flags);
  }

  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
  bool ms_get_authorizer(int dest_type,
			 AuthAuthorizer **authorizer,
			 bool force_new);

  void blacklist_self(bool set);

private:
  epoch_t epoch_barrier;
  bool retry_writes_after_first_reply;
public:
  void set_epoch_barrier(epoch_t epoch);
};

#endif
