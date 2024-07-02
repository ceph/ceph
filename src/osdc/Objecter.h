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
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include <boost/container/small_vector.hpp>
#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/append.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/defer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/post.hpp>

#include <fmt/format.h>

#include "include/buffer.h"
#include "include/ceph_assert.h"
#include "include/ceph_fs.h"
#include "include/common_fwd.h"
#include "include/expected.hpp"
#include "include/types.h"
#include "include/rados/rados_types.hpp"
#include "include/function2.hpp"
#include "include/neorados/RADOS_Decodable.hpp"

#include "common/async/completion.h"
#include "common/admin_socket.h"
#include "common/ceph_time.h"
#include "common/ceph_mutex.h"
#include "common/ceph_timer.h"
#include "common/config_obs.h"
#include "common/shunique_lock.h"
#include "common/zipkin_trace.h"
#include "common/tracer.h"
#include "common/Throttle.h"

#include "mon/MonClient.h"

#include "messages/MOSDOp.h"
#include "msg/Dispatcher.h"

#include "osd/OSDMap.h"
#include "osd/error_code.h"

class Context;
class Messenger;
class MonClient;
class Message;

class MPoolOpReply;

class MGetPoolStatsReply;
class MStatfsReply;
class MCommandReply;
class MWatchNotify;
struct ObjectOperation;
template<typename T>
struct EnumerationContext;
template<typename t>
struct CB_EnumerateReply;

inline constexpr std::size_t osdc_opvec_len = 2;
using osdc_opvec = boost::container::small_vector<OSDOp, osdc_opvec_len>;

// -----------------------------------------

struct ObjectOperation {
  osdc_opvec ops;
  int flags = 0;
  int priority = 0;

  boost::container::small_vector<ceph::buffer::list*, osdc_opvec_len> out_bl;
  boost::container::small_vector<
    fu2::unique_function<void(boost::system::error_code, int,
			      const ceph::buffer::list& bl) &&>,
    osdc_opvec_len> out_handler;
  boost::container::small_vector<int*, osdc_opvec_len> out_rval;
  boost::container::small_vector<boost::system::error_code*,
				 osdc_opvec_len> out_ec;

  ObjectOperation() = default;
  ObjectOperation(const ObjectOperation&) = delete;
  ObjectOperation& operator =(const ObjectOperation&) = delete;
  ObjectOperation(ObjectOperation&&) = default;
  ObjectOperation& operator =(ObjectOperation&&) = default;
  ~ObjectOperation() = default;

  size_t size() const {
    return ops.size();
  }

  void clear() {
    ops.clear();
    flags = 0;
    priority = 0;
    out_bl.clear();
    out_handler.clear();
    out_rval.clear();
    out_ec.clear();
  }

  void set_last_op_flags(int flags) {
    ceph_assert(!ops.empty());
    ops.rbegin()->op.flags = flags;
  }


  void set_handler(fu2::unique_function<void(boost::system::error_code, int,
					     const ceph::buffer::list&) &&> f) {
    if (f) {
      if (out_handler.back()) {
	// This happens seldom enough that we may as well keep folding
	// functions together when we get another one rather than
	// using a container.
	out_handler.back() =
	  [f = std::move(f),
	   g = std::move(std::move(out_handler.back()))]
	  (boost::system::error_code ec, int r,
	   const ceph::buffer::list& bl) mutable {
	    std::move(g)(ec, r, bl);
	    std::move(f)(ec, r, bl);
	  };
      } else {
	out_handler.back() = std::move(f);
      }
    }
    ceph_assert(ops.size() == out_handler.size());
  }

  void set_handler(Context *c) {
    if (c)
      set_handler([c = std::unique_ptr<Context>(c)](boost::system::error_code,
						    int r,
						    const ceph::buffer::list&) mutable {
		    c.release()->complete(r);
		  });

  }

  OSDOp& add_op(int op) {
    ops.emplace_back();
    ops.back().op.op = op;
    out_bl.push_back(nullptr);
    ceph_assert(ops.size() == out_bl.size());
    out_handler.emplace_back();
    ceph_assert(ops.size() == out_handler.size());
    out_rval.push_back(nullptr);
    ceph_assert(ops.size() == out_rval.size());
    out_ec.push_back(nullptr);
    ceph_assert(ops.size() == out_ec.size());
    return ops.back();
  }
  void add_data(int op, uint64_t off, uint64_t len, ceph::buffer::list& bl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    osd_op.indata.claim_append(bl);
  }
  void add_writesame(int op, uint64_t off, uint64_t write_len,
		     ceph::buffer::list& bl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.writesame.offset = off;
    osd_op.op.writesame.length = write_len;
    osd_op.op.writesame.data_length = bl.length();
    osd_op.indata.claim_append(bl);
  }
  void add_xattr(int op, const char *name, const ceph::buffer::list& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    if (name)
      osd_op.indata.append(name, osd_op.op.xattr.name_len);
    osd_op.indata.append(data);
  }
  void add_xattr_cmp(int op, const char *name, uint8_t cmp_op,
		     uint8_t cmp_mode, const ceph::buffer::list& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    osd_op.op.xattr.cmp_op = cmp_op;
    osd_op.op.xattr.cmp_mode = cmp_mode;
    if (name)
      osd_op.indata.append(name, osd_op.op.xattr.name_len);
    osd_op.indata.append(data);
  }
  void add_xattr(int op, std::string_view name, const ceph::buffer::list& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = name.size();
    osd_op.op.xattr.value_len = data.length();
    osd_op.indata.append(name.data(), osd_op.op.xattr.name_len);
    osd_op.indata.append(data);
  }
  void add_xattr_cmp(int op, std::string_view name, uint8_t cmp_op,
		     uint8_t cmp_mode, const ceph::buffer::list& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.xattr.name_len = name.size();
    osd_op.op.xattr.value_len = data.length();
    osd_op.op.xattr.cmp_op = cmp_op;
    osd_op.op.xattr.cmp_mode = cmp_mode;
    if (!name.empty())
      osd_op.indata.append(name.data(), osd_op.op.xattr.name_len);
    osd_op.indata.append(data);
  }
  void add_call(int op, std::string_view cname, std::string_view method,
		const ceph::buffer::list &indata,
		ceph::buffer::list *outbl, Context *ctx, int *prval) {
    OSDOp& osd_op = add_op(op);

    unsigned p = ops.size() - 1;
    set_handler(ctx);
    out_bl[p] = outbl;
    out_rval[p] = prval;

    osd_op.op.cls.class_len = cname.size();
    osd_op.op.cls.method_len = method.size();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
  }
  void add_call(int op, std::string_view cname, std::string_view method,
		const ceph::buffer::list &indata,
		fu2::unique_function<void(boost::system::error_code,
					  const ceph::buffer::list&) &&> f) {
    OSDOp& osd_op = add_op(op);

    set_handler([f = std::move(f)](boost::system::error_code ec,
				   int,
				   const ceph::buffer::list& bl) mutable {
		  std::move(f)(ec, bl);
		});

    osd_op.op.cls.class_len = cname.size();
    osd_op.op.cls.method_len = method.size();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
  }
  void add_call(int op, std::string_view cname, std::string_view method,
		const ceph::buffer::list &indata,
		fu2::unique_function<void(boost::system::error_code, int,
					  const ceph::buffer::list&) &&> f) {
    OSDOp& osd_op = add_op(op);

    set_handler([f = std::move(f)](boost::system::error_code ec,
				   int r,
				   const ceph::buffer::list& bl) mutable {
		  std::move(f)(ec, r, bl);
		});

    osd_op.op.cls.class_len = cname.size();
    osd_op.op.cls.method_len = method.size();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
  }
  void add_pgls(int op, uint64_t count, collection_list_handle_t cookie,
		epoch_t start_epoch) {
    using ceph::encode;
    OSDOp& osd_op = add_op(op);
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    encode(cookie, osd_op.indata);
  }
  void add_pgls_filter(int op, uint64_t count, const ceph::buffer::list& filter,
		       collection_list_handle_t cookie, epoch_t start_epoch) {
    using ceph::encode;
    OSDOp& osd_op = add_op(op);
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    std::string cname = "pg";
    std::string mname = "filter";
    encode(cname, osd_op.indata);
    encode(mname, osd_op.indata);
    osd_op.indata.append(filter);
    encode(cookie, osd_op.indata);
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
  void pg_ls(uint64_t count, ceph::buffer::list& filter,
	     collection_list_handle_t cookie, epoch_t start_epoch) {
    if (filter.length() == 0)
      add_pgls(CEPH_OSD_OP_PGLS, count, cookie, start_epoch);
    else
      add_pgls_filter(CEPH_OSD_OP_PGLS_FILTER, count, filter, cookie,
		      start_epoch);
    flags |= CEPH_OSD_FLAG_PGOP;
  }

  void pg_nls(uint64_t count, const ceph::buffer::list& filter,
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

  struct CB_ObjectOperation_stat {
    ceph::buffer::list bl;
    uint64_t *psize;
    ceph::real_time *pmtime;
    time_t *ptime;
    struct timespec *pts;
    int *prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_stat(uint64_t *ps, ceph::real_time *pm, time_t *pt, struct timespec *_pts,
			    int *prval, boost::system::error_code* pec)
      : psize(ps), pmtime(pm), ptime(pt), pts(_pts), prval(prval), pec(pec) {}
    void operator()(boost::system::error_code ec, int r, const ceph::buffer::list& bl) {
      using ceph::decode;
      if (r >= 0) {
	auto p = bl.cbegin();
	try {
	  uint64_t size;
	  ceph::real_time mtime;
	  decode(size, p);
	  decode(mtime, p);
	  if (psize)
	    *psize = size;
	  if (pmtime)
	    *pmtime = mtime;
	  if (ptime)
	    *ptime = ceph::real_clock::to_time_t(mtime);
	  if (pts)
	    *pts = ceph::real_clock::to_timespec(mtime);
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };
  void stat(uint64_t *psize, ceph::real_time *pmtime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, pmtime, nullptr, nullptr, prval,
					nullptr));
    out_rval.back() = prval;
  }
  void stat(uint64_t *psize, ceph::real_time *pmtime,
	    boost::system::error_code* ec) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, pmtime, nullptr, nullptr,
					nullptr, ec));
    out_ec.back() = ec;
  }
  void stat(uint64_t *psize, time_t *ptime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, nullptr, ptime, nullptr, prval,
					nullptr));
    out_rval.back() = prval;
  }
  void stat(uint64_t *psize, struct timespec *pts, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, nullptr, nullptr, pts, prval, nullptr));
    out_rval.back() = prval;
  }
  void stat(uint64_t *psize, ceph::real_time *pmtime, std::nullptr_t) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, pmtime, nullptr, nullptr, nullptr,
					nullptr));
  }
  void stat(uint64_t *psize, time_t *ptime, std::nullptr_t) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, nullptr, ptime, nullptr, nullptr,
					nullptr));
  }
  void stat(uint64_t *psize, struct timespec *pts, std::nullptr_t) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, nullptr, nullptr, pts, nullptr,
					nullptr));
  }
  void stat(uint64_t *psize, std::nullptr_t, std::nullptr_t) {
    add_op(CEPH_OSD_OP_STAT);
    set_handler(CB_ObjectOperation_stat(psize, nullptr, nullptr, nullptr,
					nullptr, nullptr));
  }

  // object cmpext
  struct CB_ObjectOperation_cmpext {
    int* prval = nullptr;
    boost::system::error_code* ec = nullptr;
    uint64_t* mismatch_offset = nullptr;
    explicit CB_ObjectOperation_cmpext(int *prval)
      : prval(prval) {}
    CB_ObjectOperation_cmpext(boost::system::error_code* ec,
			      uint64_t* mismatch_offset)
      : ec(ec), mismatch_offset(mismatch_offset) {}

    void operator()(boost::system::error_code ec, int r,
		    const ceph::buffer::list&) {
      if (prval)
        *prval = r;

      if (r <= -MAX_ERRNO) {
	if (this->ec) {
	  *this->ec = make_error_code(osd_errc::cmpext_mismatch);
	}
	if (mismatch_offset) {
	  *mismatch_offset = -MAX_ERRNO - r;
	}
	throw boost::system::system_error(osd_errc::cmpext_mismatch);
      } else if (r < 0) {
	if (this->ec) {
	  *this->ec = ec;
	}
	if (mismatch_offset) {
	  *mismatch_offset = -1;
	}
      } else {
	if (this->ec) {
	  this->ec->clear();
	}
	if (mismatch_offset) {
	  *mismatch_offset = -1;
	}
      }
    }
  };

  void cmpext(uint64_t off, ceph::buffer::list& cmp_bl, int *prval) {
    add_data(CEPH_OSD_OP_CMPEXT, off, cmp_bl.length(), cmp_bl);
    set_handler(CB_ObjectOperation_cmpext(prval));
    out_rval.back() = prval;
  }

  void cmpext(uint64_t off, ceph::buffer::list&& cmp_bl, boost::system::error_code* ec,
	      uint64_t* mismatch_offset) {
    add_data(CEPH_OSD_OP_CMPEXT, off, cmp_bl.length(), cmp_bl);
    set_handler(CB_ObjectOperation_cmpext(ec, mismatch_offset));
    out_ec.back() = ec;
  }

  // Used by C API
  void cmpext(uint64_t off, uint64_t cmp_len, const char *cmp_buf, int *prval) {
    ceph::buffer::list cmp_bl;
    cmp_bl.append(cmp_buf, cmp_len);
    add_data(CEPH_OSD_OP_CMPEXT, off, cmp_len, cmp_bl);
    set_handler(CB_ObjectOperation_cmpext(prval));
    out_rval.back() = prval;
  }

  void read(uint64_t off, uint64_t len, ceph::buffer::list *pbl, int *prval,
	    Context* ctx) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
    set_handler(ctx);
  }

  void read(uint64_t off, uint64_t len, boost::system::error_code* ec,
	    ceph::buffer::list* pbl) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
    out_ec.back() = ec;
    out_bl.back() = pbl;
  }

  template<typename Ex>
  struct CB_ObjectOperation_sparse_read {
    ceph::buffer::list* data_bl;
    Ex* extents;
    int* prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_sparse_read(ceph::buffer::list* data_bl,
				   Ex* extents,
				   int* prval,
				   boost::system::error_code* pec)
      : data_bl(data_bl), extents(extents), prval(prval), pec(pec) {}
    void operator()(boost::system::error_code ec, int r, const ceph::buffer::list& bl) {
      auto iter = bl.cbegin();
      if (r >= 0) {
        // NOTE: it's possible the sub-op has not been executed but the result
        // code remains zeroed. Avoid the costly exception handling on a
        // potential IO path.
        if (bl.length() > 0) {
	  try {
	    decode(*extents, iter);
	    decode(*data_bl, iter);
	  } catch (const ceph::buffer::error& e) {
	    if (prval)
              *prval = -EIO;
	    if (pec)
	      *pec = e.code();
	  }
        } else if (prval) {
          *prval = -EIO;
	  if (pec)
	    *pec = buffer::errc::end_of_buffer;
	}
      }
    }
  };
  void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t, uint64_t>* m,
		   ceph::buffer::list* data_bl, int* prval,
		   uint64_t truncate_size = 0, uint32_t truncate_seq = 0) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
    OSDOp& o = *ops.rbegin();
    o.op.extent.truncate_size = truncate_size;
    o.op.extent.truncate_seq = truncate_seq;
    set_handler(CB_ObjectOperation_sparse_read(data_bl, m, prval, nullptr));
    out_rval.back() = prval;
  }
  void sparse_read(uint64_t off, uint64_t len,
		   boost::system::error_code* ec,
		   std::vector<std::pair<uint64_t, uint64_t>>* m,
		   ceph::buffer::list* data_bl) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
    set_handler(CB_ObjectOperation_sparse_read(data_bl, m, nullptr, ec));
    out_ec.back() = ec;
  }
  void write(uint64_t off, ceph::buffer::list& bl,
	     uint64_t truncate_size,
	     uint32_t truncate_seq) {
    add_data(CEPH_OSD_OP_WRITE, off, bl.length(), bl);
    OSDOp& o = *ops.rbegin();
    o.op.extent.truncate_size = truncate_size;
    o.op.extent.truncate_seq = truncate_seq;
  }
  void write(uint64_t off, ceph::buffer::list& bl) {
    write(off, bl, 0, 0);
  }
  void write(uint64_t off, ceph::buffer::list&& bl) {
    write(off, bl, 0, 0);
  }
  void write_full(ceph::buffer::list& bl) {
    add_data(CEPH_OSD_OP_WRITEFULL, 0, bl.length(), bl);
  }
  void write_full(ceph::buffer::list&& bl) {
    add_data(CEPH_OSD_OP_WRITEFULL, 0, bl.length(), bl);
  }
  void writesame(uint64_t off, uint64_t write_len, ceph::buffer::list& bl) {
    add_writesame(CEPH_OSD_OP_WRITESAME, off, write_len, bl);
  }
  void writesame(uint64_t off, uint64_t write_len, ceph::buffer::list&& bl) {
    add_writesame(CEPH_OSD_OP_WRITESAME, off, write_len, bl);
  }
  void append(ceph::buffer::list& bl) {
    add_data(CEPH_OSD_OP_APPEND, 0, bl.length(), bl);
  }
  void append(ceph::buffer::list&& bl) {
    add_data(CEPH_OSD_OP_APPEND, 0, bl.length(), bl);
  }
  void zero(uint64_t off, uint64_t len) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_ZERO, off, len, bl);
  }
  void truncate(uint64_t off) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_TRUNCATE, off, 0, bl);
  }
  void remove() {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_DELETE, 0, 0, bl);
  }
  void mapext(uint64_t off, uint64_t len) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_MAPEXT, off, len, bl);
  }
  void sparse_read(uint64_t off, uint64_t len) {
    ceph::buffer::list bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
  }

  void checksum(uint8_t type, const ceph::buffer::list &init_value_bl,
		uint64_t off, uint64_t len, size_t chunk_size,
		ceph::buffer::list *pbl, int *prval, Context *ctx) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_CHECKSUM);
    osd_op.op.checksum.offset = off;
    osd_op.op.checksum.length = len;
    osd_op.op.checksum.type = type;
    osd_op.op.checksum.chunk_size = chunk_size;
    osd_op.indata.append(init_value_bl);

    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
    set_handler(ctx);
  }

  void checksum(uint8_t type, ceph::buffer::list&& init_value,
		uint64_t off, uint64_t len, size_t chunk_size,
		fu2::unique_function<void(boost::system::error_code, int,
					  const ceph::buffer::list&) &&> f,
		boost::system::error_code* ec) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_CHECKSUM);
    osd_op.op.checksum.offset = off;
    osd_op.op.checksum.length = len;
    osd_op.op.checksum.type = type;
    osd_op.op.checksum.chunk_size = chunk_size;
    osd_op.indata.append(std::move(init_value));

    unsigned p = ops.size() - 1;
    out_ec[p] = ec;
    set_handler(std::move(f));
  }

  // object attrs
  void getxattr(const char *name, ceph::buffer::list *pbl, int *prval) {
    ceph::buffer::list bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  void getxattr(std::string_view name, boost::system::error_code* ec,
		buffer::list *pbl) {
    ceph::buffer::list bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
    out_bl.back() = pbl;
    out_ec.back() = ec;
  }

  template<typename Vals>
  struct CB_ObjectOperation_decodevals {
    uint64_t max_entries;
    Vals* pattrs;
    bool* ptruncated;
    int* prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_decodevals(uint64_t m, Vals* pa,
				  bool *pt, int *pr,
				  boost::system::error_code* pec)
      : max_entries(m), pattrs(pa), ptruncated(pt), prval(pr), pec(pec) {
      if (ptruncated) {
	*ptruncated = false;
      }
    }
    void operator()(boost::system::error_code ec, int r, const ceph::buffer::list& bl) {
      if (r >= 0) {
	auto p = bl.cbegin();
	try {
	  if (pattrs)
	    decode(*pattrs, p);
	  if (ptruncated) {
	    Vals ignore;
	    if (!pattrs) {
	      decode(ignore, p);
	      pattrs = &ignore;
	    }
	    if (!p.end()) {
	      decode(*ptruncated, p);
	    } else {
	      // The OSD did not provide this.  Since old OSDs do not
	      // enfoce omap result limits either, we can infer it from
	      // the size of the result
	      *ptruncated = (pattrs->size() == max_entries);
	    }
	  }
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };
  template<typename Keys>
  struct CB_ObjectOperation_decodekeys {
    uint64_t max_entries;
    Keys* pattrs;
    bool *ptruncated;
    int *prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_decodekeys(uint64_t m, Keys* pa, bool *pt,
				  int *pr, boost::system::error_code* pec)
      : max_entries(m), pattrs(pa), ptruncated(pt), prval(pr), pec(pec) {
      if (ptruncated) {
	*ptruncated = false;
      }
    }
    void operator()(boost::system::error_code ec, int r, const ceph::buffer::list& bl) {
      if (r >= 0) {
	using ceph::decode;
	auto p = bl.cbegin();
	try {
	  if (pattrs)
	    decode(*pattrs, p);
	  if (ptruncated) {
	    Keys ignore;
	    if (!pattrs) {
	      decode(ignore, p);
	      pattrs = &ignore;
	    }
	    if (!p.end()) {
	      decode(*ptruncated, p);
	    } else {
	      // the OSD did not provide this.  since old OSDs do not
	      // enforce omap result limits either, we can infer it from
	      // the size of the result
	      *ptruncated = (pattrs->size() == max_entries);
	    }
	  }
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };
  struct CB_ObjectOperation_decodewatchers {
    std::list<obj_watch_t>* pwatchers;
    int* prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_decodewatchers(std::list<obj_watch_t>* pw, int* pr,
				      boost::system::error_code* pec)
      : pwatchers(pw), prval(pr), pec(pec) {}
    void operator()(boost::system::error_code ec, int r,
		    const ceph::buffer::list& bl) {
      if (r >= 0) {
	auto p = bl.cbegin();
	try {
	  obj_list_watch_response_t resp;
	  decode(resp, p);
	  if (pwatchers) {
	    for (const auto& watch_item : resp.entries) {
	      obj_watch_t ow;
	      std::string sa = watch_item.addr.get_legacy_str();
	      strncpy(ow.addr, sa.c_str(), sizeof(ow.addr) - 1);
	      ow.addr[sizeof(ow.addr) - 1] = '\0';
	      ow.watcher_id = watch_item.name.num();
	      ow.cookie = watch_item.cookie;
	      ow.timeout_seconds = watch_item.timeout_seconds;
	      pwatchers->push_back(std::move(ow));
	    }
	  }
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };

  struct CB_ObjectOperation_decodewatchersneo {
    std::vector<neorados::ObjWatcher>* pwatchers;
    int* prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_decodewatchersneo(std::vector<neorados::ObjWatcher>* pw,
					 int* pr,
					 boost::system::error_code* pec)
      : pwatchers(pw), prval(pr), pec(pec) {}
    void operator()(boost::system::error_code ec, int r,
		    const ceph::buffer::list& bl) {
      if (r >= 0) {
	auto p = bl.cbegin();
	try {
	  obj_list_watch_response_t resp;
	  decode(resp, p);
	  if (pwatchers) {
	    for (const auto& watch_item : resp.entries) {
	      neorados::ObjWatcher ow;
	      ow.addr = watch_item.addr.get_legacy_str();
	      ow.watcher_id = watch_item.name.num();
	      ow.cookie = watch_item.cookie;
	      ow.timeout_seconds = watch_item.timeout_seconds;
	      pwatchers->push_back(std::move(ow));
	    }
	  }
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };


  struct CB_ObjectOperation_decodesnaps {
    librados::snap_set_t *psnaps;
    neorados::SnapSet *neosnaps;
    int *prval;
    boost::system::error_code* pec;
    CB_ObjectOperation_decodesnaps(librados::snap_set_t* ps,
				   neorados::SnapSet* ns, int* pr,
				   boost::system::error_code* pec)
      : psnaps(ps), neosnaps(ns), prval(pr), pec(pec) {}
    void operator()(boost::system::error_code ec, int r, const ceph::buffer::list& bl) {
      if (r >= 0) {
	using ceph::decode;
	auto p = bl.cbegin();
	try {
	  obj_list_snap_response_t resp;
	  decode(resp, p);
	  if (psnaps) {
	    psnaps->clones.clear();
	    for (auto ci = resp.clones.begin();
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

	  if (neosnaps) {
	    neosnaps->clones.clear();
	    for (auto&& c : resp.clones) {
	      neorados::CloneInfo clone;

	      clone.cloneid = std::move(c.cloneid);
	      clone.snaps.reserve(c.snaps.size());
	      std::move(c.snaps.begin(), c.snaps.end(),
			std::back_inserter(clone.snaps));
	      clone.overlap = c.overlap;
	      clone.size = c.size;
	      neosnaps->clones.push_back(std::move(clone));
	    }
	    neosnaps->seq = resp.seq;
	  }
	} catch (const ceph::buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	  if (pec)
	    *pec = e.code();
	}
      }
    }
  };
  void getxattrs(std::map<std::string,ceph::buffer::list> *pattrs, int *prval) {
    add_op(CEPH_OSD_OP_GETXATTRS);
    if (pattrs || prval) {
      set_handler(CB_ObjectOperation_decodevals(0, pattrs, nullptr, prval,
						nullptr));
      out_rval.back() = prval;
    }
  }
  void getxattrs(boost::system::error_code* ec,
		 boost::container::flat_map<std::string, ceph::buffer::list> *pattrs) {
    add_op(CEPH_OSD_OP_GETXATTRS);
    set_handler(CB_ObjectOperation_decodevals(0, pattrs, nullptr, nullptr, ec));
    out_ec.back() = ec;
  }
  void setxattr(const char *name, const ceph::buffer::list& bl) {
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void setxattr(std::string_view name, const ceph::buffer::list& bl) {
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void setxattr(const char *name, const std::string& s) {
    ceph::buffer::list bl;
    bl.append(s);
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void cmpxattr(const char *name, uint8_t cmp_op, uint8_t cmp_mode,
		const ceph::buffer::list& bl) {
    add_xattr_cmp(CEPH_OSD_OP_CMPXATTR, name, cmp_op, cmp_mode, bl);
  }
  void cmpxattr(std::string_view name, uint8_t cmp_op, uint8_t cmp_mode,
		const ceph::buffer::list& bl) {
    add_xattr_cmp(CEPH_OSD_OP_CMPXATTR, name, cmp_op, cmp_mode, bl);
  }
  void rmxattr(const char *name) {
    ceph::buffer::list bl;
    add_xattr(CEPH_OSD_OP_RMXATTR, name, bl);
  }
  void rmxattr(std::string_view name) {
    ceph::buffer::list bl;
    add_xattr(CEPH_OSD_OP_RMXATTR, name, bl);
  }
  void setxattrs(std::map<std::string, ceph::buffer::list>& attrs) {
    using ceph::encode;
    ceph::buffer::list bl;
    encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, 0, bl.length());
  }
  void resetxattrs(const char *prefix, std::map<std::string, ceph::buffer::list>& attrs) {
    using ceph::encode;
    ceph::buffer::list bl;
    encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, prefix, bl);
  }

  // trivialmap
  void tmap_update(ceph::buffer::list& bl) {
    add_data(CEPH_OSD_OP_TMAPUP, 0, 0, bl);
  }

  // objectmap
  void omap_get_keys(const std::string &start_after,
		     uint64_t max_to_get,
		     std::set<std::string> *out_set,
		     bool *ptruncated,
		     int *prval) {
    using ceph::encode;
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETKEYS);
    ceph::buffer::list bl;
    encode(start_after, bl);
    encode(max_to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || ptruncated || out_set) {
      set_handler(CB_ObjectOperation_decodekeys(max_to_get, out_set, ptruncated, prval,
						nullptr));
      out_rval.back() = prval;
    }
  }
  void omap_get_keys(std::optional<std::string_view> start_after,
		     uint64_t max_to_get,
		     boost::system::error_code* ec,
		     boost::container::flat_set<std::string> *out_set,
		     bool *ptruncated) {
    OSDOp& op = add_op(CEPH_OSD_OP_OMAPGETKEYS);
    ceph::buffer::list bl;
    encode(start_after ? *start_after : std::string_view{}, bl);
    encode(max_to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    set_handler(
      CB_ObjectOperation_decodekeys(max_to_get, out_set, ptruncated, nullptr,
				    ec));
    out_ec.back() = ec;
  }

  void omap_get_vals(const std::string &start_after,
		     const std::string &filter_prefix,
		     uint64_t max_to_get,
		     std::map<std::string, ceph::buffer::list> *out_set,
		     bool *ptruncated,
		     int *prval) {
    using ceph::encode;
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALS);
    ceph::buffer::list bl;
    encode(start_after, bl);
    encode(max_to_get, bl);
    encode(filter_prefix, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set || ptruncated) {
      set_handler(CB_ObjectOperation_decodevals(max_to_get, out_set, ptruncated,
						prval, nullptr));
      out_rval.back() = prval;
    }
  }

  void omap_get_vals(std::optional<std::string_view> start_after,
		     std::optional<std::string_view> filter_prefix,
		     uint64_t max_to_get,
		     boost::system::error_code* ec,
		     boost::container::flat_map<std::string, ceph::buffer::list> *out_set,
		     bool *ptruncated) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALS);
    ceph::buffer::list bl;
    encode(start_after ? *start_after : std::string_view{}, bl);
    encode(max_to_get, bl);
    encode(filter_prefix ? *start_after : std::string_view{}, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    set_handler(CB_ObjectOperation_decodevals(max_to_get, out_set, ptruncated,
					      nullptr, ec));
    out_ec.back() = ec;
  }

  void omap_get_vals_by_keys(const std::set<std::string> &to_get,
			     std::map<std::string, ceph::buffer::list> *out_set,
			     int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
    ceph::buffer::list bl;
    encode(to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      set_handler(CB_ObjectOperation_decodevals(0, out_set, nullptr, prval,
						nullptr));
      out_rval.back() = prval;
    }
  }

  void omap_get_vals_by_keys(
    const boost::container::flat_set<std::string>& to_get,
    boost::system::error_code* ec,
    boost::container::flat_map<std::string, ceph::buffer::list> *out_set) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
    ceph::buffer::list bl;
    encode(to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    set_handler(CB_ObjectOperation_decodevals(0, out_set, nullptr, nullptr,
					      ec));
    out_ec.back() = ec;
  }

  void omap_cmp(const std::map<std::string, std::pair<ceph::buffer::list,int> > &assertions,
		int *prval) {
    using ceph::encode;
    OSDOp &op = add_op(CEPH_OSD_OP_OMAP_CMP);
    ceph::buffer::list bl;
    encode(assertions, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval) {
      unsigned p = ops.size() - 1;
      out_rval[p] = prval;
    }
  }

  void omap_cmp(ceph::buffer::list&& assertions,
		int *prval) {
    using ceph::encode;
    OSDOp &op = add_op(CEPH_OSD_OP_OMAP_CMP);
    op.op.extent.offset = 0;
    op.op.extent.length = assertions.length();
    op.indata.claim_append(assertions);
    if (prval) {
      unsigned p = ops.size() - 1;
      out_rval[p] = prval;
    }
  }
  struct C_ObjectOperation_copyget : public Context {
    ceph::buffer::list bl;
    object_copy_cursor_t *cursor;
    uint64_t *out_size;
    ceph::real_time *out_mtime;
    std::map<std::string,ceph::buffer::list,std::less<>> *out_attrs;
    ceph::buffer::list *out_data, *out_omap_header, *out_omap_data;
    std::vector<snapid_t> *out_snaps;
    snapid_t *out_snap_seq;
    uint32_t *out_flags;
    uint32_t *out_data_digest;
    uint32_t *out_omap_digest;
    mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > *out_reqids;
    mempool::osd_pglog::map<uint32_t, int> *out_reqid_return_codes;
    uint64_t *out_truncate_seq;
    uint64_t *out_truncate_size;
    int *prval;
    C_ObjectOperation_copyget(object_copy_cursor_t *c,
			      uint64_t *s,
			      ceph::real_time *m,
			      std::map<std::string,ceph::buffer::list,std::less<>> *a,
			      ceph::buffer::list *d, ceph::buffer::list *oh,
			      ceph::buffer::list *o,
			      std::vector<snapid_t> *osnaps,
			      snapid_t *osnap_seq,
			      uint32_t *flags,
			      uint32_t *dd,
			      uint32_t *od,
			      mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > *oreqids,
			      mempool::osd_pglog::map<uint32_t, int> *oreqid_return_codes,
			      uint64_t *otseq,
			      uint64_t *otsize,
			      int *r)
      : cursor(c),
	out_size(s), out_mtime(m),
	out_attrs(a), out_data(d), out_omap_header(oh),
	out_omap_data(o), out_snaps(osnaps), out_snap_seq(osnap_seq),
	out_flags(flags), out_data_digest(dd), out_omap_digest(od),
	out_reqids(oreqids),
	out_reqid_return_codes(oreqid_return_codes),
	out_truncate_seq(otseq),
	out_truncate_size(otsize),
	prval(r) {}
    void finish(int r) override {
      using ceph::decode;
      // reqids are copied on ENOENT
      if (r < 0 && r != -ENOENT)
	return;
      try {
	auto p = bl.cbegin();
	object_copy_data_t copy_reply;
	decode(copy_reply, p);
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
	if (out_reqid_return_codes)
	  *out_reqid_return_codes = copy_reply.reqid_return_codes;
	if (out_truncate_seq)
	  *out_truncate_seq = copy_reply.truncate_seq;
	if (out_truncate_size)
	  *out_truncate_size = copy_reply.truncate_size;
	*cursor = copy_reply.cursor;
      } catch (const ceph::buffer::error& e) {
	if (prval)
	  *prval = -EIO;
      }
    }
  };

  void copy_get(object_copy_cursor_t *cursor,
		uint64_t max,
		uint64_t *out_size,
		ceph::real_time *out_mtime,
		std::map<std::string,ceph::buffer::list,std::less<>> *out_attrs,
		ceph::buffer::list *out_data,
		ceph::buffer::list *out_omap_header,
		ceph::buffer::list *out_omap_data,
		std::vector<snapid_t> *out_snaps,
		snapid_t *out_snap_seq,
		uint32_t *out_flags,
		uint32_t *out_data_digest,
		uint32_t *out_omap_digest,
		mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > *out_reqids,
		mempool::osd_pglog::map<uint32_t, int> *out_reqid_return_codes,
		uint64_t *truncate_seq,
		uint64_t *truncate_size,
		int *prval) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_COPY_GET);
    osd_op.op.copy_get.max = max;
    encode(*cursor, osd_op.indata);
    encode(max, osd_op.indata);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_copyget *h =
      new C_ObjectOperation_copyget(cursor, out_size, out_mtime,
				    out_attrs, out_data, out_omap_header,
				    out_omap_data, out_snaps, out_snap_seq,
				    out_flags, out_data_digest,
				    out_omap_digest, out_reqids,
				    out_reqid_return_codes, truncate_seq,
				    truncate_size, prval);
    out_bl[p] = &h->bl;
    set_handler(h);
  }

  void undirty() {
    add_op(CEPH_OSD_OP_UNDIRTY);
  }

  struct C_ObjectOperation_isdirty : public Context {
    ceph::buffer::list bl;
    bool *pisdirty;
    int *prval;
    C_ObjectOperation_isdirty(bool *p, int *r)
      : pisdirty(p), prval(r) {}
    void finish(int r) override {
      using ceph::decode;
      if (r < 0)
	return;
      try {
	auto p = bl.cbegin();
	bool isdirty;
	decode(isdirty, p);
	if (pisdirty)
	  *pisdirty = isdirty;
      } catch (const ceph::buffer::error& e) {
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
    set_handler(h);
  }

  struct C_ObjectOperation_hit_set_ls : public Context {
    ceph::buffer::list bl;
    std::list< std::pair<time_t, time_t> > *ptls;
    std::list< std::pair<ceph::real_time, ceph::real_time> > *putls;
    int *prval;
    C_ObjectOperation_hit_set_ls(std::list< std::pair<time_t, time_t> > *t,
				 std::list< std::pair<ceph::real_time,
						      ceph::real_time> > *ut,
				 int *r)
      : ptls(t), putls(ut), prval(r) {}
    void finish(int r) override {
      using ceph::decode;
      if (r < 0)
	return;
      try {
	auto p = bl.cbegin();
	std::list< std::pair<ceph::real_time, ceph::real_time> > ls;
	decode(ls, p);
	if (ptls) {
	  ptls->clear();
	  for (auto p = ls.begin(); p != ls.end(); ++p)
	    // round initial timestamp up to the next full second to
	    // keep this a valid interval.
	    ptls->push_back(
	      std::make_pair(ceph::real_clock::to_time_t(
			  ceph::ceil(p->first,
				     // Sadly, no time literals until C++14.
				     std::chrono::seconds(1))),
			ceph::real_clock::to_time_t(p->second)));
	}
	if (putls)
	  putls->swap(ls);
      } catch (const ceph::buffer::error& e) {
	r = -EIO;
      }
      if (prval)
	*prval = r;
    }
  };

  /**
   * std::list available HitSets.
   *
   * We will get back a std::list of time intervals.  Note that the most
   * recent range may have an empty end timestamp if it is still
   * accumulating.
   *
   * @param pls [out] std::list of time intervals
   * @param prval [out] return value
   */
  void hit_set_ls(std::list< std::pair<time_t, time_t> > *pls, int *prval) {
    add_op(CEPH_OSD_OP_PG_HITSET_LS);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_hit_set_ls *h =
      new C_ObjectOperation_hit_set_ls(pls, NULL, prval);
    out_bl[p] = &h->bl;
    set_handler(h);
  }
  void hit_set_ls(std::list<std::pair<ceph::real_time, ceph::real_time> > *pls,
		  int *prval) {
    add_op(CEPH_OSD_OP_PG_HITSET_LS);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    C_ObjectOperation_hit_set_ls *h =
      new C_ObjectOperation_hit_set_ls(NULL, pls, prval);
    out_bl[p] = &h->bl;
    set_handler(h);
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
  void hit_set_get(ceph::real_time stamp, ceph::buffer::list *pbl, int *prval) {
    OSDOp& op = add_op(CEPH_OSD_OP_PG_HITSET_GET);
    op.op.hit_set_get.stamp = ceph::real_clock::to_ceph_timespec(stamp);
    unsigned p = ops.size() - 1;
    out_rval[p] = prval;
    out_bl[p] = pbl;
  }

  void omap_get_header(ceph::buffer::list *bl, int *prval) {
    add_op(CEPH_OSD_OP_OMAPGETHEADER);
    unsigned p = ops.size() - 1;
    out_bl[p] = bl;
    out_rval[p] = prval;
  }

  void omap_get_header(boost::system::error_code* ec, ceph::buffer::list *bl) {
    add_op(CEPH_OSD_OP_OMAPGETHEADER);
    out_bl.back() = bl;
    out_ec.back() = ec;
  }

  void omap_set(const std::map<std::string, ceph::buffer::list> &map) {
    ceph::buffer::list bl;
    encode(map, bl);
    add_data(CEPH_OSD_OP_OMAPSETVALS, 0, bl.length(), bl);
  }

  void omap_set(const boost::container::flat_map<std::string, ceph::buffer::list>& map) {
    ceph::buffer::list bl;
    encode(map, bl);
    add_data(CEPH_OSD_OP_OMAPSETVALS, 0, bl.length(), bl);
  }

  void omap_set_header(ceph::buffer::list& bl) {
    add_data(CEPH_OSD_OP_OMAPSETHEADER, 0, bl.length(), bl);
  }

  void omap_set_header(ceph::buffer::list&& bl) {
    add_data(CEPH_OSD_OP_OMAPSETHEADER, 0, bl.length(), bl);
  }

  void omap_clear() {
    add_op(CEPH_OSD_OP_OMAPCLEAR);
  }

  void omap_rm_keys(const std::set<std::string> &to_remove) {
    using ceph::encode;
    ceph::buffer::list bl;
    encode(to_remove, bl);
    add_data(CEPH_OSD_OP_OMAPRMKEYS, 0, bl.length(), bl);
  }
  void omap_rm_keys(const boost::container::flat_set<std::string>& to_remove) {
    ceph::buffer::list bl;
    encode(to_remove, bl);
    add_data(CEPH_OSD_OP_OMAPRMKEYS, 0, bl.length(), bl);
  }

  void omap_rm_range(std::string_view key_begin, std::string_view key_end) {
    ceph::buffer::list bl;
    using ceph::encode;
    encode(key_begin, bl);
    encode(key_end, bl);
    add_data(CEPH_OSD_OP_OMAPRMKEYRANGE, 0, bl.length(), bl);
  }

  // object classes
  void call(const char *cname, const char *method, ceph::buffer::list &indata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, NULL, NULL, NULL);
  }

  void call(const char *cname, const char *method, ceph::buffer::list &indata,
	    ceph::buffer::list *outdata, Context *ctx, int *prval) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, outdata, ctx, prval);
  }

  void call(std::string_view cname, std::string_view method,
	    const ceph::buffer::list& indata, boost::system::error_code* ec) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, NULL, NULL, NULL);
    out_ec.back() = ec;
  }

  void call(std::string_view cname, std::string_view method, const ceph::buffer::list& indata,
	    boost::system::error_code* ec, ceph::buffer::list *outdata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, outdata, nullptr, nullptr);
    out_ec.back() = ec;
  }
  void call(std::string_view cname, std::string_view method,
	    const ceph::buffer::list& indata,
	    fu2::unique_function<void (boost::system::error_code,
				       const ceph::buffer::list&) &&> f) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, std::move(f));
  }
  void call(std::string_view cname, std::string_view method,
	    const ceph::buffer::list& indata,
	    fu2::unique_function<void (boost::system::error_code, int,
				       const ceph::buffer::list&) &&> f) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata, std::move(f));
  }

  // watch/notify
  void watch(uint64_t cookie, __u8 op, uint32_t timeout = 0) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_WATCH);
    osd_op.op.watch.cookie = cookie;
    osd_op.op.watch.op = op;
    osd_op.op.watch.timeout = timeout;
  }

  void notify(uint64_t cookie, uint32_t prot_ver, uint32_t timeout,
              ceph::buffer::list &bl, ceph::buffer::list *inbl) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_NOTIFY);
    osd_op.op.notify.cookie = cookie;
    encode(prot_ver, *inbl);
    encode(timeout, *inbl);
    encode(bl, *inbl);
    osd_op.indata.append(*inbl);
  }

  void notify_ack(uint64_t notify_id, uint64_t cookie,
		  ceph::buffer::list& reply_bl) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_NOTIFY_ACK);
    ceph::buffer::list bl;
    encode(notify_id, bl);
    encode(cookie, bl);
    encode(reply_bl, bl);
    osd_op.indata.append(bl);
  }

  void list_watchers(std::list<obj_watch_t> *out,
		     int *prval) {
    add_op(CEPH_OSD_OP_LIST_WATCHERS);
    if (prval || out) {
      set_handler(CB_ObjectOperation_decodewatchers(out, prval, nullptr));
      out_rval.back() = prval;
    }
  }
  void list_watchers(std::vector<neorados::ObjWatcher>* out,
		     boost::system::error_code* ec) {
    add_op(CEPH_OSD_OP_LIST_WATCHERS);
    set_handler(CB_ObjectOperation_decodewatchersneo(out, nullptr, ec));
    out_ec.back() = ec;
  }

  void list_snaps(librados::snap_set_t *out, int *prval,
		  boost::system::error_code* ec = nullptr) {
    add_op(CEPH_OSD_OP_LIST_SNAPS);
    if (prval || out || ec) {
      set_handler(CB_ObjectOperation_decodesnaps(out, nullptr, prval, ec));
      out_rval.back() = prval;
      out_ec.back() = ec;
    }
  }

  void list_snaps(neorados::SnapSet *out, int *prval,
		  boost::system::error_code* ec = nullptr) {
    add_op(CEPH_OSD_OP_LIST_SNAPS);
    if (prval || out || ec) {
      set_handler(CB_ObjectOperation_decodesnaps(nullptr, out, prval, ec));
      out_rval.back() = prval;
      out_ec.back() = ec;
    }
  }

  void assert_version(uint64_t ver) {
    OSDOp& osd_op = add_op(CEPH_OSD_OP_ASSERT_VER);
    osd_op.op.assert_ver.ver = ver;
  }

  void cmpxattr(const char *name, const ceph::buffer::list& val,
		int op, int mode) {
    add_xattr(CEPH_OSD_OP_CMPXATTR, name, val);
    OSDOp& o = *ops.rbegin();
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
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_COPY_FROM);
    osd_op.op.copy_from.snapid = snapid;
    osd_op.op.copy_from.src_version = src_version;
    osd_op.op.copy_from.flags = flags;
    osd_op.op.copy_from.src_fadvise_flags = src_fadvise_flags;
    encode(src, osd_op.indata);
    encode(src_oloc, osd_op.indata);
  }
  void copy_from2(object_t src, snapid_t snapid, object_locator_t src_oloc,
		 version_t src_version, unsigned flags,
		 uint32_t truncate_seq, uint64_t truncate_size,
		 unsigned src_fadvise_flags) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_COPY_FROM2);
    osd_op.op.copy_from.snapid = snapid;
    osd_op.op.copy_from.src_version = src_version;
    osd_op.op.copy_from.flags = flags;
    osd_op.op.copy_from.src_fadvise_flags = src_fadvise_flags;
    encode(src, osd_op.indata);
    encode(src_oloc, osd_op.indata);
    encode(truncate_seq, osd_op.indata);
    encode(truncate_size, osd_op.indata);
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

  /*
   * Extensible tier
   */
  void set_redirect(object_t tgt, snapid_t snapid, object_locator_t tgt_oloc, 
		    version_t tgt_version, int flag) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_SET_REDIRECT);
    osd_op.op.copy_from.snapid = snapid;
    osd_op.op.copy_from.src_version = tgt_version;
    encode(tgt, osd_op.indata);
    encode(tgt_oloc, osd_op.indata);
    set_last_op_flags(flag);
  }

  void set_chunk(uint64_t src_offset, uint64_t src_length, object_locator_t tgt_oloc,
		 object_t tgt_oid, uint64_t tgt_offset, int flag) {
    using ceph::encode;
    OSDOp& osd_op = add_op(CEPH_OSD_OP_SET_CHUNK);
    encode(src_offset, osd_op.indata);
    encode(src_length, osd_op.indata);
    encode(tgt_oloc, osd_op.indata);
    encode(tgt_oid, osd_op.indata);
    encode(tgt_offset, osd_op.indata);
    set_last_op_flags(flag);
  }

  void tier_promote() {
    add_op(CEPH_OSD_OP_TIER_PROMOTE);
  }

  void unset_manifest() {
    add_op(CEPH_OSD_OP_UNSET_MANIFEST);
  }

  void tier_flush() {
    add_op(CEPH_OSD_OP_TIER_FLUSH);
  }

  void tier_evict() {
    add_op(CEPH_OSD_OP_TIER_EVICT);
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

  template<typename V>
  void dup(V& sops) {
    ops.clear();
    std::copy(sops.begin(), sops.end(),
	      std::back_inserter(ops));
    out_bl.resize(sops.size());
    out_handler.resize(sops.size());
    out_rval.resize(sops.size());
    out_ec.resize(sops.size());
    for (uint32_t i = 0; i < sops.size(); i++) {
      out_bl[i] = &sops[i].outdata;
      out_rval[i] = &sops[i].rval;
      out_ec[i] = nullptr;
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

inline std::ostream& operator <<(std::ostream& m, const ObjectOperation& oo) {
  auto i = oo.ops.cbegin();
  m << '[';
  while (i != oo.ops.cend()) {
    if (i != oo.ops.cbegin())
      m << ' ';
    m << *i;
    ++i;
  }
  m << ']';
  return m;
}


// ----------------

class Objecter : public md_config_obs_t, public Dispatcher {
  using MOSDOp = _mosdop::MOSDOp<osdc_opvec>;
public:
  using OpSignature = void(boost::system::error_code);
  using OpCompletion = boost::asio::any_completion_handler<OpSignature>;

  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;

public:
  Messenger *messenger;
  MonClient *monc;
  boost::asio::io_context& service;
  // The guaranteed sequenced, one-at-a-time execution and apparently
  // people sometimes depend on this.
  boost::asio::strand<boost::asio::io_context::executor_type>
      finish_strand{service.get_executor()};
  ZTracer::Endpoint trace_endpoint{"0.0.0.0", 0, "Objecter"};
private:
  std::unique_ptr<OSDMap> osdmap{std::make_unique<OSDMap>()};
public:
  using Dispatcher::cct;
  std::multimap<std::string,std::string> crush_location;

  std::atomic<bool> initialized{false};

private:
  std::atomic<uint64_t> last_tid{0};
  std::atomic<unsigned> inflight_ops{0};
  std::atomic<int> client_inc{-1};
  uint64_t max_linger_id{0};
  std::atomic<unsigned> num_in_flight{0};
  std::atomic<int> global_op_flags{0}; // flags which are applied to each IO op
  bool keep_balanced_budget = false;
  bool honor_pool_full = true;

  // If this is true, accumulate a set of blocklisted entities
  // to be drained by consume_blocklist_events.
  bool blocklist_events_enabled = false;
  std::set<entity_addr_t> blocklist_events;
  struct pg_mapping_t {
    epoch_t epoch = 0;
    std::vector<int> up;
    int up_primary = -1;
    std::vector<int> acting;
    int acting_primary = -1;

    pg_mapping_t() {}
    pg_mapping_t(epoch_t epoch, const std::vector<int>& up, int up_primary,
                 const std::vector<int>& acting, int acting_primary)
               : epoch(epoch), up(up), up_primary(up_primary),
                 acting(acting), acting_primary(acting_primary) {}
  };
  ceph::shared_mutex pg_mapping_lock =
    ceph::make_shared_mutex("Objecter::pg_mapping_lock");
  // pool -> pg mapping
  std::map<int64_t, std::vector<pg_mapping_t>> pg_mappings;

  // convenient accessors
  bool lookup_pg_mapping(const pg_t& pg, epoch_t epoch, std::vector<int> *up,
                         int *up_primary, std::vector<int> *acting,
                         int *acting_primary) {
    std::shared_lock l{pg_mapping_lock};
    auto it = pg_mappings.find(pg.pool());
    if (it == pg_mappings.end())
      return false;
    auto& mapping_array = it->second;
    if (pg.ps() >= mapping_array.size())
      return false;
    if (mapping_array[pg.ps()].epoch != epoch) // stale
      return false;
    auto& pg_mapping = mapping_array[pg.ps()];
    *up = pg_mapping.up;
    *up_primary = pg_mapping.up_primary;
    *acting = pg_mapping.acting;
    *acting_primary = pg_mapping.acting_primary;
    return true;
  }
  void update_pg_mapping(const pg_t& pg, pg_mapping_t&& pg_mapping) {
    std::lock_guard l{pg_mapping_lock};
    auto& mapping_array = pg_mappings[pg.pool()];
    ceph_assert(pg.ps() < mapping_array.size());
    mapping_array[pg.ps()] = std::move(pg_mapping);
  }
  void prune_pg_mapping(const mempool::osdmap::map<int64_t,pg_pool_t>& pools) {
    std::lock_guard l{pg_mapping_lock};
    for (auto& pool : pools) {
      auto& mapping_array = pg_mappings[pool.first];
      size_t pg_num = pool.second.get_pg_num();
      if (mapping_array.size() != pg_num) {
        // catch both pg_num increasing & decreasing
        mapping_array.resize(pg_num);
      }
    }
    for (auto it = pg_mappings.begin(); it != pg_mappings.end(); ) {
      if (!pools.count(it->first)) {
        // pool is gone
        pg_mappings.erase(it++);
        continue;
      }
      it++;
    }
  }

public:
  void maybe_request_map();

  void enable_blocklist_events();
private:

  void _maybe_request_map();

  version_t last_seen_osdmap_version = 0;
  version_t last_seen_pgmap_version = 0;

  mutable ceph::shared_mutex rwlock =
	   ceph::make_shared_mutex("Objecter::rwlock");
  ceph::timer<ceph::coarse_mono_clock> timer;

  PerfCounters* logger = nullptr;

  uint64_t tick_event = 0;

  void start_tick();
  void tick();
  void update_crush_location();

  class RequestStateHook;

  RequestStateHook *m_request_state_hook = nullptr;

public:
  /*** track pending operations ***/
  // read

  struct OSDSession;

  struct op_target_t {
    int flags = 0;

    epoch_t epoch = 0;  ///< latest epoch we calculated the mapping

    object_t base_oid;
    object_locator_t base_oloc;
    object_t target_oid;
    object_locator_t target_oloc;

    ///< true if we are directed at base_pgid, not base_oid
    bool precalc_pgid = false;

    ///< true if we have ever mapped to a valid pool
    bool pool_ever_existed = false;

    ///< explcit pg target, if any
    pg_t base_pgid;

    pg_t pgid; ///< last (raw) pg we mapped to
    spg_t actual_pgid; ///< last (actual) spg_t we mapped to
    unsigned pg_num = 0; ///< last pg_num we mapped to
    unsigned pg_num_mask = 0; ///< last pg_num_mask we mapped to
    unsigned pg_num_pending = 0; ///< last pg_num we mapped to
    std::vector<int> up; ///< set of up osds for last pg we mapped to
    std::vector<int> acting; ///< set of acting osds for last pg we mapped to
    int up_primary = -1; ///< last up_primary we mapped to
    int acting_primary = -1;  ///< last acting_primary we mapped to
    int size = -1; ///< the size of the pool when were were last mapped
    int min_size = -1; ///< the min size of the pool when were were last mapped
    bool sort_bitwise = false; ///< whether the hobject_t sort order is bitwise
    bool recovery_deletes = false; ///< whether the deletes are performed during recovery instead of peering
    uint32_t peering_crush_bucket_count = 0;
    uint32_t peering_crush_bucket_target = 0;
    uint32_t peering_crush_bucket_barrier = 0;
    int32_t peering_crush_mandatory_member = CRUSH_ITEM_NONE;

    bool used_replica = false;
    bool paused = false;

    int osd = -1;      ///< the final target osd, or -1

    epoch_t last_force_resend = 0;

    op_target_t(const object_t& oid, const object_locator_t& oloc, int flags)
      : flags(flags),
	base_oid(oid),
	base_oloc(oloc)
      {}

    explicit op_target_t(pg_t pgid)
      : base_oloc(pgid.pool(), pgid.ps()),
	precalc_pgid(true),
	base_pgid(pgid)
      {}

    op_target_t() = default;

    hobject_t get_hobj() {
      return hobject_t(target_oid,
		       target_oloc.key,
		       CEPH_NOSNAP,
		       target_oloc.hash >= 0 ? target_oloc.hash : pgid.ps(),
		       target_oloc.pool,
		       target_oloc.nspace);
    }

    bool contained_by(const hobject_t& begin, const hobject_t& end) {
      hobject_t h = get_hobj();
      int r = cmp(h, begin);
      return r == 0 || (r > 0 && h < end);
    }

    bool respects_full() const {
      return
	(flags & (CEPH_OSD_FLAG_WRITE | CEPH_OSD_FLAG_RWORDERED)) &&
	!(flags & (CEPH_OSD_FLAG_FULL_TRY | CEPH_OSD_FLAG_FULL_FORCE));
    }

    void dump(ceph::Formatter *f) const;
  };

  boost::asio::any_completion_handler<void(boost::system::error_code)>
  OpContextVert(Context* c) {
    if (c) {
      auto e = boost::asio::prefer(
	service.get_executor(),
	boost::asio::execution::outstanding_work.tracked);

      return boost::asio::bind_executor(
	std::move(e),
	[c = std::unique_ptr<Context>(c)]
	(boost::system::error_code e) mutable {
	  c.release()->complete(e);
	});
    }
    else
      return nullptr;
  }

  template<typename T>
  boost::asio::any_completion_handler<void(boost::system::error_code, T)>
  OpContextVert(Context* c, T* p) {

    if (c || p) {
      auto e = boost::asio::prefer(
	service.get_executor(),
	boost::asio::execution::outstanding_work.tracked);
      return
	boost::asio::bind_executor(
	  e,
	  [c = std::unique_ptr<Context>(c), p]
	  (boost::system::error_code e, T r) mutable {
	      if (p)
		*p = std::move(r);
	      if (c)
		c.release()->complete(ceph::from_error_code(e));
	  });
    } else {
      return nullptr;
    }
  }

  template<typename T>
  boost::asio::any_completion_handler<void(boost::system::error_code, T)>
  OpContextVert(Context* c, T& p) {
    if (c) {
      auto e = boost::asio::prefer(
	service.get_executor(),
	boost::asio::execution::outstanding_work.tracked);
      return boost::asio::bind_executor(
	e,
	[c = std::unique_ptr<Context>(c), &p]
	(boost::system::error_code e, T r) mutable {
	  p = std::move(r);
	  if (c)
	    c.release()->complete(ceph::from_error_code(e));
	});
    } else {
      return nullptr;
    }
  }

  boost::asio::any_completion_handler<void(boost::system::error_code)>
  OpCompletionVert(std::unique_ptr<ceph::async::Completion<
		     void(boost::system::error_code)>> c) {
    if (c)
      return [c = std::move(c)](boost::system::error_code ec) mutable {
	c->dispatch(std::move(c), ec);
      };
    else
      return nullptr;
  }

  template<typename T>
  boost::asio::any_completion_handler<void(boost::system::error_code, T)>
  OpCompletionVert(std::unique_ptr<ceph::async::Completion<
		     void(boost::system::error_code, T)>> c) {
    if (c) {
      return [c = std::move(c)](boost::system::error_code ec, T t) mutable {
	c->dispatch(std::move(c), ec, std::move(t));
      };
    } else {
      return nullptr;
    }
  }

  struct Op : public RefCountedObject {
    OSDSession *session = nullptr;
    int incarnation = 0;

    op_target_t target;

    ConnectionRef con = nullptr;  // for rx buffer only
    uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT; // explicitly specified op features

    osdc_opvec ops;

    snapid_t snapid = CEPH_NOSNAP;
    SnapContext snapc;
    ceph::real_time mtime;

    ceph::buffer::list *outbl = nullptr;
    boost::container::small_vector<ceph::buffer::list*, osdc_opvec_len> out_bl;
    boost::container::small_vector<
      fu2::unique_function<void(boost::system::error_code, int,
				const ceph::buffer::list& bl) &&>,
      osdc_opvec_len> out_handler;
    boost::container::small_vector<int*, osdc_opvec_len> out_rval;
    boost::container::small_vector<boost::system::error_code*,
				   osdc_opvec_len> out_ec;

    int priority = 0;
    using OpSig = void(boost::system::error_code);
    using OpComp = boost::asio::any_completion_handler<OpSig>;
    // Due to an irregularity of cmpxattr, we actualy need the 'int'
    // value for onfinish for legacy librados users. As such just
    // preserve the Context* in this one case. That way we can have
    // our callers just pass in a unique_ptr<OpComp> and not deal with
    // our signature in Objecter being different than the exposed
    // signature in RADOS.
    //
    // Add a function for the linger case, where we want better
    // semantics than Context, but still need to be under the completion_lock.
    std::variant<OpComp, fu2::unique_function<OpSig>,
		 Context*> onfinish;
    uint64_t ontimeout = 0;

    ceph_tid_t tid = 0;
    int attempts = 0;

    version_t *objver;
    epoch_t *reply_epoch = nullptr;

    ceph::coarse_mono_time stamp;

    epoch_t map_dne_bound = 0;

    int budget = -1;

    /// true if we should resend this message on failure
    bool should_resend = true;

    /// true if the throttle budget is get/put on a series of OPs,
    /// instead of per OP basis, when this flag is set, the budget is
    /// acquired before sending the very first OP of the series and
    /// released upon receiving the last OP reply.
    bool ctx_budgeted = false;

    int *data_offset;

    osd_reqid_t reqid; // explicitly setting reqid
    ZTracer::Trace trace;
    const jspan_context* otel_trace = nullptr;

    static bool has_completion(decltype(onfinish)& f) {
      return std::visit([](auto&& arg) { return bool(arg);}, f);
    }
    bool has_completion() {
      return has_completion(onfinish);
    }

    static void complete(decltype(onfinish)&& f, boost::system::error_code ec,
			 int r, boost::asio::io_context::executor_type e) {
      std::visit([ec, r, e](auto&& arg) {
		   if constexpr (std::is_same_v<std::decay_t<decltype(arg)>,
				 Context*>) {
		     arg->complete(r);
		   } else if constexpr (std::is_same_v<std::decay_t<decltype(arg)>,
			      fu2::unique_function<OpSig>>) {
		     std::move(arg)(ec);
                   } else {
		     boost::asio::defer(e,
					boost::asio::append(std::move(arg), ec));
		   }
		 }, std::move(f));
    }
    void complete(boost::system::error_code ec, int r,
		  boost::asio::io_context::executor_type e) {
      complete(std::move(onfinish), ec, r, e);
    }

    Op(const object_t& o, const object_locator_t& ol,  osdc_opvec&& _ops,
       int f, OpComp&& fin, version_t *ov, int *offset = nullptr,
       ZTracer::Trace *parent_trace = nullptr) :
      target(o, ol, f),
      ops(std::move(_ops)),
      out_bl(ops.size(), nullptr),
      out_handler(ops.size()),
      out_rval(ops.size(), nullptr),
      out_ec(ops.size(), nullptr),
      onfinish(std::move(fin)),
      objver(ov),
      data_offset(offset) {
      if (target.base_oloc.key == o)
	target.base_oloc.key.clear();
      if (parent_trace && parent_trace->valid()) {
        trace.init("op", nullptr, parent_trace);
        trace.event("start");
      }
    }

    Op(const object_t& o, const object_locator_t& ol, osdc_opvec&& _ops,
       int f, Context* fin, version_t *ov, int *offset = nullptr,
       ZTracer::Trace *parent_trace = nullptr, const jspan_context *otel_trace = nullptr) :
      target(o, ol, f),
      ops(std::move(_ops)),
      out_bl(ops.size(), nullptr),
      out_handler(ops.size()),
      out_rval(ops.size(), nullptr),
      out_ec(ops.size(), nullptr),
      onfinish(fin),
      objver(ov),
      data_offset(offset),
      otel_trace(otel_trace) {
      if (target.base_oloc.key == o)
	target.base_oloc.key.clear();
      if (parent_trace && parent_trace->valid()) {
        trace.init("op", nullptr, parent_trace);
        trace.event("start");
      }
    }

    Op(const object_t& o, const object_locator_t& ol, osdc_opvec&&  _ops,
       int f, fu2::unique_function<OpSig>&& fin, version_t *ov, int *offset = nullptr,
       ZTracer::Trace *parent_trace = nullptr) :
      target(o, ol, f),
      ops(std::move(_ops)),
      out_bl(ops.size(), nullptr),
      out_handler(ops.size()),
      out_rval(ops.size(), nullptr),
      out_ec(ops.size(), nullptr),
      onfinish(std::move(fin)),
      objver(ov),
      data_offset(offset) {
      if (target.base_oloc.key == o)
	target.base_oloc.key.clear();
      if (parent_trace && parent_trace->valid()) {
        trace.init("op", nullptr, parent_trace);
        trace.event("start");
      }
    }

    bool operator<(const Op& other) const {
      return tid < other.tid;
    }

  private:
    ~Op() override {
      trace.event("finish");
    }
  };

  struct CB_Op_Map_Latest {
    Objecter *objecter;
    ceph_tid_t tid;
    CB_Op_Map_Latest(Objecter *o, ceph_tid_t t) : objecter(o), tid(t) {}
    void operator()(boost::system::error_code err, version_t latest, version_t);
  };

  struct CB_Command_Map_Latest {
    Objecter *objecter;
    uint64_t tid;
    CB_Command_Map_Latest(Objecter *o, ceph_tid_t t) :  objecter(o), tid(t) {}
    void operator()(boost::system::error_code err, version_t latest, version_t);
  };

  struct C_Stat : public Context {
    ceph::buffer::list bl;
    uint64_t *psize;
    ceph::real_time *pmtime;
    Context *fin;
    C_Stat(uint64_t *ps, ceph::real_time *pm, Context *c) :
      psize(ps), pmtime(pm), fin(c) {}
    void finish(int r) override {
      using ceph::decode;
      if (r >= 0) {
	auto p = bl.cbegin();
	uint64_t s;
	ceph::real_time m;
	decode(s, p);
	decode(m, p);
	if (psize)
	  *psize = s;
	if (pmtime)
	  *pmtime = m;
      }
      fin->complete(r);
    }
  };

  struct C_GetAttrs : public Context {
    ceph::buffer::list bl;
    std::map<std::string,ceph::buffer::list>& attrset;
    Context *fin;
    C_GetAttrs(std::map<std::string, ceph::buffer::list>& set, Context *c) : attrset(set),
							   fin(c) {}
    void finish(int r) override {
      using ceph::decode;
      if (r >= 0) {
	auto p = bl.cbegin();
	decode(attrset, p);
      }
      fin->complete(r);
    }
  };


  // Pools and statistics
  struct NListContext {
    collection_list_handle_t pos;

    // these are for !sortbitwise compat only
    int current_pg = 0;
    int starting_pg_num = 0;
    bool sort_bitwise = false;

    bool at_end_of_pool = false; ///< publicly visible end flag

    int64_t pool_id = -1;
    int pool_snap_seq = 0;
    uint64_t max_entries = 0;
    std::string nspace;

    ceph::buffer::list bl;   // raw data read to here
    std::list<librados::ListObjectImpl> list;

    ceph::buffer::list filter;

    // The budget associated with this context, once it is set (>= 0),
    // the budget is not get/released on OP basis, instead the budget
    // is acquired before sending the first OP and released upon receiving
    // the last op reply.
    int ctx_budget = -1;

    bool at_end() const {
      return at_end_of_pool;
    }

    uint32_t get_pg_hash_position() const {
      return pos.get_hash();
    }
  };

  struct C_NList : public Context {
    NListContext *list_context;
    Context *final_finish;
    Objecter *objecter;
    epoch_t epoch;
    C_NList(NListContext *lc, Context * finish, Objecter *ob) :
      list_context(lc), final_finish(finish), objecter(ob), epoch(0) {}
    void finish(int r) override {
      if (r >= 0) {
	objecter->_nlist_reply(list_context, r, final_finish, epoch);
      } else {
	final_finish->complete(r);
      }
    }
  };

  struct PoolStatOp {
    ceph_tid_t tid;
    std::vector<std::string> pools;
    using OpSig = void(boost::system::error_code,
		       boost::container::flat_map<std::string, pool_stat_t>,
		       bool);
    using OpComp = boost::asio::any_completion_handler<OpSig>;
    OpComp onfinish;
    std::uint64_t ontimeout;
    ceph::coarse_mono_time last_submit;
  };

  struct StatfsOp {
    ceph_tid_t tid;
    std::optional<int64_t> data_pool;
    using OpSig = void(boost::system::error_code,
		       const struct ceph_statfs);
    using OpComp = boost::asio::any_completion_handler<OpSig>;

    OpComp onfinish;
    uint64_t ontimeout;

    ceph::coarse_mono_time last_submit;
  };

  struct PoolOp {
    ceph_tid_t tid = 0;
    int64_t pool = 0;
    std::string name;
    using OpSig = void(boost::system::error_code, ceph::buffer::list);
    using OpComp = boost::asio::any_completion_handler<OpSig>;
    OpComp onfinish;
    uint64_t ontimeout = 0;
    int pool_op = 0;
    int16_t crush_rule = 0;
    snapid_t snapid = 0;
    ceph::coarse_mono_time last_submit;

    PoolOp() {}
  };

  // -- osd commands --
  struct CommandOp : public RefCountedObject {
    OSDSession *session = nullptr;
    ceph_tid_t tid = 0;
    std::vector<std::string> cmd;
    ceph::buffer::list inbl;

    // target_osd == -1 means target_pg is valid
    const int target_osd = -1;
    const pg_t target_pg;

    op_target_t target;

    epoch_t map_dne_bound = 0;
    int map_check_error = 0; // error to return if std::map check fails
    const char *map_check_error_str = nullptr;

    using OpSig = void(boost::system::error_code, std::string,
		       ceph::buffer::list);
    using OpComp = boost::asio::any_completion_handler<OpSig>;
    OpComp onfinish;

    uint64_t ontimeout = 0;
    ceph::coarse_mono_time last_submit;

    CommandOp(
      int target_osd,
      std::vector<std::string>&& cmd,
      ceph::buffer::list&& inbl,
      decltype(onfinish)&& onfinish)
      : cmd(std::move(cmd)),
	inbl(std::move(inbl)),
	target_osd(target_osd),
	onfinish(std::move(onfinish)) {}

    CommandOp(
      pg_t pgid,
      std::vector<std::string>&& cmd,
      ceph::buffer::list&& inbl,
      decltype(onfinish)&& onfinish)
      : cmd(std::move(cmd)),
	inbl(std::move(inbl)),
	target_pg(pgid),
	target(pgid),
	onfinish(std::move(onfinish)) {}
  };

  void submit_command(CommandOp *c, ceph_tid_t *ptid);
  int _calc_command_target(CommandOp *c,
			   ceph::shunique_lock<ceph::shared_mutex> &sul);
  void _assign_command_session(CommandOp *c,
			       ceph::shunique_lock<ceph::shared_mutex> &sul);
  void _send_command(CommandOp *c);
  int command_op_cancel(OSDSession *s, ceph_tid_t tid,
			boost::system::error_code ec);
  void _finish_command(CommandOp *c, boost::system::error_code ec,
		       std::string&& rs, ceph::buffer::list&& bl);
  void handle_command_reply(MCommandReply *m);

  // -- lingering ops --

  struct LingerOp : public RefCountedObject {
    Objecter *objecter;
    uint64_t linger_id{0};
    op_target_t target{object_t(), object_locator_t(), 0};
    snapid_t snap{CEPH_NOSNAP};
    SnapContext snapc;
    ceph::real_time mtime;

    osdc_opvec ops;
    ceph::buffer::list inbl;
    version_t *pobjver{nullptr};

    bool is_watch{false};
    ceph::coarse_mono_time watch_valid_thru; ///< send time for last acked ping
    boost::system::error_code last_error;  ///< error from last failed ping|reconnect, if any
    ceph::shared_mutex watch_lock;

    // queue of pending async operations, with the timestamp of
    // when they were queued.
    std::list<ceph::coarse_mono_time> watch_pending_async;

    uint32_t register_gen{0};
    bool registered{false};
    bool canceled{false};
    using OpSig = void(boost::system::error_code, ceph::buffer::list);
    using OpComp = boost::asio::any_completion_handler<OpSig>;
    OpComp on_reg_commit;
    OpComp on_notify_finish;
    uint64_t notify_id{0};

    fu2::unique_function<void(boost::system::error_code,
			      uint64_t notify_id,
			      uint64_t cookie,
			      uint64_t notifier_id,
			      ceph::buffer::list&& bl)> handle;
    OSDSession *session{nullptr};

    int ctx_budget{-1};
    ceph_tid_t register_tid{0};
    ceph_tid_t ping_tid{0};
    epoch_t map_dne_bound{0};

    void _queued_async() {
      // watch_lock ust be locked unique
      watch_pending_async.push_back(ceph::coarse_mono_clock::now());
    }
    void finished_async() {
      std::unique_lock l(watch_lock);
      ceph_assert(!watch_pending_async.empty());
      watch_pending_async.pop_front();
    }

    LingerOp(Objecter *o, uint64_t linger_id);
    const LingerOp& operator=(const LingerOp& r) = delete;
    LingerOp(const LingerOp& o) = delete;

    uint64_t get_cookie() {
      return reinterpret_cast<uint64_t>(this);
    }
  };

  struct CB_Linger_Commit {
    Objecter *objecter;
    boost::intrusive_ptr<LingerOp> info;
    ceph::buffer::list outbl;  // used for notify only
    CB_Linger_Commit(Objecter *o, LingerOp *l) : objecter(o), info(l) {}
    ~CB_Linger_Commit() = default;

    void operator()(boost::system::error_code ec) {
      objecter->_linger_commit(info.get(), ec, outbl);
    }
  };

  struct CB_Linger_Reconnect {
    Objecter *objecter;
    boost::intrusive_ptr<LingerOp> info;
    CB_Linger_Reconnect(Objecter *o, LingerOp *l) : objecter(o), info(l) {}
    ~CB_Linger_Reconnect() = default;

    void operator()(boost::system::error_code ec) {
      objecter->_linger_reconnect(info.get(), ec);
      info.reset();
    }
  };

  struct CB_Linger_Ping {
    Objecter *objecter;
    boost::intrusive_ptr<LingerOp> info;
    ceph::coarse_mono_time sent;
    uint32_t register_gen;
    CB_Linger_Ping(Objecter *o, LingerOp *l, ceph::coarse_mono_time s)
      : objecter(o), info(l), sent(s), register_gen(info->register_gen) {}
    void operator()(boost::system::error_code ec) {
      objecter->_linger_ping(info.get(), ec, sent, register_gen);
      info.reset();
    }
  };

  struct CB_Linger_Map_Latest {
    Objecter *objecter;
    uint64_t linger_id;
    CB_Linger_Map_Latest(Objecter *o, uint64_t id) : objecter(o), linger_id(id) {}
    void operator()(boost::system::error_code err, version_t latest, version_t);
  };

  // -- osd sessions --
  struct OSDBackoff {
    spg_t pgid;
    uint64_t id;
    hobject_t begin, end;
  };

  struct OSDSession : public RefCountedObject {
    // pending ops
    std::map<ceph_tid_t,Op*> ops;
    std::map<uint64_t, LingerOp*> linger_ops;
    std::map<ceph_tid_t,CommandOp*> command_ops;

    // backoffs
    std::map<spg_t,std::map<hobject_t,OSDBackoff>> backoffs;
    std::map<uint64_t,OSDBackoff*> backoffs_by_id;

    int osd;
    // NB locking two sessions at the same time is only safe because
    // it is only done in _recalc_linger_op_target with s and
    // linger_op->session, and it holds rwlock for write.  We disable
    // lockdep (using std::sharedMutex) because lockdep doesn't know
    // that.
    std::shared_mutex lock;

    int incarnation;
    ConnectionRef con;
    int num_locks;
    std::unique_ptr<std::mutex[]> completion_locks;

    OSDSession(CephContext *cct, int o) :
      osd(o), incarnation(0), con(NULL),
      num_locks(cct->_conf->objecter_completion_locks_per_session),
      completion_locks(new std::mutex[num_locks]) {}

    ~OSDSession() override;

    bool is_homeless() { return (osd == -1); }

    std::unique_lock<std::mutex> get_lock(object_t& oid);
  };
  std::map<int,OSDSession*> osd_sessions;

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
  bool _osdmap_pool_full(const pg_pool_t &p) const {
    return p.has_flag(pg_pool_t::FLAG_FULL) && honor_pool_full;
  }
  void update_pool_full_map(std::map<int64_t, bool>& pool_full_map);

  std::map<uint64_t, LingerOp*> linger_ops;
  // we use this just to confirm a cookie is valid before dereferencing the ptr
  std::unordered_set<LingerOp*> linger_ops_set;

  std::map<ceph_tid_t,PoolStatOp*> poolstat_ops;
  std::map<ceph_tid_t,StatfsOp*> statfs_ops;
  std::map<ceph_tid_t,PoolOp*> pool_ops;
  std::atomic<unsigned> num_homeless_ops{0};

  OSDSession* homeless_session = new OSDSession(cct, -1);


  // ops waiting for an osdmap with a new pool or confirmation that
  // the pool does not exist (may be expanded to other uses later)
  std::map<uint64_t, LingerOp*> check_latest_map_lingers;
  std::map<ceph_tid_t, Op*> check_latest_map_ops;
  std::map<ceph_tid_t, CommandOp*> check_latest_map_commands;

  std::map<epoch_t,
	   std::vector<std::pair<OpCompletion,
				 boost::system::error_code>>> waiting_for_map;

  ceph::timespan mon_timeout;
  ceph::timespan osd_timeout;

  MOSDOp *_prepare_osd_op(Op *op);
  void _send_op(Op *op);
  void _send_op_account(Op *op);
  void _cancel_linger_op(Op *op);
  void _finish_op(Op *op, int r);
  static bool is_pg_changed(
    int oldprimary,
    const std::vector<int>& oldacting,
    int newprimary,
    const std::vector<int>& newacting,
    bool any_change=false);
  enum recalc_op_target_result {
    RECALC_OP_TARGET_NO_ACTION = 0,
    RECALC_OP_TARGET_NEED_RESEND,
    RECALC_OP_TARGET_POOL_DNE,
    RECALC_OP_TARGET_OSD_DNE,
    RECALC_OP_TARGET_OSD_DOWN,
    RECALC_OP_TARGET_POOL_EIO,
  };
  bool _osdmap_full_flag() const;
  bool _osdmap_has_pool_full() const;
  void _prune_snapc(
    const mempool::osdmap::map<int64_t, snap_interval_set_t>& new_removed_snaps,
    Op *op);

  bool target_should_be_paused(op_target_t *op);
  int _calc_target(op_target_t *t, Connection *con,
		   bool any_change = false);
  int _map_session(op_target_t *op, OSDSession **s,
		   ceph::shunique_lock<ceph::shared_mutex>& lc);

  void _session_op_assign(OSDSession *s, Op *op);
  void _session_op_remove(OSDSession *s, Op *op);
  void _session_linger_op_assign(OSDSession *to, LingerOp *op);
  void _session_linger_op_remove(OSDSession *from, LingerOp *op);
  void _session_command_op_assign(OSDSession *to, CommandOp *op);
  void _session_command_op_remove(OSDSession *from, CommandOp *op);

  int _assign_op_target_session(Op *op, ceph::shunique_lock<ceph::shared_mutex>& lc,
				bool src_session_locked,
				bool dst_session_locked);
  int _recalc_linger_op_target(LingerOp *op,
			       ceph::shunique_lock<ceph::shared_mutex>& lc);

  void _linger_submit(LingerOp *info,
		      ceph::shunique_lock<ceph::shared_mutex>& sul);
  void _send_linger(LingerOp *info,
		    ceph::shunique_lock<ceph::shared_mutex>& sul);
  void _linger_commit(LingerOp *info, boost::system::error_code ec,
		      ceph::buffer::list& outbl);
  void _linger_reconnect(LingerOp *info, boost::system::error_code ec);
  void _send_linger_ping(LingerOp *info);
  void _linger_ping(LingerOp *info, boost::system::error_code ec,
		    ceph::coarse_mono_time sent, uint32_t register_gen);
  boost::system::error_code _normalize_watch_error(boost::system::error_code ec);

  friend class CB_Objecter_GetVersion;
  friend class CB_DoWatchError;
public:

  bool is_valid_watch(LingerOp* op) {
    std::shared_lock l(rwlock);
    return linger_ops_set.contains(op);
  }

  template<typename CT>
  auto linger_callback_flush(CT&& ct) {
    auto consigned = boost::asio::consign(
      std::forward<CT>(ct), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), void()>(
      [this](auto handler) {
	boost::asio::defer(finish_strand, std::move(handler));
      }, consigned);
  }

private:
  void _check_op_pool_dne(Op *op, std::unique_lock<std::shared_mutex> *sl);
  void _check_op_pool_eio(Op *op, std::unique_lock<std::shared_mutex> *sl);
  void _send_op_map_check(Op *op);
  void _op_cancel_map_check(Op *op);
  void _check_linger_pool_dne(LingerOp *op, bool *need_unregister);
  void _check_linger_pool_eio(LingerOp *op);
  void _send_linger_map_check(LingerOp *op);
  void _linger_cancel_map_check(LingerOp *op);
  void _check_command_map_dne(CommandOp *op);
  void _send_command_map_check(CommandOp *op);
  void _command_cancel_map_check(CommandOp *op);

  void _kick_requests(OSDSession *session, std::map<uint64_t, LingerOp *>& lresend);
  void _linger_ops_resend(std::map<uint64_t, LingerOp *>& lresend,
			  std::unique_lock<ceph::shared_mutex>& ul);

  int _get_session(int osd, OSDSession **session,
		   ceph::shunique_lock<ceph::shared_mutex>& sul);
  void put_session(OSDSession *s);
  void get_session(OSDSession *s);
  void _reopen_session(OSDSession *session);
  void close_session(OSDSession *session);

  void _nlist_reply(NListContext *list_context, int r, Context *final_finish,
		   epoch_t reply_epoch);

  void resend_mon_ops();

  /**
   * handle a budget for in-flight ops
   * budget is taken whenever an op goes into the ops std::map
   * and returned whenever an op is removed from the std::map
   * If throttle_op needs to throttle it will unlock client_lock.
   */
  int calc_op_budget(const boost::container::small_vector_base<OSDOp>& ops);
  void _throttle_op(Op *op, ceph::shunique_lock<ceph::shared_mutex>& sul,
		    int op_size = 0);
  int _take_op_budget(Op *op, ceph::shunique_lock<ceph::shared_mutex>& sul) {
    ceph_assert(sul && sul.mutex() == &rwlock);
    int op_budget = calc_op_budget(op->ops);
    if (keep_balanced_budget) {
      _throttle_op(op, sul, op_budget);
    } else { // update take_linger_budget to match this!
      op_throttle_bytes.take(op_budget);
      op_throttle_ops.take(1);
    }
    op->budget = op_budget;
    return op_budget;
  }
  int take_linger_budget(LingerOp *info);
  void put_op_budget_bytes(int op_budget) {
    ceph_assert(op_budget >= 0);
    op_throttle_bytes.put(op_budget);
    op_throttle_ops.put(1);
  }
  void put_nlist_context_budget(NListContext *list_context);
  Throttle op_throttle_bytes{cct, "objecter_bytes",
			     static_cast<int64_t>(
			       cct->_conf->objecter_inflight_op_bytes)};
  Throttle op_throttle_ops{cct, "objecter_ops",
			   static_cast<int64_t>(
			     cct->_conf->objecter_inflight_ops)};
 public:
  Objecter(CephContext *cct, Messenger *m, MonClient *mc,
	   boost::asio::io_context& service);
  ~Objecter() override;

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
  decltype(auto) with_osdmap(Callback&& cb, Args&&... args) const {
    std::shared_lock l(rwlock);
    return std::forward<Callback>(cb)(*osdmap, std::forward<Args>(args)...);
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

  void set_honor_pool_full() { honor_pool_full = true; }
  void unset_honor_pool_full() { honor_pool_full = false; }

  void _scan_requests(
    OSDSession *s,
    bool skipped_map,
    bool cluster_full,
    std::map<int64_t, bool> *pool_full_map,
    std::map<ceph_tid_t, Op*>& need_resend,
    std::list<LingerOp*>& need_resend_linger,
    std::map<ceph_tid_t, CommandOp*>& need_resend_command,
    ceph::shunique_lock<ceph::shared_mutex>& sul);

  int64_t get_object_hash_position(int64_t pool, const std::string& key,
				   const std::string& ns);
  int64_t get_object_pg_hash_position(int64_t pool, const std::string& key,
				      const std::string& ns);

  // messages
 public:
  bool ms_dispatch(Message *m) override;
  bool ms_can_fast_dispatch_any() const override {
    return true;
  }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OPREPLY:
    case CEPH_MSG_WATCH_NOTIFY:
      return true;
    default:
      return false;
    }
  }
  void ms_fast_dispatch(Message *m) override {
    if (!ms_dispatch(m)) {
      m->put();
    }
  }

  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_backoff(class MOSDBackoff *m);
  void handle_watch_notify(class MWatchNotify *m);
  void handle_osd_map(class MOSDMap *m);
  void wait_for_osd_map(epoch_t e=0);

  template<typename CompletionToken>
  auto wait_for_osd_map(CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), void()>(
      [this](auto handler) {
	std::unique_lock l(rwlock);
	if (osdmap->get_epoch()) {
	  l.unlock();
	  boost::asio::post(std::move(handler));
	} else {
	  auto e = boost::asio::get_associated_executor(
	    handler, service.get_executor());
	  waiting_for_map[0].emplace_back(
	    boost::asio::bind_executor(
	      e, [c = std::move(handler)]
	      (boost::system::error_code) mutable {
		boost::asio::dispatch(std::move(c));
	      }),
	    boost::system::error_code{});
	  l.unlock();
	}
      }, consigned);
  }


  /**
   * Get std::list of entities blocklisted since this was last called,
   * and reset the std::list.
   *
   * Uses a std::set because typical use case is to compare some
   * other std::list of clients to see which overlap with the blocklisted
   * addrs.
   *
   */
  void consume_blocklist_events(std::set<entity_addr_t> *events);

  int pool_snap_by_name(int64_t poolid,
			const char *snap_name,
			snapid_t *snap) const;
  int pool_snap_get_info(int64_t poolid, snapid_t snap,
			 pool_snap_info_t *info) const;
  int pool_snap_list(int64_t poolid, std::vector<uint64_t> *snaps);
private:

  void emit_blocklist_events(const OSDMap::Incremental &inc);
  void emit_blocklist_events(const OSDMap &old_osd_map,
                             const OSDMap &new_osd_map);

  // low-level
  void _op_submit(Op *op, ceph::shunique_lock<ceph::shared_mutex>& lc,
		  ceph_tid_t *ptid);
  void _op_submit_with_budget(Op *op,
			      ceph::shunique_lock<ceph::shared_mutex>& lc,
			      ceph_tid_t *ptid,
			      int *ctx_budget = NULL);
  // public interface
public:
  void op_submit(Op *op, ceph_tid_t *ptid = NULL, int *ctx_budget = NULL);
  bool is_active() {
    std::shared_lock l(rwlock);
    return !((!inflight_ops) && linger_ops.empty() &&
	     poolstat_ops.empty() && statfs_ops.empty());
  }

  /**
   * Output in-flight requests
   */
  void _dump_active(OSDSession *s);
  void _dump_active();
  void dump_active();
  void dump_requests(ceph::Formatter *fmt);
  void _dump_ops(const OSDSession *s, ceph::Formatter *fmt);
  void dump_ops(ceph::Formatter *fmt);
  void _dump_linger_ops(const OSDSession *s, ceph::Formatter *fmt);
  void dump_linger_ops(ceph::Formatter *fmt);
  void _dump_command_ops(const OSDSession *s, ceph::Formatter *fmt);
  void dump_command_ops(ceph::Formatter *fmt);
  void dump_pool_ops(ceph::Formatter *fmt) const;
  void dump_pool_stat_ops(ceph::Formatter *fmt) const;
  void dump_statfs_ops(ceph::Formatter *fmt) const;

  int get_client_incarnation() const { return client_inc; }
  void set_client_incarnation(int inc) { client_inc = inc; }

  bool have_map(epoch_t epoch);

  struct CB_Objecter_GetVersion {
    Objecter *objecter;
    OpCompletion fin;

    CB_Objecter_GetVersion(Objecter *o, OpCompletion c)
      : objecter(o), fin(std::move(c)) {}
    void operator()(boost::system::error_code ec, version_t newest,
		    version_t oldest) {
      if (ec == boost::system::errc::resource_unavailable_try_again) {
	// try again as instructed
	objecter->_wait_for_latest_osdmap(std::move(*this));
      } else if (ec) {
	boost::asio::post(objecter->service.get_executor(),
			  boost::asio::append(std::move(fin), ec));
      } else {
	auto l = std::unique_lock(objecter->rwlock);
	objecter->_get_latest_version(oldest, newest, std::move(fin),
				      std::move(l));
      }
    }
  };

  template<typename CompletionToken>
  auto wait_for_map(epoch_t epoch, CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), OpSignature>(
      [epoch, this](auto handler) {
	if (osdmap->get_epoch() >= epoch) {
	  boost::asio::post(boost::asio::append(
			      std::move(handler),
			      boost::system::error_code{}));
	} else {
	  monc->get_version(
	    "osdmap",
	    CB_Objecter_GetVersion(this, std::move(handler)));
	}
      }, consigned);
  }
  void _wait_for_new_map(OpCompletion, epoch_t epoch,
			 boost::system::error_code = {});

private:
  void _wait_for_latest_osdmap(CB_Objecter_GetVersion&& c) {
    monc->get_version("osdmap", std::move(c));
  }

public:

  template<typename CompletionToken>
  auto wait_for_latest_osdmap(CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    boost::asio::async_initiate<decltype(consigned), OpSignature>(
      [this](auto handler) {
	monc->get_version("osdmap",
			  CB_Objecter_GetVersion(
			    this,
			    std::move(handler)));
      }, consigned);
  }

  auto wait_for_latest_osdmap(std::unique_ptr<ceph::async::Completion<OpSignature>> c) {
    wait_for_latest_osdmap([c = std::move(c)](boost::system::error_code e) mutable {
      c->dispatch(std::move(c), e);
    });
  }

  template<typename CompletionToken>
  auto get_latest_version(epoch_t oldest, epoch_t newest,
			  CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), OpSignature>(
      [oldest, newest, this](auto handler) {
	std::unique_lock wl(rwlock);
	_get_latest_version(oldest, newest,
			    std::move(handler), std::move(wl));
      }, consigned);
  }

  void _get_latest_version(epoch_t oldest, epoch_t neweset,
			   OpCompletion fin,
			   std::unique_lock<ceph::shared_mutex>&& ul);

  /** Get the current set of global op flags */
  int get_global_op_flags() const { return global_op_flags; }
  /** Add a flag to the global op flags, not really atomic operation */
  void add_global_op_flags(int flag) {
    global_op_flags.fetch_or(flag);
  }
  /** Clear the passed flags from the global op flag set */
  void clear_global_op_flag(int flags) {
    global_op_flags.fetch_and(~flags);
  }

  /// cancel an in-progress request with the given return code
private:
  int op_cancel(OSDSession *s, ceph_tid_t tid, int r);
  int _op_cancel(ceph_tid_t tid, int r);
public:
  int op_cancel(ceph_tid_t tid, int r);
  int op_cancel(const std::vector<ceph_tid_t>& tidls, int r);

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
  void osd_command_(int osd, std::vector<std::string> cmd,
		   ceph::buffer::list inbl, ceph_tid_t *ptid,
		   decltype(CommandOp::onfinish)&& onfinish) {
    ceph_assert(osd >= 0);
    auto c = new CommandOp(
      osd,
      std::move(cmd),
      std::move(inbl),
      std::move(onfinish));
    submit_command(c, ptid);
  }
  template<typename CompletionToken>
  auto osd_command(int osd, std::vector<std::string> cmd,
		   ceph::buffer::list inbl, ceph_tid_t *ptid,
		   CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), CommandOp::OpSig>(
      [osd, cmd = std::move(cmd), inbl = std::move(inbl), ptid, this]
      (auto handler) {
	osd_command_(osd, std::move(cmd), std::move(inbl), ptid,
		     std::move(handler));
      }, consigned);
  }

  void pg_command_(pg_t pgid, std::vector<std::string> cmd,
		   ceph::buffer::list inbl, ceph_tid_t *ptid,
		   decltype(CommandOp::onfinish)&& onfinish) {
    auto *c = new CommandOp(
      pgid,
      std::move(cmd),
      std::move(inbl),
      std::move(onfinish));
    submit_command(c, ptid);
  }

  template<typename CompletionToken>
  auto pg_command(pg_t pgid, std::vector<std::string> cmd,
		  ceph::buffer::list inbl, ceph_tid_t *ptid,
		  CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(service.get_executor()));
    return async_initiate<decltype(consigned), CommandOp::OpSig> (
      [pgid, cmd = std::move(cmd), inbl = std::move(inbl), ptid, this]
      (auto handler) {
	pg_command_(pgid, std::move(cmd), std::move(inbl), ptid,
		    std::move(handler));
      }, consigned);
  }

  // mid-level helpers
  Op *prepare_mutate_op(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op, const SnapContext& snapc,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    osd_reqid_t reqid = osd_reqid_t(),
    ZTracer::Trace *parent_trace = nullptr,
    const jspan_context *otel_trace = nullptr) {
    Op *o = new Op(oid, oloc, std::move(op.ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver,
		   nullptr, nullptr, otel_trace);
    o->priority = op.priority;
    o->mtime = mtime;
    o->snapc = snapc;
    o->out_rval.swap(op.out_rval);
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_ec.swap(op.out_ec);
    o->reqid = reqid;
    op.clear();
    return o;
  }
  ceph_tid_t mutate(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op, const SnapContext& snapc,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    osd_reqid_t reqid = osd_reqid_t()) {
    Op *o = prepare_mutate_op(oid, oloc, op, snapc, mtime, flags,
			      oncommit, objver, reqid);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  void mutate(const object_t& oid, const object_locator_t& oloc,
	      ObjectOperation&& op, const SnapContext& snapc,
	      ceph::real_time mtime, int flags,
	      Op::OpComp oncommit,
	      version_t *objver = NULL, osd_reqid_t reqid = osd_reqid_t(),
	      ZTracer::Trace *parent_trace = nullptr) {
    Op *o = new Op(oid, oloc, std::move(op.ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, std::move(oncommit), objver,
		   nullptr, parent_trace);
    o->priority = op.priority;
    o->mtime = mtime;
    o->snapc = snapc;
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->out_ec.swap(op.out_ec);
    o->reqid = reqid;
    op.clear();
    op_submit(o);
  }

  void mutate(const object_t& oid, const object_locator_t& oloc,
	      ObjectOperation&& op, const SnapContext& snapc,
	      ceph::real_time mtime, int flags,
	      std::unique_ptr<ceph::async::Completion<Op::OpSig>> oncommit,
	      version_t *objver = NULL, osd_reqid_t reqid = osd_reqid_t(),
	      ZTracer::Trace *parent_trace = nullptr) {
    mutate(oid, oloc, std::move(op), snapc, mtime, flags,
	   [c = std::move(oncommit)](boost::system::error_code ec) mutable {
	     c->dispatch(std::move(c), ec);
	   }, objver, reqid, parent_trace);
  }

  Op *prepare_read_op(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op,
    snapid_t snapid, ceph::buffer::list *pbl, int flags,
    Context *onack, version_t *objver = NULL,
    int *data_offset = NULL,
    uint64_t features = 0,
    ZTracer::Trace *parent_trace = nullptr) {
    Op *o = new Op(oid, oloc, std::move(op.ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onack, objver,
		   data_offset, parent_trace);
    o->priority = op.priority;
    o->snapid = snapid;
    o->outbl = pbl;
    if (!o->outbl && op.size() == 1 && op.out_bl[0] && op.out_bl[0]->length())
	o->outbl = op.out_bl[0];
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->out_ec.swap(op.out_ec);
    op.clear();
    return o;
  }
  ceph_tid_t read(
    const object_t& oid, const object_locator_t& oloc,
    ObjectOperation& op,
    snapid_t snapid, ceph::buffer::list *pbl, int flags,
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

  void read(const object_t& oid, const object_locator_t& oloc,
	    ObjectOperation&& op, snapid_t snapid, ceph::buffer::list *pbl,
	    int flags, Op::OpComp onack,
	    version_t *objver = nullptr, int *data_offset = nullptr,
	    uint64_t features = 0, ZTracer::Trace *parent_trace = nullptr) {
    Op *o = new Op(oid, oloc, std::move(op.ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, std::move(onack), objver,
		   data_offset, parent_trace);
    o->priority = op.priority;
    o->snapid = snapid;
    o->outbl = pbl;
    // XXX
    if (!o->outbl && op.size() == 1 && op.out_bl[0] && op.out_bl[0]->length()) {
      o->outbl = op.out_bl[0];
    }
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->out_ec.swap(op.out_ec);
    if (features)
      o->features = features;
    op.clear();
    op_submit(o);
  }

  void read(const object_t& oid, const object_locator_t& oloc,
	    ObjectOperation&& op, snapid_t snapid, ceph::buffer::list *pbl,
	    int flags, std::unique_ptr<ceph::async::Completion<Op::OpSig>> onack,
	    version_t *objver = nullptr, int *data_offset = nullptr,
	    uint64_t features = 0, ZTracer::Trace *parent_trace = nullptr) {
    read(oid, oloc, std::move(op), snapid, pbl, flags,
	 [c = std::move(onack)](boost::system::error_code e) mutable {
	   c->dispatch(std::move(c), e);
	 }, objver, data_offset, features, parent_trace);
  }


  Op *prepare_pg_read_op(
    uint32_t hash, object_locator_t oloc,
    ObjectOperation& op, ceph::buffer::list *pbl, int flags,
    Context *onack, epoch_t *reply_epoch,
    int *ctx_budget) {
    Op *o = new Op(object_t(), oloc,
		   std::move(op.ops),
		   flags | global_op_flags | CEPH_OSD_FLAG_READ |
		   CEPH_OSD_FLAG_IGNORE_OVERLAY,
		   onack, NULL);
    o->target.precalc_pgid = true;
    o->target.base_pgid = pg_t(hash, oloc.pool);
    o->priority = op.priority;
    o->snapid = CEPH_NOSNAP;
    o->outbl = pbl;
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->out_ec.swap(op.out_ec);
    o->reply_epoch = reply_epoch;
    if (ctx_budget) {
      // budget is tracked by listing context
      o->ctx_budgeted = true;
    }
    op.clear();
    return o;
  }
  ceph_tid_t pg_read(
    uint32_t hash, object_locator_t oloc,
    ObjectOperation& op, ceph::buffer::list *pbl, int flags,
    Context *onack, epoch_t *reply_epoch,
    int *ctx_budget) {
    Op *o = prepare_pg_read_op(hash, oloc, op, pbl, flags,
			       onack, reply_epoch, ctx_budget);
    ceph_tid_t tid;
    op_submit(o, &tid, ctx_budget);
    return tid;
  }

  ceph_tid_t pg_read(
    uint32_t hash, object_locator_t oloc,
    ObjectOperation& op, ceph::buffer::list *pbl, int flags,
    Op::OpComp onack, epoch_t *reply_epoch, int *ctx_budget) {
    ceph_tid_t tid;
    Op *o = new Op(object_t(), oloc,
		   std::move(op.ops),
		   flags | global_op_flags | CEPH_OSD_FLAG_READ |
		   CEPH_OSD_FLAG_IGNORE_OVERLAY,
		   std::move(onack), nullptr);
    o->target.precalc_pgid = true;
    o->target.base_pgid = pg_t(hash, oloc.pool);
    o->priority = op.priority;
    o->snapid = CEPH_NOSNAP;
    o->outbl = pbl;
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    o->out_ec.swap(op.out_ec);
    o->reply_epoch = reply_epoch;
    if (ctx_budget) {
      // budget is tracked by listing context
      o->ctx_budgeted = true;
    }
    op_submit(o, &tid, ctx_budget);
    op.clear();
    return tid;
  }

  // caller owns a ref
  LingerOp *linger_register(const object_t& oid, const object_locator_t& oloc,
			    int flags);
  ceph_tid_t linger_watch(LingerOp *info,
			  ObjectOperation& op,
			  const SnapContext& snapc, ceph::real_time mtime,
			  ceph::buffer::list& inbl,
			  decltype(info->on_reg_commit)&& oncommit,
			  version_t *objver);
  ceph_tid_t linger_watch(LingerOp *info,
			  ObjectOperation& op,
			  const SnapContext& snapc, ceph::real_time mtime,
			  ceph::buffer::list& inbl,
			  Context* onfinish,
			  version_t *objver) {
    return linger_watch(info, op, snapc, mtime, inbl,
			OpContextVert<ceph::buffer::list>(onfinish, nullptr), objver);
  }
  ceph_tid_t linger_watch(LingerOp *info,
			  ObjectOperation& op,
			  const SnapContext& snapc, ceph::real_time mtime,
			  ceph::buffer::list& inbl,
			  std::unique_ptr<ceph::async::Completion<
			    void(boost::system::error_code,
			         ceph::buffer::list)>> onfinish,
			  version_t *objver) {
    return linger_watch(info, op, snapc, mtime, inbl,
			OpCompletionVert<ceph::buffer::list>(
			  std::move(onfinish)), objver);
  }
  ceph_tid_t linger_notify(LingerOp *info,
			   ObjectOperation& op,
			   snapid_t snap, ceph::buffer::list& inbl,
			   decltype(LingerOp::on_reg_commit)&& onfinish,
			   version_t *objver);
  ceph_tid_t linger_notify(LingerOp *info,
			   ObjectOperation& op,
			   snapid_t snap, ceph::buffer::list& inbl,
			   ceph::buffer::list *poutbl,
			   Context* onack,
			   version_t *objver) {
    return linger_notify(info, op, snap, inbl,
			 OpContextVert(onack, poutbl),
			 objver);
  }
  ceph_tid_t linger_notify(LingerOp *info,
			   ObjectOperation& op,
			   snapid_t snap, ceph::buffer::list& inbl,
			   std::unique_ptr<ceph::async::Completion<
			     void(boost::system::error_code,
			          ceph::buffer::list)>> onack,
			   version_t *objver) {
    return linger_notify(info, op, snap, inbl,
			 OpCompletionVert<ceph::buffer::list>(
			   std::move(onack)), objver);
  }
  tl::expected<ceph::timespan,
	       boost::system::error_code> linger_check(LingerOp *info);
  void linger_cancel(LingerOp *info);  // releases a reference
  void _linger_cancel(LingerOp *info);

  void _do_watch_notify(boost::intrusive_ptr<LingerOp> info,
                        boost::intrusive_ptr<MWatchNotify> m);

  /**
   * set up initial ops in the op std::vector, and allocate a final op slot.
   *
   * The caller is responsible for filling in the final ops_count ops.
   *
   * @param ops op std::vector
   * @param ops_count number of final ops the caller will fill in
   * @param extra_ops pointer to [array of] initial op[s]
   * @return index of final op (for caller to fill in)
   */
  int init_ops(boost::container::small_vector_base<OSDOp>& ops, int ops_count,
	       ObjectOperation *extra_ops) {
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
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_STAT;
    C_Stat *fin = new C_Stat(psize, pmtime, onfinish);
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, fin, objver);
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
    uint64_t off, uint64_t len, snapid_t snap, ceph::buffer::list *pbl,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0,
    ZTracer::Trace *parent_trace = nullptr) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onfinish, objver,
		   nullptr, parent_trace);
    o->snapid = snap;
    o->outbl = pbl;
    return o;
  }
  ceph_tid_t read(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, snapid_t snap, ceph::buffer::list *pbl,
    int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_read_op(oid, oloc, off, len, snap, pbl, flags,
			    onfinish, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  Op *prepare_cmpext_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, ceph::buffer::list &cmp_bl,
    snapid_t snap, int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_CMPEXT;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = cmp_bl.length();
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = cmp_bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onfinish, objver);
    o->snapid = snap;
    return o;
  }

  ceph_tid_t cmpext(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, ceph::buffer::list &cmp_bl,
    snapid_t snap, int flags, Context *onfinish, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_cmpext_op(oid, oloc, off, cmp_bl, snap,
			      flags, onfinish, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t read_trunc(const object_t& oid, const object_locator_t& oloc,
			uint64_t off, uint64_t len, snapid_t snap,
			ceph::buffer::list *pbl, int flags, uint64_t trunc_size,
			__u32 trunc_seq, Context *onfinish,
			version_t *objver = NULL,
			ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onfinish, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t mapext(const object_t& oid, const object_locator_t& oloc,
		    uint64_t off, uint64_t len, snapid_t snap, ceph::buffer::list *pbl,
		    int flags, Context *onfinish, version_t *objver = NULL,
		    ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_MAPEXT;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onfinish, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t getxattr(const object_t& oid, const object_locator_t& oloc,
	     const char *name, snapid_t snap, ceph::buffer::list *pbl, int flags,
	     Context *onfinish,
	     version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name, ops[i].op.xattr.name_len);
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, onfinish, objver);
    o->snapid = snap;
    o->outbl = pbl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t getxattrs(const object_t& oid, const object_locator_t& oloc,
		       snapid_t snap, std::map<std::string,ceph::buffer::list>& attrset,
		       int flags, Context *onfinish, version_t *objver = NULL,
		       ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTRS;
    C_GetAttrs *fin = new C_GetAttrs(attrset, onfinish);
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_READ, fin, objver);
    o->snapid = snap;
    o->outbl = &fin->bl;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t read_full(const object_t& oid, const object_locator_t& oloc,
		       snapid_t snap, ceph::buffer::list *pbl, int flags,
		       Context *onfinish, version_t *objver = NULL,
		       ObjectOperation *extra_ops = NULL) {
    return read(oid, oloc, 0, 0, snap, pbl, flags | global_op_flags |
		CEPH_OSD_FLAG_READ, onfinish, objver, extra_ops);
  }


  // writes
  ceph_tid_t _modify(const object_t& oid, const object_locator_t& oloc,
		     osdc_opvec& ops,
		     ceph::real_time mtime,
		     const SnapContext& snapc, int flags,
		     Context *oncommit,
		     version_t *objver = NULL) {
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_write_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, const SnapContext& snapc,
    const ceph::buffer::list &bl, ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0,
    ZTracer::Trace *parent_trace = nullptr) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, std::move(oncommit), objver,
                   nullptr, parent_trace);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t write(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t off, uint64_t len, const SnapContext& snapc,
    const ceph::buffer::list &bl, ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_write_op(oid, oloc, off, len, snapc, bl, mtime, flags,
			     oncommit, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_append_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t len, const SnapContext& snapc,
    const ceph::buffer::list &bl, ceph::real_time mtime, int flags,
    Context *oncommit,
    version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_APPEND;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t append(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t len, const SnapContext& snapc,
    const ceph::buffer::list &bl, ceph::real_time mtime, int flags,
    Context *oncommit,
    version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL) {
    Op *o = prepare_append_op(oid, oloc, len, snapc, bl, mtime, flags,
			      oncommit, objver, extra_ops);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t write_trunc(const object_t& oid, const object_locator_t& oloc,
			 uint64_t off, uint64_t len, const SnapContext& snapc,
			 const ceph::buffer::list &bl, ceph::real_time mtime, int flags,
			 uint64_t trunc_size, __u32 trunc_seq,
			 Context *oncommit,
			 version_t *objver = NULL,
			 ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_write_full_op(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, const ceph::buffer::list &bl,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = bl.length();
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t write_full(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, const ceph::buffer::list &bl,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {
    Op *o = prepare_write_full_op(oid, oloc, snapc, bl, mtime, flags,
				  oncommit, objver, extra_ops, op_flags);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_writesame_op(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t write_len, uint64_t off,
    const SnapContext& snapc, const ceph::buffer::list &bl,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {

    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITESAME;
    ops[i].op.writesame.offset = off;
    ops[i].op.writesame.length = write_len;
    ops[i].op.writesame.data_length = bl.length();
    ops[i].indata = bl;
    ops[i].op.flags = op_flags;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t writesame(
    const object_t& oid, const object_locator_t& oloc,
    uint64_t write_len, uint64_t off,
    const SnapContext& snapc, const ceph::buffer::list &bl,
    ceph::real_time mtime, int flags,
    Context *oncommit, version_t *objver = NULL,
    ObjectOperation *extra_ops = NULL, int op_flags = 0) {

    Op *o = prepare_writesame_op(oid, oloc, write_len, off, snapc, bl,
				 mtime, flags, oncommit, objver,
				 extra_ops, op_flags);

    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t trunc(const object_t& oid, const object_locator_t& oloc,
		   const SnapContext& snapc, ceph::real_time mtime, int flags,
		   uint64_t trunc_size, __u32 trunc_seq,
		   Context *oncommit, version_t *objver = NULL,
		   ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_TRUNCATE;
    ops[i].op.extent.offset = trunc_size;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t zero(const object_t& oid, const object_locator_t& oloc,
		  uint64_t off, uint64_t len, const SnapContext& snapc,
		  ceph::real_time mtime, int flags, Context *oncommit,
	     version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ZERO;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t rollback_object(const object_t& oid, const object_locator_t& oloc,
			     const SnapContext& snapc, snapid_t snapid,
			     ceph::real_time mtime, Context *oncommit,
			     version_t *objver = NULL,
			     ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ROLLBACK;
    ops[i].op.snap.snapid = snapid;
    Op *o = new Op(oid, oloc, std::move(ops), CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t create(const object_t& oid, const object_locator_t& oloc,
		    const SnapContext& snapc, ceph::real_time mtime, int global_flags,
		    int create_flags, Context *oncommit,
		    version_t *objver = NULL,
		    ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_CREATE;
    ops[i].op.flags = create_flags;
    Op *o = new Op(oid, oloc, std::move(ops), global_flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  Op *prepare_remove_op(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, ceph::real_time mtime, int flags,
    Context *oncommit,
    version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_DELETE;
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return o;
  }
  ceph_tid_t remove(
    const object_t& oid, const object_locator_t& oloc,
    const SnapContext& snapc, ceph::real_time mtime, int flags,
    Context *oncommit,
    version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    Op *o = prepare_remove_op(oid, oloc, snapc, mtime, flags,
			      oncommit, objver, extra_ops);
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  ceph_tid_t setxattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc, const ceph::buffer::list &bl,
	      ceph::real_time mtime, int flags,
	      Context *oncommit,
	      version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_SETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = bl.length();
    if (name)
      ops[i].indata.append(name, ops[i].op.xattr.name_len);
    ops[i].indata.append(bl);
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit,
		   objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }
  ceph_tid_t removexattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc,
	      ceph::real_time mtime, int flags,
	      Context *oncommit,
	      version_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    osdc_opvec ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_RMXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name, ops[i].op.xattr.name_len);
    Op *o = new Op(oid, oloc, std::move(ops), flags | global_op_flags |
		   CEPH_OSD_FLAG_WRITE, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    ceph_tid_t tid;
    op_submit(o, &tid);
    return tid;
  }

  void list_nobjects(NListContext *p, Context *onfinish);
  uint32_t list_nobjects_seek(NListContext *p, uint32_t pos);
  uint32_t list_nobjects_seek(NListContext *list_context, const hobject_t& c);
  void list_nobjects_get_cursor(NListContext *list_context, hobject_t *c);

  hobject_t enumerate_objects_begin();
  hobject_t enumerate_objects_end();

  template<typename T>
  friend struct EnumerationContext;
  template<typename T>
  friend struct CB_EnumerateReply;
  template<typename T>
  void enumerate_objects(
    int64_t pool_id,
    std::string_view ns,
    hobject_t start,
    hobject_t end,
    const uint32_t max,
    const ceph::buffer::list& filter_bl,
    fu2::unique_function<void(boost::system::error_code,
			      std::vector<T>,
			      hobject_t) &&> on_finish);
  template<typename T>
  void _issue_enumerate(hobject_t start,
			std::unique_ptr<EnumerationContext<T>>);
  template<typename T>
  void _enumerate_reply(
    ceph::buffer::list&& bl,
    boost::system::error_code ec,
    std::unique_ptr<EnumerationContext<T>>&& ectx);

  // -------------------------
  // pool ops
private:
  void pool_op_submit(PoolOp *op);
  void _pool_op_submit(PoolOp *op);
  void _finish_pool_op(PoolOp *op, int r);
  void _do_delete_pool(int64_t pool,
		       decltype(PoolOp::onfinish)&& onfinish);

public:
  void create_pool_snap(int64_t pool, std::string_view snapName,
			decltype(PoolOp::onfinish)&& onfinish);
  void create_pool_snap(int64_t pool, std::string_view snapName,
			Context* c) {
    create_pool_snap(pool, snapName,
		     OpContextVert<ceph::buffer::list>(c, nullptr));
  }
  void create_pool_snap(
    int64_t pool, std::string_view snapName,
    std::unique_ptr<ceph::async::Completion<PoolOp::OpSig>> c) {
    create_pool_snap(pool, snapName,
		     OpCompletionVert<ceph::buffer::list>(std::move(c)));
  }
  void allocate_selfmanaged_snap(int64_t pool,
				 boost::asio::any_completion_handler<
				 void(boost::system::error_code,
				      snapid_t)> onfinish);
  void allocate_selfmanaged_snap(int64_t pool, snapid_t* psnapid,
				 Context* c) {
    allocate_selfmanaged_snap(pool,
			      OpContextVert(c, psnapid));
  }
  void allocate_selfmanaged_snap(int64_t pool,
				 std::unique_ptr<ceph::async::Completion<void(
				   boost::system::error_code, snapid_t)>> c) {
    allocate_selfmanaged_snap(pool,
			      OpCompletionVert<snapid_t>(std::move(c)));
  }
  void delete_pool_snap(int64_t pool, std::string_view snapName,
			decltype(PoolOp::onfinish)&& onfinish);
  void delete_pool_snap(int64_t pool, std::string_view snapName,
			Context* c) {
    delete_pool_snap(pool, snapName,
		     OpContextVert<ceph::buffer::list>(c, nullptr));
  }
  void delete_pool_snap(int64_t pool, std::string_view snapName,
			std::unique_ptr<ceph::async::Completion<void(
                          boost::system::error_code, ceph::buffer::list)>> c) {
    delete_pool_snap(pool, snapName,
		     OpCompletionVert<ceph::buffer::list>(std::move(c)));
  }

  void delete_selfmanaged_snap(int64_t pool, snapid_t snap,
			       decltype(PoolOp::onfinish)&& onfinish);
  void delete_selfmanaged_snap(int64_t pool, snapid_t snap,
			       Context* c) {
    delete_selfmanaged_snap(pool, snap,
			    OpContextVert<ceph::buffer::list>(c, nullptr));
  }
  void delete_selfmanaged_snap(int64_t pool, snapid_t snap,
			       std::unique_ptr<ceph::async::Completion<void(
                                 boost::system::error_code, ceph::buffer::list)>> c) {
    delete_selfmanaged_snap(pool, snap,
			    OpCompletionVert<ceph::buffer::list>(std::move(c)));
  }


  void create_pool(std::string_view name,
		   decltype(PoolOp::onfinish)&& onfinish,
		   int crush_rule=-1);
  void create_pool(std::string_view name, Context *onfinish,
		  int crush_rule=-1) {
    create_pool(name,
		OpContextVert<ceph::buffer::list>(onfinish, nullptr),
		crush_rule);
  }
  void create_pool(std::string_view name,
		   std::unique_ptr<ceph::async::Completion<void(
                     boost::system::error_code, ceph::buffer::list)>> c,
		   int crush_rule=-1) {
    create_pool(name,
		OpCompletionVert<ceph::buffer::list>(std::move(c)),
		crush_rule);
  }
  void delete_pool(int64_t pool,
		   decltype(PoolOp::onfinish)&& onfinish);
  void delete_pool(int64_t pool,
		   Context* onfinish) {
    delete_pool(pool, OpContextVert<ceph::buffer::list>(onfinish, nullptr));
  }
  void delete_pool(int64_t pool,
		   std::unique_ptr<ceph::async::Completion<void(
                    boost::system::error_code, ceph::buffer::list)>> c) {
    delete_pool(pool, OpCompletionVert<ceph::buffer::list>(std::move(c)));
  }

  void delete_pool(std::string_view name,
		   decltype(PoolOp::onfinish)&& onfinish);

  void delete_pool(std::string_view name,
		   Context* onfinish) {
    delete_pool(name, OpContextVert<ceph::buffer::list>(onfinish, nullptr));
  }
  void delete_pool(std::string_view name,
		   std::unique_ptr<ceph::async::Completion<void(
                     boost::system::error_code, ceph::buffer::list)>> c) {
    delete_pool(name, OpCompletionVert<ceph::buffer::list>(std::move(c)));
  }

  void handle_pool_op_reply(MPoolOpReply *m);
  int pool_op_cancel(ceph_tid_t tid, int r);

  // --------------------------
  // pool stats
private:
  void _poolstat_submit(PoolStatOp *op);
public:
  void handle_get_pool_stats_reply(MGetPoolStatsReply *m);
  void get_pool_stats_(const std::vector<std::string>& pools,
		       decltype(PoolStatOp::onfinish)&& onfinish);
  template<typename CompletionToken>
  auto get_pool_stats(std::vector<std::string> pools,
		      CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), PoolStatOp::OpSig>(
      [pools = std::move(pools), this](auto handler) {
	get_pool_stats_(pools, std::move(handler));
      }, consigned);
  }
  int pool_stat_op_cancel(ceph_tid_t tid, int r);
  void _finish_pool_stat_op(PoolStatOp *op, int r);

  // ---------------------------
  // df stats
private:
  void _fs_stats_submit(StatfsOp *op);
public:
  void handle_fs_stats_reply(MStatfsReply *m);
  void get_fs_stats_(std::optional<int64_t> poolid,
		     decltype(StatfsOp::onfinish)&& onfinish);
  template<typename CompletionToken>
  auto get_fs_stats(std::optional<int64_t> poolid,
		    CompletionToken&& token) {
    auto consigned = boost::asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(
	service.get_executor()));
    return boost::asio::async_initiate<decltype(consigned), StatfsOp::OpSig>(
      [poolid, this](auto handler) {
	get_fs_stats_(poolid, std::move(handler));
      }, consigned);
  }
  void get_fs_stats(struct ceph_statfs& result, std::optional<int64_t> poolid,
		    Context *onfinish) {
    get_fs_stats_(poolid, OpContextVert(onfinish, result));
  }
  void get_fs_stats(std::optional<int64_t> poolid,
		    std::unique_ptr<ceph::async::Completion<void(
                      boost::system::error_code, struct ceph_statfs)>> c) {
    get_fs_stats_(poolid, OpCompletionVert<struct ceph_statfs>(std::move(c)));
  }
  int statfs_op_cancel(ceph_tid_t tid, int r);
  void _finish_statfs_op(StatfsOp *op, int r);

  // ---------------------------
  // some scatter/gather hackery

  void _sg_read_finish(std::vector<ObjectExtent>& extents,
		       std::vector<ceph::buffer::list>& resultbl,
		       ceph::buffer::list *bl, Context *onfinish);

  struct C_SGRead : public Context {
    Objecter *objecter;
    std::vector<ObjectExtent> extents;
    std::vector<ceph::buffer::list> resultbl;
    ceph::buffer::list *bl;
    Context *onfinish;
    C_SGRead(Objecter *ob,
	     std::vector<ObjectExtent>& e, std::vector<ceph::buffer::list>& r, ceph::buffer::list *b,
	     Context *c) :
      objecter(ob), bl(b), onfinish(c) {
      extents.swap(e);
      resultbl.swap(r);
    }
    void finish(int r) override {
      objecter->_sg_read_finish(extents, resultbl, bl, onfinish);
    }
  };

  void sg_read_trunc(std::vector<ObjectExtent>& extents, snapid_t snap,
		     ceph::buffer::list *bl, int flags, uint64_t trunc_size,
		     __u32 trunc_seq, Context *onfinish, int op_flags = 0) {
    if (extents.size() == 1) {
      read_trunc(extents[0].oid, extents[0].oloc, extents[0].offset,
		 extents[0].length, snap, bl, flags, extents[0].truncate_size,
		 trunc_seq, onfinish, 0, 0, op_flags);
    } else {
      C_GatherBuilder gather(cct);
      std::vector<ceph::buffer::list> resultbl(extents.size());
      int i=0;
      for (auto p = extents.begin(); p != extents.end(); ++p) {
	read_trunc(p->oid, p->oloc, p->offset, p->length, snap, &resultbl[i++],
		   flags, p->truncate_size, trunc_seq, gather.new_sub(),
		   0, 0, op_flags);
      }
      gather.set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
      gather.activate();
    }
  }

  void sg_read(std::vector<ObjectExtent>& extents, snapid_t snap, ceph::buffer::list *bl,
	       int flags, Context *onfinish, int op_flags = 0) {
    sg_read_trunc(extents, snap, bl, flags, 0, 0, onfinish, op_flags);
  }

  void sg_write_trunc(std::vector<ObjectExtent>& extents, const SnapContext& snapc,
		      const ceph::buffer::list& bl, ceph::real_time mtime, int flags,
		      uint64_t trunc_size, __u32 trunc_seq,
		      Context *oncommit, int op_flags = 0) {
    if (extents.size() == 1) {
      write_trunc(extents[0].oid, extents[0].oloc, extents[0].offset,
		  extents[0].length, snapc, bl, mtime, flags,
		  extents[0].truncate_size, trunc_seq, oncommit,
		  0, 0, op_flags);
    } else {
      C_GatherBuilder gcom(cct, oncommit);
      auto it = bl.cbegin();
      for (auto p = extents.begin(); p != extents.end(); ++p) {
	ceph::buffer::list cur;
	for (auto bit = p->buffer_extents.begin();
	     bit != p->buffer_extents.end();
	     ++bit) {
	  if (it.get_off() != bit->first) {
	    it.seek(bit->first);
	  }
	  it.copy(bit->second, cur);
	}
	ceph_assert(cur.length() == p->length);
	write_trunc(p->oid, p->oloc, p->offset, p->length,
	      snapc, cur, mtime, flags, p->truncate_size, trunc_seq,
	      oncommit ? gcom.new_sub():0,
	      0, 0, op_flags);
      }
      gcom.activate();
    }
  }

  void sg_write(std::vector<ObjectExtent>& extents, const SnapContext& snapc,
		const ceph::buffer::list& bl, ceph::real_time mtime, int flags,
		Context *oncommit, int op_flags = 0) {
    sg_write_trunc(extents, snapc, bl, mtime, flags, 0, 0, oncommit,
		   op_flags);
  }

  void ms_handle_connect(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;

  void blocklist_self(bool set);

private:
  epoch_t epoch_barrier = 0;
  bool retry_writes_after_first_reply =
    cct->_conf->objecter_retry_writes_after_first_reply;

public:
  void set_epoch_barrier(epoch_t epoch);

  PerfCounters *get_logger() {
    return logger;
  }
};

#endif
