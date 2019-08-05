// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ops_executer.h"

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/thread.hh>

#include "crimson/osd/exceptions.h"
#include "osd/ClassHandler.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

seastar::future<> OpsExecuter::do_op_call(OSDOp& osd_op)
{
  std::string cname, mname;
  ceph::bufferlist indata;
  try {
    auto bp = std::begin(osd_op.indata);
    bp.copy(osd_op.op.cls.class_len, cname);
    bp.copy(osd_op.op.cls.method_len, mname);
    bp.copy(osd_op.op.cls.indata_len, indata);
  } catch (buffer::error&) {
    logger().warn("call unable to decode class + method + indata");
    throw ceph::osd::invalid_argument{};
  }

  // NOTE: opening a class can actually result in dlopen(), and thus
  // blocking the entire reactor. Thankfully to ClassHandler's cache
  // this is supposed to be extremely infrequent.
  ClassHandler::ClassData* cls;
  int r = ClassHandler::get_instance().open_class(cname, &cls);
  if (r) {
    logger().warn("class {} open got {}", cname, cpp_strerror(r));
    if (r == -ENOENT) {
      throw ceph::osd::operation_not_supported{};
    } else if (r == -EPERM) {
      // propagate permission errors
      throw ceph::osd::permission_denied{};
    }
    throw ceph::osd::input_output_error{};
  }

  ClassHandler::ClassMethod* method = cls->get_method(mname);
  if (!method) {
    logger().warn("call method {}.{} does not exist", cname, mname);
    throw ceph::osd::operation_not_supported{};
  }

  const auto flags = method->get_flags();
  if (!os->exists && (flags & CLS_METHOD_WR) == 0) {
    throw ceph::osd::object_not_found{};
  }

#if 0
  if (flags & CLS_METHOD_WR) {
    ctx->user_modify = true;
  }
#endif

  logger().debug("calling method {}.{}", cname, mname);
#if 0
  int prev_rd = ctx->num_read;
  int prev_wr = ctx->num_write;
#endif


  return seastar::async([this, &osd_op, method, indata=std::move(indata)]() mutable {
    ceph::bufferlist outdata;
    const auto ret = method->exec(reinterpret_cast<cls_method_context_t>(this),
                                  indata, outdata);
    if (ret < 0) {
      throw ceph::osd::make_error(ret);
    }
#if 0
	if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	  derr << "method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	  result = -EIO;
	  break;
	}
	if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	  derr << "method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	  result = -EIO;
	  break;
	}
#endif

	logger().debug("method called response length={}", outdata.length());
	osd_op.op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
  });

}
seastar::future<ceph::bufferlist>
OpsExecuter::do_pgnls(ceph::bufferlist& indata,
                      const std::string& nspace,
                      uint64_t limit)
{
  hobject_t lower_bound;
  try {
    ceph::decode(lower_bound, indata);
  } catch (const buffer::error& e) {
    throw std::invalid_argument("unable to decode PGNLS handle");
  }
  const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
  const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pool().info.get_pg_num());
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }
  return backend.list_objects(lower_bound, limit).then(
    [lower_bound, pg_end, nspace](auto objects, auto next) {
      auto in_my_namespace = [&nspace](const hobject_t& o) {
        using ceph::common::local_conf;
        if (o.get_namespace() == local_conf()->osd_hit_set_namespace) {
          return false;
        } else if (nspace == librados::all_nspaces) {
          return true;
        } else {
          return o.get_namespace() == nspace;
        }
      };
      pg_nls_response_t response;
      boost::copy(objects |
        boost::adaptors::filtered(in_my_namespace) |
        boost::adaptors::transformed([](const hobject_t& o) {
          return librados::ListObjectImpl{o.get_namespace(),
                                          o.oid.name,
                                          o.get_key()}; }),
        std::back_inserter(response.entries));
      response.handle = next.is_max() ? pg_end : next;
      bufferlist bl;
      encode(response, bl);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
  });
}

// TODO: split the method accordingly to os' constness needs
seastar::future<>
OpsExecuter::do_osd_op(OSDOp& osd_op)
{
  // TODO: dispatch via call table?
  // TODO: we might want to find a way to unify both input and output
  // of each op.
  logger().debug("handling op {}", ceph_osd_op_name(osd_op.op.op));
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return backend.read(os->oi,
                        op.extent.offset,
                        op.extent.length,
                        op.extent.truncate_size,
                        op.extent.truncate_seq,
                        op.flags).then([&osd_op](bufferlist bl) {
      osd_op.rval = bl.length();
      osd_op.outdata = std::move(bl);
      return seastar::now();
    });
  case CEPH_OSD_OP_GETXATTR:
    return backend.getxattr(os, osd_op);
  case CEPH_OSD_OP_WRITE:
    return backend.write(*os, osd_op, txn);
  case CEPH_OSD_OP_WRITEFULL:
    // XXX: os = backend.write(std::move(os), ...) instead?
    return backend.writefull(*os, osd_op, txn);
  case CEPH_OSD_OP_SETALLOCHINT:
    return seastar::now();
  case CEPH_OSD_OP_SETXATTR:
    return backend.setxattr(*os, osd_op, txn);
  case CEPH_OSD_OP_PGNLS:
    return do_pgnls(osd_op.indata, os->oi.soid.get_namespace(), op.pgls.count)
      .then([&osd_op](bufferlist bl) {
        osd_op.outdata = std::move(bl);
	return seastar::now();
    });
  case CEPH_OSD_OP_DELETE:
    return backend.remove(*os, txn);
  case CEPH_OSD_OP_CALL:
    return this->do_op_call(osd_op);
  case CEPH_OSD_OP_STAT:
    return backend.stat(*os, osd_op);
  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

} // namespace ceph::osd
