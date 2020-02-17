// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ops_executer.h"

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/thread.hh>

#include "crimson/osd/exceptions.h"
#include "osd/ClassHandler.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

OpsExecuter::call_errorator::future<> OpsExecuter::do_op_call(OSDOp& osd_op)
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
    return crimson::ct_error::invarg::make();
  }

  // NOTE: opening a class can actually result in dlopen(), and thus
  // blocking the entire reactor. Thankfully to ClassHandler's cache
  // this is supposed to be extremely infrequent.
  ClassHandler::ClassData* cls;
  int r = ClassHandler::get_instance().open_class(cname, &cls);
  if (r) {
    logger().warn("class {} open got {}", cname, cpp_strerror(r));
    if (r == -ENOENT) {
      return crimson::ct_error::operation_not_supported::make();
    } else if (r == -EPERM) {
      // propagate permission errors
      return crimson::ct_error::permission_denied::make();
    }
    return crimson::ct_error::input_output_error::make();
  }

  ClassHandler::ClassMethod* method = cls->get_method(mname);
  if (!method) {
    logger().warn("call method {}.{} does not exist", cname, mname);
    return crimson::ct_error::operation_not_supported::make();
  }

  const auto flags = method->get_flags();
  if (!obc->obs.exists && (flags & CLS_METHOD_WR) == 0) {
    return crimson::ct_error::enoent::make();
  }

#if 0
  if (flags & CLS_METHOD_WR) {
    ctx->user_modify = true;
  }
#endif

  logger().debug("calling method {}.{}", cname, mname);
  return seastar::async(
    [this, method, indata=std::move(indata)]() mutable {
      ceph::bufferlist outdata;
      auto cls_context = reinterpret_cast<cls_method_context_t>(this);
      const auto ret = method->exec(cls_context, indata, outdata);
      return std::make_pair(ret, std::move(outdata));
    }
  ).then(
    [prev_rd = num_read, prev_wr = num_write, this, &osd_op, flags]
    (auto outcome) -> call_errorator::future<> {
      auto& [ret, outdata] = outcome;

      if (num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
        logger().error("method tried to read object but is not marked RD");
        return crimson::ct_error::input_output_error::make();
      }
      if (num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
        logger().error("method tried to update object but is not marked WR");
        return crimson::ct_error::input_output_error::make();
      }

      // for write calls we never return data expect errors. For details refer
      // to cls/cls_hello.cc.
      if (ret < 0 || (flags & CLS_METHOD_WR) == 0) {
        logger().debug("method called response length={}", outdata.length());
        osd_op.op.extent.length = outdata.length();
        osd_op.outdata.claim_append(outdata);
      }
      if (ret < 0) {
        return crimson::stateful_ec{ std::error_code(-ret, std::generic_category()) };
      }
      return seastar::now();
    }
  );
}

static inline std::unique_ptr<const PGLSFilter> get_pgls_filter(
  const std::string& type,
  bufferlist::const_iterator& iter)
{
  // storing non-const PGLSFilter for the sake of ::init()
  std::unique_ptr<PGLSFilter> filter;
  if (type.compare("plain") == 0) {
    filter = std::make_unique<PGLSPlainFilter>();
  } else {
    std::size_t dot = type.find(".");
    if (dot == type.npos || dot == 0 || dot == type.size() - 1) {
      throw crimson::osd::invalid_argument{};
    }

    const std::string class_name = type.substr(0, dot);
    const std::string filter_name = type.substr(dot + 1);
    ClassHandler::ClassData *cls = nullptr;
    int r = ClassHandler::get_instance().open_class(class_name, &cls);
    if (r != 0) {
      logger().warn("can't open class {}: {}", class_name, cpp_strerror(r));
      if (r == -EPERM) {
        // propogate permission error
        throw crimson::osd::permission_denied{};
      } else {
        throw crimson::osd::invalid_argument{};
      }
    } else {
      ceph_assert(cls);
    }

    ClassHandler::ClassFilter * const class_filter = cls->get_filter(filter_name);
    if (class_filter == nullptr) {
      logger().warn("can't find filter {} in class {}", filter_name, class_name);
      throw crimson::osd::invalid_argument{};
    }

    filter.reset(class_filter->fn());
    if (!filter) {
      // Object classes are obliged to return us something, but let's
      // give an error rather than asserting out.
      logger().warn("buggy class {} failed to construct filter {}",
                    class_name, filter_name);
      throw crimson::osd::invalid_argument{};
    }
  }

  ceph_assert(filter);
  int r = filter->init(iter);
  if (r < 0) {
    logger().warn("error initializing filter {}: {}", type, cpp_strerror(r));
    throw crimson::osd::invalid_argument{};
  }

  // successfully constructed and initialized, return it.
  return filter;
}

static seastar::future<hobject_t> pgls_filter(
  const PGLSFilter& filter,
  const PGBackend& backend,
  const hobject_t& sobj)
{
  if (const auto xattr = filter.get_xattr(); !xattr.empty()) {
    logger().debug("pgls_filter: filter is interested in xattr={} for obj={}",
                   xattr, sobj);
    return backend.getxattr(sobj, xattr).safe_then(
      [&filter, sobj] (ceph::bufferptr bp) {
        logger().debug("pgls_filter: got xvalue for obj={}", sobj);

        ceph::bufferlist val;
        val.push_back(std::move(bp));
        const bool filtered = filter.filter(sobj, val);
        return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
      }, PGBackend::get_attr_errorator::all_same_way([&filter, sobj] {
        logger().debug("pgls_filter: got error for obj={}", sobj);

        if (filter.reject_empty_xattr()) {
          return seastar::make_ready_future<hobject_t>(hobject_t{});
        }
        ceph::bufferlist val;
        const bool filtered = filter.filter(sobj, val);
        return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
      }));
  } else {
    ceph::bufferlist empty_lvalue_bl;
    const bool filtered = filter.filter(sobj, empty_lvalue_bl);
    return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
  }
}

static seastar::future<ceph::bufferlist> do_pgnls_common(
  const hobject_t& pg_start,
  const hobject_t& pg_end,
  const PGBackend& backend,
  const hobject_t& lower_bound,
  const std::string& nspace,
  const uint64_t limit,
  const PGLSFilter* const filter)
{
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }

  return backend.list_objects(lower_bound, limit).then(
    [&backend, filter, nspace](auto objects, auto next) {
      auto in_my_namespace = [&nspace](const hobject_t& obj) {
        using crimson::common::local_conf;
        if (obj.get_namespace() == local_conf()->osd_hit_set_namespace) {
          return false;
        } else if (nspace == librados::all_nspaces) {
          return true;
        } else {
          return obj.get_namespace() == nspace;
        }
      };
      auto to_pglsed = [&backend, filter] (const hobject_t& obj) {
        // this transformation looks costly. However, I don't have any
        // reason to think PGLS* operations are critical for, let's say,
        // general performance.
        //
        // from tchaikov: "another way is to use seastar::map_reduce(),
        // to 1) save the effort to filter the already filtered objects
        // 2) avoid the space to keep the tuple<bool, object> even if
        // the object is filtered out".
        if (filter) {
          return pgls_filter(*filter, backend, obj);
        } else {
          return seastar::make_ready_future<hobject_t>(obj);
        }
      };

      auto range = objects | boost::adaptors::filtered(in_my_namespace)
                           | boost::adaptors::transformed(to_pglsed);
      logger().debug("do_pgnls_common: finishing the 1st stage of pgls");
      return seastar::when_all_succeed(std::begin(range),
                                       std::end(range)).then(
        [next=std::move(next)] (auto items) mutable {
          // the sole purpose of this chaining is to pass `next` to 2nd
          // stage altogether with items
          logger().debug("do_pgnls_common: 1st done");
          return seastar::make_ready_future<decltype(items), decltype(next)>(
            std::move(items), std::move(next));
      });
  }).then(
    [pg_end, filter] (const std::vector<hobject_t>& items, auto next) {
      auto is_matched = [] (const auto& obj) {
        return !obj.is_min();
      };
      auto to_entry = [] (const auto& obj) {
        return librados::ListObjectImpl{
          obj.get_namespace(), obj.oid.name, obj.get_key()
        };
      };

      pg_nls_response_t response;
      boost::push_back(response.entries, items | boost::adaptors::filtered(is_matched)
                                               | boost::adaptors::transformed(to_entry));
      response.handle = next.is_max() ? pg_end : next;
      ceph::bufferlist out;
      encode(response, out);
      logger().debug("{}: response.entries.size()=",
                     __func__, response.entries.size());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(out));
  });
}

static seastar::future<> do_pgnls(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  hobject_t lower_bound;
  try {
    ceph::decode(lower_bound, osd_op.indata);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGNLS handle");
  }
  const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
  const auto pg_end = \
    pg.get_pgid().pgid.get_hobj_end(pg.get_pool().info.get_pg_num());
  return do_pgnls_common(pg_start,
                         pg_end,
                         pg.get_backend(),
                         lower_bound,
                         nspace,
                         osd_op.op.pgls.count,
                         nullptr /* no filter */)
    .then([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
  });
}

static seastar::future<> do_pgnls_filtered(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  std::string cname, mname, type;
  auto bp = osd_op.indata.cbegin();
  try {
    ceph::decode(cname, bp);
    ceph::decode(mname, bp);
    ceph::decode(type, bp);
  } catch (const buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  auto filter = get_pgls_filter(type, bp);

  hobject_t lower_bound;
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGNLS_FILTER description");
  }

  logger().debug("{}: cname={}, mname={}, type={}, lower_bound={}, filter={}",
                 __func__, cname, mname, type, lower_bound,
                 static_cast<const void*>(filter.get()));
  return seastar::do_with(std::move(filter),
    [&, lower_bound=std::move(lower_bound)](auto&& filter) {
      const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pool().info.get_pg_num());
      return do_pgnls_common(pg_start,
                             pg_end,
                             pg.get_backend(),
                             lower_bound,
                             nspace,
                             osd_op.op.pgls.count,
                             filter.get())
        .then([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

OpsExecuter::osd_op_errorator::future<>
OpsExecuter::execute_osd_op(OSDOp& osd_op)
{
  // TODO: dispatch via call table?
  // TODO: we might want to find a way to unify both input and output
  // of each op.
  logger().debug(
    "handling op {} on object {}",
    ceph_osd_op_name(osd_op.op.op),
    get_target());
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.read(os.oi,
                          osd_op.op.extent.offset,
                          osd_op.op.extent.length,
                          osd_op.op.extent.truncate_size,
                          osd_op.op.extent.truncate_seq,
                          osd_op.op.flags).safe_then(
        [&osd_op](ceph::bufferlist&& bl) {
          osd_op.rval = bl.length();
          osd_op.outdata = std::move(bl);
          return osd_op_errorator::now();
        });
    });
  case CEPH_OSD_OP_GETXATTR:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.getxattr(os, osd_op);
    });
  case CEPH_OSD_OP_CREATE:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.create(os, osd_op, txn);
    });
  case CEPH_OSD_OP_WRITE:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.write(os, osd_op, txn);
    });
  case CEPH_OSD_OP_WRITEFULL:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.writefull(os, osd_op, txn);
    });
  case CEPH_OSD_OP_SETALLOCHINT:
    return osd_op_errorator::now();
  case CEPH_OSD_OP_SETXATTR:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.setxattr(os, osd_op, txn);
    });
  case CEPH_OSD_OP_DELETE:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.remove(os, txn);
    });
  case CEPH_OSD_OP_CALL:
    return this->do_op_call(osd_op);
  case CEPH_OSD_OP_STAT:
    // note: stat does not require RD
    return do_const_op([&osd_op] (/* const */auto& backend, const auto& os) {
      return backend.stat(os, osd_op);
    });
  case CEPH_OSD_OP_TMAPUP:
    // TODO: there was an effort to kill TMAP in ceph-osd. According to
    // @dzafman this isn't possible yet. Maybe it could be accomplished
    // before crimson's readiness and we'd luckily don't need to carry.
    return dont_do_legacy_op();

  // OMAP
  case CEPH_OSD_OP_OMAPGETKEYS:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.omap_get_keys(os, osd_op);
    });
  case CEPH_OSD_OP_OMAPGETVALS:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.omap_get_vals(os, osd_op);
    });
  case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.omap_get_vals_by_keys(os, osd_op);
    });
  case CEPH_OSD_OP_OMAPSETVALS:
#if 0
    if (!pg.get_pool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
#endif
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.omap_set_vals(os, osd_op, txn);
    });

  // watch/notify
  case CEPH_OSD_OP_WATCH:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      logger().warn("CEPH_OSD_OP_WATCH is not implemented yet; ignoring");
      return osd_op_errorator::now();
    });

  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

static seastar::future<ceph::bufferlist> do_pgls_common(
  const hobject_t& pg_start,
  const hobject_t& pg_end,
  const PGBackend& backend,
  const hobject_t& lower_bound,
  const std::string& nspace,
  const uint64_t limit,
  const PGLSFilter* const filter)
{
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }

  using entries_t = decltype(pg_ls_response_t::entries);
  return backend.list_objects(lower_bound, limit).then(
    [&backend, filter, nspace](auto objects, auto next) {
      return seastar::when_all_succeed(
        seastar::map_reduce(std::move(objects),
          [&backend, filter, nspace](const hobject_t& obj) {
            if (obj.get_namespace() == nspace) {
              if (filter) {
                return pgls_filter(*filter, backend, obj);
              } else {
                return seastar::make_ready_future<hobject_t>(obj);
              }
            } else {
              return seastar::make_ready_future<hobject_t>(hobject_t{});
            }
          },
          entries_t{},
          [](entries_t&& entries, hobject_t obj) {
            if (!obj.is_min()) {
              entries.emplace_back(obj.oid, obj.get_key());
            }
            return entries;
          }),
        seastar::make_ready_future<hobject_t>(next));
    }).then([pg_end](entries_t entries, hobject_t next) {
      pg_ls_response_t response;
      response.handle = next.is_max() ? pg_end : next;
      response.entries = std::move(entries);
      ceph::bufferlist out;
      encode(response, out);
      logger().debug("{}: response.entries.size()=",
                     __func__, response.entries.size());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(out));
  });
}

static seastar::future<> do_pgls(
   const PG& pg,
   const std::string& nspace,
   OSDOp& osd_op)
{
  hobject_t lower_bound;
  auto bp = osd_op.indata.cbegin();
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument{"unable to decode PGLS handle"};
  }
  const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
  const auto pg_end =
    pg.get_pgid().pgid.get_hobj_end(pg.get_pool().info.get_pg_num());
  return do_pgls_common(pg_start,
			pg_end,
			pg.get_backend(),
			lower_bound,
			nspace,
			osd_op.op.pgls.count,
			nullptr /* no filter */)
    .then([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
    });
}

static seastar::future<> do_pgls_filtered(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  std::string cname, mname, type;
  auto bp = osd_op.indata.cbegin();
  try {
    ceph::decode(cname, bp);
    ceph::decode(mname, bp);
    ceph::decode(type, bp);
  } catch (const buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  auto filter = get_pgls_filter(type, bp);

  hobject_t lower_bound;
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGLS_FILTER description");
  }

  logger().debug("{}: cname={}, mname={}, type={}, lower_bound={}, filter={}",
                 __func__, cname, mname, type, lower_bound,
                 static_cast<const void*>(filter.get()));
  return seastar::do_with(std::move(filter),
    [&, lower_bound=std::move(lower_bound)](auto&& filter) {
      const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pool().info.get_pg_num());
      return do_pgls_common(pg_start,
                            pg_end,
                            pg.get_backend(),
                            lower_bound,
                            nspace,
                            osd_op.op.pgls.count,
                            filter.get())
        .then([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

seastar::future<>
OpsExecuter::execute_pg_op(OSDOp& osd_op)
{
  logger().warn("handling op {}", ceph_osd_op_name(osd_op.op.op));
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_PGLS:
    return do_pg_op([&osd_op] (const auto& pg, const auto& nspace) {
      return do_pgls(pg, nspace, osd_op);
    });
  case CEPH_OSD_OP_PGLS_FILTER:
    return do_pg_op([&osd_op] (const auto& pg, const auto& nspace) {
      return do_pgls_filtered(pg, nspace, osd_op);
    });
  case CEPH_OSD_OP_PGNLS:
    return do_pg_op([&osd_op] (const auto& pg, const auto& nspace) {
      return do_pgnls(pg, nspace, osd_op);
    });
  case CEPH_OSD_OP_PGNLS_FILTER:
    return do_pg_op([&osd_op] (const auto& pg, const auto& nspace) {
      return do_pgnls_filtered(pg, nspace, osd_op);
    });
  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

} // namespace crimson::osd
