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
#include "crimson/osd/watch.h"
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

  logger().debug("calling method {}.{}, num_read={}, num_write={}",
                 cname, mname, num_read, num_write);
  const auto prev_rd = num_read;
  const auto prev_wr = num_write;
  return seastar::async(
    [this, method, indata=std::move(indata)]() mutable {
      ceph::bufferlist outdata;
      auto cls_context = reinterpret_cast<cls_method_context_t>(this);
      const auto ret = method->exec(cls_context, indata, outdata);
      return std::make_pair(ret, std::move(outdata));
    }
  ).then(
    [this, prev_rd, prev_wr, &osd_op, flags]
    (auto outcome) -> call_errorator::future<> {
      auto& [ret, outdata] = outcome;
      osd_op.rval = ret;

      logger().debug("do_op_call: method returned ret={}, outdata.length()={}"
                     " while num_read={}, num_write={}",
                     ret, outdata.length(), num_read, num_write);
      if (num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
        logger().error("method tried to read object but is not marked RD");
        osd_op.rval = -EIO;
        return crimson::ct_error::input_output_error::make();
      }
      if (num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
        logger().error("method tried to update object but is not marked WR");
        osd_op.rval = -EIO;
        return crimson::ct_error::input_output_error::make();
      }
      // ceph-osd has this implemented in `PrimaryLogPG::execute_ctx`,
      // grep for `ignore_out_data`.
      using crimson::common::local_conf;
      if (op_info->allows_returnvec() &&
          op_info->may_write() &&
          ret >= 0 &&
          outdata.length() > local_conf()->osd_max_write_op_reply_len) {
        // the justification of this limit it to not inflate the pg log.
        // that's the reason why we don't worry about pure reads.
        logger().error("outdata overflow due to .length()={}, limit={}",
                       outdata.length(),
                       local_conf()->osd_max_write_op_reply_len);
        osd_op.rval = -EOVERFLOW;
        return crimson::ct_error::value_too_large::make();
      }
      // for write calls we never return data expect errors or RETURNVEC.
      // please refer cls/cls_hello.cc to details.
      if (!op_info->may_write() || op_info->allows_returnvec() || ret < 0) {
        osd_op.op.extent.length = outdata.length();
        osd_op.outdata.claim_append(outdata);
      }
      if (ret < 0) {
        return crimson::stateful_ec{
          std::error_code(-ret, std::generic_category()) };
      } else {
        return seastar::now();
      }
    }
  );
}

static watch_info_t create_watch_info(const OSDOp& osd_op,
                                      const MOSDOp& msg)
{
  using crimson::common::local_conf;
  const uint32_t timeout =
    osd_op.op.watch.timeout == 0 ? local_conf()->osd_client_watch_timeout
                                 : osd_op.op.watch.timeout;
  return {
    osd_op.op.watch.cookie,
    timeout,
    msg.get_connection()->get_peer_addr()
  };
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_watch_subop_watch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  struct connect_ctx_t {
    ObjectContext::watch_key_t key;
    crimson::net::ConnectionRef conn;
    watch_info_t info;

    connect_ctx_t(const OSDOp& osd_op, const MOSDOp& msg)
      : key(osd_op.op.watch.cookie, msg.get_reqid().name),
        conn(msg.get_connection()),
        info(create_watch_info(osd_op, msg)) {
    }
  };
  return with_effect_on_obc(connect_ctx_t{ osd_op, get_message() },
    [&] (auto& ctx) {
      const auto& entity = ctx.key.second;
      auto [it, emplaced] =
        os.oi.watchers.try_emplace(ctx.key, std::move(ctx.info));
      if (emplaced) {
        logger().info("registered new watch {} by {}", it->second, entity);
        txn.nop();
      } else {
        logger().info("found existing watch {} by {}", it->second, entity);
      }
      return seastar::now();
    },
    [] (auto&& ctx, ObjectContextRef obc) {
      auto [it, emplaced] = obc->watchers.try_emplace(ctx.key, nullptr);
      if (emplaced) {
        const auto& [cookie, entity] = ctx.key;
        it->second = crimson::osd::Watch::create(obc, ctx.info, entity);
        logger().info("op_effect: added new watcher: {}", ctx.key);
      } else {
        logger().info("op_effect: found existing watcher: {}", ctx.key);
      }
      return it->second->connect(std::move(ctx.conn), true /* will_ping */);
    });
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_watch_subop_reconnect(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  const entity_name_t& entity = get_message().get_reqid().name;
  const auto& cookie = osd_op.op.watch.cookie;
  if (!os.oi.watchers.count(std::make_pair(cookie, entity))) {
    return crimson::ct_error::not_connected::make();
  } else {
    logger().info("found existing watch by {}", entity);
    return do_op_watch_subop_watch(osd_op, os, txn);
  }
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_watch_subop_unwatch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().info("{}", __func__);

  struct disconnect_ctx_t {
    ObjectContext::watch_key_t key;
    bool send_disconnect{ false };

    disconnect_ctx_t(const OSDOp& osd_op, const MOSDOp& msg)
      : key(osd_op.op.watch.cookie, msg.get_reqid().name) {
    }
  };
  return with_effect_on_obc(disconnect_ctx_t{ osd_op, get_message() },
    [&] (auto& ctx) {
      const auto& entity = ctx.key.second;
      if (auto nh = os.oi.watchers.extract(ctx.key); !nh.empty()) {
        logger().info("removed watch {} by {}", nh.mapped(), entity);
        txn.nop();
      } else {
        logger().info("can't remove: no watch by {}", entity);
      }
      return seastar::now();
    },
    [] (auto&& ctx, ObjectContextRef obc) {
      if (auto nh = obc->watchers.extract(ctx.key); !nh.empty()) {
        return seastar::do_with(std::move(nh.mapped()),
                         [ctx](auto&& watcher) {
          logger().info("op_effect: disconnect watcher {}", ctx.key);
          return watcher->remove(ctx.send_disconnect);
        });
      } else {
        logger().info("op_effect: disconnect failed to find watcher {}", ctx.key);
        return seastar::now();
      }
    });
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_watch_subop_ping(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  const entity_name_t& entity = get_message().get_reqid().name;
  const auto& cookie = osd_op.op.watch.cookie;
  const auto key = std::make_pair(cookie, entity);

  // Note: WATCH with PING doesn't cause may_write() to return true,
  // so if there is nothing else in the transaction, this is going
  // to run do_osd_op_effects, but not write out a log entry */
  if (!os.oi.watchers.count(key)) {
    return crimson::ct_error::not_connected::make();
  }
  auto it = obc->watchers.find(key);
  if (it == std::end(obc->watchers) || !it->second->is_connected()) {
    return crimson::ct_error::timed_out::make();
  }
  logger().info("found existing watch by {}", entity);
  it->second->got_ping(ceph_clock_now());
  return seastar::now();
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_watch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().debug("{}", __func__);
  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }
  switch (osd_op.op.watch.op) {
    case CEPH_OSD_WATCH_OP_WATCH:
      return do_op_watch_subop_watch(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_RECONNECT:
      return do_op_watch_subop_reconnect(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_PING:
      return do_op_watch_subop_ping(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_UNWATCH:
      return do_op_watch_subop_unwatch(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_LEGACY_WATCH:
      logger().warn("ignoring CEPH_OSD_WATCH_OP_LEGACY_WATCH");
      return crimson::ct_error::invarg::make();
  }
  logger().warn("unrecognized WATCH subop: {}", osd_op.op.watch.op);
  return crimson::ct_error::invarg::make();
}

static uint64_t get_next_notify_id(epoch_t e)
{
  // FIXME
  static std::uint64_t next_notify_id = 0;
  return (((uint64_t)e) << 32) | ((uint64_t)(next_notify_id++));
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_notify(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}, msg epoch: {}", __func__, get_message().get_map_epoch());

  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }
  struct notify_ctx_t {
    crimson::net::ConnectionRef conn;
    notify_info_t ninfo;
    const uint64_t client_gid;
    const epoch_t epoch;

    notify_ctx_t(const MOSDOp& msg)
      : conn(msg.get_connection()),
        client_gid(msg.get_reqid().name.num()),
        epoch(msg.get_map_epoch()) {
    }
  };
  return with_effect_on_obc(notify_ctx_t{ get_message() },
    [&] (auto& ctx) {
      try {
        auto bp = osd_op.indata.cbegin();
        uint32_t ver; // obsolete
        ceph::decode(ver, bp);
        ceph::decode(ctx.ninfo.timeout, bp);
        ceph::decode(ctx.ninfo.bl, bp);
      } catch (const buffer::error&) {
        ctx.ninfo.timeout = 0;
      }
      if (!ctx.ninfo.timeout) {
        using crimson::common::local_conf;
        ctx.ninfo.timeout = local_conf()->osd_default_notify_timeout;
      }
      ctx.ninfo.notify_id = get_next_notify_id(ctx.epoch);
      ctx.ninfo.cookie = osd_op.op.notify.cookie;
      // return our unique notify id to the client
      ceph::encode(ctx.ninfo.notify_id, osd_op.outdata);
      return seastar::now();
    },
    [] (auto&& ctx, ObjectContextRef obc) {
      auto alive_watchers = obc->watchers | boost::adaptors::map_values
                                          | boost::adaptors::filtered(
        [] (const auto& w) {
          // FIXME: filter as for the `is_ping` in `Watch::start_notify`
          return w->is_alive();
        });
      return crimson::osd::Notify::create_n_propagate(
        std::begin(alive_watchers),
        std::end(alive_watchers),
        std::move(ctx.conn),
        ctx.ninfo,
        ctx.client_gid,
        obc->obs.oi.user_version);
  });
}

OpsExecuter::watch_errorator::future<> OpsExecuter::do_op_notify_ack(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}", __func__);

  struct notifyack_ctx_t {
    const entity_name_t entity;
    uint64_t watch_cookie;
    uint64_t notify_id;
    ceph::bufferlist reply_bl;

    notifyack_ctx_t(const MOSDOp& msg) : entity(msg.get_reqid().name) {
    }
  };
  return with_effect_on_obc(notifyack_ctx_t{ get_message() },
    [&] (auto& ctx) -> watch_errorator::future<> {
      try {
        auto bp = osd_op.indata.cbegin();
        ceph::decode(ctx.notify_id, bp);
        ceph::decode(ctx.watch_cookie, bp);
        if (!bp.end()) {
          ceph::decode(ctx.reply_bl, bp);
        }
      } catch (const buffer::error&) {
        // here we behave differently than ceph-osd. For historical reasons,
        // it falls back to using `osd_op.op.watch.cookie` as `ctx.notify_id`.
        // crimson just returns EINVAL if the data cannot be decoded.
        return crimson::ct_error::invarg::make();
      }
      return watch_errorator::now();
    },
    [] (auto&& ctx, ObjectContextRef obc) {
      logger().info("notify_ack watch_cookie={}, notify_id={}",
                    ctx.watch_cookie, ctx.notify_id);
      return seastar::do_for_each(obc->watchers,
        [ctx=std::move(ctx)] (auto& kv) {
          const auto& [key, watchp] = kv;
          static_assert(
            std::is_same_v<std::decay_t<decltype(watchp)>,
                           seastar::shared_ptr<crimson::osd::Watch>>);
          auto& [cookie, entity] = key;
          if (ctx.entity != entity) {
            logger().debug("skipping watch {}; entity name {} != {}",
                           key, entity, ctx.entity);
            return seastar::now();
          }
          if (ctx.watch_cookie != cookie) {
            logger().debug("skipping watch {}; cookie {} != {}",
                           key, ctx.watch_cookie, cookie);
            return seastar::now();
          }
          logger().info("acking notify on watch {}", key);
          return watchp->notify_ack(ctx.notify_id, ctx.reply_bl);
        });
  });
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
    [&backend, filter, nspace](auto&& ret) {
      auto& [objects, next] = ret;
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
          return seastar::make_ready_future<
            std::tuple<std::vector<hobject_t>, hobject_t>>(
              std::make_tuple(std::move(items), std::move(next)));
      });
  }).then(
    [pg_end] (auto&& ret) {
      auto& [items, next] = ret;
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
    }, true);
  case CEPH_OSD_OP_WRITE:
    return do_write_op([this, &osd_op] (auto& backend, auto& os, auto& txn) {
      osd_op_params = osd_op_params_t();
      return backend.write(os, osd_op, txn, *osd_op_params);
    }, true);
  case CEPH_OSD_OP_WRITEFULL:
    return do_write_op([this, &osd_op] (auto& backend, auto& os, auto& txn) {
      osd_op_params = osd_op_params_t();
      return backend.writefull(os, osd_op, txn, *osd_op_params);
    }, true);
  case CEPH_OSD_OP_SETALLOCHINT:
    return osd_op_errorator::now();
  case CEPH_OSD_OP_SETXATTR:
    return do_write_op([&osd_op] (auto& backend, auto& os, auto& txn) {
      return backend.setxattr(os, osd_op, txn);
    }, true);
  case CEPH_OSD_OP_DELETE:
    return do_write_op([] (auto& backend, auto& os, auto& txn) {
      return backend.remove(os, txn);
    }, true);
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
  case CEPH_OSD_OP_OMAPGETHEADER:
    return do_read_op([&osd_op] (auto& backend, const auto& os) {
      return backend.omap_get_header(os, osd_op);
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
    return do_write_op([this, &osd_op] (auto& backend, auto& os, auto& txn) {
      osd_op_params = osd_op_params_t();
      return backend.omap_set_vals(os, osd_op, txn, *osd_op_params);
    }, true);

  // watch/notify
  case CEPH_OSD_OP_WATCH:
    return do_write_op([this, &osd_op] (auto& backend, auto& os, auto& txn) {
      return do_op_watch(osd_op, os, txn);
    }, false);
  case CEPH_OSD_OP_NOTIFY:
    return do_read_op([this, &osd_op] (auto&, const auto& os) {
      return do_op_notify(osd_op, os);
    });
  case CEPH_OSD_OP_NOTIFY_ACK:
    return do_read_op([this, &osd_op] (auto&, const auto& os) {
      return do_op_notify_ack(osd_op, os);
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
    [&backend, filter, nspace](auto&& ret) {
      auto& [objects, next] = ret;
      return seastar::when_all(
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
          [](entries_t entries, hobject_t obj) {
            if (!obj.is_min()) {
              entries.emplace_back(obj.oid, obj.get_key());
            }
            return entries;
          }),
        seastar::make_ready_future<hobject_t>(next));
    }).then([pg_end](auto&& ret) {
      auto entries = std::move(std::get<0>(ret).get0());
      auto next = std::move(std::get<1>(ret).get0());
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
