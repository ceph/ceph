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
#include "crimson/osd/pg.h"
#include "crimson/osd/watch.h"
#include "osd/ClassHandler.h"
#include "osd/SnapMapper.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

OpsExecuter::call_ierrorator::future<> OpsExecuter::do_op_call(OSDOp& osd_op)
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
  return interruptor::async(
    [this, method, indata=std::move(indata)]() mutable {
      ceph::bufferlist outdata;
      auto cls_context = reinterpret_cast<cls_method_context_t>(this);
      const auto ret = method->exec(cls_context, indata, outdata);
      return std::make_pair(ret, std::move(outdata));
    }
  ).then_interruptible(
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
      if (op_info.allows_returnvec() &&
          op_info.may_write() &&
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
      if (!op_info.may_write() || op_info.allows_returnvec() || ret < 0) {
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
                                      const OpsExecuter::ExecutableMessage& msg,
                                      entity_addr_t peer_addr)
{
  using crimson::common::local_conf;
  const uint32_t timeout =
    osd_op.op.watch.timeout == 0 ? local_conf()->osd_client_watch_timeout
                                 : osd_op.op.watch.timeout;
  return {
    osd_op.op.watch.cookie,
    timeout,
    peer_addr
  };
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_watch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().debug("{}", __func__);
  struct connect_ctx_t {
    ObjectContext::watch_key_t key;
    crimson::net::ConnectionXcoreRef conn;
    watch_info_t info;

    connect_ctx_t(
      const OSDOp& osd_op,
      const ExecutableMessage& msg,
      crimson::net::ConnectionXcoreRef conn)
      : key(osd_op.op.watch.cookie, msg.get_reqid().name),
        conn(conn),
        info(create_watch_info(osd_op, msg, conn->get_peer_addr())) {
    }
  };

  return with_effect_on_obc(
    connect_ctx_t{ osd_op, get_message(), conn },
    [&](auto& ctx) {
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
    [](auto&& ctx, ObjectContextRef obc, Ref<PG> pg) {
      assert(pg);
      auto [it, emplaced] = obc->watchers.try_emplace(ctx.key, nullptr);
      if (emplaced) {
        const auto& [cookie, entity] = ctx.key;
        it->second = crimson::osd::Watch::create(
          obc, ctx.info, entity, std::move(pg));
        logger().info("op_effect: added new watcher: {}", ctx.key);
      } else {
        logger().info("op_effect: found existing watcher: {}", ctx.key);
      }
      return it->second->connect(std::move(ctx.conn), true /* will_ping */);
    }
  );
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_reconnect(
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

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_unwatch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().info("{}", __func__);

  struct disconnect_ctx_t {
    ObjectContext::watch_key_t key;
    disconnect_ctx_t(const OSDOp& osd_op, const ExecutableMessage& msg)
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
    [] (auto&& ctx, ObjectContextRef obc, Ref<PG>) {
      if (auto nh = obc->watchers.extract(ctx.key); !nh.empty()) {
        return seastar::do_with(std::move(nh.mapped()),
                         [ctx](auto&& watcher) {
          logger().info("op_effect: disconnect watcher {}", ctx.key);
          return watcher->remove();
        });
      } else {
        logger().info("op_effect: disconnect failed to find watcher {}", ctx.key);
        return seastar::now();
      }
    });
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_ping(
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

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch(
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

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_notify(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}, msg epoch: {}", __func__, get_message().get_map_epoch());

  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }
  struct notify_ctx_t {
    crimson::net::ConnectionXcoreRef conn;
    notify_info_t ninfo;
    const uint64_t client_gid;
    const epoch_t epoch;

    notify_ctx_t(const ExecutableMessage& msg,
                 crimson::net::ConnectionXcoreRef conn)
      : conn(conn),
        client_gid(msg.get_reqid().name.num()),
        epoch(msg.get_map_epoch()) {
    }
  };
  return with_effect_on_obc(
    notify_ctx_t{ get_message(), conn },
    [&](auto& ctx) {
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
    [](auto&& ctx, ObjectContextRef obc, Ref<PG>) {
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
    }
  );
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_list_watchers(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}", __func__);

  obj_list_watch_response_t response;
  for (const auto& [key, info] : os.oi.watchers) {
    logger().debug("{}: key cookie={}, entity={}",
                   __func__, key.first, key.second);
    assert(key.first == info.cookie);
    assert(key.second.is_client());
    response.entries.emplace_back(watch_item_t{
      key.second, info.cookie, info.timeout_seconds, info.addr});
  }
  response.encode(osd_op.outdata, get_message().get_features());
  return watch_ierrorator::now();
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_notify_ack(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}", __func__);

  struct notifyack_ctx_t {
    const entity_name_t entity;
    uint64_t watch_cookie;
    uint64_t notify_id;
    ceph::bufferlist reply_bl;

    notifyack_ctx_t(const ExecutableMessage& msg)
      : entity(msg.get_reqid().name) {
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
    [] (auto&& ctx, ObjectContextRef obc, Ref<PG>) {
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

// Defined here because there is a circular dependency between OpsExecuter and PG
template <class Func>
auto OpsExecuter::do_const_op(Func&& f) {
  // TODO: pass backend as read-only
  return std::forward<Func>(f)(pg->get_backend(), std::as_const(obc->obs));
}

// Defined here because there is a circular dependency between OpsExecuter and PG
template <class Func>
auto OpsExecuter::do_write_op(Func&& f, OpsExecuter::modified_by m) {
  ++num_write;
  if (!osd_op_params) {
    osd_op_params.emplace();
    fill_op_params_bump_pg_version();
  }
  user_modify = (m == modified_by::user);
  return std::forward<Func>(f)(pg->get_backend(), obc->obs, txn);
}
OpsExecuter::call_errorator::future<> OpsExecuter::do_assert_ver(
  OSDOp& osd_op,
  const ObjectState& os)
{
  if (!osd_op.op.assert_ver.ver) {
    return crimson::ct_error::invarg::make();
  } else if (osd_op.op.assert_ver.ver < os.oi.user_version) {
    return crimson::ct_error::erange::make();
  } else if (osd_op.op.assert_ver.ver > os.oi.user_version) {
    return crimson::ct_error::value_too_large::make();
  }
  return seastar::now();
}

OpsExecuter::list_snaps_iertr::future<> OpsExecuter::do_list_snaps(
  OSDOp& osd_op,
  const ObjectState& os,
  const SnapSet& ss)
{
  obj_list_snap_response_t resp;
  resp.clones.reserve(ss.clones.size() + 1);
  for (auto &clone: ss.clones) {
    clone_info ci;
    ci.cloneid = clone;

    {
      auto p = ss.clone_snaps.find(clone);
      if (p == ss.clone_snaps.end()) {
	logger().error(
	  "OpsExecutor::do_list_snaps: {} has inconsistent "
	  "clone_snaps, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.snaps.reserve(p->second.size());
      ci.snaps.insert(ci.snaps.end(), p->second.rbegin(), p->second.rend());
    }

    {
      auto p = ss.clone_overlap.find(clone);
      if (p == ss.clone_overlap.end()) {
	logger().error(
	  "OpsExecutor::do_list_snaps: {} has inconsistent "
	  "clone_overlap, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.overlap.reserve(p->second.num_intervals());
      ci.overlap.insert(ci.overlap.end(), p->second.begin(), p->second.end());
    }

    {
      auto p = ss.clone_size.find(clone);
      if (p == ss.clone_size.end()) {
	logger().error(
	  "OpsExecutor::do_list_snaps: {} has inconsistent "
	  "clone_size, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.size = p->second;
    }
    resp.clones.push_back(std::move(ci));
  }

  if (!os.oi.is_whiteout()) {
    clone_info ci;
    ci.cloneid = CEPH_NOSNAP;
    ci.size = os.oi.size;
    resp.clones.push_back(std::move(ci));
  }
  resp.seq = ss.seq;
  logger().error(
    "OpsExecutor::do_list_snaps: {}, resp.clones.size(): {}",
    os.oi.soid,
    resp.clones.size());
  resp.encode(osd_op.outdata);
  return read_ierrorator::now();
}

OpsExecuter::interruptible_errorated_future<OpsExecuter::osd_op_errorator>
OpsExecuter::execute_op(OSDOp& osd_op)
{
  return do_execute_op(osd_op).handle_error_interruptible(
    osd_op_errorator::all_same_way([&osd_op](auto e, auto&& e_raw)
      -> OpsExecuter::osd_op_errorator::future<> {
        // All ops except for CMPEXT should have rval set to -e.value(),
        // CMPEXT sets rval itself and shouldn't be overridden.
        if (e.value() != ct_error::cmp_fail_error_value) {
          osd_op.rval = -e.value();
        }
        if ((osd_op.op.flags & CEPH_OSD_OP_FLAG_FAILOK) &&
	  e.value() != EAGAIN && e.value() != EINPROGRESS) {
          return osd_op_errorator::now();
        } else {
          return std::move(e_raw);
	}
      }));
}

OpsExecuter::interruptible_errorated_future<OpsExecuter::osd_op_errorator>
OpsExecuter::do_execute_op(OSDOp& osd_op)
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
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.read(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_SPARSE_READ:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.sparse_read(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_CHECKSUM:
    return do_read_op([&osd_op](auto& backend, const auto& os) {
      return backend.checksum(os, osd_op);
    });
  case CEPH_OSD_OP_CMPEXT:
    return do_read_op([&osd_op](auto& backend, const auto& os) {
      return backend.cmp_ext(os, osd_op);
    });
  case CEPH_OSD_OP_GETXATTR:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.getxattr(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_GETXATTRS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.get_xattrs(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_CMPXATTR:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.cmp_xattr(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_RMXATTR:
    return do_write_op([&osd_op](auto& backend, auto& os, auto& txn) {
      return backend.rm_xattr(os, osd_op, txn);
    });
  case CEPH_OSD_OP_CREATE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.create(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_WRITE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.write(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_WRITESAME:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.write_same(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_WRITEFULL:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.writefull(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_ROLLBACK:
    return do_write_op([this, &head=obc,
                        &osd_op](auto& backend, auto& os, auto& txn) {
      ceph_assert(obc->ssc);
      return backend.rollback(os, obc->ssc->snapset,
			      osd_op, txn, *osd_op_params, delta_stats,
                              head, pg->obc_loader, snapc);
    });
  case CEPH_OSD_OP_APPEND:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.append(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_TRUNCATE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      // FIXME: rework needed. Move this out to do_write_op(), introduce
      // do_write_op_no_user_modify()...
      return backend.truncate(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_ZERO:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.zero(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_SETALLOCHINT:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.set_allochint(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_SETXATTR:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.setxattr(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_DELETE:
  {
    bool whiteout = false;
    if (should_whiteout(obc->ssc->snapset, snapc)) {  // existing obj is old
      logger().debug("{} has or will have clones, will whiteout {}",
                     __func__, obc->obs.oi.soid);
      whiteout = true;
    }
    return do_write_op([this, whiteout](auto& backend, auto& os, auto& txn) {
      int num_bytes = 0;
      // Calculate num_bytes to be removed
      if (obc->obs.oi.soid.is_snap()) {
        ceph_assert(obc->ssc->snapset.clone_overlap.count(obc->obs.oi.soid.snap));
        num_bytes = obc->ssc->snapset.get_clone_bytes(obc->obs.oi.soid.snap);
      } else {
        num_bytes = obc->obs.oi.size;
      }
      return backend.remove(os, txn, *osd_op_params,
                            delta_stats, whiteout, num_bytes);
    });
  }
  case CEPH_OSD_OP_CALL:
    return this->do_op_call(osd_op);
  case CEPH_OSD_OP_STAT:
    // note: stat does not require RD
    return do_const_op([this, &osd_op] (/* const */auto& backend, const auto& os) {
      return backend.stat(os, osd_op, delta_stats);
    });

  case CEPH_OSD_OP_TMAPPUT:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.tmapput(os, osd_op, txn, delta_stats, *osd_op_params);
    });
  case CEPH_OSD_OP_TMAPUP:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto &txn) {
      return backend.tmapup(os, osd_op, txn, delta_stats, *osd_op_params);
    });
  case CEPH_OSD_OP_TMAPGET:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.tmapget(os, osd_op, delta_stats);
    });

  // OMAP
  case CEPH_OSD_OP_OMAPGETKEYS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_keys(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETVALS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_vals(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAP_CMP:
    return  do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_cmp(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETHEADER:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_header(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_vals_by_keys(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPSETVALS:
#if 0
    if (!pg.get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
#endif
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_set_vals(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_OMAPSETHEADER:
#if 0
    if (!pg.get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
#endif
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_set_header(os, osd_op, txn, *osd_op_params,
        delta_stats);
    });
  case CEPH_OSD_OP_OMAPRMKEYRANGE:
#if 0
    if (!pg.get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
#endif
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_remove_range(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_OMAPRMKEYS:
    /** TODO: Implement supports_omap()
    if (!pg.get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }*/
    return do_write_op([&osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_remove_key(os, osd_op, txn);
    });
  case CEPH_OSD_OP_OMAPCLEAR:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_clear(os, osd_op, txn, *osd_op_params, delta_stats);
    });

  // watch/notify
  case CEPH_OSD_OP_WATCH:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return do_op_watch(osd_op, os, txn);
    }, modified_by::sys);
  case CEPH_OSD_OP_LIST_WATCHERS:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_list_watchers(osd_op, os);
    });
  case CEPH_OSD_OP_NOTIFY:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_notify(osd_op, os);
    });
  case CEPH_OSD_OP_NOTIFY_ACK:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_notify_ack(osd_op, os);
    });
  case CEPH_OSD_OP_ASSERT_VER:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_assert_ver(osd_op, os);
    });
  case CEPH_OSD_OP_LIST_SNAPS:
    return do_snapset_op([this, &osd_op](const auto &os, const auto &ss) {
      return do_list_snaps(osd_op, os, ss);
    });

  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

void OpsExecuter::fill_op_params_bump_pg_version()
{
  osd_op_params->req_id = msg->get_reqid();
  osd_op_params->mtime = msg->get_mtime();
  osd_op_params->at_version = pg->get_next_version();
  osd_op_params->pg_trim_to = pg->get_pg_trim_to();
  osd_op_params->min_last_complete_ondisk = pg->get_min_last_complete_ondisk();
  osd_op_params->last_complete = pg->get_info().last_complete;
}

std::vector<pg_log_entry_t> OpsExecuter::prepare_transaction(
  const std::vector<OSDOp>& ops)
{
  // let's ensure we don't need to inform SnapMapper about this particular
  // entry.
  assert(obc->obs.oi.soid.snap >= CEPH_MAXSNAP);
  std::vector<pg_log_entry_t> log_entries;
  log_entries.emplace_back(
    obc->obs.exists ?
      pg_log_entry_t::MODIFY : pg_log_entry_t::DELETE,
    obc->obs.oi.soid,
    osd_op_params->at_version,
    obc->obs.oi.version,
    osd_op_params->user_modify ? osd_op_params->at_version.version : 0,
    osd_op_params->req_id,
    osd_op_params->mtime,
    op_info.allows_returnvec() && !ops.empty() ? ops.back().rval.code : 0);
  osd_op_params->at_version.version++;
  if (op_info.allows_returnvec()) {
    // also the per-op values are recorded in the pg log
    log_entries.back().set_op_returns(ops);
    logger().debug("{} op_returns: {}",
                   __func__, log_entries.back().op_returns);
  }
  log_entries.back().clean_regions = std::move(osd_op_params->clean_regions);
  return log_entries;
}

OpsExecuter::interruptible_future<> OpsExecuter::snap_map_remove(
  const hobject_t& soid,
  SnapMapper& snap_mapper,
  OSDriver& osdriver,
  ceph::os::Transaction& txn)
{
  logger().debug("{}: soid {}", __func__, soid);
  return interruptor::async([soid, &snap_mapper,
                             _t=osdriver.get_transaction(&txn)]() mutable {
    const auto r = snap_mapper.remove_oid(soid, &_t);
    if (r) {
      logger().error("{}: remove_oid {} failed with {}",
                     __func__, soid, r);
    }
    // On removal tolerate missing key corruption
    assert(r == 0 || r == -ENOENT);
  });
}

OpsExecuter::interruptible_future<> OpsExecuter::snap_map_modify(
  const hobject_t& soid,
  const std::set<snapid_t>& snaps,
  SnapMapper& snap_mapper,
  OSDriver& osdriver,
  ceph::os::Transaction& txn)
{
  logger().debug("{}: soid {}, snaps {}", __func__, soid, snaps);
  return interruptor::async([soid, snaps, &snap_mapper,
                             _t=osdriver.get_transaction(&txn)]() mutable {
    assert(std::size(snaps) > 0);
    [[maybe_unused]] const auto r = snap_mapper.update_snaps(
      soid, snaps, 0, &_t);
    assert(r == 0);
  });
}

OpsExecuter::interruptible_future<> OpsExecuter::snap_map_clone(
  const hobject_t& soid,
  const std::set<snapid_t>& snaps,
  SnapMapper& snap_mapper,
  OSDriver& osdriver,
  ceph::os::Transaction& txn)
{
  logger().debug("{}: soid {}, snaps {}", __func__, soid, snaps);
  return interruptor::async([soid, snaps, &snap_mapper,
                             _t=osdriver.get_transaction(&txn)]() mutable {
    assert(std::size(snaps) > 0);
    snap_mapper.add_oid(soid, snaps, &_t);
  });
}

// Defined here because there is a circular dependency between OpsExecuter and PG
uint32_t OpsExecuter::get_pool_stripe_width() const {
  return pg->get_pgpool().info.get_stripe_width();
}

// Defined here because there is a circular dependency between OpsExecuter and PG
version_t OpsExecuter::get_last_user_version() const
{
  return pg->get_last_user_version();
}

std::unique_ptr<OpsExecuter::CloningContext> OpsExecuter::execute_clone(
  const SnapContext& snapc,
  const ObjectState& initial_obs,
  const SnapSet& initial_snapset,
  PGBackend& backend,
  ceph::os::Transaction& txn)
{
  const hobject_t& soid = initial_obs.oi.soid;
  logger().debug("{} {} snapset={} snapc={}",
                 __func__, soid,
                 initial_snapset, snapc);

  auto cloning_ctx = std::make_unique<CloningContext>();
  cloning_ctx->new_snapset = initial_snapset;

  // clone object, the snap field is set to the seq of the SnapContext
  // at its creation.
  hobject_t coid = soid;
  coid.snap = snapc.seq;

  // existing snaps are stored in descending order in snapc,
  // cloned_snaps vector will hold all the snaps stored until snapset.seq
  const std::vector<snapid_t> cloned_snaps = [&] {
    auto last = std::find_if(
      std::begin(snapc.snaps), std::end(snapc.snaps),
      [&](snapid_t snap_id) { return snap_id <= initial_snapset.seq; });
    return std::vector<snapid_t>{std::begin(snapc.snaps), last};
  }();

  auto [snap_oi, clone_obc] = prepare_clone(coid);
  // make clone
  backend.clone(snap_oi, initial_obs, clone_obc->obs, txn);

  delta_stats.num_objects++;
  if (snap_oi.is_omap()) {
    delta_stats.num_objects_omap++;
  }
  delta_stats.num_object_clones++;
  // newsnapset is obc's ssc
  cloning_ctx->new_snapset.clones.push_back(coid.snap);
  cloning_ctx->new_snapset.clone_size[coid.snap] = initial_obs.oi.size;
  cloning_ctx->new_snapset.clone_snaps[coid.snap] = cloned_snaps;

  // clone_overlap should contain an entry for each clone
  // (an empty interval_set if there is no overlap)
  auto &overlap = cloning_ctx->new_snapset.clone_overlap[coid.snap];
  if (initial_obs.oi.size) {
    overlap.insert(0, initial_obs.oi.size);
  }

  // log clone
  logger().debug("cloning v {} to {} v {} snaps={} snapset={}",
                 initial_obs.oi.version, coid,
                 osd_op_params->at_version, cloned_snaps, cloning_ctx->new_snapset);

  cloning_ctx->log_entry = {
    pg_log_entry_t::CLONE,
    coid,
    snap_oi.version,
    initial_obs.oi.version,
    initial_obs.oi.user_version,
    osd_reqid_t(),
    initial_obs.oi.mtime, // will be replaced in `apply_to()`
    0
  };
  osd_op_params->at_version.version++;
  encode(cloned_snaps, cloning_ctx->log_entry.snaps);

  // update most recent clone_overlap and usage stats
  assert(cloning_ctx->new_snapset.clones.size() > 0);
  // In classic, we check for evicted clones before
  // adjusting the clone_overlap.
  // This check is redundant here since `clone_obc`
  // was just created (See prepare_clone()).
  interval_set<uint64_t> &newest_overlap =
    cloning_ctx->new_snapset.clone_overlap.rbegin()->second;
  osd_op_params->modified_ranges.intersection_of(newest_overlap);
  delta_stats.num_bytes += osd_op_params->modified_ranges.size();
  newest_overlap.subtract(osd_op_params->modified_ranges);
  return cloning_ctx;
}

void OpsExecuter::CloningContext::apply_to(
  std::vector<pg_log_entry_t>& log_entries,
  ObjectContext& processed_obc) &&
{
  log_entry.mtime = processed_obc.obs.oi.mtime;
  log_entries.insert(log_entries.begin(), std::move(log_entry));
  processed_obc.ssc->snapset = std::move(new_snapset);
}

OpsExecuter::interruptible_future<std::vector<pg_log_entry_t>>
OpsExecuter::flush_clone_metadata(
  std::vector<pg_log_entry_t>&& log_entries,
  SnapMapper& snap_mapper,
  OSDriver& osdriver,
  ceph::os::Transaction& txn)
{
  assert(!txn.empty());
  auto maybe_snap_mapped = interruptor::now();
  if (cloning_ctx) {
    std::move(*cloning_ctx).apply_to(log_entries, *obc);
    const auto& coid = log_entries.front().soid;
    const auto& cloned_snaps = obc->ssc->snapset.clone_snaps[coid.snap];
    maybe_snap_mapped = snap_map_clone(
      coid,
      std::set<snapid_t>{std::begin(cloned_snaps), std::end(cloned_snaps)},
      snap_mapper,
      osdriver,
      txn);
  }
  if (snapc.seq > obc->ssc->snapset.seq) {
     // update snapset with latest snap context
     obc->ssc->snapset.seq = snapc.seq;
     obc->ssc->snapset.snaps.clear();
  }
  logger().debug("{} done, initial snapset={}, new snapset={}",
    __func__, obc->obs.oi.soid, obc->ssc->snapset);
  return std::move(
    maybe_snap_mapped
  ).then_interruptible([log_entries=std::move(log_entries)]() mutable {
    return interruptor::make_ready_future<std::vector<pg_log_entry_t>>(
      std::move(log_entries));
  });
}

// TODO: make this static
std::pair<object_info_t, ObjectContextRef> OpsExecuter::prepare_clone(
  const hobject_t& coid)
{
  object_info_t static_snap_oi(coid);
  static_snap_oi.version = osd_op_params->at_version;
  static_snap_oi.prior_version = obc->obs.oi.version;
  static_snap_oi.copy_user_bits(obc->obs.oi);
  if (static_snap_oi.is_whiteout()) {
    // clone shouldn't be marked as whiteout
    static_snap_oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }

  ObjectContextRef clone_obc;
  if (pg->is_primary()) {
    // lookup_or_create
    auto [c_obc, existed] =
      pg->obc_registry.get_cached_obc(std::move(coid));
    assert(!existed);
    c_obc->obs.oi = static_snap_oi;
    c_obc->obs.exists = true;
    c_obc->ssc = obc->ssc;
    logger().debug("clone_obc: {}", c_obc->obs.oi);
    clone_obc = std::move(c_obc);
  }
  return std::make_pair(std::move(static_snap_oi), std::move(clone_obc));
}

void OpsExecuter::apply_stats()
{
  pg->get_peering_state().apply_op_stats(get_target(), delta_stats);
  pg->scrubber.handle_op_stats(get_target(), delta_stats);
  pg->publish_stats_to_osd();
}

OpsExecuter::OpsExecuter(Ref<PG> pg,
                         ObjectContextRef _obc,
                         const OpInfo& op_info,
                         abstracted_msg_t&& msg,
                         crimson::net::ConnectionXcoreRef conn,
                         const SnapContext& _snapc)
  : pg(std::move(pg)),
    obc(std::move(_obc)),
    op_info(op_info),
    msg(std::move(msg)),
    conn(conn),
    snapc(_snapc)
{
  if (op_info.may_write() && should_clone(*obc, snapc)) {
    do_write_op([this](auto& backend, auto& os, auto& txn) {
      cloning_ctx = execute_clone(std::as_const(snapc),
                                  std::as_const(obc->obs),
                                  std::as_const(obc->ssc->snapset),
                                  backend,
                                  txn);
    });
  }
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

static PG::interruptible_future<hobject_t> pgls_filter(
  const PGLSFilter& filter,
  const PGBackend& backend,
  const hobject_t& sobj)
{
  if (const auto xattr = filter.get_xattr(); !xattr.empty()) {
    logger().debug("pgls_filter: filter is interested in xattr={} for obj={}",
                   xattr, sobj);
    return backend.getxattr(sobj, std::move(xattr)).safe_then_interruptible(
      [&filter, sobj] (ceph::bufferlist val) {
        logger().debug("pgls_filter: got xvalue for obj={}", sobj);

        const bool filtered = filter.filter(sobj, val);
        return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
      }, PGBackend::get_attr_errorator::all_same_way([&filter, sobj] {
        logger().debug("pgls_filter: got error for obj={}", sobj);

        if (filter.reject_empty_xattr()) {
          return seastar::make_ready_future<hobject_t>();
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

static PG::interruptible_future<ceph::bufferlist> do_pgnls_common(
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

  return backend.list_objects(lower_bound, limit).then_interruptible(
    [&backend, filter, nspace](auto&& ret)
    -> PG::interruptible_future<std::tuple<std::vector<hobject_t>, hobject_t>> {
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
      auto to_pglsed = [&backend, filter] (const hobject_t& obj)
		       -> PG::interruptible_future<hobject_t> {
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
              std::move(items), std::move(next));
      });
  }).then_interruptible(
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
      logger().debug("do_pgnls_common: response.entries.size()= {}",
                     response.entries.size());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(out));
  });
}

static PG::interruptible_future<> do_pgnls(
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
    pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
  return do_pgnls_common(pg_start,
                         pg_end,
                         pg.get_backend(),
                         lower_bound,
                         nspace,
                         osd_op.op.pgls.count,
                         nullptr /* no filter */)
    .then_interruptible([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
  });
}

static PG::interruptible_future<> do_pgnls_filtered(
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
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
      return do_pgnls_common(pg_start,
                             pg_end,
                             pg.get_backend(),
                             lower_bound,
                             nspace,
                             osd_op.op.pgls.count,
                             filter.get())
        .then_interruptible([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

static PG::interruptible_future<ceph::bufferlist> do_pgls_common(
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
  return backend.list_objects(lower_bound, limit).then_interruptible(
    [&backend, filter, nspace](auto&& ret) {
      auto& [objects, next] = ret;
      return PG::interruptor::when_all(
        PG::interruptor::map_reduce(std::move(objects),
          [&backend, filter, nspace](const hobject_t& obj)
	  -> PG::interruptible_future<hobject_t>{
            if (obj.get_namespace() == nspace) {
              if (filter) {
                return pgls_filter(*filter, backend, obj);
              } else {
                return seastar::make_ready_future<hobject_t>(obj);
              }
            } else {
              return seastar::make_ready_future<hobject_t>();
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
    }).then_interruptible([pg_end](auto&& ret) {
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

static PG::interruptible_future<> do_pgls(
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
    pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
  return do_pgls_common(pg_start,
			pg_end,
			pg.get_backend(),
			lower_bound,
			nspace,
			osd_op.op.pgls.count,
			nullptr /* no filter */)
    .then_interruptible([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
    });
}

static PG::interruptible_future<> do_pgls_filtered(
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
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
      return do_pgls_common(pg_start,
                            pg_end,
                            pg.get_backend(),
                            lower_bound,
                            nspace,
                            osd_op.op.pgls.count,
                            filter.get())
        .then_interruptible([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

PgOpsExecuter::interruptible_future<>
PgOpsExecuter::execute_op(OSDOp& osd_op)
{
  logger().warn("handling op {}", ceph_osd_op_name(osd_op.op.op));
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_PGLS:
    return do_pgls(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGLS_FILTER:
    return do_pgls_filtered(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGNLS:
    return do_pgnls(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGNLS_FILTER:
    return do_pgnls_filtered(pg, nspace, osd_op);
  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

} // namespace crimson::osd
