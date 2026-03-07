// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "objecter.h"

#include <boost/intrusive_ptr.hpp>

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "common/entity_name.h"
#include "common/hobject.h"
#include "crimson/mon/MonClient.h"
#include "include/rados.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_objecter);
  }
}

namespace crimson::osdc {

Objecter::Objecter(crimson::net::Messenger& msgr,
                   crimson::mon::Client& monc)
  : msgr(msgr),
    monc(monc),
    osdmap(std::make_unique<OSDMap>())
{}

Objecter::~Objecter() {}

seastar::future<> Objecter::start()
{
  logger().info("Objecter::start");
  ceph_assert(!started);
  started = true;
  osdmap_ready.emplace();

  // Subscribe to osdmap updates from the monitor
  if (monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME)) {
    return monc.renew_subs();
  }
  return seastar::now();
}

seastar::future<> Objecter::stop()
{
  logger().info("Objecter::stop");
  ceph_assert(started);
  started = false;
  return dispatch_gate.close();
}

std::optional<seastar::future<>>
Objecter::ms_dispatch(crimson::net::ConnectionRef conn,
                      MessageRef m)
{
  const int msg_type = m->get_type();

  // Only handle messages from MON (OSDMap) or OSD (OpReply)
  if (conn->peer_is_mon()) {
    if (msg_type != CEPH_MSG_OSD_MAP) {
      return std::nullopt;
    }
    auto mosdmap = boost::static_pointer_cast<MOSDMap>(m);
    return seastar::with_gate(dispatch_gate, [this, mosdmap] {
      return handle_osd_map(std::move(mosdmap));
    });
  }

  if (conn->peer_is_osd()) {
    if (msg_type != CEPH_MSG_OSD_OPREPLY) {
      return std::nullopt;
    }
    auto reply = boost::static_pointer_cast<MOSDOpReply>(m);
    return seastar::with_gate(dispatch_gate, [this, conn, reply] {
      handle_osd_op_reply(conn, reply.get());
      return seastar::now();
    });
  }

  return std::nullopt;
}

seastar::future<> Objecter::handle_osd_map(Ref<MOSDMap> m)
{
  logger().debug("handle_osd_map: epochs [{},{}]",
                m->get_first(), m->get_last());

  if (m->fsid != monc.get_fsid()) {
    logger().warn("handle_osd_map: fsid mismatch {} != {}",
                  m->fsid, monc.get_fsid());
    return seastar::now();
  }

  return seastar::with_lock(osdmap_mutex, [this, m = std::move(m)] {
    seastar::future<> renew_fut = seastar::now();

    if (m->get_last() <= osdmap->get_epoch()) {
      logger().debug("handle_osd_map: ignoring stale epochs [{},{}] <= {}",
                    m->get_first(), m->get_last(), osdmap->get_epoch());
    } else {
      if (osdmap->get_epoch()) {
        // Apply incremental maps
        for (epoch_t e = osdmap->get_epoch() + 1; e <= m->get_last(); ++e) {
          if (osdmap->get_epoch() == e - 1 && m->incremental_maps.count(e)) {
            logger().debug("handle_osd_map: applying incremental epoch {}", e);
            OSDMap::Incremental inc(m->incremental_maps[e]);
            osdmap->apply_incremental(inc);
          } else if (m->maps.count(e)) {
            logger().debug("handle_osd_map: applying full epoch {}", e);
            auto new_osdmap = std::make_unique<OSDMap>();
            new_osdmap->decode(m->maps[e]);
            osdmap = std::move(new_osdmap);
          }
        }
      } else {
        // First map - need full
        if (m->maps.count(m->get_last())) {
          logger().debug("handle_osd_map: decoding initial full epoch {}",
                        m->get_last());
          osdmap->decode(m->maps[m->get_last()]);
        } else {
          logger().debug("handle_osd_map: need full map, requesting");
          renew_fut = maybe_request_map();
        }
      }
    }

    monc.sub_got("osdmap", osdmap->get_epoch());

    if (osdmap->get_epoch() > 0 && osdmap_ready && !osdmap_ready_fulfilled) {
      osdmap_ready_fulfilled = true;
      osdmap_ready->set_value();
    }
    return renew_fut;
  });
}

void Objecter::handle_osd_op_reply(crimson::net::ConnectionRef conn,
                                  MOSDOpReply* m)
{
  const ceph_tid_t tid = m->get_tid();
  const int r = m->get_result();

  if (!conn->peer_is_osd()) {
    return;
  }
  const int osd_id = conn->get_peer_id();
  auto it = sessions.find(osd_id);
  if (it == sessions.end()) {
    logger().warn("handle_osd_op_reply: tid={} from unknown osd.{}", tid, osd_id);
    return;
  }
  auto op_it = it->second.ops.find(tid);
  if (op_it == it->second.ops.end()) {
    logger().debug("handle_osd_op_reply: tid={} not found (may be stale)", tid);
    return;
  }
  auto completion = std::move(op_it->second);
  it->second.ops.erase(op_it);

  std::vector<OSDOp> ops;
  m->claim_ops(ops);
  completion(r, ops);
}

seastar::future<> Objecter::wait_for_osdmap()
{
  return with_osdmap([](const OSDMap& o) { return o.get_epoch(); })
    .then([this](epoch_t epoch) {
      if (epoch > 0) {
        return seastar::now();
      }
      return osdmap_ready->get_shared_future();
    });
}

seastar::future<> Objecter::maybe_request_map()
{
  if (monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME)) {
    return monc.renew_subs();
  }
  return seastar::now();
}

#ifdef UNIT_TESTS_BUILT
seastar::future<> Objecter::inject_osdmap_for_test(std::unique_ptr<OSDMap> m)
{
  return seastar::with_lock(osdmap_mutex, [this, m = std::move(m)]() mutable {
    osdmap = std::move(m);
    monc.sub_got("osdmap", osdmap->get_epoch());
    if (osdmap->get_epoch() > 0 && osdmap_ready && !osdmap_ready_fulfilled) {
      osdmap_ready_fulfilled = true;
      osdmap_ready->set_value();
    }
  });
}
#endif

seastar::future<std::optional<OsdTarget>>
Objecter::calc_target(const object_t& oid,
                     const object_locator_t& oloc) const
{
  return seastar::with_shared(osdmap_mutex, [this, oid, oloc] {
    if (!osdmap->get_epoch()) {
      return std::optional<OsdTarget>{};  // no OSDMap yet
    }

    pg_t raw_pg;
    const int ret = osdmap->object_locator_to_pg(oid, oloc, raw_pg);
    if (ret != 0) {
      return std::optional<OsdTarget>{};  // pool does not exist (e.g. -ENOENT)
    }

    const pg_t actual_pgid = osdmap->raw_pg_to_pg(raw_pg);

    int up_primary = -1;
    int acting_primary = -1;
    std::vector<int> up;
    std::vector<int> acting;
    osdmap->pg_to_up_acting_osds(actual_pgid,
                                 &up, &up_primary,
                                 &acting, &acting_primary);

    if (acting_primary < 0 || !osdmap->exists(acting_primary)) {
      return std::optional<OsdTarget>{};
    }

    const entity_addr_t addr = osdmap->get_addrs(acting_primary).front();
    return std::optional<OsdTarget>{OsdTarget{actual_pgid, raw_pg, acting_primary, addr}};
  });
}

seastar::future<crimson::net::ConnectionRef>
Objecter::get_or_connect(const entity_addr_t& addr, int osd_id)
{
  auto it = sessions.find(osd_id);
  if (it != sessions.end() && it->second.conn && it->second.conn->is_connected()) {
    return seastar::make_ready_future<crimson::net::ConnectionRef>(it->second.conn);
  }

  auto pend_it = pending_connects.find(osd_id);
  if (pend_it != pending_connects.end()) {
    return pend_it->second.get_shared_future();
  }

  crimson::net::ConnectionRef conn = msgr.connect(
    addr, entity_name_t(CEPH_ENTITY_TYPE_OSD, osd_id));

  if (conn->is_connected()) {
    if (it == sessions.end()) {
      sessions.emplace(osd_id, OsdSession{osd_id, addr, conn});
    } else {
      it->second.conn = conn;
      it->second.addr = addr;
    }
    return seastar::make_ready_future<crimson::net::ConnectionRef>(conn);
  }

  if (it == sessions.end()) {
    sessions.emplace(osd_id, OsdSession{osd_id, addr, conn});
  } else {
    it->second.conn = conn;
    it->second.addr = addr;
  }

  auto [promise_it, inserted] = pending_connects.emplace(
    osd_id, seastar::shared_promise<crimson::net::ConnectionRef>{});
  ceph_assert(inserted);
  return promise_it->second.get_shared_future();
}

seastar::future<> Objecter::send_to_osd(const OsdTarget& target, MessageURef msg)
{
  return get_or_connect(target.primary_addr, target.primary_osd)
    .then([msg = std::move(msg)](crimson::net::ConnectionRef conn) mutable {
      return conn->send(std::move(msg));
    });
}

seastar::future<ceph::bufferlist> Objecter::read(const object_t& oid,
                                                 const object_locator_t& oloc,
                                                 uint64_t off, uint64_t len,
                                                 snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, off, len, snap](std::optional<OsdTarget> target) {
      if (!target) {
        return seastar::make_exception_future<ceph::bufferlist>(
          std::system_error(ENOENT, std::generic_category(), "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc, off, len, snap](epoch_t epoch) {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const int flags = CEPH_OSD_FLAG_RETURNVEC;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid, epoch, flags, 0);
          msg->set_snapid(snap);
          msg->read(off, len);

          seastar::promise<ceph::bufferlist> promise;
          auto future = promise.get_future();
          const int osd_id = target.primary_osd;

          auto p = std::make_shared<seastar::promise<ceph::bufferlist>>(std::move(promise));
          OpCompletion completion = [p](int r, std::vector<OSDOp>& ops) {
            if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "read failed")));
            } else if (!ops.empty()) {
              p->set_value(std::move(ops[0].outdata));
            } else {
              p->set_value(ceph::bufferlist{});
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, c = std::move(completion), msg = std::move(msg)](
                  crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              it->second.ops[tid] = std::move(c);
              return conn->send(std::move(msg));
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

seastar::future<> Objecter::write(const object_t& oid,
                                  const object_locator_t& oloc,
                                  uint64_t off, ceph::bufferlist&& bl,
                                  snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, off, bl = std::move(bl), snap](std::optional<OsdTarget> target) mutable {
      if (!target) {
        return seastar::make_exception_future<>(
          std::system_error(ENOENT, std::generic_category(),
                            "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc, off, bl = std::move(bl), snap](epoch_t epoch) mutable {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const size_t len = bl.length();
          const int osd_id = target.primary_osd;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid, epoch, 0, 0);
          msg->set_snapid(snap);
          msg->write(off, len, bl);
          // MOSDOp::write puts data in Message::data, but OSD expects it in
          // ops[0].indata for proper encode/decode. Move it there.
          msg->ops[0].indata = std::move(msg->get_data());

          seastar::promise<> promise;
          auto future = promise.get_future();

          auto p = std::make_shared<seastar::promise<>>(std::move(promise));
          OpCompletion completion = [this, osd_id, len, p](int r, std::vector<OSDOp>&) {
            auto it = sessions.find(osd_id);
            if (it != sessions.end()) {
              it->second.write_throttle.put(len);
            }
            if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "write failed")));
            } else {
              p->set_value();
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, len, c = std::move(completion), msg = std::move(msg)](
                  crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              return it->second.write_throttle.get(len).then(
                [this, conn, tid, osd_id, c = std::move(c),
                 msg = std::move(msg)]() mutable {
                  auto it2 = sessions.find(osd_id);
                  ceph_assert(it2 != sessions.end());
                  it2->second.ops[tid] = std::move(c);
                  return conn->send(std::move(msg));
                }).handle_exception([this, tid, osd_id, len](std::exception_ptr e) {
                  auto it2 = sessions.find(osd_id);
                  if (it2 != sessions.end()) {
                    it2->second.ops.erase(tid);
                    it2->second.write_throttle.put(len);
                  }
                  return seastar::make_exception_future<>(e);
                });
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

seastar::future<> Objecter::discard(const object_t& oid,
                                    const object_locator_t& oloc,
                                    uint64_t off, uint64_t len,
                                    snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, off, len, snap](std::optional<OsdTarget> target) {
      if (!target) {
        return seastar::make_exception_future<>(
          std::system_error(ENOENT, std::generic_category(),
                            "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc, off, len, snap](epoch_t epoch) {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const int osd_id = target.primary_osd;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid, epoch, 0, 0);
          msg->set_snapid(snap);
          msg->zero(off, len);

          seastar::promise<> promise;
          auto future = promise.get_future();
          auto p = std::make_shared<seastar::promise<>>(std::move(promise));
          OpCompletion completion = [p](int r, std::vector<OSDOp>&) {
            if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "discard failed")));
            } else {
              p->set_value();
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, c = std::move(completion), msg = std::move(msg)](
                  crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              it->second.ops[tid] = std::move(c);
              return conn->send(std::move(msg));
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

seastar::future<> Objecter::write_zeroes(const object_t& oid,
                                         const object_locator_t& oloc,
                                         uint64_t off, uint64_t len,
                                         snapid_t snap)
{
  return discard(oid, oloc, off, len, snap);
}

seastar::future<> Objecter::compare_and_write(const object_t& oid,
                                              const object_locator_t& oloc,
                                              uint64_t cmp_off, ceph::bufferlist&& cmp_bl,
                                              uint64_t write_off, ceph::bufferlist&& write_bl,
                                              snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, cmp_off, cmp_bl = std::move(cmp_bl),
           write_off, write_bl = std::move(write_bl), snap](std::optional<OsdTarget> target) mutable {
      if (!target) {
        return seastar::make_exception_future<>(
          std::system_error(ENOENT, std::generic_category(),
                            "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc,
               cmp_off, cmp_bl = std::move(cmp_bl),
               write_off, write_bl = std::move(write_bl), snap](epoch_t epoch) mutable {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const size_t write_len = write_bl.length();
          const int osd_id = target.primary_osd;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid, epoch, 0, 0);
          msg->set_snapid(snap);
          msg->add_op_with_data(CEPH_OSD_OP_CMPEXT, cmp_off, cmp_bl.length(), std::move(cmp_bl));
          msg->add_op_with_data(CEPH_OSD_OP_WRITE, write_off, write_len, std::move(write_bl));

          seastar::promise<> promise;
          auto future = promise.get_future();
          auto p = std::make_shared<seastar::promise<>>(std::move(promise));
          OpCompletion completion = [this, osd_id, write_len, p](int r, std::vector<OSDOp>&) {
            auto it = sessions.find(osd_id);
            if (it != sessions.end()) {
              it->second.write_throttle.put(write_len);
            }
            if (r == -EILSEQ) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(EILSEQ, std::generic_category(), "compare-and-write mismatch")));
            } else if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "compare-and-write failed")));
            } else {
              p->set_value();
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, write_len, c = std::move(completion), msg = std::move(msg)](
                  crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              return it->second.write_throttle.get(write_len).then(
                [this, conn, tid, osd_id, c = std::move(c),
                 msg = std::move(msg)]() mutable {
                  auto it2 = sessions.find(osd_id);
                  ceph_assert(it2 != sessions.end());
                  it2->second.ops[tid] = std::move(c);
                  return conn->send(std::move(msg));
                }).handle_exception([this, tid, osd_id, write_len](std::exception_ptr e) {
                  auto it2 = sessions.find(osd_id);
                  if (it2 != sessions.end()) {
                    it2->second.ops.erase(tid);
                    it2->second.write_throttle.put(write_len);
                  }
                  return seastar::make_exception_future<>(e);
                });
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

seastar::future<ceph::bufferlist> Objecter::exec(const object_t& oid,
                                                 const object_locator_t& oloc,
                                                 std::string_view cname,
                                                 std::string_view method,
                                                 ceph::bufferlist&& indata,
                                                 snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, cname, method, indata = std::move(indata), snap](
            std::optional<OsdTarget> target) mutable {
      if (!target) {
        return seastar::make_exception_future<ceph::bufferlist>(
          std::system_error(ENOENT, std::generic_category(),
                            "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc, cname, method,
               indata = std::move(indata), snap](epoch_t epoch) mutable {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const int flags = CEPH_OSD_FLAG_RETURNVEC;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid,
                                                  epoch, flags, 0);
          msg->set_snapid(snap);
          msg->add_call(cname, method, indata);

          seastar::promise<ceph::bufferlist> promise;
          auto future = promise.get_future();
          const int osd_id = target.primary_osd;

          auto p = std::make_shared<seastar::promise<ceph::bufferlist>>(
            std::move(promise));
          OpCompletion completion = [p](int r, std::vector<OSDOp>& ops) {
            if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "exec failed")));
            } else if (!ops.empty()) {
              p->set_value(std::move(ops[0].outdata));
            } else {
              p->set_value(ceph::bufferlist{});
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, c = std::move(completion),
                   msg = std::move(msg)](crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              it->second.ops[tid] = std::move(c);
              return conn->send(std::move(msg));
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

seastar::future<std::pair<uint64_t, ceph::real_time>>
Objecter::stat(const object_t& oid,
               const object_locator_t& oloc,
               snapid_t snap)
{
  return calc_target(oid, oloc)
    .then([this, oid, oloc, snap](std::optional<OsdTarget> target) {
      if (!target) {
        return seastar::make_exception_future<std::pair<uint64_t, ceph::real_time>>(
          std::system_error(ENOENT, std::generic_category(),
                            "pool or object mapping failed"));
      }
      return with_osdmap([](const OSDMap& m) { return m.get_epoch(); })
        .then([this, target = *target, oid, oloc, snap](epoch_t epoch) {
          const ceph_tid_t tid = next_tid++;
          const uint32_t hash = oloc.hash >= 0 ? static_cast<uint32_t>(oloc.hash)
                                               : target.raw_pg.ps();
          const hobject_t hobj(oid, oloc.key, snap, hash,
                              oloc.get_pool(), oloc.nspace);
          spg_t spgid(target.pgid);
          const int flags = CEPH_OSD_FLAG_RETURNVEC;

          auto msg = crimson::make_message<MOSDOp>(client_inc, tid, hobj, spgid, epoch, flags, 0);
          msg->set_snapid(snap);
          msg->stat();

          seastar::promise<std::pair<uint64_t, ceph::real_time>> promise;
          auto future = promise.get_future();
          const int osd_id = target.primary_osd;

          auto p = std::make_shared<seastar::promise<std::pair<uint64_t, ceph::real_time>>>(
            std::move(promise));
          OpCompletion completion = [p](int r, std::vector<OSDOp>& ops) {
            if (r < 0) {
              p->set_exception(std::make_exception_ptr(
                std::system_error(-r, std::generic_category(), "stat failed")));
              return;
            }
            if (ops.empty() || ops[0].outdata.length() == 0) {
              p->set_exception(std::make_exception_ptr(
                std::runtime_error("stat reply empty")));
              return;
            }
            try {
              auto p_iter = ops[0].outdata.cbegin();
              uint64_t size;
              ceph::real_time mtime;
              ceph::decode(size, p_iter);
              ceph::decode(mtime, p_iter);
              p->set_value(std::make_pair(size, mtime));
            } catch (const ceph::buffer::error&) {
              p->set_exception(std::make_exception_ptr(
                std::runtime_error("stat reply decode failed")));
            }
          };

          return get_or_connect(target.primary_addr, osd_id)
            .then([this, tid, osd_id, c = std::move(completion), msg = std::move(msg)](
                  crimson::net::ConnectionRef conn) mutable {
              auto it = sessions.find(osd_id);
              ceph_assert(it != sessions.end());
              it->second.ops[tid] = std::move(c);
              return conn->send(std::move(msg));
            })
            .then([f = std::move(future)]() mutable {
              return std::move(f);
            });
        });
    });
}

void Objecter::ms_handle_connect(crimson::net::ConnectionRef conn,
                                seastar::shard_id prv_shard)
{
  if (!conn->peer_is_osd()) {
    return;
  }
  const int osd_id = conn->get_peer_id();
  logger().debug("ms_handle_connect: osd.{}", osd_id);

  auto it = sessions.find(osd_id);
  if (it != sessions.end()) {
    it->second.conn = conn;
    it->second.addr = conn->get_peer_addr();
  } else {
    sessions.emplace(osd_id,
                    OsdSession{osd_id, conn->get_peer_addr(), conn});
  }

  auto pend = pending_connects.find(osd_id);
  if (pend != pending_connects.end()) {
    pend->second.set_value(conn);
    pending_connects.erase(pend);
  }
}

void Objecter::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  if (!conn->peer_is_osd()) {
    return;
  }
  const int osd_id = conn->get_peer_id();
  logger().debug("ms_handle_reset: osd.{} is_replace={}", osd_id, is_replace);

  auto it = sessions.find(osd_id);
  if (it != sessions.end()) {
    if (it->second.conn.get() == conn.get()) {
      it->second.conn = nullptr;
    }
    const auto ex = std::make_exception_ptr(
      std::runtime_error(fmt::format("connection to osd.{} reset", osd_id)));
    for (auto& [tid, completion] : it->second.ops) {
      std::vector<OSDOp> empty_ops;
      completion(-ECONNRESET, empty_ops);
    }
    it->second.ops.clear();
  }

  auto pend = pending_connects.find(osd_id);
  if (pend != pending_connects.end()) {
    pend->second.set_exception(std::make_exception_ptr(
      std::runtime_error(fmt::format("connection to osd.{} reset", osd_id))));
    pending_connects.erase(pend);
  }
}

} // namespace crimson::osdc
