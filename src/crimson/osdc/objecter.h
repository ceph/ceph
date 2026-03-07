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

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_mutex.hh>

#include "include/ceph_fs.h"
#include "include/types.h"
#include "include/utime.h"

#include "common/ceph_time.h"
#include "crimson/common/throttle.h"
#include "crimson/common/type_helpers.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

#include "msg/MessageRef.h"
#include "msg/msg_types.h"
#include "messages/MOSDOp.h"
#include "osd/OSDMap.h"

class MOSDOpReply;
class MOSDMap;

namespace crimson::mon {
class Client;
}

namespace crimson::osdc {

/// Result of mapping an object to its target OSD
struct OsdTarget {
  pg_t pgid;           ///< actual pg (after raw_pg_to_pg)
  pg_t raw_pg;         ///< raw pg (for hobject_t hash - must match classic Objecter)
  int primary_osd;
  entity_addr_t primary_addr;
};

/// Completion callback for op reply: (result, ops) -> void
using OpCompletion = std::function<void(int r, std::vector<OSDOp>&)>;

/// Per-OSD session holding connection and in-flight ops
struct OsdSession {
  int osd_id = -1;
  entity_addr_t addr;
  crimson::net::ConnectionRef conn;

  /// In-flight ops by tid (read, write, stat)
  std::unordered_map<ceph_tid_t, OpCompletion> ops;

  /// Throttle for write bytes in flight
  crimson::common::Throttle write_throttle{64 * 1024 * 1024};  // 64 MiB default
};

/**
 * Crimson Objecter - Seastar-native RADOS client objecter.
 *
 * - Registers as Dispatcher for MOSDOpReply (stub) and MOSDMap
 * - Subscribes to osdmap via MonClient, receives and stores OSDMap
 * - Exposes with_osdmap() for pool resolution and PG calculation
 * - Exposes calc_target() for object -> PG -> primary OSD mapping
 * - Exposes get_or_connect() and send_to_osd() for OSD connection and message send
 */
class Objecter : public crimson::net::Dispatcher {
public:
  Objecter(crimson::net::Messenger& msgr,
           crimson::mon::Client& monc);

  ~Objecter() override;

  seastar::future<> start();
  seastar::future<> stop();

  /// Invoke a callback with read access to the current OSDMap.
  /// Returns a future with the result of the callback. OSDMap may be empty
  /// (epoch 0) until the first map is received from the monitor.
  template<typename Func>
  seastar::futurize_t<std::invoke_result_t<Func, const OSDMap&>>
  with_osdmap(Func&& f) const {
    return seastar::with_shared(
      osdmap_mutex,
      [this, f = std::forward<Func>(f)]() {
        return seastar::futurize_invoke(f, static_cast<const OSDMap&>(*osdmap));
      });
  }

  /// Map object + locator to target PG and primary OSD address.
  /// Uses OSDMap::object_locator_to_pg, raw_pg_to_pg, pg_to_up_acting_osds.
  /// Returns nullopt if pool does not exist or mapping fails.
  seastar::future<std::optional<OsdTarget>> calc_target(const object_t& oid,
                                                       const object_locator_t& oloc) const;

  /// Get or create connection to OSD. Resolves when handshake completes.
  seastar::future<crimson::net::ConnectionRef>
  get_or_connect(const entity_addr_t& addr, int osd_id);

  /// Send message to OSD (connects if needed). Requires OsdTarget from calc_target.
  seastar::future<> send_to_osd(const OsdTarget& target, MessageURef msg);

  /// Read object data. Returns bufferlist on success; completes with
  /// exception on error (e.g. -ENOENT).
  seastar::future<ceph::bufferlist> read(const object_t& oid,
                                         const object_locator_t& oloc,
                                         uint64_t off, uint64_t len,
                                         snapid_t snap = CEPH_NOSNAP);

  /// Write object data. Throttled per OSD.
  seastar::future<> write(const object_t& oid,
                         const object_locator_t& oloc,
                         uint64_t off, ceph::bufferlist&& bl,
                         snapid_t snap = CEPH_NOSNAP);

  /// Stat object. Returns (size, mtime).
  seastar::future<std::pair<uint64_t, ceph::real_time>> stat(
      const object_t& oid,
      const object_locator_t& oloc,
      snapid_t snap = CEPH_NOSNAP);

  /// Discard (zero) object range. For RBD trim/UNMAP.
  seastar::future<> discard(const object_t& oid,
                           const object_locator_t& oloc,
                           uint64_t off, uint64_t len,
                           snapid_t snap = CEPH_NOSNAP);

  /// Write zeroes to object range. For RBD write_zeroes.
  seastar::future<> write_zeroes(const object_t& oid,
                                const object_locator_t& oloc,
                                uint64_t off, uint64_t len,
                                snapid_t snap = CEPH_NOSNAP);

  /// Compare-and-write: compare at cmp_off with cmp_bl; if match, write
  /// write_bl at write_off. Returns -EILSEQ on mismatch.
  seastar::future<> compare_and_write(const object_t& oid,
                                      const object_locator_t& oloc,
                                      uint64_t cmp_off, ceph::bufferlist&& cmp_bl,
                                      uint64_t write_off, ceph::bufferlist&& write_bl,
                                      snapid_t snap = CEPH_NOSNAP);

  /// Execute cls method on object. Returns output bufferlist.
  /// For use by librbd_crimson (cls/rbd get_size, get_features, etc.).
  seastar::future<ceph::bufferlist> exec(const object_t& oid,
                                        const object_locator_t& oloc,
                                        std::string_view cname,
                                        std::string_view method,
                                        ceph::bufferlist&& indata,
                                        snapid_t snap = CEPH_NOSNAP);

  /// Wait until OSDMap has been received (epoch > 0).
  seastar::future<> wait_for_osdmap();

  /// Set client incarnation for MOSDOp reqid. Call before first op to avoid
  /// duplicate detection with previous sessions. Default 0.
  void set_client_incarnation(int inc) { client_inc = inc; }

#ifdef UNIT_TESTS_BUILT
  /// Inject OSDMap for unit tests (bypasses monitor). Also fulfills wait_for_osdmap.
  seastar::future<> inject_osdmap_for_test(std::unique_ptr<OSDMap> m);
#endif

  // Dispatcher interface
  std::optional<seastar::future<>>
  ms_dispatch(crimson::net::ConnectionRef conn,
             MessageRef m) override;

  void ms_handle_connect(crimson::net::ConnectionRef conn,
                        seastar::shard_id prv_shard) override;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) override;

private:
  crimson::net::Messenger& msgr;
  crimson::mon::Client& monc;

  std::unique_ptr<OSDMap> osdmap;
  mutable seastar::shared_mutex osdmap_mutex;

  std::unordered_map<int, OsdSession> sessions;
  std::unordered_map<int, seastar::shared_promise<crimson::net::ConnectionRef>>
    pending_connects;

  seastar::gate dispatch_gate;
  bool started = false;
  int client_inc = 0;
  ceph_tid_t next_tid = 1;

  std::optional<seastar::shared_promise<>> osdmap_ready;
  bool osdmap_ready_fulfilled = false;

  seastar::future<> handle_osd_map(Ref<MOSDMap> m);
  void handle_osd_op_reply(crimson::net::ConnectionRef conn, MOSDOpReply* m);
  seastar::future<> maybe_request_map();
};

} // namespace crimson::osdc
