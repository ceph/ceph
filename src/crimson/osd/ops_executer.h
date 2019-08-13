// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <optional>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "common/dout.h"
#include "crimson/net/Fwd.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"

class PGLSFilter;
class OSDOp;

namespace ceph::osd {
class OpsExecuter {
  PGBackend::cached_os_t os;
  PG& pg;
  PGBackend& backend;
  ceph::os::Transaction txn;

  size_t num_read = 0;    ///< count read ops
  size_t num_write = 0;   ///< count update ops

  seastar::future<> do_op_call(class OSDOp& osd_op);

  template <class Func>
  auto do_const_op(Func&& f) {
    // TODO: pass backend as read-only
    return std::forward<Func>(f)(backend, std::as_const(*os));
  }

  template <class Func>
  auto do_read_op(Func&& f) {
    ++num_read;
    // TODO: pass backend as read-only
    return do_const_op(std::forward<Func>(f));
  }

  template <class Func>
  auto do_write_op(Func&& f) {
    ++num_write;
    return std::forward<Func>(f)(backend, *os, txn);
  }

  // PG operations are being provided with pg instead of os.
  template <class Func>
  auto do_pg_op(Func&& f) {
    return std::forward<Func>(f)(std::as_const(pg),
                                 std::as_const(os->oi.soid.get_namespace()));
  }

public:
  OpsExecuter(PGBackend::cached_os_t os, PG& pg)
    : os(std::move(os)), pg(pg), backend(pg.get_backend()) {
  }

  seastar::future<> do_osd_op(class OSDOp& osd_op);

  template <typename Func> seastar::future<> submit_changes(Func&& f) && {
    return std::forward<Func>(f)(std::move(txn), std::move(os));
  }
};

} // namespace ceph::osd
