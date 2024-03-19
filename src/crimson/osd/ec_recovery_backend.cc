// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/future.hh>
#include <seastar/core/do_with.hh>

#include "crimson/osd/ec_recovery_backend.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "msg/Message.h"
#include "osd/osd_types_fmt.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

RecoveryBackend::interruptible_future<>
ECRecoveryBackend::recover_object(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
ECRecoveryBackend::handle_recovery_op(
  Ref<MOSDFastDispatchOp> m,
  crimson::net::ConnectionXcoreRef conn)
{
  switch (m->get_header().type) {
  default:
    // delegate to parent class for handling backend-agnostic recovery ops.
    return RecoveryBackend::handle_recovery_op(std::move(m), conn);
  }
}
