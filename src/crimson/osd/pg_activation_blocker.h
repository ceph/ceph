// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "crimson/common/operation.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

class PG;

class PGActivationBlocker : public crimson::BlockerT<PGActivationBlocker> {
  PG *pg;

  const spg_t pgid;
  seastar::shared_promise<> p;

protected:
  void dump_detail(Formatter *f) const;

public:
  static constexpr const char *type_name = "PGActivationBlocker";

  PGActivationBlocker(PG *pg) : pg(pg) {}
  void unblock();
  seastar::future<> wait(PGActivationBlocker::BlockingEvent::TriggerI&&);
  seastar::future<> stop();
};

} // namespace crimson::osd
