// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/operation.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

struct CommonClientRequest {

  static InterruptibleOperation::template interruptible_future<>
  do_recover_missing(
    Ref<PG> pg,
    const hobject_t& soid,
    const osd_reqid_t& reqid);

  static bool should_abort_request(
    const crimson::Operation& op, std::exception_ptr eptr);
};

} // namespace crimson::osd
