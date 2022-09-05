// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/operation.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "osd/osd_op_util.h"

namespace crimson::osd {

using kickback_recovery_ertr = crimson::errorator<
  crimson::ct_error::eagain>;

struct CommonClientRequest {
  static InterruptibleOperation::template interruptible_errorated_future<kickback_recovery_ertr>
  do_recover_missing(Ref<PG>& pg, const hobject_t& soid, const OpInfo& op_info);

  static bool should_abort_request(
    const crimson::Operation& op, std::exception_ptr eptr);
};

} // namespace crimson::osd
