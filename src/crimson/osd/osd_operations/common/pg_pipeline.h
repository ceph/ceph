// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

class CommonPGPipeline {
protected:
  friend class InternalClientRequest;

  OrderedExclusivePhase wait_for_active = {
    "CommonPGPipeline:::wait_for_active"
  };
  OrderedExclusivePhase recover_missing = {
    "CommonPGPipeline::recover_missing"
  };
  OrderedExclusivePhase get_obc = {
    "CommonPGPipeline::get_obc"
  };
  OrderedExclusivePhase process = {
    "CommonPGPipeline::process"
  };
};

} // namespace crimson::osd
