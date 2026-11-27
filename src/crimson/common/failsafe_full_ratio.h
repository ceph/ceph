// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "crimson/common/config_proxy.h"

namespace crimson::common {

/**
 * osd_failsafe_full_ratio, as a fraction of the store's capacity.
 *
 * As in classic (OSDService::get_failsafe_full_ratio()), a value above 1 is
 * taken as a percentage: 97 means 0.97. The option has no maximum, so such a
 * value is accepted.
 *
 * Both the store's local full limit and the full state the OSD reports to
 * the monitor are derived from it, so that they agree.
 */
inline double get_failsafe_full_ratio()
{
  double ratio = get_conf<double>("osd_failsafe_full_ratio");
  if (ratio > 1.0) {
    ratio /= 100.0;
  }
  return ratio;
}

} // namespace crimson::common
