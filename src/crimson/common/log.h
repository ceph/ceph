// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/util/log.hh>
#include "common/subsys_types.h"

namespace crimson {
seastar::logger& get_logger(int subsys);
static inline seastar::log_level to_log_level(int level) {
  if (level < 0) {
    return seastar::log_level::error;
  } else if (level < 1) {
    return seastar::log_level::warn;
  } else if (level < 5) {
    return seastar::log_level::info;
  } else if (level <= 20) {
    return seastar::log_level::debug;
  } else {
    return seastar::log_level::trace;
  }
}
}
