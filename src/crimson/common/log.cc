// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "log.h"

static std::array<seastar::logger, ceph_subsys_get_num()> loggers{
#define SUBSYS(name, log_level, gather_level) \
  seastar::logger(#name),
#define DEFAULT_SUBSYS(log_level, gather_level) \
  seastar::logger("none"),
  #include "common/subsys.h"
#undef SUBSYS
#undef DEFAULT_SUBSYS
};

namespace crimson {
seastar::logger& get_logger(int subsys) {
  assert(subsys < ceph_subsys_max);
  return loggers[subsys];
}
}
