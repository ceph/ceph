// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_SERVICE_DAEMON_TYPES_H
#define CEPH_RBD_MIRROR_SERVICE_DAEMON_TYPES_H

#include "include/int_types.h"
#include <iosfwd>
#include <string>
#include <boost/variant.hpp>

namespace rbd {
namespace mirror {
namespace service_daemon {

typedef uint64_t CalloutId;
const uint64_t CALLOUT_ID_NONE {0};

enum CalloutLevel {
  CALLOUT_LEVEL_INFO,
  CALLOUT_LEVEL_WARNING,
  CALLOUT_LEVEL_ERROR
};

std::ostream& operator<<(std::ostream& os, const CalloutLevel& callout_level);

typedef boost::variant<bool, uint64_t, std::string> AttributeValue;

} // namespace service_daemon
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_SERVICE_DAEMON_TYPES_H
