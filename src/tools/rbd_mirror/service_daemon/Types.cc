// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/service_daemon/Types.h"
#include <iostream>

namespace rbd {
namespace mirror {
namespace service_daemon {

std::ostream& operator<<(std::ostream& os, const CalloutLevel& callout_level) {
  switch (callout_level) {
  case CALLOUT_LEVEL_INFO:
    os << "info";
    break;
  case CALLOUT_LEVEL_WARNING:
    os << "warning";
    break;
  case CALLOUT_LEVEL_ERROR:
    os << "error";
    break;
  }
  return os;
}

} // namespace service_daemon
} // namespace mirror
} // namespace rbd

