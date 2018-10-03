// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OSDPerfMetricQuery.h"

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query) {
  return os << "simple";
}
