// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#ifdef WITH_SEASTAR
namespace ceph::common {
  class ConfigProxy;
}
using ConfigProxy = ceph::common::ConfigProxy;
#else
class ConfigProxy;
#endif
