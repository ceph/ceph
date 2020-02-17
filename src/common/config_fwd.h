// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#ifdef WITH_SEASTAR
namespace crimson::common {
  class ConfigProxy;
}
using ConfigProxy = crimson::common::ConfigProxy;
#else
class ConfigProxy;
#endif
