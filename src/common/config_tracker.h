// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <string>

// @ConfigTracker is queried to see if any added observers is tracking one or
// more changed settings.
//
// this class is introduced in hope to decouple @c md_config_t from any instantiated
// class of @c ObserverMgr, as what the former wants is but @c is_tracking(), and to
// make ObserverMgr a template parameter of md_config_t's methods just complicates
// the dependencies between header files, and slows down the compiling.
class ConfigTracker {
public:
  virtual ~ConfigTracker() = default;
  virtual bool is_tracking(const std::string& name) const = 0;
};
