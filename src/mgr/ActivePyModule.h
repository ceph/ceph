
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#pragma once

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"

#include "common/cmdparse.h"
#include "common/LogEntry.h"
#include "common/Mutex.h"
#include "common/Thread.h"
#include "mon/health_check.h"
#include "mgr/Gil.h"

#include "PyModuleRunner.h"

#include <vector>
#include <string>


class ActivePyModule;
class ActivePyModules;

class ActivePyModule : public PyModuleRunner
{
private:
  health_check_map_t health_checks;

  // Optional, URI exposed by plugins that implement serve()
  std::string uri;

public:
  ActivePyModule(PyModuleRef py_module_,
      LogChannelRef clog_)
    : PyModuleRunner(py_module_, clog_)
  {}

  int load(ActivePyModules *py_modules);
  void notify(const std::string &notify_type, const std::string &notify_id);
  void notify_clog(const LogEntry &le);

  int handle_command(
    const cmdmap_t &cmdmap,
    std::stringstream *ds,
    std::stringstream *ss);

  void set_health_checks(health_check_map_t&& c) {
    health_checks = std::move(c);
  }
  void get_health_checks(health_check_map_t *checks);

  void set_uri(const std::string &str)
  {
    uri = str;
  }

  std::string get_uri() const
  {
    return uri;
  }
};

std::string handle_pyerror();

