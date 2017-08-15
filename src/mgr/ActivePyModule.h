
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

#include <vector>
#include <string>


class ActivePyModule;
class ActivePyModules;

/**
 * A Ceph CLI command description provided from a Python module
 */
class ModuleCommand {
public:
  std::string cmdstring;
  std::string helpstring;
  std::string perm;
  ActivePyModule *handler;
};

class ServeThread : public Thread
{
  ActivePyModule *mod;

public:
  ServeThread(ActivePyModule *mod_)
    : mod(mod_) {}

  void *entry() override;
};

class ActivePyModule
{
private:
  const std::string module_name;

  // Passed in by whoever loaded our python module and looked up
  // the symbols in it.
  PyObject *pClass = nullptr;

  // Passed in by whoever created our subinterpreter for us
  SafeThreadState pMyThreadState = nullptr;

  // Populated when we construct our instance of pClass in load()
  PyObject *pClassInstance = nullptr;

  health_check_map_t health_checks;

  std::vector<ModuleCommand> commands;

  int load_commands();

  // Optional, URI exposed by plugins that implement serve()
  std::string uri;

public:
  ActivePyModule(const std::string &module_name,
      PyObject *pClass_,
      PyThreadState *my_ts);
  ~ActivePyModule();

  ServeThread thread;

  int load(ActivePyModules *py_modules);
  int serve();
  void shutdown();
  void notify(const std::string &notify_type, const std::string &notify_id);
  void notify_clog(const LogEntry &le);

  const std::vector<ModuleCommand> &get_commands() const
  {
    return commands;
  }

  const std::string &get_name() const
  {
    return module_name;
  }

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

