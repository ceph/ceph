
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


#ifndef MGR_PY_MODULE_H_
#define MGR_PY_MODULE_H_

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"

#include "common/cmdparse.h"
#include "common/LogEntry.h"

#include <vector>
#include <string>


class MgrPyModule;

/**
 * A Ceph CLI command description provided from a Python module
 */
class ModuleCommand {
public:
  std::string cmdstring;
  std::string helpstring;
  std::string perm;
  MgrPyModule *handler;
};

class MgrPyModule
{
private:
  const std::string module_name;
  PyObject *pClassInstance;
  PyThreadState *pMainThreadState;
  PyThreadState *pMyThreadState = nullptr;

  std::vector<ModuleCommand> commands;

  int load_commands();

public:
  MgrPyModule(const std::string &module_name, const std::string &sys_path, PyThreadState *main_ts);
  ~MgrPyModule();

  int load();
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
};

std::string handle_pyerror();

#endif

