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
#include "common/Thread.h"
#include "common/Finisher.h"
#include "mon/health_check.h"
#include "mgr/Gil.h"

#include "PyModuleRunner.h"
#include "PyModule.h"

#include <vector>
#include <string>


class ActivePyModule;
class ActivePyModules;
class MgrSession;
class ModuleCommand;

class ActivePyModule : public PyModuleRunner
{
private:
  health_check_map_t health_checks;

  // Optional, URI exposed by plugins that implement serve()
  std::string uri;

  std::string m_command_perms;
  const MgrSession* m_session = nullptr;
  std::string fin_thread_name;
public:
  Finisher finisher; // per active module finisher to execute commands

public:
  ActivePyModule(const PyModuleRef &py_module_,
      LogChannelRef clog_)
    : PyModuleRunner(py_module_, clog_),
      fin_thread_name(std::string("m-fin-" + py_module->get_name()).substr(0,15)),
      finisher(g_ceph_context, thread_name, fin_thread_name)

  {
  }

  int load(ActivePyModules *py_modules);
  void notify(const std::string &notify_type, const std::string &notify_id);
  void notify_clog(const LogEntry &le);

  bool method_exists(const std::string &method) const;

  PyObject *dispatch_remote(
      const std::string &method,
      PyObject *args,
      PyObject *kwargs,
      std::string *err);

  int handle_command(
    const ModuleCommand& module_command,
    const MgrSession& session,
    const cmdmap_t &cmdmap,
    const bufferlist &inbuf,
    std::stringstream *ds,
    std::stringstream *ss);


  bool set_health_checks(health_check_map_t&& c) {
    // when health checks change a report is immediately sent to the monitors.
    // currently modules have static health check details, but this equality
    // test could be made smarter if too much noise shows up in the future.
    bool changed = health_checks != c;
    health_checks = std::move(c);
    return changed;
  }
  void get_health_checks(health_check_map_t *checks);
  void config_notify();

  void set_uri(const std::string &str)
  {
    uri = str;
  }

  std::string get_uri() const
  {
    return uri;
  }

  std::string get_fin_thread_name() const
  {
    return fin_thread_name;
  }

  bool is_authorized(const std::map<std::string, std::string>& arguments) const;

};


