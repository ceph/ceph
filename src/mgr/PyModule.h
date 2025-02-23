// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <boost/optional.hpp>
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/perf_counters.h"
#include "Python.h"
#include "Gil.h"
#include "mon/MgrMap.h"


class MonClient;

std::string handle_pyerror(bool generate_crash_dump = false,
			   std::string module = {},
			   std::string caller = {});

std::string peek_pyerror();
enum {
  l_pym_first = 10000,
  l_pym_cpu,
  l_pym_mem,
  l_pym_last
};

/**
 * A Ceph CLI command description provided from a Python module
 */
class ModuleCommand {
public:
  std::string cmdstring;
  std::string helpstring;
  std::string perm;
  bool polling;

  // Call the ActivePyModule of this name to handle the command
  std::string module_name;
};

class PyModule
{
  mutable ceph::mutex lock = ceph::make_mutex("PyModule::lock");
private:
  const std::string module_name;
  int load_subclass_of(const char* class_name, PyObject** py_class);

  // Did the MgrMap identify this module as one that should run?
  bool enabled = false;

  // Did the MgrMap flag this module as always on?
  bool always_on = false;

  // Did we successfully import this python module and look up symbols?
  // (i.e. is it possible to instantiate a MgrModule subclass instance?)
  bool loaded = false;

  // Did the module identify itself as being able to run?
  // (i.e. should we expect instantiating and calling serve() to work?)
  bool can_run = false;

  // Did the module encounter an unexpected error while running?
  // (e.g. throwing an exception from serve())
  bool failed = false;

  // Populated if loaded, can_run or failed indicates a problem
  std::string error_string;

  // Helper for loading MODULE_OPTIONS and COMMANDS members
  int walk_dict_list(
      const std::string &attr_name,
      std::function<int(PyObject*)> fn);

  int load_commands();
  std::vector<ModuleCommand> commands;

  int register_options(PyObject *cls);
  int load_options();
  std::map<std::string, MgrMap::ModuleOption> options;

  int load_notify_types();
  std::set<std::string> notify_types;

public:
  static std::string mgr_store_prefix;

  SafeThreadState pMyThreadState;
  PyObject *pClass = nullptr;
  PyObject *pStandbyClass = nullptr;

  explicit PyModule(const std::string &module_name_);

  ~PyModule();

  bool is_option(const std::string &option_name);
  const std::map<std::string,MgrMap::ModuleOption>& get_options() const {
    return options;
  }

  PyObject *get_typed_option_value(
    const std::string& option,
    const std::string& value);

  int load(PyThreadState *pMainThreadState);
  static PyObject* init_ceph_logger();
  static PyObject* init_ceph_module();

  void set_enabled(const bool enabled_)
  {
    enabled = enabled_;
  }

  void set_always_on(const bool always_on_) {
    always_on = always_on_;
  }

  /**
   * Extend `out` with the contents of `this->commands`
   */
  void get_commands(std::vector<ModuleCommand> *out) const
  {
    std::lock_guard l(lock);
    ceph_assert(out != nullptr);
    out->insert(out->end(), commands.begin(), commands.end());
  }


  /**
   * Mark the module as failed, recording the reason in the error
   * string.
   */
  void fail(const std::string &reason)
  {
    std::lock_guard l(lock);
    failed = true;
    error_string = reason;
  }

  bool is_enabled() const {
    std::lock_guard l(lock);
    return enabled || always_on;
  }

  bool is_failed() const { std::lock_guard l(lock) ; return failed; }
  bool is_loaded() const { std::lock_guard l(lock) ; return loaded; }
  bool is_always_on() const { std::lock_guard l(lock) ; return always_on; }

  bool should_notify(const std::string& notify_type) const {
    return notify_types.count(notify_type);
  }

  const std::string &get_name() const {
    return module_name;
  }
  std::string get_error_string() const {
    std::lock_guard l(lock) ; return error_string;
  }
  bool get_can_run() const {
    std::lock_guard l(lock) ; return can_run;
  }

  // perf counters for python modules
  std::unique_ptr<PerfCounters> perfcounter;
  std::shared_ptr<PyObject*> process_obj;
  pid_t tid = 0;

  void set_thread_tid(pid_t tid_) {
    tid = tid_;
  }
  void set_process_obj(std::shared_ptr<PyObject*> process_obj_) {
    process_obj = process_obj_;
  }

  PyObject* get_process_obj() {
    return *process_obj;
  }

  int pymodule_perf_counters_start(CephContext *cct) {
    PerfCountersBuilder pcb(cct, module_name, l_pym_first, l_pym_last);
    pcb.add_u64_counter(l_pym_cpu, "cpu", "CPU time", "cpu", 0);
    pcb.add_u64_counter(l_pym_mem, "mem", "Memory usage", "mem", 0);
    perfcounter = std::unique_ptr<PerfCounters>(pcb.create_perf_counters());
    cct->get_perfcounters_collection()->add(perfcounter.get());

    return 0;
  }

  void update_perf_counters();
  void _update_cpu_perf_counter();
  void _update_memory_perf_counter();

  std::pair<uint64_t, uint64_t> get_perf_counters() {
    ceph_assert(perfcounter);
    return std::make_pair(perfcounter->get(l_pym_cpu), perfcounter->get(l_pym_mem));
  }

};

typedef std::shared_ptr<PyModule> PyModuleRef;

class PyModuleConfig {
public:
  mutable ceph::mutex lock = ceph::make_mutex("PyModuleConfig::lock");
  std::map<std::string, std::string> config;

  PyModuleConfig();
  
  PyModuleConfig(PyModuleConfig &mconfig);
  
  ~PyModuleConfig();

  std::pair<int, std::string> set_config(
    MonClient *monc,
    const std::string &module_name,
    const std::string &key, const std::optional<std::string>& val);

};
