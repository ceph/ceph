// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_context.h"

#include <mutex>
#include <iostream>

#include <pthread.h>

#include <boost/algorithm/string.hpp>

#include "include/common_fwd.h"
#include "include/mempool.h"
#include "include/stringify.h"
#include "common/admin_socket.h"
#include "common/code_environment.h"
#include "common/ceph_mutex.h"
#include "common/debug.h"
#include "common/config.h"
#include "common/ceph_crypto.h"
#include "common/hostname.h"
#include "common/HeartbeatMap.h"
#include "common/errno.h"
#include "common/Graylog.h"
#ifdef CEPH_DEBUG_MUTEX
#include "common/lockdep.h"
#endif

#include "log/Log.h"

#include "auth/Crypto.h"
#include "include/str_list.h"
#include "common/config.h"
#include "common/config_obs.h"
#include "common/PluginRegistry.h"
#include "common/valgrind.h"
#include "include/spinlock.h"
#if !(defined(WITH_SEASTAR) && !defined(WITH_ALIEN))
#include "mon/MonMap.h"
#endif

// for CINIT_FLAGS
#include "common/common_init.h"

#include <iostream>
#include <pthread.h>

using namespace std::literals;

using ceph::bufferlist;
using ceph::HeartbeatMap;


#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
namespace crimson::common {
CephContext::CephContext()
  : _conf{crimson::common::local_conf()},
    _perf_counters_collection{crimson::common::local_perf_coll()},
    _crypto_random{std::make_unique<CryptoRandom>()}
{}

// define the dtor in .cc as CryptoRandom is an incomplete type in the header
CephContext::~CephContext()
{}

uint32_t CephContext::get_module_type() const
{
  return CEPH_ENTITY_TYPE_OSD;
}

CryptoRandom* CephContext::random() const
{
  return _crypto_random.get();
}

CephContext* CephContext::get()
{
  ++nref;
  return this;
}

void CephContext::put()
{
  if (--nref == 0) {
    delete this;
  }
}

PerfCountersCollectionImpl* CephContext::get_perfcounters_collection()
{
  return _perf_counters_collection.get_perf_collection();
}

}
#else  // WITH_SEASTAR
namespace {

#ifdef CEPH_DEBUG_MUTEX
class LockdepObs : public md_config_obs_t {
public:
  explicit LockdepObs(CephContext *cct)
    : m_cct(cct), m_registered(false), lock(ceph::make_mutex("lock_dep_obs")) {
  }
  ~LockdepObs() override {
    if (m_registered) {
      lockdep_unregister_ceph_context(m_cct);
    }
  }

  const char** get_tracked_conf_keys() const override {
    static const char *KEYS[] = {"lockdep", NULL};
    return KEYS;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override {
    std::unique_lock locker(lock);
    if (conf->lockdep && !m_registered) {
      lockdep_register_ceph_context(m_cct);
      m_registered = true;
    } else if (!conf->lockdep && m_registered) {
      lockdep_unregister_ceph_context(m_cct);
      m_registered = false;
    }
  }
private:
  CephContext *m_cct;
  bool m_registered;
  ceph::mutex lock;
};
#endif // CEPH_DEBUG_MUTEX

class MempoolObs : public md_config_obs_t,
		  public AdminSocketHook {
  CephContext *cct;
  ceph::mutex lock;

public:
  explicit MempoolObs(CephContext *cct)
    : cct(cct), lock(ceph::make_mutex("mem_pool_obs")) {
    cct->_conf.add_observer(this);
    int r = cct->get_admin_socket()->register_command(
      "dump_mempools",
      this,
      "get mempool stats");
    ceph_assert(r == 0);
  }
  ~MempoolObs() override {
    cct->_conf.remove_observer(this);
    cct->get_admin_socket()->unregister_commands(this);
  }

  // md_config_obs_t
  const char** get_tracked_conf_keys() const override {
    static const char *KEYS[] = {
      "mempool_debug",
      NULL
    };
    return KEYS;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override {
    std::unique_lock locker(lock);
    if (changed.count("mempool_debug")) {
      mempool::set_debug_mode(cct->_conf->mempool_debug);
    }
  }

  // AdminSocketHook
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist& inbl,
	   ceph::Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    if (command == "dump_mempools") {
      f->open_object_section("mempools");
      mempool::dump(f);
      f->close_section();
      return 0;
    }
    return -ENOSYS;
  }
};

} // anonymous namespace

namespace ceph::common {
class CephContextServiceThread : public Thread
{
public:
  explicit CephContextServiceThread(CephContext *cct)
    : _reopen_logs(false), _exit_thread(false), _cct(cct)
  {
  }

  ~CephContextServiceThread() override {}

  void *entry() override
  {
    while (1) {
      std::unique_lock l(_lock);
      if (_exit_thread) {
        break;
      }

      if (_cct->_conf->heartbeat_interval) {
        auto interval = ceph::make_timespan(_cct->_conf->heartbeat_interval);
        _cond.wait_for(l, interval);
      } else
        _cond.wait(l);

      if (_exit_thread) {
        break;
      }

      if (_reopen_logs) {
        _cct->_log->reopen_log_file();
        _reopen_logs = false;
      }
      _cct->_heartbeat_map->check_touch_file();

      // refresh the perf coutners
      _cct->_refresh_perf_values();
    }
    return NULL;
  }

  void reopen_logs()
  {
    std::lock_guard l(_lock);
    _reopen_logs = true;
    _cond.notify_all();
  }

  void exit_thread()
  {
    std::lock_guard l(_lock);
    _exit_thread = true;
    _cond.notify_all();
  }

private:
  ceph::mutex _lock = ceph::make_mutex("CephContextServiceThread::_lock");
  ceph::condition_variable _cond;
  bool _reopen_logs;
  bool _exit_thread;
  CephContext *_cct;
};
}

/**
 * observe logging config changes
 *
 * The logging subsystem sits below most of the ceph code, including
 * the config subsystem, to keep it simple and self-contained.  Feed
 * logging-related config changes to the log.
 */
class LogObs : public md_config_obs_t {
  ceph::logging::Log *log;
  ceph::mutex lock;

public:
  explicit LogObs(ceph::logging::Log *l)
    : log(l), lock(ceph::make_mutex("log_obs")) {
  }

  const char** get_tracked_conf_keys() const override {
    static const char *KEYS[] = {
      "log_file",
      "log_max_new",
      "log_max_recent",
      "log_to_file",
      "log_to_syslog",
      "err_to_syslog",
      "log_stderr_prefix",
      "log_to_stderr",
      "err_to_stderr",
      "log_to_graylog",
      "err_to_graylog",
      "log_graylog_host",
      "log_graylog_port",
      "log_to_journald",
      "err_to_journald",
      "log_coarse_timestamps",
      "fsid",
      "host",
      NULL
    };
    return KEYS;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override {
    std::unique_lock locker(lock);
    // stderr
    if (changed.count("log_to_stderr") || changed.count("err_to_stderr")) {
      int l = conf->log_to_stderr ? 99 : (conf->err_to_stderr ? -1 : -2);
      log->set_stderr_level(l, l);
    }

    // syslog
    if (changed.count("log_to_syslog")) {
      int l = conf->log_to_syslog ? 99 : (conf->err_to_syslog ? -1 : -2);
      log->set_syslog_level(l, l);
    }

    // file
    if (changed.count("log_file") ||
	changed.count("log_to_file")) {
      if (conf->log_to_file) {
	log->set_log_file(conf->log_file);
      } else {
	log->set_log_file({});
      }
      log->reopen_log_file();
    }

    if (changed.count("log_stderr_prefix")) {
      log->set_log_stderr_prefix(conf.get_val<std::string>("log_stderr_prefix"));
    }

    if (changed.count("log_max_new")) {

      log->set_max_new(conf->log_max_new);
    }

    if (changed.count("log_max_recent")) {
      log->set_max_recent(conf->log_max_recent);
    }

    // graylog
    if (changed.count("log_to_graylog") || changed.count("err_to_graylog")) {
      int l = conf->log_to_graylog ? 99 : (conf->err_to_graylog ? -1 : -2);
      log->set_graylog_level(l, l);

      if (conf->log_to_graylog || conf->err_to_graylog) {
	log->start_graylog(conf->host, conf.get_val<uuid_d>("fsid"));
      } else if (! (conf->log_to_graylog && conf->err_to_graylog)) {
	log->stop_graylog();
      }
    }

    if (log->graylog() && (changed.count("log_graylog_host") || changed.count("log_graylog_port"))) {
      log->graylog()->set_destination(conf->log_graylog_host, conf->log_graylog_port);
    }

    // journald
    if (changed.count("log_to_journald") || changed.count("err_to_journald")) {
      int l = conf.get_val<bool>("log_to_journald") ? 99 : (conf.get_val<bool>("err_to_journald") ? -1 : -2);
      log->set_journald_level(l, l);

      if (l > -2) {
        log->start_journald_logger();
      } else {
        log->stop_journald_logger();
      }
    }

    if (changed.find("log_coarse_timestamps") != changed.end()) {
      log->set_coarse_timestamps(conf.get_val<bool>("log_coarse_timestamps"));
    }

    // metadata
    if (log->graylog() && changed.count("host")) {
      log->graylog()->set_hostname(conf->host);
    }

    if (log->graylog() && changed.count("fsid")) {
      log->graylog()->set_fsid(conf.get_val<uuid_d>("fsid"));
    }
  }
};


namespace ceph::common {
// cct config watcher
class CephContextObs : public md_config_obs_t {
  CephContext *cct;

public:
  explicit CephContextObs(CephContext *cct) : cct(cct) {}

  const char** get_tracked_conf_keys() const override {
    static const char *KEYS[] = {
      "enable_experimental_unrecoverable_data_corrupting_features",
      "crush_location",
      "container_image",  // just so we don't hear complaints about it!
      NULL
    };
    return KEYS;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override {
    if (changed.count(
	  "enable_experimental_unrecoverable_data_corrupting_features")) {
      std::lock_guard lg(cct->_feature_lock);

      cct->_experimental_features.clear();
      auto add_experimental_feature = [this] (auto feature) {
        cct->_experimental_features.emplace(std::string{feature});
      };
      for_each_substr(conf->enable_experimental_unrecoverable_data_corrupting_features,
          ";,= \t", add_experimental_feature);

      if (getenv("CEPH_DEV") == NULL) {
        if (!cct->_experimental_features.empty()) {
          if (cct->_experimental_features.count("*")) {
            lderr(cct) << "WARNING: all dangerous and experimental features are enabled." << dendl;
          } else {
            lderr(cct) << "WARNING: the following dangerous and experimental features are enabled: "
              << cct->_experimental_features << dendl;
          }
        }
      }

    }
    if (changed.count("crush_location")) {
      cct->crush_location.update_from_conf();
    }
  }
};
// perfcounter hooks

class CephContextHook : public AdminSocketHook {
  CephContext *m_cct;

public:
  explicit CephContextHook(CephContext *cct) : m_cct(cct) {}

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist& inbl,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    try {
      return m_cct->do_command(command, cmdmap, f, errss, &out);
    } catch (const bad_cmd_get& e) {
      return -EINVAL;
    }
  }
};


bool CephContext::check_experimental_feature_enabled(const std::string& feat)
{
  std::stringstream message;
  bool enabled = check_experimental_feature_enabled(feat, &message);
  lderr(this) << message.str() << dendl;
  return enabled;
}

bool CephContext::check_experimental_feature_enabled(const std::string& feat,
						     std::ostream *message)
{
  std::unique_lock<ceph::spinlock> lg(_feature_lock);

  bool enabled = (_experimental_features.count(feat) ||
		  _experimental_features.count("*"));

  if (enabled) {
    (*message) << "WARNING: experimental feature '" << feat << "' is enabled\n";
    (*message) << "Please be aware that this feature is experimental, untested,\n";
    (*message) << "unsupported, and may result in data corruption, data loss,\n";
    (*message) << "and/or irreparable damage to your cluster.  Do not use\n";
    (*message) << "feature with important data.\n";
  } else {
    (*message) << "*** experimental feature '" << feat << "' is not enabled ***\n";
    (*message) << "This feature is marked as experimental, which means it\n";
    (*message) << " - is untested\n";
    (*message) << " - is unsupported\n";
    (*message) << " - may corrupt your data\n";
    (*message) << " - may break your cluster is an unrecoverable fashion\n";
    (*message) << "To enable this feature, add this to your ceph.conf:\n";
    (*message) << "  enable experimental unrecoverable data corrupting features = " << feat << "\n";
  }
  return enabled;
}

int CephContext::do_command(std::string_view command, const cmdmap_t& cmdmap,
			    Formatter *f,
			    std::ostream& ss,
			    bufferlist *out)
{
  try {
    return _do_command(command, cmdmap, f, ss, out);
  } catch (const bad_cmd_get& e) {
    ss << e.what();
    return -EINVAL;
  }
}

#pragma GCC push_options
#pragma GCC optimize ("O0")
static void leak_some_memory() {
  volatile char *foo = new char[1234];
  (void)foo;
}
#pragma GCC pop_options

int CephContext::_do_command(
  std::string_view command, const cmdmap_t& cmdmap,
  Formatter *f,
  std::ostream& ss,
  bufferlist *out)
{
  int r = 0;
  lgeneric_dout(this, 1) << "do_command '" << command << "' '" << cmdmap << "'"
			 << dendl;
  ceph_assert_always(!(command == "assert" && _conf->debug_asok_assert_abort));
  if (command == "abort") {
    if (_conf->debug_asok_assert_abort) {
      ceph_abort();
    } else {
      return -EPERM;
    }
  }
  if (command == "leak_some_memory") {
    leak_some_memory();
  }
  else if (command == "perfcounters_dump" || command == "1" ||
      command == "perf dump") {
    std::string logger;
    std::string counter;
    cmd_getval(cmdmap, "logger", logger);
    cmd_getval(cmdmap, "counter", counter);
    _perf_counters_collection->dump_formatted(f, false, false, logger, counter);
  }
  else if (command == "perfcounters_schema" || command == "2" ||
    command == "perf schema") {
    _perf_counters_collection->dump_formatted(f, true, false);
  }
  else if (command == "counter dump") {
    _perf_counters_collection->dump_formatted(f, false, true);
  }
  else if (command == "counter schema") {
    _perf_counters_collection->dump_formatted(f, true, true);
  }
  else if (command == "perf histogram dump") {
    std::string logger;
    std::string counter;
    cmd_getval(cmdmap, "logger", logger);
    cmd_getval(cmdmap, "counter", counter);
    _perf_counters_collection->dump_formatted_histograms(f, false, logger,
                                                         counter);
  }
  else if (command == "perf histogram schema") {
    _perf_counters_collection->dump_formatted_histograms(f, true);
  }
  else if (command == "perf reset") {
    std::string var;
    std::string section(command);
    f->open_object_section(section.c_str());
    if (!cmd_getval(cmdmap, "var", var)) {
      f->dump_string("error", "syntax error: 'perf reset <var>'");
    } else {
     if(!_perf_counters_collection->reset(var))
        f->dump_stream("error") << "Not find: " << var;
     else
       f->dump_string("success", std::string(command) + ' ' + var);
    }
    f->close_section();
  }
  else {
    std::string section(command);
    boost::replace_all(section, " ", "_");
    f->open_object_section(section.c_str());
    if (command == "config show") {
      _conf.show_config(f);
    }
    else if (command == "config unset") {
      std::string var;
      if (!(cmd_getval(cmdmap, "var", var))) {
	r = -EINVAL;
      } else {
        r = _conf.rm_val(var.c_str());
        if (r < 0 && r != -ENOENT) {
          ss << "error unsetting '" << var << "': "
	     << cpp_strerror(r);
        } else {
          _conf.apply_changes(&ss);
	  r = 0;
        }
      }

    }
    else if (command == "config set") {
      std::string var;
      std::vector<std::string> val;

      if (!(cmd_getval(cmdmap, "var", var)) ||
          !(cmd_getval(cmdmap, "val", val))) {
	r = -EINVAL;
      } else {
	// val may be multiple words
	auto valstr = str_join(val, " ");
        r = _conf.set_val(var.c_str(), valstr.c_str());
        if (r < 0) {
          ss << "error setting '" << var << "' to '" << valstr << "': "
	     << cpp_strerror(r);
        } else {
	  std::stringstream ss;
          _conf.apply_changes(&ss);
	  f->dump_string("success", ss.str());
        }
      }
    } else if (command == "config get") {
      std::string var;
      if (!cmd_getval(cmdmap, "var", var)) {
	r = -EINVAL;
      } else {
	char buf[4096];
	// FIPS zeroization audit 20191115: this memset is not security related.
	memset(buf, 0, sizeof(buf));
	char *tmp = buf;
	r = _conf.get_val(var.c_str(), &tmp, sizeof(buf));
	if (r < 0) {
	  ss << "error getting '" << var << "': " << cpp_strerror(r);
	} else {
	  f->dump_string(var.c_str(), buf);
	}
      }
    } else if (command == "config help") {
      std::string var;
      if (cmd_getval(cmdmap, "var", var)) {
        // Output a single one
        std::string key = ConfFile::normalize_key_name(var);
	auto schema = _conf.get_schema(key);
        if (!schema) {
          ss << "Setting not found: '" << key << "'";
	  r = -ENOENT;
        } else {
          f->dump_object("option", *schema);
        }
      } else {
        // Output all
        f->open_array_section("options");
        for (const auto &option : ceph_options) {
          f->dump_object("option", option);
        }
        f->close_section();
      }
    } else if (command == "config diff") {
      f->open_object_section("diff");
      _conf.diff(f);
      f->close_section(); // unknown
    } else if (command == "config diff get") {
      std::string setting;
      f->open_object_section("diff");
      _conf.diff(f, setting);
      f->close_section(); // unknown
    }
    else if (command == "injectargs") {
      std::vector<std::string> argsvec;
      cmd_getval(cmdmap, "injected_args", argsvec);
      if (!argsvec.empty()) {
	auto args = joinify<std::string>(argsvec.begin(), argsvec.end(), " ");
	r = _conf.injectargs(args, &ss);
      }
    }
    else if (command == "log flush") {
      _log->flush();
    }
    else if (command == "log dump") {
      _log->dump_recent();
    }
    else if (command == "log reopen") {
      _log->reopen_log_file();
    }
    else {
      ceph_abort_msg("registered under wrong command?");    
    }
    f->close_section();
  }
  lgeneric_dout(this, 1) << "do_command '" << command << "' '" << cmdmap
		         << "' result is " << out->length() << " bytes" << dendl;
  return r;
}

CephContext::CephContext(uint32_t module_type_,
                         enum code_environment_t code_env,
                         int init_flags_)
  : CephContext(module_type_, create_options{code_env, init_flags_, nullptr})
{}

CephContext::CephContext(uint32_t module_type_,
			 const create_options& options)
  : nref(1),
    _conf{options.code_env == CODE_ENVIRONMENT_DAEMON},
    _log(NULL),
    _module_type(module_type_),
    _init_flags(options.init_flags),
    _set_uid(0),
    _set_gid(0),
    _set_uid_string(),
    _set_gid_string(),
    _crypto_inited(0),
    _service_thread(NULL),
    _log_obs(NULL),
    _admin_socket(NULL),
    _perf_counters_collection(NULL),
    _perf_counters_conf_obs(NULL),
    _heartbeat_map(NULL),
    _crypto_none(NULL),
    _crypto_aes(NULL),
    _plugin_registry(NULL),
#ifdef CEPH_DEBUG_MUTEX
    _lockdep_obs(NULL),
#endif
    crush_location(this)
{
  if (options.create_log) {
    _log = options.create_log(&_conf->subsys);
  } else {
    _log = new ceph::logging::Log(&_conf->subsys);
  }

  _log_obs = new LogObs(_log);
  _conf.add_observer(_log_obs);

  _cct_obs = new CephContextObs(this);
  _conf.add_observer(_cct_obs);
#ifdef CEPH_DEBUG_MUTEX
  _lockdep_obs = new LockdepObs(this);
  _conf.add_observer(_lockdep_obs);
#endif
  _perf_counters_collection = new PerfCountersCollection(this);
 
  _admin_socket = new AdminSocket(this);
  _heartbeat_map = new HeartbeatMap(this);

  _plugin_registry = new PluginRegistry(this);

  _admin_hook = new CephContextHook(this);
  _admin_socket->register_command("assert", _admin_hook, "");
  _admin_socket->register_command("abort", _admin_hook, "");
  _admin_socket->register_command("leak_some_memory", _admin_hook, "");
  _admin_socket->register_command("perfcounters_dump", _admin_hook, "");
  _admin_socket->register_command("1", _admin_hook, "");
  _admin_socket->register_command("perf dump name=logger,type=CephString,req=false name=counter,type=CephString,req=false", _admin_hook, "dump non-labeled counters and their values");
  _admin_socket->register_command("perfcounters_schema", _admin_hook, "");
  _admin_socket->register_command("perf histogram dump name=logger,type=CephString,req=false name=counter,type=CephString,req=false", _admin_hook, "dump perf histogram values");
  _admin_socket->register_command("2", _admin_hook, "");
  _admin_socket->register_command("perf schema", _admin_hook, "dump non-labeled counters schemas");
  _admin_socket->register_command("counter dump", _admin_hook, "dump all labeled and non-labeled counters and their values");
  _admin_socket->register_command("counter schema", _admin_hook, "dump all labeled and non-labeled counters schemas");
  _admin_socket->register_command("perf histogram schema", _admin_hook, "dump perf histogram schema");
  _admin_socket->register_command("perf reset name=var,type=CephString", _admin_hook, "perf reset <name>: perf reset all or one perfcounter name");
  _admin_socket->register_command("config show", _admin_hook, "dump current config settings");
  _admin_socket->register_command("config help name=var,type=CephString,req=false", _admin_hook, "get config setting schema and descriptions");
  _admin_socket->register_command("config set name=var,type=CephString name=val,type=CephString,n=N",  _admin_hook, "config set <field> <val> [<val> ...]: set a config variable");
  _admin_socket->register_command("config unset name=var,type=CephString",  _admin_hook, "config unset <field>: unset a config variable");
  _admin_socket->register_command("config get name=var,type=CephString", _admin_hook, "config get <field>: get the config value");
  _admin_socket->register_command(
      "config diff", _admin_hook,
      "dump diff of current config and default config");
  _admin_socket->register_command(
      "config diff get name=var,type=CephString", _admin_hook,
      "dump diff get <field>: dump diff of current and default config setting <field>");
  _admin_socket->register_command("injectargs name=injected_args,type=CephString,n=N", _admin_hook, "inject configuration arguments into running daemon"),
  _admin_socket->register_command("log flush", _admin_hook, "flush log entries to log file");
  _admin_socket->register_command("log dump", _admin_hook, "dump recent log entries to log file");
  _admin_socket->register_command("log reopen", _admin_hook, "reopen log file");

  _crypto_none = CryptoHandler::create(CEPH_CRYPTO_NONE);
  _crypto_aes = CryptoHandler::create(CEPH_CRYPTO_AES);
  _crypto_random.reset(new CryptoRandom());

  lookup_or_create_singleton_object<MempoolObs>("mempool_obs", false, this);
}

CephContext::~CephContext()
{
  associated_objs.clear();
  join_service_thread();

  if (_cct_perf) {
    _perf_counters_collection->remove(_cct_perf);
    delete _cct_perf;
    _cct_perf = NULL;
  }

  delete _plugin_registry;

  _admin_socket->unregister_commands(_admin_hook);
  delete _admin_hook;
  delete _admin_socket;

  delete _heartbeat_map;

  delete _perf_counters_collection;
  _perf_counters_collection = NULL;

  delete _perf_counters_conf_obs;
  _perf_counters_conf_obs = NULL;

  _conf.remove_observer(_log_obs);
  delete _log_obs;
  _log_obs = NULL;

  _conf.remove_observer(_cct_obs);
  delete _cct_obs;
  _cct_obs = NULL;
#ifdef CEPH_DEBUG_MUTEX
  _conf.remove_observer(_lockdep_obs);
  delete _lockdep_obs;
  _lockdep_obs = NULL;
#endif
  _log->stop();
  delete _log;
  _log = NULL;

  delete _crypto_none;
  delete _crypto_aes;
  if (_crypto_inited > 0) {
    ceph_assert(_crypto_inited == 1);  // or else someone explicitly did
				  // init but not shutdown
    shutdown_crypto();
  }
}

void CephContext::put() {
  if (--nref == 0) {
    ANNOTATE_HAPPENS_AFTER(&nref);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&nref);
    if (g_ceph_context == this)
      g_ceph_context = nullptr;
    delete this;
  } else {
    ANNOTATE_HAPPENS_BEFORE(&nref);
  }
}

void CephContext::init_crypto()
{
  if (_crypto_inited++ == 0) {
    TOPNSPC::crypto::init();
  }
}

void CephContext::shutdown_crypto()
{
  if (--_crypto_inited == 0) {
    TOPNSPC::crypto::shutdown(g_code_env == CODE_ENVIRONMENT_LIBRARY);
  }
}

void CephContext::start_service_thread()
{
  {
    std::lock_guard lg(_service_thread_lock);
    if (_service_thread) {
      return;
    }
    _service_thread = new CephContextServiceThread(this);
    _service_thread->create("service");
  }

  if (!(get_init_flags() & CINIT_FLAG_NO_CCT_PERF_COUNTERS))
    _enable_perf_counter();

  // make logs flush on_exit()
  if (_conf->log_flush_on_exit)
    _log->set_flush_on_exit();

  // Trigger callbacks on any config observers that were waiting for
  // it to become safe to start threads.
  _conf.set_safe_to_start_threads();
  _conf.call_all_observers();

  // start admin socket
  if (_conf->admin_socket.length())
    _admin_socket->init(_conf->admin_socket);
}

void CephContext::reopen_logs()
{
  std::lock_guard lg(_service_thread_lock);
  if (_service_thread)
    _service_thread->reopen_logs();
}

void CephContext::join_service_thread()
{
  std::unique_lock<ceph::spinlock> lg(_service_thread_lock);

  CephContextServiceThread *thread = _service_thread;
  if (!thread) {
    return;
  }
  _service_thread = NULL;

  lg.unlock();

  thread->exit_thread();
  thread->join();
  delete thread;

  if (!(get_init_flags() & CINIT_FLAG_NO_CCT_PERF_COUNTERS))
    _disable_perf_counter();
}

uint32_t CephContext::get_module_type() const
{
  return _module_type;
}

void CephContext::set_init_flags(int flags)
{
  _init_flags = flags;
}

int CephContext::get_init_flags() const
{
  return _init_flags;
}

PerfCountersCollection *CephContext::get_perfcounters_collection()
{
  return _perf_counters_collection;
}

void CephContext::_enable_perf_counter()
{
  assert(!_cct_perf);
  PerfCountersBuilder plb(this, "cct", l_cct_first, l_cct_last);
  plb.add_u64(l_cct_total_workers, "total_workers", "Total workers");
  plb.add_u64(l_cct_unhealthy_workers, "unhealthy_workers", "Unhealthy workers");
  _cct_perf = plb.create_perf_counters();
  _perf_counters_collection->add(_cct_perf);

  assert(_mempool_perf_names.empty());
  assert(_mempool_perf_descriptions.empty());
  _mempool_perf_names.reserve(mempool::num_pools * 2);
  _mempool_perf_descriptions.reserve(mempool::num_pools * 2);
  for (unsigned i = 0; i < mempool::num_pools; ++i) {
    std::string n = mempool::get_pool_name(mempool::pool_index_t(i));
    _mempool_perf_names.push_back(n + "_bytes"s);
    _mempool_perf_descriptions.push_back(
      "mempool "s + n + " total bytes");
    _mempool_perf_names.push_back(n + "_items"s);
    _mempool_perf_descriptions.push_back(
      "mempool "s + n + " total items"s);
  }

  PerfCountersBuilder plb2(this, "mempool", l_mempool_first,
			  l_mempool_first + 1 + 2*mempool::num_pools);
  unsigned l = l_mempool_first + 1;
  for (unsigned i = 0; i < mempool::num_pools; ++i) {
    plb2.add_u64(l++, _mempool_perf_names[i*2].c_str(),
		 _mempool_perf_descriptions[i*2].c_str());
    plb2.add_u64(l++, _mempool_perf_names[i*2+1].c_str(),
		 _mempool_perf_descriptions[i*2+1].c_str());
  }
  _mempool_perf = plb2.create_perf_counters();
  _perf_counters_collection->add(_mempool_perf);
}

void CephContext::_disable_perf_counter()
{
  if (!_cct_perf) {
    return;
  }
  _perf_counters_collection->remove(_cct_perf);
  delete _cct_perf;
  _cct_perf = nullptr;

  _perf_counters_collection->remove(_mempool_perf);
  delete _mempool_perf;
  _mempool_perf = nullptr;
  _mempool_perf_names.clear();
  _mempool_perf_descriptions.clear();
}

void CephContext::_refresh_perf_values()
{
  if (_cct_perf) {
    _cct_perf->set(l_cct_total_workers, _heartbeat_map->get_total_workers());
    _cct_perf->set(l_cct_unhealthy_workers, _heartbeat_map->get_unhealthy_workers());
  }
  unsigned l = l_mempool_first + 1;
  for (unsigned i = 0; i < mempool::num_pools; ++i) {
    mempool::pool_t& p = mempool::get_pool(mempool::pool_index_t(i));
    _mempool_perf->set(l++, p.allocated_bytes());
    _mempool_perf->set(l++, p.allocated_items());
  }
}

AdminSocket *CephContext::get_admin_socket()
{
  return _admin_socket;
}

CryptoHandler *CephContext::get_crypto_handler(int type)
{
  switch (type) {
  case CEPH_CRYPTO_NONE:
    return _crypto_none;
  case CEPH_CRYPTO_AES:
    return _crypto_aes;
  default:
    return NULL;
  }
}

void CephContext::notify_pre_fork()
{
  {
    std::lock_guard lg(_fork_watchers_lock);
    for (auto &&t : _fork_watchers) {
      t->handle_pre_fork();
    }
  }
  {
    // note: we don't hold a lock here, but we assume we are idle at
    // fork time, which happens during process init and startup.
    auto i = associated_objs.begin();
    while (i != associated_objs.end()) {
      if (associated_objs_drop_on_fork.count(i->first.first)) {
	i = associated_objs.erase(i);
      } else {
	++i;
      }
    }
    associated_objs_drop_on_fork.clear();
  }
}

void CephContext::notify_post_fork()
{
  ceph::spin_unlock(&_fork_watchers_lock);
  for (auto &&t : _fork_watchers)
    t->handle_post_fork();
}

void CephContext::set_mon_addrs(const MonMap& mm) {
  std::vector<entity_addrvec_t> mon_addrs;
  for (auto& i : mm.mon_info) {
    mon_addrs.push_back(i.second.public_addrs);
  }

  set_mon_addrs(mon_addrs);
}
}
#endif	// WITH_SEASTAR
