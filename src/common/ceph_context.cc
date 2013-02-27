// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <time.h>

#include "common/admin_socket.h"
#include "common/perf_counters.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/HeartbeatMap.h"
#include "common/errno.h"
#include "common/lockdep.h"
#include "common/Formatter.h"
#include "log/Log.h"
#include "auth/Crypto.h"

#include <iostream>
#include <pthread.h>
#include <semaphore.h>

using ceph::HeartbeatMap;

class CephContextServiceThread : public Thread
{
public:
  CephContextServiceThread(CephContext *cct)
    : _reopen_logs(false), _exit_thread(false), _cct(cct)
  {
    sem_init(&_sem, 0, 0);
  };

  ~CephContextServiceThread()
  {
    sem_destroy(&_sem);
  };

  void *entry()
  {
    while (1) {
      if (_cct->_conf->heartbeat_interval) {
	struct timespec timeout;
	clock_gettime(CLOCK_REALTIME, &timeout);
	timeout.tv_sec += _cct->_conf->heartbeat_interval;
	sem_timedwait(&_sem, &timeout);
      } else {
	sem_wait(&_sem);
      }
      if (_exit_thread) {
	break;
      }
      if (_reopen_logs) {
	_cct->_log->reopen_log_file();
	_reopen_logs = false;
      }
      _cct->_heartbeat_map->check_touch_file();
    }
    return NULL;
  }

  void reopen_logs()
  {
    _reopen_logs = true;
    sem_post(&_sem);
  }

  void exit_thread()
  {
    _exit_thread = true;
    sem_post(&_sem);
  }

private:
  volatile bool _reopen_logs;
  volatile bool _exit_thread;
  sem_t _sem;
  CephContext *_cct;
};


/**
 * observe logging config changes
 *
 * The logging subsystem sits below most of the ceph code, including
 * the config subsystem, to keep it simple and self-contained.  Feed
 * logging-related config changes to the log.
 */
class LogObs : public md_config_obs_t {
  ceph::log::Log *log;

public:
  LogObs(ceph::log::Log *l) : log(l) {}

  const char** get_tracked_conf_keys() const {
    static const char *KEYS[] = {
      "log_file",
      "log_max_new",
      "log_max_recent",
      "log_to_syslog",
      "err_to_syslog",
      "log_to_stderr",
      "err_to_stderr",
      NULL
    };
    return KEYS;
  }

  void handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed) {
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
    if (changed.count("log_file")) {
      log->set_log_file(conf->log_file);
      log->reopen_log_file();
    }

    if (changed.count("log_max_new")) {
      log->set_max_new(conf->log_max_new);
    }

    if (changed.count("log_max_recent")) {
      log->set_max_recent(conf->log_max_recent);
    }
  }
};


// perfcounter hooks

class CephContextHook : public AdminSocketHook {
  CephContext *m_cct;

public:
  CephContextHook(CephContext *cct) : m_cct(cct) {}

  bool call(std::string command, std::string args, bufferlist& out) {
    m_cct->do_command(command, args, &out);
    return true;
  }
};

void CephContext::do_command(std::string command, std::string args, bufferlist *out)
{
  lgeneric_dout(this, 1) << "do_command '" << command << "' '" << args << "'" << dendl;
  if (command == "perfcounters_dump" || command == "1" ||
      command == "perf dump") {
    _perf_counters_collection->write_json_to_buf(*out, false);
  }
  else if (command == "perfcounters_schema" || command == "2" ||
	   command == "perf schema") {
    _perf_counters_collection->write_json_to_buf(*out, true);
  }
  else {
    JSONFormatter jf(true);
    jf.open_object_section(command.c_str());
    if (command == "config show") {
      _conf->show_config(&jf);
    }
    else if (command == "config set") {
      std::string var = args;
      size_t pos = var.find(' ');
      if (pos == string::npos) {
	jf.dump_string("error", "syntax error: 'config set <var> <value>'");
      } else {
	std::string val = var.substr(pos+1);
	var.resize(pos);
	int r = _conf->set_val(var.c_str(), val.c_str());
	if (r < 0) {
	  jf.dump_stream("error") << "error setting '" << var << "' to '" << val << "': " << cpp_strerror(r);
	} else {
	  ostringstream ss;
	  _conf->apply_changes(&ss);
	  jf.dump_string("success", ss.str());
	}
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
      assert(0 == "registered under wrong command?");    
    }
    ostringstream ss;
    jf.close_section();
    jf.flush(ss);
    out->append(ss.str());
  }
  lgeneric_dout(this, 1) << "do_command '" << command << "' '" << args << "' result is " << out->length() << " bytes" << dendl;
};


CephContext::CephContext(uint32_t module_type_)
  : nref(1),
    _conf(new md_config_t()),
    _log(NULL),
    _module_type(module_type_),
    _service_thread(NULL),
    _log_obs(NULL),
    _admin_socket(NULL),
    _perf_counters_collection(NULL),
    _perf_counters_conf_obs(NULL),
    _heartbeat_map(NULL),
    _crypto_none(NULL),
    _crypto_aes(NULL)
{
  pthread_spin_init(&_service_thread_lock, PTHREAD_PROCESS_SHARED);

  _log = new ceph::log::Log(&_conf->subsys);
  _log->start();

  _log_obs = new LogObs(_log);
  _conf->add_observer(_log_obs);

  _perf_counters_collection = new PerfCountersCollection(this);
  _admin_socket = new AdminSocket(this);
  _heartbeat_map = new HeartbeatMap(this);

  _admin_hook = new CephContextHook(this);
  _admin_socket->register_command("perfcounters_dump", _admin_hook, "");
  _admin_socket->register_command("1", _admin_hook, "");
  _admin_socket->register_command("perf dump", _admin_hook, "dump perfcounters value");
  _admin_socket->register_command("perfcounters_schema", _admin_hook, "");
  _admin_socket->register_command("2", _admin_hook, "");
  _admin_socket->register_command("perf schema", _admin_hook, "dump perfcounters schema");
  _admin_socket->register_command("config show", _admin_hook, "dump current config settings");
  _admin_socket->register_command("config set", _admin_hook, "config set <field> <val>: set a config variable");
  _admin_socket->register_command("log flush", _admin_hook, "flush log entries to log file");
  _admin_socket->register_command("log dump", _admin_hook, "dump recent log entries to log file");
  _admin_socket->register_command("log reopen", _admin_hook, "reopen log file");

  _crypto_none = new CryptoNone;
  _crypto_aes = new CryptoAES;
}

CephContext::~CephContext()
{
  if (_conf->lockdep) {
    lockdep_unregister_ceph_context(this);
  }

  join_service_thread();

  _admin_socket->unregister_command("perfcounters_dump");
  _admin_socket->unregister_command("perf dump");
  _admin_socket->unregister_command("1");
  _admin_socket->unregister_command("perfcounters_schema");
  _admin_socket->unregister_command("perf schema");
  _admin_socket->unregister_command("2");
  _admin_socket->unregister_command("config show");
  _admin_socket->unregister_command("config set");
  _admin_socket->unregister_command("log flush");
  _admin_socket->unregister_command("log dump");
  _admin_socket->unregister_command("log reopen");
  delete _admin_hook;
  delete _admin_socket;

  delete _heartbeat_map;

  delete _perf_counters_collection;
  _perf_counters_collection = NULL;

  delete _perf_counters_conf_obs;
  _perf_counters_conf_obs = NULL;

  _conf->remove_observer(_log_obs);
  delete _log_obs;
  _log_obs = NULL;

  _log->stop();
  delete _log;
  _log = NULL;

  delete _conf;
  pthread_spin_destroy(&_service_thread_lock);

  delete _crypto_none;
  delete _crypto_aes;
}

void CephContext::start_service_thread()
{
  pthread_spin_lock(&_service_thread_lock);
  if (_service_thread) {
    pthread_spin_unlock(&_service_thread_lock);
    return;
  }
  _service_thread = new CephContextServiceThread(this);
  _service_thread->create();
  pthread_spin_unlock(&_service_thread_lock);

  // make logs flush on_exit()
  if (_conf->log_flush_on_exit)
    _log->set_flush_on_exit();

  // Trigger callbacks on any config observers that were waiting for
  // it to become safe to start threads.
  _conf->set_val("internal_safe_to_start_threads", "true");
  _conf->call_all_observers();

  // start admin socket
  if (_conf->admin_socket.length())
    _admin_socket->init(_conf->admin_socket);
}

void CephContext::reopen_logs()
{
  pthread_spin_lock(&_service_thread_lock);
  if (_service_thread)
    _service_thread->reopen_logs();
  pthread_spin_unlock(&_service_thread_lock);
}

void CephContext::join_service_thread()
{
  pthread_spin_lock(&_service_thread_lock);
  CephContextServiceThread *thread = _service_thread;
  if (!thread) {
    pthread_spin_unlock(&_service_thread_lock);
    return;
  }
  _service_thread = NULL;
  pthread_spin_unlock(&_service_thread_lock);

  thread->exit_thread();
  thread->join();
  delete thread;
}

uint32_t CephContext::get_module_type() const
{
  return _module_type;
}

PerfCountersCollection *CephContext::get_perfcounters_collection()
{
  return _perf_counters_collection;
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
