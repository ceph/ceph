// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */



#include <string>

#include "ProfLogType.h"
#include "ProfLogger.h"

#include <iostream>
#include <memory>
#include "Clock.h"

#include "common/config.h"

#include <sys/stat.h>
#include <sys/types.h>

#include "common/Timer.h"

//////////////// C_FlushProfLoggers ////////////////
class C_FlushProfLoggers : public Context
{
public:
  C_FlushProfLoggers(ProfLoggerCollection *coll_)
    : coll(coll_)
  {
  }

  void finish(int r) {
    coll->flush_all_loggers();
  }

private:
  ProfLoggerCollection *coll;
};

//////////////// ProfLoggerCollection ////////////////
ProfLoggerCollection::
ProfLoggerCollection(CephContext *cct_)
  : lock("ProfLoggerCollection::lock"),
    logger_timer(cct_, lock), logger_event(NULL),
    last_flush(0), need_reopen(true), need_reset(false), cct(cct_)
{
}

ProfLoggerCollection::
~ProfLoggerCollection()
{
  try {
    lock.Lock();
    logger_timer.shutdown();
    lock.Unlock();
  }
  catch (...) {
  }
}

void ProfLoggerCollection::
logger_reopen_all()
{
  Mutex::Locker l(lock);
  need_reopen = true;
}

void ProfLoggerCollection::
logger_reset_all()
{
  Mutex::Locker l(lock);
  need_reopen = true;
  need_reset = true;
}

void ProfLoggerCollection::
logger_start()
{
  Mutex::Locker l(lock);
  logger_timer.init();
  flush_all_loggers();
}

void ProfLoggerCollection::
logger_tare(utime_t s)
{
  Mutex::Locker l(lock);

  ldout(cct, 10) << "logger_tare " << s << dendl;

  start = s;

  utime_t fromstart = ceph_clock_now(cct);
  if (fromstart < start) {
    lderr(cct) << "logger_tare time jumped backwards from "
	 << start << " to " << fromstart << dendl;
    fromstart = start;
  }
  fromstart -= start;
  last_flush = fromstart.sec();
}

void ProfLoggerCollection::
logger_add(ProfLogger *logger)
{
  Mutex::Locker l(lock);

  if (logger_list.empty()) {
    if (start == utime_t())
      start = ceph_clock_now(cct);
    last_flush = 0;
  }
  logger_list.push_back(logger);
  logger->lock = &lock;
}

void ProfLoggerCollection::
logger_remove(ProfLogger *logger)
{
  Mutex::Locker l(lock);

  for (list<ProfLogger*>::iterator p = logger_list.begin();
       p != logger_list.end();
       p++) {
    if (*p == logger) {
      logger_list.erase(p);
      delete logger;
      if (logger_list.empty() && logger_event) {
	// If there are no timers, stop the timer events.
	logger_timer.cancel_event(logger_event);
	logger_event = 0;
      }
      return;
    }
  }
}

void ProfLoggerCollection::
flush_all_loggers()
{
  // ProfLoggerCollection lock must be held here.
  ldout(cct, 20) << "flush_all_loggers" << dendl;

  if (!g_conf->profiling_logger)
    return;

  utime_t now = ceph_clock_now(cct);
  utime_t fromstart = now;
  if (fromstart < start) {
    lderr(cct) << "logger time jumped backwards from " << start << " to "
	 << fromstart << dendl;
    //assert(0);
    start = fromstart;
  }
  fromstart -= start;
  int now_sec = fromstart.sec();

  // do any catching up we need to
  bool twice = now_sec - last_flush >= 2 * g_conf->profiling_logger_interval;
 again:
  ldout(cct, 20) << "fromstart " << fromstart << " last_flush " << last_flush << " flushing" << dendl;

  // This logic seems unecessary. We're holding the mutex the whole time here,
  // so need_reopen and need_reset can't change unless we change them.
  // TODO: clean this up slightly
  bool reopen = need_reopen;
  bool reset = need_reset;

  for (list<ProfLogger*>::iterator p = logger_list.begin();
       p != logger_list.end();
       ++p)
    (*p)->_flush(need_reopen, need_reset, last_flush);

  // did full pass while true?
  if (reopen && need_reopen)
    need_reopen = false;
  if (reset && need_reset)
    need_reset = false;

  last_flush = now_sec - (now_sec % g_conf->profiling_logger_interval);
  if (twice) {
    twice = false;
    goto again;
  }

  // schedule next flush event
  utime_t next;
  next.sec_ref() = start.sec() + last_flush + g_conf->profiling_logger_interval;
  next.nsec_ref() = start.nsec();
  ldout(cct, 20) << "logger now=" << now
		   << "  start=" << start
		   << "  next=" << next
		   << dendl;
  logger_event = new C_FlushProfLoggers(this);
  logger_timer.add_event_at(next, logger_event);
}

//////////////// ProfLoggerConfObs ////////////////
ProfLoggerConfObs::ProfLoggerConfObs(ProfLoggerCollection *coll_)
  : coll(coll_)
{
}

ProfLoggerConfObs::~ProfLoggerConfObs()
{
}

const char **ProfLoggerConfObs::get_tracked_conf_keys() const
{
  static const char *KEYS[] = {
    "profiling_logger", "profiling_logger_interval", "profiling_logger_calc_variance",
    "profiling_logger_subdir", "profiling_logger_dir", NULL
  };
  return KEYS;
}

void ProfLoggerConfObs::handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed)
{
  // This could be done a *lot* smarter, if anyone cares to spend time
  // fixing this up.
  // We could probably just take the mutex and call _open_log from here.
  coll->logger_reopen_all();
}

//////////////// ProfLogger ////////////////
void ProfLogger::_open_log()
{
  struct stat st;

  filename = "";
  if ((!g_conf->chdir.empty()) &&
      (g_conf->profiling_logger_dir.substr(0,1) != "/")) {
    char cwd[PATH_MAX];
    char *c = getcwd(cwd, sizeof(cwd));
    assert(c);
    filename = c;
    filename += "/";
  }

  filename = g_conf->profiling_logger_dir;

  // make (feeble) attempt to create logger_dir
  if (::stat(filename.c_str(), &st))
    ::mkdir(filename.c_str(), 0750);

  filename += "/";
  if (!g_conf->profiling_logger_subdir.empty()) {
    filename += g_conf->profiling_logger_subdir;
    ::mkdir( filename.c_str(), 0755 );   // make sure dir exists
    filename += "/";
  }
  filename += name;

  ldout(cct, 10) << "ProfLogger::_open " << filename << dendl;
  if (out.is_open())
    out.close();
  out.open(filename.c_str(),
	   (need_reset || need_reset) ? ofstream::out : ofstream::out|ofstream::app);
  if (!out.is_open()) {
    ldout(cct, 10) << "failed to open '" << filename << "'" << dendl;
    return; // we fail
  }

  // success
  need_open = false;
}

ProfLogger::~ProfLogger()
{
  out.close();
}

void ProfLogger::reopen()
{
  Mutex::Locker l(*lock);
  need_open = true;
}

void ProfLogger::reset()
{
  Mutex::Locker l(*lock);
  need_open = true;
  need_reset = true;
}


void ProfLogger::_flush(bool need_reopen, bool need_reset, int last_flush)
{
  if (need_reopen)
    _open_log();
  if (need_reset) {
    // reset the counters
    for (int i=0; i<type->num_keys; i++) {
      this->vals[i] = 0;
      this->fvals[i] = 0;
    }
    need_reset = false;
  }

  ldout(cct, 20) << "ProfLogger::_flush on " << this << dendl;

  // header?
  wrote_header_last++;
  if (wrote_header_last > 10) {
    out << "#" << type->num_keys;
    for (int i=0; i<type->num_keys; i++) {
      out << "\t" << (type->key_name[i] ? type->key_name[i] : "???");
      if (type->avg_keys[i])
	out << "\t(n)\t(var)";
    }
    out << std::endl;  //out << "\t (" << type->keymap.size() << ")" << endl;
    wrote_header_last = 0;
  }

  // write line to log
  out << last_flush;
  for (int i=0; i<type->num_keys; i++) {
    if (type->avg_keys[i]) {
      if (vals[i] > 0) {
	double avg = (fvals[i] / (double)vals[i]);
	double var = 0.0;
	if (g_conf->profiling_logger_calc_variance &&
	    (unsigned)vals[i] == vals_to_avg[i].size()) {
	  for (vector<double>::iterator p = vals_to_avg[i].begin(); p != vals_to_avg[i].end(); ++p)
	    var += (avg - *p) * (avg - *p);
	}
	char s[256];
	snprintf(s, sizeof(s), "\t%.5lf\t%lld\t%.5lf", avg, (long long int)vals[i], var);
	out << s;
      } else
	out << "\t0\t0\t0";
    } else {
      if (fvals[i] > 0 && vals[i] == 0)
	out << "\t" << fvals[i];
      else {
	//cout << this << " p " << i << " and size is " << vals.size() << std::endl;
	out << "\t" << vals[i];
      }
    }
  }

  // reset the counters
  for (int i=0; i<type->num_keys; i++) {
    if (type->inc_keys[i]) {
      this->vals[i] = 0;
      this->fvals[i] = 0;
    }
  }

  out << std::endl;
}



int64_t ProfLogger::inc(int key, int64_t v)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  vals[i] += v;
  int64_t r = vals[i];
  lock->Unlock();
  return r;
}

double ProfLogger::finc(int key, double v)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  fvals[i] += v;
  double r = fvals[i];
  lock->Unlock();
  return r;
}

int64_t ProfLogger::set(int key, int64_t v)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  //cout << this << " set " << i << " to " << v << std::endl;
  int64_t r = vals[i] = v;
  lock->Unlock();
  return r;
}


double ProfLogger::fset(int key, double v)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  //cout << this << " fset " << i << " to " << v << std::endl;
  double r = fvals[i] = v;
  lock->Unlock();
  return r;
}

double ProfLogger::favg(int key, double v)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  vals[i]++;
  double r = fvals[i] += v;
  if (g_conf->profiling_logger_calc_variance)
    vals_to_avg[i].push_back(v);
  lock->Unlock();
  return r;
}

int64_t ProfLogger::get(int key)
{
  if (!g_conf->profiling_logger)
    return 0;
  lock->Lock();
  int i = type->lookup_key(key);
  int64_t r = 0;
  if (i >= 0 && i < (int)vals.size())
    r = vals[i];
  lock->Unlock();
  return r;
}

