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

#include "LogType.h"
#include "Logger.h"

#include <iostream>
#include "Clock.h"

#include "config.h"

#include <sys/stat.h>
#include <sys/types.h>

#include "common/Timer.h"

// per-process lock.  lame, but this way I protect LogType too!
Mutex logger_lock("logger_lock");
SafeTimer logger_timer(logger_lock);
Context *logger_event = 0;
list<Logger*> logger_list;
utime_t start;
int last_flush = 0; // in seconds since start

bool logger_need_reopen = true;
bool logger_need_reset = false;

static void flush_all_loggers();
static void stop();

static struct FlusherStopper {
  ~FlusherStopper() {
    stop();
  }
} stopper;

class C_FlushLoggers : public Context {
public:
  void finish(int r) {
    if (logger_event == this) {
      logger_event = 0;
      flush_all_loggers();
    }
  }
};


void logger_reopen_all()
{
  logger_need_reopen = true;
}

void logger_reset_all()
{
  logger_need_reopen = true;
  logger_need_reset = true;
}

void logger_start()
{
  Mutex::Locker l(logger_lock);
  flush_all_loggers();
}

void logger_tare(utime_t s)
{
  Mutex::Locker l(logger_lock);

  generic_dout(0) << "logger_tare " << s << dendl;

  start = s;

  utime_t fromstart = g_clock.now();
  if (fromstart < start) {
    cerr << "logger_tare time jumped backwards from "
	 << start << " to " << fromstart << std::endl;
    fromstart = start;
  }
  fromstart -= start;
  last_flush = fromstart.sec();
}

void logger_add(Logger *logger)
{
  Mutex::Locker l(logger_lock);

  if (logger_list.empty()) {
    if (start == utime_t())
      start = g_clock.now();
    last_flush = 0;
  }
  logger_list.push_back(logger);
}

void logger_remove(Logger *logger)
{
  Mutex::Locker l(logger_lock);

  for (list<Logger*>::iterator p = logger_list.begin();
       p != logger_list.end();
       p++) {
    if (*p == logger) {
      logger_list.erase(p);
      break;
    }
  }
}

static void flush_all_loggers()
{
  generic_dout(0) << "flush_all_loggers" << dendl;

  if (!g_conf.logger)
    return;

  utime_t now = g_clock.now();
  utime_t fromstart = now;
  if (fromstart < start) {
    cerr << "logger time jumped backwards from " << start << " to " << fromstart << std::endl;
    //assert(0);
    start = fromstart;
  }
  fromstart -= start;
  int now_sec = fromstart.sec();

  // do any catching up we need to
  bool twice = now_sec - last_flush >= 2 * g_conf.logger_interval;
 again:
  generic_dout(0) << "fromstart " << fromstart << " last_flush " << last_flush << " flushing" << dendl;
  
  bool reopen = logger_need_reopen;
  bool reset = logger_need_reset;

  for (list<Logger*>::iterator p = logger_list.begin();
       p != logger_list.end();
       ++p) 
    (*p)->_flush();
  
  // did full pass while true?
  if (reopen && logger_need_reopen)
    logger_need_reopen = false;
  if (reset && logger_need_reset)
    logger_need_reset = false;

  last_flush = now_sec - (now_sec % g_conf.logger_interval);
  if (twice) {
    twice = false;
    goto again;
  }

  // schedule next flush event
  utime_t next;
  next.sec_ref() = start.sec() + last_flush + g_conf.logger_interval;
  next.usec_ref() = start.usec();
  generic_dout(0) << "logger now=" << now
		   << "  start=" << start 
		   << "  next=" << next 
		   << dendl;
  logger_event = new C_FlushLoggers;
  logger_timer.add_event_at(next, logger_event);
}

static void stop()
{
  logger_lock.Lock();
  logger_timer.cancel_all();
  logger_timer.join();
  logger_lock.Unlock();
}



// ---------

void Logger::_open_log()
{
  struct stat st;

  filename = "";
  if (g_conf.chdir && g_conf.chdir[0] && g_conf.logger_dir[0] != '/') {
    char cwd[PATH_MAX];
    char *c = getcwd(cwd, sizeof(cwd));
    assert(c);
    filename = c;
    filename += "/";
  }
  
  filename = g_conf.logger_dir;

  // make (feeble) attempt to create logger_dir
  if (::stat(filename.c_str(), &st))
    ::mkdir(filename.c_str(), 0750);

  filename += "/";
  if (g_conf.logger_subdir) {
    filename += g_conf.logger_subdir;
    ::mkdir( filename.c_str(), 0755 );   // make sure dir exists
    filename += "/";
  }
  filename += name;

  generic_dout(0) << "Logger::_open " << filename << dendl;
  if (out.is_open())
    out.close();
  out.open(filename.c_str(),
	   (need_reset || logger_need_reset) ? ofstream::out : ofstream::out|ofstream::app);
  if (!out.is_open()) {
    generic_dout(0) << "failed to open '" << filename << "'" << dendl;
    return; // we fail
  }

  // success
  need_open = false;
}


Logger::~Logger()
{
  Mutex::Locker l(logger_lock);
  
  _flush();
  out.close();
  logger_list.remove(this); // slow, but rare.
  if (logger_list.empty()) 
    logger_event = 0;       // stop the timer events.
}

void Logger::reopen()
{
  Mutex::Locker l(logger_lock);
  need_open = true;
}

void Logger::reset()
{
  Mutex::Locker l(logger_lock);
  need_open = true;
  need_reset = true;
}


void Logger::_flush()
{
  if (need_open || logger_need_reopen)
    _open_log();
  if (need_reset || logger_need_reset) {
    // reset the counters
    for (int i=0; i<type->num_keys; i++) {
      if (type->inc_keys[i]) {
	this->vals[i] = 0;
	this->fvals[i] = 0;
      }
    }
    need_reset = false;
  }

  generic_dout(0) << "Logger::_flush on " << this << dendl;

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
	if (g_conf.logger_calc_variance &&
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
  out << std::endl;
}



int64_t Logger::inc(int key, int64_t v)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  vals[i] += v;
  int64_t r = vals[i];
  logger_lock.Unlock();
  return r;
}

double Logger::finc(int key, double v)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  fvals[i] += v;
  double r = fvals[i];
  logger_lock.Unlock();
  return r;
}

int64_t Logger::set(int key, int64_t v)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  //cout << this << " set " << i << " to " << v << std::endl;
  int64_t r = vals[i] = v;
  logger_lock.Unlock();
  return r;
}


double Logger::fset(int key, double v)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  //cout << this << " fset " << i << " to " << v << std::endl;
  double r = fvals[i] = v;
  logger_lock.Unlock();
  return r;
}

double Logger::favg(int key, double v)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  vals[i]++;
  double r = fvals[i] += v;
  if (g_conf.logger_calc_variance)
    vals_to_avg[i].push_back(v);
  logger_lock.Unlock();
  return r;
}

int64_t Logger::get(int key)
{
  if (!g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  int64_t r = 0;
  if (i >= 0 && i < (int)vals.size())
    r = vals[i];
  logger_lock.Unlock();
  return r;
}

