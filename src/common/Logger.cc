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
int last_flush; // in seconds since start

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

void Logger::set_start(utime_t s)
{
  logger_lock.Lock();

  start = s;

  utime_t fromstart = g_clock.now();
  if (fromstart < start) {
    cerr << "set_start: logger time jumped backwards from " << start << " to " << fromstart << std::endl;
    fromstart = start;
  }
  fromstart -= start;
  last_flush = fromstart.sec();

  logger_lock.Unlock();
}

static void flush_all_loggers()
{
  generic_dout(20) << "flush_all_loggers" << dendl;

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
  generic_dout(20) << "fromstart " << fromstart << " last_flush " << last_flush << " flushing" << dendl;
  for (list<Logger*>::iterator p = logger_list.begin();
       p != logger_list.end();
       ++p) 
    (*p)->_flush();
  last_flush = now_sec - (now_sec % g_conf.logger_interval);
  if (twice) {
    twice = false;
    goto again;
  }

  // schedule next flush event
  utime_t next;
  next.sec_ref() = start.sec() + last_flush + g_conf.logger_interval;
  next.usec_ref() = start.usec();
  generic_dout(20) << "logger now=" << now
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
  Mutex::Locker l(logger_lock);
  struct stat st;

  if (!g_conf.logger)
    return;

  filename = "";
  if (g_conf.chdir && g_conf.chdir[0] && g_conf.logger_dir[0] != '/') {
    char cwd[200];
    getcwd(cwd, 200);
    filename = cwd;
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

  out.open(filename.c_str(), append ? ofstream::out|ofstream::app : ofstream::out);
  if (!out.is_open()) {
    generic_dout(0) << "failed to open '" << filename << "'" << dendl;
    return; // we fail
  }

  // success
  open = true;

  if (logger_list.empty()) {
    // init logger
    if (!g_conf.clock_tare)
      start = g_clock.now();  // time 0!  otherwise g_clock does it for us.
    
    last_flush = 0;
    
    // call manually the first time; then it'll schedule itself.
    flush_all_loggers();      
  }
  logger_list.push_back(this);
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


void Logger::_flush(bool reset)
{
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
	if (g_conf.logger_calc_variance) {
	  __s64 n = vals[i];
	  for (vector<double>::iterator p = vals_to_avg[i].begin(); n--; ++p) 
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
  
  if (reset) {
    // reset the counters
    for (int i=0; i<type->num_keys; i++) {
      if (type->inc_keys[i]) {
	this->vals[i] = 0;
	this->fvals[i] = 0;
      }
    }
  }
}



__s64 Logger::inc(int key, __s64 v)
{
  if (!open || !g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  vals[i] += v;
  __s64 r = vals[i];
  logger_lock.Unlock();
  return r;
}

double Logger::finc(int key, double v)
{
  if (!open || !g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  fvals[i] += v;
  double r = fvals[i];
  logger_lock.Unlock();
  return r;
}

__s64 Logger::set(int key, __s64 v)
{
  if (!open || !g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  //cout << this << " set " << i << " to " << v << std::endl;
  __s64 r = vals[i] = v;
  logger_lock.Unlock();
  return r;
}


double Logger::fset(int key, double v)
{
  if (!open || !g_conf.logger)
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
  if (!open || !g_conf.logger)
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

__s64 Logger::get(int key)
{
  if (!open || !g_conf.logger)
    return 0;
  logger_lock.Lock();
  int i = type->lookup_key(key);
  __s64 r = 0;
  if (i >= 0 && i < (int)vals.size())
    r = vals[i];
  logger_lock.Unlock();
  return r;
}

