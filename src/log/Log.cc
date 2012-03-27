// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Log.h"

#include <errno.h>
#include <syslog.h>

#include <iostream>
#include <sstream>

#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Clock.h"
#include "include/assert.h"

#define DEFAULT_MAX_NEW     1000
#define DEFAULT_MAX_RECENT 10000

namespace ceph {
namespace log {

Log::Log(SubsystemMap *s)
  : m_subs(s),
    m_new(DEFAULT_MAX_NEW), m_recent(DEFAULT_MAX_RECENT),
    m_fd(-1),
    m_syslog_log(-1), m_syslog_crash(-1),
    m_stderr_log(1), m_stderr_crash(-1),
    m_stop(false)
{
  int ret;

  ret = pthread_spin_init(&m_lock, PTHREAD_PROCESS_SHARED);
  assert(ret == 0);

  ret = pthread_mutex_init(&m_flush_mutex, NULL);
  assert(ret == 0);

  ret = pthread_mutex_init(&m_queue_mutex, NULL);
  assert(ret == 0);

  ret = pthread_cond_init(&m_cond, NULL);
  assert(ret == 0);
}

Log::~Log()
{
  assert(!is_started());
  if (m_fd >= 0)
    TEMP_FAILURE_RETRY(::close(m_fd));

  pthread_spin_destroy(&m_lock);
  pthread_mutex_destroy(&m_queue_mutex);
  pthread_mutex_destroy(&m_flush_mutex);
  pthread_cond_destroy(&m_cond);
}


///

void Log::set_log_file(string fn)
{
  m_log_file = fn;
}

void Log::reopen_log_file()
{
  if (m_fd >= 0)
    TEMP_FAILURE_RETRY(::close(m_fd));
  if (m_log_file.length()) {
    m_fd = ::open(m_log_file.c_str(), O_CREAT|O_WRONLY|O_APPEND, 0644);
  } else {
    m_fd = -1;
  }
}

void Log::set_syslog_level(int log, int crash)
{
  pthread_mutex_lock(&m_flush_mutex);
  m_syslog_log = log;
  m_syslog_crash = crash;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::set_stderr_level(int log, int crash)
{
  pthread_mutex_lock(&m_flush_mutex);
  m_stderr_log = log;
  m_stderr_crash = crash;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::submit_entry(Entry *e)
{
  pthread_mutex_lock(&m_queue_mutex);
  m_new.enqueue(e);
  pthread_cond_signal(&m_cond);
  pthread_mutex_unlock(&m_queue_mutex);
}

void Log::flush()
{
  pthread_mutex_lock(&m_flush_mutex);
  pthread_mutex_lock(&m_queue_mutex);
  EntryQueue t;
  t.swap(m_new);
  pthread_mutex_unlock(&m_queue_mutex);
  _flush(&t, &m_recent, false);
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::_flush(EntryQueue *t, EntryQueue *requeue, bool crash)
{
  Entry *e;
  while ((e = t->dequeue()) != NULL) {
    unsigned sub = e->m_subsys;

    ostringstream ss;
    ss << e->m_stamp
       << ' '
       << std::hex << e->m_thread << std::dec
       << ' '
       << std::setw(2) << std::setfill('0') << e->m_prio << std::setw(0)
      /*
       << ':'
       << std::setiosflags(ios::left)
       << std::setw(m_subs->get_max_subsys_len()) << std::setfill(' ') << m_subs->get_name(sub)
       << std::setiosflags(ios::right)
      */
       << ' ';

    if ((crash || m_subs->get_log_level(sub) >= e->m_prio)) {
      if (m_fd >= 0) {
	int r = safe_write(m_fd, ss.str().data(), ss.str().size());
	if (r >= 0)
	  r = safe_write(m_fd, e->m_str.data(), e->m_str.size());
	if (r >= 0)
	  r = safe_write(m_fd, "\n", 1);
	if (r < 0)
	  cerr << "problem writing to " << m_log_file << ": " << cpp_strerror(r) << std::endl;
      }

      if ((crash ? m_syslog_crash : m_syslog_log) >= e->m_prio) {
	syslog(LOG_USER, "%s%s", ss.str().c_str(), e->m_str.c_str());
      }

      if ((crash ? m_stderr_crash : m_stderr_log) >= e->m_prio) {
	cerr << ss.str() << e->m_str << std::endl;
      }
    }

    requeue->enqueue(e);
  }
}

void Log::_log_message(const char *s, bool crash)
{
  if (m_fd >= 0) {
    int r = safe_write(m_fd, s, strlen(s));
    if (r >= 0)
      r = safe_write(m_fd, "\n", 1);
    if (r < 0)
      cerr << "problem writing to " << m_log_file << ": " << cpp_strerror(r) << std::endl;
  }
  if ((crash ? m_syslog_crash : m_syslog_log) >= 0) {
    syslog(LOG_USER, "%s", s);
  }
  
  if ((crash ? m_stderr_crash : m_stderr_log) >= 0) {
    cerr << s << std::endl;
  }
}

void Log::dump_recent()
{
  pthread_mutex_unlock(&m_flush_mutex);

  pthread_mutex_lock(&m_queue_mutex);
  EntryQueue t(1);
  t.swap(m_new);
  pthread_mutex_unlock(&m_queue_mutex);
  _flush(&t, &m_recent, false);

  EntryQueue old(0);

  _log_message("--- begin dump of recent events ---", true);
  _flush(&m_recent, &old, true);  
  _log_message("--- end dump of recent events ---", true);

  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::start()
{
  assert(!is_started());
  m_stop = false;
  create();
}

void Log::stop()
{
  assert(is_started());
  pthread_mutex_lock(&m_queue_mutex);
  m_stop = true;
  pthread_cond_signal(&m_cond);
  pthread_mutex_unlock(&m_queue_mutex);
  join();
}

void *Log::entry()
{
  pthread_mutex_lock(&m_queue_mutex);
  while (!m_stop) {
    if (!m_new.empty()) {
      pthread_mutex_unlock(&m_queue_mutex);
      flush();
      pthread_mutex_lock(&m_queue_mutex);
      continue;
    }

    pthread_cond_wait(&m_cond, &m_queue_mutex);
  }
  pthread_mutex_unlock(&m_queue_mutex);
  flush();
  return NULL;
}

} // ceph::log::
} // ceph::
