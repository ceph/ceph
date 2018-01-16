// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_LOG_H
#define __CEPH_LOG_LOG_H

#include <memory>

#include "common/Thread.h"

#include "EntryQueue.h"

namespace ceph {
namespace logging {

class Graylog;
class SubsystemMap;
class Entry;

class Log : private Thread
{
  Log **m_indirect_this;
  log_clock clock;

  SubsystemMap *m_subs;

  pthread_mutex_t m_queue_mutex;
  pthread_mutex_t m_flush_mutex;
  pthread_cond_t m_cond_loggers;
  pthread_cond_t m_cond_flusher;

  pthread_t m_queue_mutex_holder;
  pthread_t m_flush_mutex_holder;

  EntryQueue m_new;    ///< new entries
  EntryQueue m_recent; ///< recent (less new) entries we've already written at low detail

  std::string m_log_file;
  int m_fd;
  uid_t m_uid;
  gid_t m_gid;

  int m_fd_last_error;  ///< last error we say writing to fd (if any)

  int m_syslog_log, m_syslog_crash;
  int m_stderr_log, m_stderr_crash;
  int m_graylog_log, m_graylog_crash;

  std::string m_log_stderr_prefix;

  std::shared_ptr<Graylog> m_graylog;

  bool m_stop;

  int m_max_new, m_max_recent;

  bool m_inject_segv;

  void *entry() override;

  void _flush(EntryQueue *q, EntryQueue *requeue, bool crash);

  void _log_message(const char *s, bool crash);

public:
  Log(SubsystemMap *s);
  ~Log() override;

  void set_flush_on_exit();

  void set_coarse_timestamps(bool coarse);
  void set_max_new(int n);
  void set_max_recent(int n);
  void set_log_file(std::string fn);
  void reopen_log_file();
  void chown_log_file(uid_t uid, gid_t gid);
  void set_log_stderr_prefix(const std::string& p);

  void flush();

  void dump_recent();

  void set_syslog_level(int log, int crash);
  void set_stderr_level(int log, int crash);
  void set_graylog_level(int log, int crash);

  void start_graylog();
  void stop_graylog();

  std::shared_ptr<Graylog> graylog() { return m_graylog; }

  Entry *create_entry(int level, int subsys, const char* msg = nullptr);
  Entry *create_entry(int level, int subsys, size_t* expected_size);
  void submit_entry(Entry *e);

  void start();
  void stop();

  /// true if the log lock is held by our thread
  bool is_inside_log_lock();

  /// induce a segv on the next log event
  void inject_segv();
  void reset_segv();
};

}
}

#endif
