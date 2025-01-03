// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_LOG_H
#define __CEPH_LOG_LOG_H

#include <boost/circular_buffer.hpp>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>

#include "common/Thread.h"
#include "common/ceph_time.h"
#include "common/likely.h"

#include "log/Entry.h"

#include <unistd.h>

struct uuid_d;

namespace ceph {
namespace logging {

class Graylog;
class JournaldLogger;
class SubsystemMap;

class Log : private Thread
{
public:
  using Thread::is_started;

  Log(const SubsystemMap *s);
  ~Log() override;

  void set_flush_on_exit();

  void set_coarse_timestamps(bool coarse);
  void set_max_new(std::size_t n);
  void set_max_recent(std::size_t n);
  void set_log_file(std::string_view fn);
  void reopen_log_file();
  void chown_log_file(uid_t uid, gid_t gid);
  void set_log_stderr_prefix(std::string_view p);
  void set_stderr_fd(int fd);

  void flush();

  void dump_recent();

  void set_syslog_level(int log, int crash);
  void set_stderr_level(int log, int crash);
  void set_graylog_level(int log, int crash);

  void start_graylog(const std::string& host,
		     const uuid_d& fsid);
  void stop_graylog();

  void set_journald_level(int log, int crash);

  void start_journald_logger();
  void stop_journald_logger();

  std::shared_ptr<Graylog> graylog() { return m_graylog; }

  void submit_entry(Entry&& e);

  void start();
  void stop();

  /// true if the log lock is held by our thread
  bool is_inside_log_lock();

  /// induce a segv on the next log event
  void inject_segv();
  void reset_segv();

protected:
  using EntryVector = std::vector<ConcreteEntry>;

  virtual void _flush(EntryVector& q, bool crash);

private:
  using EntryRing = boost::circular_buffer<ConcreteEntry>;
  using mono_clock = ceph::coarse_mono_clock;
  using mono_time = ceph::coarse_mono_time;

  using RecentThreadNames = std::map<pthread_t, std::pair<mono_time, boost::circular_buffer<std::string> > >;

  static const std::size_t DEFAULT_MAX_NEW = 100;
  static const std::size_t DEFAULT_MAX_RECENT = 10000;
  static constexpr std::size_t DEFAULT_MAX_THREAD_NAMES = 4;

  Log **m_indirect_this;

  const SubsystemMap *m_subs;

  std::mutex m_queue_mutex;
  std::mutex m_flush_mutex;
  std::condition_variable m_cond_loggers;
  std::condition_variable m_cond_flusher;

  pthread_t m_queue_mutex_holder;
  pthread_t m_flush_mutex_holder;

  RecentThreadNames m_recent_thread_names; // protected by m_flush_mutex
  EntryVector m_new;    ///< new entries
  EntryRing m_recent; ///< recent (less new) entries we've already written at low detail
  EntryVector m_flush; ///< entries to be flushed (here to optimize heap allocations)

  std::string m_log_file;
  int m_fd = -1;
  uid_t m_uid = 0;
  gid_t m_gid = 0;

  int m_fd_stderr = STDERR_FILENO;

  int m_fd_last_error = 0;  ///< last error we say writing to fd (if any)

  int m_syslog_log = -2, m_syslog_crash = -2;
  int m_stderr_log = -1, m_stderr_crash = -1;
  int m_graylog_log = -3, m_graylog_crash = -3;
  int m_journald_log = -3, m_journald_crash = -3;

  std::string m_log_stderr_prefix;
  bool do_stderr_poll = false;

  std::shared_ptr<Graylog> m_graylog;
  std::unique_ptr<JournaldLogger> m_journald;

  std::vector<char> m_log_buf;

  bool m_stop = false;

  std::size_t m_max_new = DEFAULT_MAX_NEW;

  bool m_inject_segv = false;

  void *entry() override;

  void _log_safe_write(std::string_view sv);
  void _flush_logbuf();
  void _log_message(std::string_view s, bool crash);
  void _configure_stderr();
  void _log_stderr(std::string_view strv);



};

}
}

#endif
