// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Log.h"

#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Graylog.h"
#include "common/valgrind.h"

#include "include/ceph_assert.h"
#include "include/compat.h"
#include "include/on_exit.h"

#include "Entry.h"
#include "LogClock.h"
#include "SubsystemMap.h"

#include <boost/container/vector.hpp>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <syslog.h>

#include <algorithm>
#include <iostream>
#include <set>

#include <fmt/format.h>

#define MAX_LOG_BUF 65536

namespace ceph {
namespace logging {

static OnExitManager exit_callbacks;

static void log_on_exit(void *p)
{
  Log *l = *(Log **)p;
  if (l)
    l->flush();
  delete (Log **)p;// Delete allocated pointer (not Log object, the pointer only!)
}

Log::Log(const SubsystemMap *s)
  : m_indirect_this(nullptr),
    m_subs(s),
    m_recent(DEFAULT_MAX_RECENT)
{
  m_log_buf.reserve(MAX_LOG_BUF);
  _configure_stderr();
}

Log::~Log()
{
  if (m_indirect_this) {
    *m_indirect_this = nullptr;
  }

  ceph_assert(!is_started());
  if (m_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(m_fd));
    m_fd = -1;
  }
}

void Log::_configure_stderr()
{
#ifndef _WIN32
  struct stat info;
  if (int rc = fstat(m_fd_stderr, &info); rc == -1) {
    std::cerr << "failed to stat stderr: " << cpp_strerror(errno) << std::endl;
    return;
  }

  if (S_ISFIFO(info.st_mode)) {
    /* Set O_NONBLOCK on FIFO stderr file. We want to ensure atomic debug log
     * writes so they do not get partially read by e.g. buggy container
     * runtimes. See also IEEE Std 1003.1-2017 and Log::_log_stderr below.
     *
     * This isn't required on Windows.
     */
    int flags = fcntl(m_fd_stderr, F_GETFL);
    if (flags == -1) {
      std::cerr << "failed to get fcntl flags for stderr: " << cpp_strerror(errno) << std::endl;
      return;
    }
    if (!(flags & O_NONBLOCK)) {
      flags |= O_NONBLOCK;
      flags = fcntl(m_fd_stderr, F_SETFL, flags);
      if (flags == -1) {
        std::cerr << "failed to set fcntl flags for stderr: " << cpp_strerror(errno) << std::endl;
        return;
      }
    }
    do_stderr_poll = true;
  }
#endif // !_WIN32
}


///
void Log::set_coarse_timestamps(bool coarse) {
  std::scoped_lock lock(m_flush_mutex);
  if (coarse)
    Entry::clock().coarsen();
  else
    Entry::clock().refine();
}

void Log::set_flush_on_exit()
{
  std::scoped_lock lock(m_flush_mutex);
  // Make sure we flush on shutdown.  We do this by deliberately
  // leaking an indirect pointer to ourselves (on_exit() can't
  // unregister a callback).  This is not racy only becuase we
  // assume that exit() won't race with ~Log().
  if (m_indirect_this == NULL) {
    m_indirect_this = new (Log*)(this);
    exit_callbacks.add_callback(log_on_exit, m_indirect_this);
  }
}

void Log::set_max_new(std::size_t n)
{
  std::scoped_lock lock(m_queue_mutex);
  m_max_new = n;
}

void Log::set_max_recent(std::size_t n)
{
  std::scoped_lock lock(m_flush_mutex);
  m_max_recent = n;
}

void Log::set_log_file(std::string_view fn)
{
  std::scoped_lock lock(m_flush_mutex);
  m_log_file = fn;
}

void Log::set_log_stderr_prefix(std::string_view p)
{
  std::scoped_lock lock(m_flush_mutex);
  m_log_stderr_prefix = p;
}

void Log::reopen_log_file()
{
  std::scoped_lock lock(m_flush_mutex);
  if (!is_started()) {
    return;
  }
  m_flush_mutex_holder = pthread_self();
  if (m_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(m_fd));
    m_fd = -1;
  }
  if (m_log_file.length()) {
    m_fd = ::open(m_log_file.c_str(), O_CREAT|O_WRONLY|O_APPEND|O_CLOEXEC, 0644);
    if (m_fd >= 0 && (m_uid || m_gid)) {
      if (::fchown(m_fd, m_uid, m_gid) < 0) {
	int e = errno;
	std::cerr << "failed to chown " << m_log_file << ": " << cpp_strerror(e)
	     << std::endl;
      }
    }
  }
  m_flush_mutex_holder = 0;
}

void Log::chown_log_file(uid_t uid, gid_t gid)
{
  std::scoped_lock lock(m_flush_mutex);
  if (m_fd >= 0) {
    int r = ::fchown(m_fd, uid, gid);
    if (r < 0) {
      r = -errno;
      std::cerr << "failed to chown " << m_log_file << ": " << cpp_strerror(r)
	   << std::endl;
    }
  }
}

void Log::set_syslog_level(int log, int crash)
{
  std::scoped_lock lock(m_flush_mutex);
  m_syslog_log = log;
  m_syslog_crash = crash;
}

void Log::set_stderr_level(int log, int crash)
{
  std::scoped_lock lock(m_flush_mutex);
  m_stderr_log = log;
  m_stderr_crash = crash;
}

void Log::set_graylog_level(int log, int crash)
{
  std::scoped_lock lock(m_flush_mutex);
  m_graylog_log = log;
  m_graylog_crash = crash;
}

void Log::start_graylog()
{
  std::scoped_lock lock(m_flush_mutex);
  if (! m_graylog.get())
    m_graylog = std::make_shared<Graylog>(m_subs, "dlog");
}


void Log::stop_graylog()
{
  std::scoped_lock lock(m_flush_mutex);
  m_graylog.reset();
}

void Log::submit_entry(Entry&& e)
{
  std::unique_lock lock(m_queue_mutex);
  m_queue_mutex_holder = pthread_self();

  if (unlikely(m_inject_segv))
    *(volatile int *)(0) = 0xdead;

  // wait for flush to catch up
  while (is_started() &&
	 m_new.size() > m_max_new) {
    if (m_stop) break; // force addition
    m_cond_loggers.wait(lock);
  }

  m_new.emplace_back(std::move(e));
  m_cond_flusher.notify_all();
  m_queue_mutex_holder = 0;
}

void Log::flush()
{
  std::scoped_lock lock1(m_flush_mutex);
  m_flush_mutex_holder = pthread_self();

  {
    std::scoped_lock lock2(m_queue_mutex);
    m_queue_mutex_holder = pthread_self();
    assert(m_flush.empty());
    m_flush.swap(m_new);
    m_cond_loggers.notify_all();
    m_queue_mutex_holder = 0;
  }

  _flush(m_flush, false);
  m_flush_mutex_holder = 0;
}

void Log::_log_safe_write(std::string_view sv)
{
  if (m_fd < 0)
    return;
  int r = safe_write(m_fd, sv.data(), sv.size());
  if (r != m_fd_last_error) {
    if (r < 0)
      std::cerr << "problem writing to " << m_log_file
           << ": " << cpp_strerror(r)
           << std::endl;
    m_fd_last_error = r;
  }
}

void Log::set_stderr_fd(int fd)
{
  m_fd_stderr = fd;
  _configure_stderr();
}

void Log::_log_stderr(std::string_view strv)
{
  if (do_stderr_poll) {
    auto& prefix = m_log_stderr_prefix;
    size_t const len = prefix.size() + strv.size();
    boost::container::small_vector<char, PIPE_BUF> buf;
    buf.resize(len+1, '\0');
    memcpy(buf.data(), prefix.c_str(), prefix.size());
    memcpy(buf.data()+prefix.size(), strv.data(), strv.size());

    char const* const start = buf.data();
    char const* current = start;
    while ((size_t)(current-start) < len) {
      auto chunk = std::min<ssize_t>(PIPE_BUF, len-(ssize_t)(current-start));
      while (1) {
        ssize_t rc = write(m_fd_stderr, current, chunk);
        if (rc == chunk) {
          current += chunk;
          break;
        } else if (rc > 0) {
          /* According to IEEE Std 1003.1-2017, this cannot happen:
           *
           * Write requests to a pipe or FIFO shall be handled in the same way as a regular file with the following exceptions:
           * ...
           *   If the O_NONBLOCK flag is set ...
           *   ...
           *     A write request for {PIPE_BUF} or fewer bytes shall have the
           *     following effect: if there is sufficient space available in
           *     the pipe, write() shall transfer all the data and return the
           *     number of bytes requested. Otherwise, write() shall transfer
           *     no data and return -1 with errno set to [EAGAIN].
           *
           * In any case, handle misbehavior gracefully by incrementing current.
           */
          current += rc;
          break;
        } else if (rc == -1) {
          if (errno == EAGAIN) {
            struct pollfd pfd[1];
            pfd[0].fd = m_fd_stderr;
            pfd[0].events = POLLOUT;
            poll(pfd, 1, -1);
            /* ignore errors / success, just retry the write */
          } else if (errno == EINTR) {
            continue;
          } else {
            /* some other kind of error, no point logging if stderr writes fail */
            return;
          }
        }
      }
    }
  } else {
    std::cerr << m_log_stderr_prefix << strv << std::endl;
  }
}

void Log::_flush_logbuf()
{
  if (m_log_buf.size()) {
    _log_safe_write(std::string_view(m_log_buf.data(), m_log_buf.size()));
    m_log_buf.resize(0);
  }
}

void Log::_flush(EntryVector& t, bool crash)
{
  long len = 0;
  if (t.empty()) {
    assert(m_log_buf.empty());
    return;
  }
  if (crash) {
    len = t.size();
  }
  for (auto& e : t) {
    auto prio = e.m_prio;
    auto stamp = e.m_stamp;
    auto sub = e.m_subsys;
    auto thread = e.m_thread;
    auto str = e.strv();

    bool should_log = crash || m_subs->get_log_level(sub) >= prio;
    bool do_fd = m_fd >= 0 && should_log;
    bool do_syslog = m_syslog_crash >= prio && should_log;
    bool do_stderr = m_stderr_crash >= prio && should_log;
    bool do_graylog2 = m_graylog_crash >= prio && should_log;

    if (do_fd || do_syslog || do_stderr) {
      const std::size_t cur = m_log_buf.size();
      std::size_t used = 0;
      const std::size_t allocated = e.size() + 80;
      m_log_buf.resize(cur + allocated);

      char* const start = m_log_buf.data();
      char* pos = start + cur;

      if (crash) {
        used += (std::size_t)snprintf(pos + used, allocated - used, "%6ld> ", -(--len));
      }
      used += (std::size_t)append_time(stamp, pos + used, allocated - used);
      used += (std::size_t)snprintf(pos + used, allocated - used, " %lx %2d ", (unsigned long)thread, prio);
      memcpy(pos + used, str.data(), str.size());
      used += str.size();
      pos[used] = '\0';
      ceph_assert((used + 1 /* '\n' */) < allocated);

      if (do_syslog) {
        syslog(LOG_USER|LOG_INFO, "%s", pos);
      }

      /* now add newline */
      pos[used++] = '\n';

      if (do_stderr) {
        _log_stderr(std::string_view(pos, used));
      }

      if (do_fd) {
        m_log_buf.resize(cur + used);
      } else {
        m_log_buf.resize(0);
      }

      if (m_log_buf.size() > MAX_LOG_BUF) {
        _flush_logbuf();
      }
    }

    if (do_graylog2 && m_graylog) {
      m_graylog->log_entry(e);
    }

    m_recent.push_back(std::move(e));
  }
  t.clear();

  _flush_logbuf();
}

void Log::_log_message(std::string_view s, bool crash)
{
  if (m_fd >= 0) {
    std::string b = fmt::format("{}\n", s);
    int r = safe_write(m_fd, b.data(), b.size());
    if (r < 0)
      std::cerr << "problem writing to " << m_log_file << ": " << cpp_strerror(r) << std::endl;
  }
  if ((crash ? m_syslog_crash : m_syslog_log) >= 0) {
    syslog(LOG_USER|LOG_INFO, "%.*s", static_cast<int>(s.size()), s.data());
  }

  if ((crash ? m_stderr_crash : m_stderr_log) >= 0) {
    std::cerr << s << std::endl;
  }
}

template<typename T>
static uint64_t tid_to_int(T tid)
{
  if constexpr (std::is_pointer_v<T>) {
    return reinterpret_cast<std::uintptr_t>(tid);
  } else {
    return tid;
  }
}

void Log::dump_recent()
{
  std::scoped_lock lock1(m_flush_mutex);
  m_flush_mutex_holder = pthread_self();

  {
    std::scoped_lock lock2(m_queue_mutex);
    m_queue_mutex_holder = pthread_self();
    assert(m_flush.empty());
    m_flush.swap(m_new);
    m_queue_mutex_holder = 0;
  }

  _flush(m_flush, false);

  _log_message("--- begin dump of recent events ---", true);
  std::set<pthread_t> recent_pthread_ids;
  {
    EntryVector t;
    t.insert(t.end(), std::make_move_iterator(m_recent.begin()), std::make_move_iterator(m_recent.end()));
    m_recent.clear();
    for (const auto& e : t) {
      recent_pthread_ids.emplace(e.m_thread);
    }
    _flush(t, true);
  }

  _log_message("--- logging levels ---", true);
  for (const auto& p : m_subs->m_subsys) {
    _log_message(fmt::format("  {:2d}/{:2d} {}",
			     p.log_level, p.gather_level, p.name), true);
  }
  _log_message(fmt::format("  {:2d}/{:2d} (syslog threshold)",
			   m_syslog_log, m_syslog_crash), true);
  _log_message(fmt::format("  {:2d}/{:2d} (stderr threshold)",
			   m_stderr_log, m_stderr_crash), true);

  _log_message("--- pthread ID / name mapping for recent threads ---", true);
  for (const auto pthread_id : recent_pthread_ids)
  {
    char pthread_name[16] = {0}; //limited by 16B include terminating null byte.
    ceph_pthread_getname(pthread_id, pthread_name, sizeof(pthread_name));
    // we want the ID to be printed in the same format as we use for a log entry.
    // The reason is easier grepping.
    _log_message(fmt::format("  {:x} / {}",
			     tid_to_int(pthread_id), pthread_name), true);
  }

  _log_message(fmt::format("  max_recent {:9}", m_max_recent), true);
  _log_message(fmt::format("  max_new    {:9}", m_max_recent), true);
  _log_message(fmt::format("  log_file {}", m_log_file), true);

  _log_message("--- end dump of recent events ---", true);

  assert(m_log_buf.empty());

  m_flush_mutex_holder = 0;
}

void Log::start()
{
  ceph_assert(!is_started());
  {
    std::scoped_lock lock(m_queue_mutex);
    m_stop = false;
  }
  create("log");
}

void Log::stop()
{
  if (is_started()) {
    {
      std::scoped_lock lock(m_queue_mutex);
      m_stop = true;
      m_cond_flusher.notify_one();
      m_cond_loggers.notify_all();
    }
    join();
  }
}

void *Log::entry()
{
  reopen_log_file();
  {
    std::unique_lock lock(m_queue_mutex);
    m_queue_mutex_holder = pthread_self();
    while (!m_stop) {
      if (!m_new.empty()) {
        m_queue_mutex_holder = 0;
        lock.unlock();
        flush();
        lock.lock();
        m_queue_mutex_holder = pthread_self();
        continue;
      }

      m_cond_flusher.wait(lock);
    }
    m_queue_mutex_holder = 0;
  }
  flush();
  return NULL;
}

bool Log::is_inside_log_lock()
{
  return
    pthread_self() == m_queue_mutex_holder ||
    pthread_self() == m_flush_mutex_holder;
}

void Log::inject_segv()
{
  m_inject_segv = true;
}

void Log::reset_segv()
{
  m_inject_segv = false;
}

} // ceph::logging::
} // ceph::
