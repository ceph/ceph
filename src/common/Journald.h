// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_JOURNALD_H
#define CEPH_COMMON_JOURNALD_H

#include "acconfig.h"
#include <memory>
#include <sys/types.h>
#include <sys/socket.h>

struct LogEntry;

namespace ceph::logging {

class Entry;
class SubsystemMap;

#ifdef WITH_SYSTEMD

namespace detail {

class EntryEncoder;
class LogEntryEncoder;

class JournaldClient {
 public:
  JournaldClient();
  ~JournaldClient();
  int send();
  struct msghdr m_msghdr;
 private:
  int fd;

  enum class MemFileMode;
  MemFileMode mem_file_mode;

  void detect_mem_file_mode();
  int open_mem_file();
};
}

/**
 * Logger to send local logs to journald
 * 
 * local logs means @code dout(0) << ... @endcode and similars
 * 
 * @see JournaldClusterLogger
 */
class JournaldLogger {
 public:
  JournaldLogger(const SubsystemMap *s);
  ~JournaldLogger();

  /**
   * @returns 0 if log entry is successfully sent, -1 otherwise.
   */
  int log_entry(const Entry &e);

 private:
  detail::JournaldClient client;

  std::unique_ptr<detail::EntryEncoder> m_entry_encoder;

  const SubsystemMap * m_subs;
};

/**
 * Logger to send cluster log recieved by MON to journald
 * 
 * @see JournaldLogger
 */
class JournaldClusterLogger {
 public:
  JournaldClusterLogger();
  ~JournaldClusterLogger();

  /**
   * @returns 0 if log entry is successfully sent, -1 otherwise.
   */
  int log_log_entry(const LogEntry &le);

 private:
  detail::JournaldClient client;

  std::unique_ptr<detail::LogEntryEncoder> m_log_entry_encoder;
};

#else  // WITH_SYSTEMD

class JournaldLogger {
public:
  JournaldLogger(const SubsystemMap *) {}
  int log_entry(const Entry &) {
    return 0;
  }
};

class JournaldClusterLogger {
public:
  int log_log_entry(const LogEntry &le) {
    return 0;
  }
};

#endif // WITH_SYSTEMD

} // ceph::logging

#endif
