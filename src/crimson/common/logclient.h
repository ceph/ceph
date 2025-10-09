#ifndef CEPH_LOGCLIENT_H
#define CEPH_LOGCLIENT_H

#include "common/LogEntry.h"
#include "common/ostream_temp.h"
#include "common/ref.h"
#include "include/health.h"
#include "crimson/net/Fwd.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

class LogClient;
class MLog;
class MLogAck;
class Message;
struct uuid_d;
struct Connection;

class LogChannel;

namespace ceph {
namespace logging {
  class Graylog;
}
}

template<typename Message> using Ref = boost::intrusive_ptr<Message>;
namespace crimson::net {
  class Messenger;
}

enum class log_flushing_t {
  NO_FLUSH,
  FLUSH
};

int parse_log_client_options(CephContext *cct,
			     std::map<std::string,std::string> &log_to_monitors,
			     std::map<std::string,std::string> &log_to_syslog,
			     std::map<std::string,std::string> &log_channels,
			     std::map<std::string,std::string> &log_prios,
			     std::map<std::string,std::string> &log_to_graylog,
			     std::map<std::string,std::string> &log_to_graylog_host,
			     std::map<std::string,std::string> &log_to_graylog_port,
			     uuid_d &fsid,
			     std::string &host);

/** Manage where we output to and at which priority
 *
 * Not to be confused with the LogClient, which is the almighty coordinator
 * of channels.  We just deal with the boring part of the logging: send to
 * syslog, send to file, generate LogEntry and queue it for the LogClient.
 *
 * Past queueing the LogEntry, the LogChannel is done with the whole thing.
 * LogClient will deal with sending and handling of LogEntries.
 */
class LogChannel : public LoggerSinkSet
{
public:
  LogChannel(LogClient *lc, const std::string &channel);
  LogChannel(LogClient *lc, const std::string &channel,
             const std::string &facility, const std::string &prio);

  OstreamTemp debug() {
    return OstreamTemp(CLOG_DEBUG, this);
  }
  void debug(std::stringstream &s) final {
    do_log(CLOG_DEBUG, s);
  }
  /**
   * Convenience function mapping health status to
   * the appropriate cluster log severity.
   */
  OstreamTemp health(health_status_t health) {
    switch(health) {
      case HEALTH_OK:
        return info();
      case HEALTH_WARN:
        return warn();
      case HEALTH_ERR:
        return error();
      default:
        // Invalid health_status_t value
        ceph_abort();
    }
  }
  OstreamTemp info() final {
    return OstreamTemp(CLOG_INFO, this);
  }
  void info(std::stringstream &s) final {
    do_log(CLOG_INFO, s);
  }
  OstreamTemp warn() final {
    return OstreamTemp(CLOG_WARN, this);
  }
  void warn(std::stringstream &s) final {
    do_log(CLOG_WARN, s);
  }
  OstreamTemp error() final {
    return OstreamTemp(CLOG_ERROR, this);
  }
  void error(std::stringstream &s) final {
    do_log(CLOG_ERROR, s);
  }
  OstreamTemp sec() final {
    return OstreamTemp(CLOG_SEC, this);
  }
  void sec(std::stringstream &s) final {
    do_log(CLOG_SEC, s);
  }

  void set_log_to_monitors(bool v);
  void set_log_to_syslog(bool v) {
    log_to_syslog = v;
  }
  void set_log_channel(const std::string& v) {
    log_channel = v;
  }
  void set_log_prio(const std::string& v) {
    log_prio = v;
  }
  void set_syslog_facility(const std::string& v) {
    syslog_facility = v;
  }
  const std::string& get_log_prio() const { return log_prio; }
  const std::string& get_log_channel() const { return log_channel; }
  const std::string& get_syslog_facility() const { return syslog_facility; }
  bool must_log_to_syslog() const { return log_to_syslog; }
  /**
   * Do we want to log to syslog?
   *
   * @return true if log_to_syslog is true and both channel and prio
   *         are not empty; false otherwise.
   */
  bool do_log_to_syslog() {
    return must_log_to_syslog() &&
          !log_prio.empty() && !log_channel.empty();
  }
  bool must_log_to_monitors() { return log_to_monitors; }

  bool do_log_to_graylog() {
    return (graylog != nullptr);
  }

  using Ref = seastar::lw_shared_ptr<LogChannel>;

  /**
   * update config values from parsed k/v std::map for each config option
   *
   * Pick out the relevant value based on our channel.
   */
  void update_config(std::map<std::string,std::string> &log_to_monitors,
		     std::map<std::string,std::string> &log_to_syslog,
		     std::map<std::string,std::string> &log_channels,
		     std::map<std::string,std::string> &log_prios,
		     std::map<std::string,std::string> &log_to_graylog,
		     std::map<std::string,std::string> &log_to_graylog_host,
		     std::map<std::string,std::string> &log_to_graylog_port,
		     uuid_d &fsid,
		     std::string &host);

  void do_log(clog_type prio, std::stringstream& ss) final;
  void do_log(clog_type prio, const std::string& s) final;

private:
  LogClient *parent;
  std::string log_channel;
  std::string log_prio;
  std::string syslog_facility;
  bool log_to_syslog;
  bool log_to_monitors;
  seastar::shared_ptr<ceph::logging::Graylog> graylog;
};

using LogChannelRef = LogChannel::Ref;

class LogClient
{
public:
  enum logclient_flag_t {
    NO_FLAGS = 0,
    FLAG_MON = 0x1,
  };

  LogClient(crimson::net::Messenger *m, logclient_flag_t flags);

  virtual ~LogClient() = default;

  seastar::future<> handle_log_ack(Ref<MLogAck> m);
  MessageURef get_mon_log_message(log_flushing_t flush_flag);
  bool are_pending() const;

  LogChannelRef create_channel() {
    return create_channel(CLOG_CHANNEL_DEFAULT);
  }

  LogChannelRef create_channel(const std::string& name);

  void destroy_channel(const std::string& name) {
    channels.erase(name);
  }

  void shutdown() {
    channels.clear();
  }

  uint64_t get_next_seq();
  entity_addrvec_t get_myaddrs() const;
  const EntityName& get_myname() const;
  entity_name_t get_myrank();
  version_t queue(LogEntry &entry);
  void reset();
  seastar::future<> set_fsid(const uuid_d& fsid);

private:
  MessageURef _get_mon_log_message();

  crimson::net::Messenger *messenger;
  bool is_mon;
  version_t last_log_sent;
  version_t last_log;
  std::deque<LogEntry> log_queue;

  std::map<std::string, LogChannelRef> channels;
  uuid_d m_fsid;
};
#endif

