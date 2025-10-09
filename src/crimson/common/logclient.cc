#include "crimson/common/logclient.h"
#include <fmt/ranges.h>
#include "include/str_map.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"
#include "messages/MMonGetVersion.h"
#include "crimson/net/Messenger.h"
#include "crimson/mon/MonClient.h"
#include "mon/MonMap.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/Graylog.h"

using std::map;
using std::ostream;
using std::ostringstream;
using std::string;
using crimson::common::local_conf;

namespace {
  seastar::logger& logger()
  {
    return crimson::get_logger(ceph_subsys_monc);
  }
}

//TODO: in order to avoid unnecessary maps declarations and moving around,
//	create a named structure containing the maps and return optional
//	fit to it.
int parse_log_client_options(CephContext *cct,
			     map<string,string> &log_to_monitors,
			     map<string,string> &log_to_syslog,
			     map<string,string> &log_channels,
			     map<string,string> &log_prios,
			     map<string,string> &log_to_graylog,
			     map<string,string> &log_to_graylog_host,
			     map<string,string> &log_to_graylog_port,
			     uuid_d &fsid,
			     string &host)
{
  ostringstream oss;

  int r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_monitors"), oss,
    &log_to_monitors, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_monitors'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_syslog"), oss,
                              &log_to_syslog, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_syslog'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_syslog_facility"), oss,
    &log_channels, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_syslog_facility'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_syslog_level"), oss,
    &log_prios, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_syslog_level'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_graylog"), oss,
    &log_to_graylog, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_graylog'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_graylog_host"), oss,
    &log_to_graylog_host, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_graylog_host'", __func__);
    return r;
  }

  r = get_conf_str_map_helper(
    cct->_conf.get_val<string>("clog_to_graylog_port"), oss,
    &log_to_graylog_port, CLOG_CONFIG_DEFAULT_KEY);
  if (r < 0) {
    logger().error("{} error parsing 'clog_to_graylog_port'", __func__);
    return r;
  }

  fsid = cct->_conf.get_val<uuid_d>("fsid");
  host = cct->_conf->host;
  return 0;
}

LogChannel::LogChannel(LogClient *lc, const string &channel)
  : parent(lc), log_channel(channel), log_to_syslog(false),
    log_to_monitors(false)
{
}

LogChannel::LogChannel(LogClient *lc, const string &channel,
                       const string &facility, const string &prio)
  : parent(lc), log_channel(channel), log_prio(prio),
    syslog_facility(facility), log_to_syslog(false),
    log_to_monitors(false)
{
}

LogClient::LogClient(crimson::net::Messenger *m,
		     logclient_flag_t flags)
  : messenger(m), is_mon(flags & FLAG_MON),
    last_log_sent(0), last_log(0)
{
}

void LogChannel::set_log_to_monitors(bool v)
{
  if (log_to_monitors != v) {
    parent->reset();
    log_to_monitors = v;
  }
}

void LogChannel::update_config(map<string,string> &log_to_monitors,
			       map<string,string> &log_to_syslog,
			       map<string,string> &log_channels,
			       map<string,string> &log_prios,
			       map<string,string> &log_to_graylog,
			       map<string,string> &log_to_graylog_host,
			       map<string,string> &log_to_graylog_port,
			       uuid_d &fsid,
			       string &host)
{
  logger().debug(
    "{} log_to_monitors {} log_to_syslog {} log_channels {} log_prios {}",
    __func__, log_to_monitors, log_to_syslog, log_channels, log_prios);
  bool to_monitors = (get_str_map_key(log_to_monitors, log_channel,
                                      &CLOG_CONFIG_DEFAULT_KEY) == "true");
  bool to_syslog = (get_str_map_key(log_to_syslog, log_channel,
                                    &CLOG_CONFIG_DEFAULT_KEY) == "true");
  string syslog_facility = get_str_map_key(log_channels, log_channel,
					   &CLOG_CONFIG_DEFAULT_KEY);
  string prio = get_str_map_key(log_prios, log_channel,
				&CLOG_CONFIG_DEFAULT_KEY);
  bool to_graylog = (get_str_map_key(log_to_graylog, log_channel,
				     &CLOG_CONFIG_DEFAULT_KEY) == "true");
  string graylog_host = get_str_map_key(log_to_graylog_host, log_channel,
				       &CLOG_CONFIG_DEFAULT_KEY);
  string graylog_port_str = get_str_map_key(log_to_graylog_port, log_channel,
					    &CLOG_CONFIG_DEFAULT_KEY);
  int graylog_port = atoi(graylog_port_str.c_str());

  set_log_to_monitors(to_monitors);
  set_log_to_syslog(to_syslog);
  set_syslog_facility(syslog_facility);
  set_log_prio(prio);

  if (to_graylog && !graylog) { /* should but isn't */
    graylog = seastar::make_shared<ceph::logging::Graylog>("clog");
  } else if (!to_graylog && graylog) { /* shouldn't but is */
    graylog = nullptr;
  }

  if (to_graylog && graylog) {
    graylog->set_fsid(fsid);
    graylog->set_hostname(host);
  }

  if (graylog && (!graylog_host.empty()) && (graylog_port != 0)) {
    graylog->set_destination(graylog_host, graylog_port);
  }

  logger().debug("{} to_monitors: {} to_syslog: {}"
	  "syslog_facility: {} prio: {} to_graylog: {} graylog_host: {}"
	  "graylog_port: {}", __func__, (to_monitors ? "true" : "false"),
	  (to_syslog ? "true" : "false"), syslog_facility, prio,
	  (to_graylog ? "true" : "false"), graylog_host, graylog_port);
}

void LogChannel::do_log(clog_type prio, std::stringstream& ss)
{
  while (!ss.eof()) {
    string s;
    getline(ss, s);
    if (!s.empty()) {
      do_log(prio, s);
    }
  }
}

void LogChannel::do_log(clog_type prio, const std::string& s)
{
  if (CLOG_ERROR == prio) {
    logger().error("log {} : {}", prio, s);
  } else {
    logger().warn("log {} : {}", prio, s);
  }
  LogEntry e;
  e.stamp = ceph_clock_now();
  e.addrs = parent->get_myaddrs();
  e.name = parent->get_myname();
  e.rank = parent->get_myrank();
  e.prio = prio;
  e.msg = s;
  e.channel = get_log_channel();

  // seq and who should be set for syslog/graylog/log_to_mon
  // log to monitor?
  if (log_to_monitors) {
    e.seq = parent->queue(e);
  } else {
    e.seq = parent->get_next_seq();
  }

  // log to syslog?
  if (do_log_to_syslog()) {
    logger().warn("{} log to syslog", __func__);
    e.log_to_syslog(get_log_prio(), get_syslog_facility());
  }

  // log to graylog?
  if (do_log_to_graylog()) {
    logger().warn("{} log to graylog", __func__);
    graylog->log_log_entry(&e);
  }
}

MessageURef LogClient::get_mon_log_message(log_flushing_t flush_flag)
{
  if (flush_flag == log_flushing_t::FLUSH) {
    if (log_queue.empty()) {
      return {};
    }
    // reset session
    last_log_sent = log_queue.front().seq;
  }
  return _get_mon_log_message();
}

bool LogClient::are_pending() const
{
  return last_log > last_log_sent;
}

MessageURef LogClient::_get_mon_log_message()
{
  if (log_queue.empty()) {
    return {};
  }

  // only send entries that haven't been sent yet during this mon
  // session!  monclient needs to call reset_session() on mon session
  // reset for this to work right.

  if (last_log_sent == last_log) {
    return {};
  }

  // limit entries per message
  const int64_t num_unsent = last_log - last_log_sent;
  int64_t num_to_send;
  if (local_conf()->mon_client_max_log_entries_per_message > 0) {
    num_to_send = std::min(num_unsent,
		 local_conf()->mon_client_max_log_entries_per_message);
  } else {
    num_to_send = num_unsent;
  }

  logger().debug("log_queue is {} last_log {} sent {} num {} unsent {}"
		" sending {}", log_queue.size(), last_log,
		last_log_sent, log_queue.size(), num_unsent, num_to_send);
  ceph_assert((unsigned)num_unsent <= log_queue.size());
  auto log_iter = log_queue.begin();
  std::deque<LogEntry> out_log_queue; /* will send the logs contained here */
  while (log_iter->seq <= last_log_sent) {
    ++log_iter;
    ceph_assert(log_iter != log_queue.end());
  }
  while (num_to_send--) {
    ceph_assert(log_iter != log_queue.end());
    out_log_queue.push_back(*log_iter);
    last_log_sent = log_iter->seq;
    logger().debug(" will send {}", *log_iter);
    ++log_iter;
  }
  
  return crimson::make_message<MLog>(m_fsid,
				  std::move(out_log_queue));
}

version_t LogClient::queue(LogEntry &entry)
{
  entry.seq = ++last_log;
  log_queue.push_back(entry);

  return entry.seq;
}

void LogClient::reset()
{
  if (log_queue.size()) {
    log_queue.clear();
  }
  last_log_sent = last_log;
}

uint64_t LogClient::get_next_seq()
{
  return ++last_log;
}

entity_addrvec_t LogClient::get_myaddrs() const
{
  return messenger->get_myaddrs();
}

entity_name_t LogClient::get_myrank()
{
  return messenger->get_myname();
}

const EntityName& LogClient::get_myname() const
{
  return local_conf()->name;
}

seastar::future<> LogClient::handle_log_ack(Ref<MLogAck> m)
{
  logger().debug("handle_log_ack {}", *m);

  version_t last = m->last;

  auto q = log_queue.begin();
  while (q != log_queue.end()) {
    const LogEntry &entry(*q);
    if (entry.seq > last)
      break;
    logger().debug(" logged {}", entry);
    q = log_queue.erase(q);
  }
  return seastar::now();
}

LogChannelRef LogClient::create_channel(const std::string& name) {
  auto it = channels.find(name);
  if (it == channels.end()) {
    it = channels.insert(it,
           {name, seastar::make_lw_shared<LogChannel>(this, name)});
  }
  return it->second;
}

seastar::future<> LogClient::set_fsid(const uuid_d& fsid) {
  m_fsid = fsid;
  return seastar::now();
}

