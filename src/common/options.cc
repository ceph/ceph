// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "options.h"
#include "common/Formatter.h"

// Helpers for validators
#include "include/stringify.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>


void Option::dump_value(const char *field_name,
    const Option::value_t &v, Formatter *f) const
{
  if (boost::get<boost::blank>(&v)) {
    // This should be nil but Formatter doesn't allow it.
    f->dump_string(field_name, "");
  } else if (type == TYPE_UINT) {
    f->dump_unsigned(field_name, boost::get<uint64_t>(v));
  } else if (type == TYPE_INT) {
    f->dump_int(field_name, boost::get<int64_t>(v));
  } else if (type == TYPE_STR) {
    f->dump_string(field_name, boost::get<std::string>(v));
  } else if (type == TYPE_FLOAT) {
    f->dump_float(field_name, boost::get<double>(v));
  } else if (type == TYPE_BOOL) {
    f->dump_bool(field_name, boost::get<bool>(v));
  } else {
    f->dump_stream(field_name) << v;
  }
}

int Option::pre_validate(std::string *new_value, std::string *err) const
{
  if (validator) {
    return validator(new_value, err);
  } else {
    return 0;
  }
}

int Option::validate(const Option::value_t &new_value, std::string *err) const
{
  // Generic validation: min
  if (!boost::get<boost::blank>(&(min))) {
    if (new_value < min) {
      std::ostringstream oss;
      oss << "Value '" << new_value << "' is below minimum " << min;
      *err = oss.str();
      return -EINVAL;
    }
  }

  // Generic validation: max
  if (!boost::get<boost::blank>(&(max))) {
    if (new_value > max) {
      std::ostringstream oss;
      oss << "Value '" << new_value << "' exceeds maximum " << max;
      *err = oss.str();
      return -EINVAL;
    }
  }

  // Generic validation: enum
  if (!enum_allowed.empty() && type == Option::TYPE_STR) {
    auto found = std::find(enum_allowed.begin(), enum_allowed.end(),
                           boost::get<std::string>(new_value));
    if (found == enum_allowed.end()) {
      std::ostringstream oss;
      oss << "'" << new_value << "' is not one of the permitted "
                 "values: " << joinify(enum_allowed.begin(),
                                       enum_allowed.end(),
                                       std::string(", "));
      *err = oss.str();
      return -EINVAL;
    }
  }

  return 0;
}

void Option::dump(Formatter *f) const
{
  f->open_object_section("option");
  f->dump_string("name", name);

  f->dump_string("type", type_to_str(type));

  f->dump_string("level", level_to_str(level));

  f->dump_string("desc", desc);
  f->dump_string("long_desc", long_desc);

  dump_value("default", value, f);
  dump_value("daemon_default", daemon_value, f);

  f->open_array_section("tags");
  for (const auto t : tags) {
    f->dump_string("tag", t);
  }
  f->close_section();

  f->open_array_section("services");
  for (const auto s : services) {
    f->dump_string("service", s);
  }
  f->close_section();

  f->open_array_section("see_also");
  for (const auto sa : see_also) {
    f->dump_string("see_also", sa);
  }
  f->close_section();

  if (type == TYPE_STR) {
    f->open_array_section("enum_values");
    for (const auto &ea : enum_allowed) {
      f->dump_string("enum_value", ea);
    }
    f->close_section();
  }

  dump_value("min", min, f);
  dump_value("max", max, f);

  f->close_section();
}

constexpr unsigned long long operator"" _min (unsigned long long min) {
  return min * 60;
}
constexpr unsigned long long operator"" _hr (unsigned long long hr) {
  return hr * 60 * 60;
}
constexpr unsigned long long operator"" _day (unsigned long long day) {
  return day * 60 * 60 * 24;
}
constexpr unsigned long long operator"" _K (unsigned long long n) {
  return n << 10;
}
constexpr unsigned long long operator"" _M (unsigned long long n) {
  return n << 20;
}
constexpr unsigned long long operator"" _G (unsigned long long n) {
  return n << 30;
}

std::vector<Option> get_global_options() {
  return std::vector<Option>({
    Option("host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("local hostname")
    .set_long_description("if blank, ceph assumes the short hostname (hostname -s)")
    .add_service("common")
    .add_tag("network"),

    Option("fsid", Option::TYPE_UUID, Option::LEVEL_BASIC)
    .set_description("cluster fsid (uuid)")
    .add_service("common")
    .add_tag("service"),

    Option("public_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("public-facing address to bind to")
    .add_service({"mon", "mds", "osd", "mgr"}),

    Option("public_bind_addr", Option::TYPE_ADDR, Option::LEVEL_ADVANCED)
    .set_default(entity_addr_t())
    .add_service("mon")
    .set_description(""),

    Option("cluster_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("cluster-facing address to bind to")
    .add_service("osd")
    .add_tag("network"),

    Option("public_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service({"mon", "mds", "osd", "mgr"})
    .add_tag("network")
    .set_description(""),

    Option("cluster_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service("osd")
    .add_tag("network")
    .set_description(""),

    Option("monmap", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to MonMap file")
    .set_long_description("This option is normally used during mkfs, but can also "
  			"be used to identify which monitors to connect to.")
    .add_service("mon")
    .add_tag("mkfs"),

    Option("mon_host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("list of hosts or addresses to search for a monitor")
    .set_long_description("This is a comma, whitespace, or semicolon separated "
  			"list of IP addresses or hostnames. Hostnames are "
  			"resolved via DNS and all A or AAAA records are "
  			"included in the search list.")
    .add_service("common"),

    Option("mon_dns_srv_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ceph-mon")
    .set_description("name of DNS SRV record to check for monitor addresses")
    .add_service("common")
    .add_tag("network")
    .add_see_also("mon_host"),

    // lockdep
    Option("lockdep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable lockdep lock dependency analyzer")
    .add_service("common"),

    Option("lockdep_force_backtrace", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("always gather current backtrace at every lock")
    .add_service("common")
    .add_see_also("lockdep"),

    Option("run_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/run/ceph")
    .set_description("path for the 'run' directory for storing pid and socket files")
    .add_service("common")
    .add_see_also("admin_socket"),

    Option("admin_socket", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_daemon_default("$run_dir/$cluster-$name.asok")
    .set_description("path for the runtime control socket file, used by the 'ceph daemon' command")
    .add_service("common"),

    Option("admin_socket_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("file mode to set for the admin socket file, e.g, '0755'")
    .add_service("common")
    .add_see_also("admin_socket"),

    Option("crushtool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("name of the 'crushtool' utility")
    .add_service("mon"),

    // daemon
    Option("daemonize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_daemon_default(true)
    .set_description("whether to daemonize (background) after startup")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"pid_file", "chdir"}),

    Option("setuser", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("uid or user name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("setgroup"),

    Option("setgroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("gid or group name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("setuser"),

    Option("setuser_match_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("if set, setuser/setgroup is condition on this path matching ownership")
    .set_long_description("If setuser or setgroup are specified, and this option is non-empty, then the uid/gid of the daemon will only be changed if the file or directory specified by this option has a matching uid and/or gid.  This exists primarily to allow switching to user ceph for OSDs to be conditional on whether the osd data contents have also been chowned after an upgrade.  This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"setuser", "setgroup"}),

    Option("pid_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to write a pid file (if any)")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    Option("chdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to chdir(2) to after daemonizing")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("daemonize"),

    Option("fatal_signal_handlers", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to register signal handlers for SIGABRT etc that dump a stack trace")
    .set_long_description("This is normally true for daemons and values for libraries.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    // restapi
    Option("restapi_log_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("restapi_base_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("erasure_code_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_PKGLIBDIR"/erasure-code")
    .set_description("directory where erasure-code plugins can be found")
    .add_service({"mon", "osd"})
    .set_safe(),

    // logging
    Option("log_file", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_daemon_default("/var/log/ceph/$cluster-$name.log")
    .set_description("path to log file")
    .add_see_also({"log_to_stderr",
                   "err_to_stderr",
                   "log_to_syslog",
                   "err_to_syslog"}),

    Option("log_max_new", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("max unwritten log entries to allow before waiting to flush to the log")
    .add_see_also("log_max_recent"),

    Option("log_max_recent", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_daemon_default(10000)
    .set_description("recent log entries to keep in memory to dump in the event of a crash")
    .set_long_description("The purpose of this option is to log at a higher debug level only to the in-memory buffer, and write out the detailed log messages only if there is a crash.  Only log entries below the lower log level will be written unconditionally to the log.  For example, debug_osd=1/5 will write everything <= 1 to the log unconditionally but keep entries at levels 2-5 in memory.  If there is a seg fault or assertion failure, all entries will be dumped to the log."),

    Option("log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_daemon_default(false)
    .set_description("send log lines to stderr"),

    Option("err_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_daemon_default(true)
    .set_description("send critical error log lines to stderr"),

    Option("log_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("send log lines to syslog facility"),

    Option("err_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("send critical error log lines to syslog facility"),

    Option("log_flush_on_exit", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("set a process exit handler to ensure the log is flushed on exit"),

    Option("log_stop_at_utilization", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_default(.97)
    .set_min_max(0.0, 1.0)
    .set_description("stop writing to the log file when device utilization reaches this ratio")
    .add_see_also("log_file"),

    Option("log_to_graylog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("send log lines to remote graylog server")
    .add_see_also({"err_to_graylog",
                   "log_graylog_host",
                   "log_graylog_port"}),

    Option("err_to_graylog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("send critical error log lines to remote graylog server")
    .add_see_also({"log_to_graylog",
                   "log_graylog_host",
                   "log_graylog_port"}),

    Option("log_graylog_host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("127.0.0.1")
    .set_description("address or hostname of graylog server to log to")
    .add_see_also({"log_to_graylog",
                   "err_to_graylog",
                   "log_graylog_port"}),

    Option("log_graylog_port", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(12201)
    .set_description("port number for the remote graylog server")
    .add_see_also("log_graylog_host"),



    // unmodified
    Option("clog_to_monitors", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=true")
    .set_description(""),

    Option("clog_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_description(""),

    Option("clog_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("info")
    .set_description(""),

    Option("clog_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=daemon audit=local0")
    .set_description(""),

    Option("clog_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_description(""),

    Option("clog_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description(""),

    Option("clog_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("12201")
    .set_description(""),

    Option("mon_cluster_log_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=false")
    .set_description(""),

    Option("mon_cluster_log_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("info")
    .set_description(""),

    Option("mon_cluster_log_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("daemon")
    .set_description(""),

    Option("mon_cluster_log_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=/var/log/ceph/$cluster.$channel.log cluster=/var/log/ceph/$cluster.log")
    .set_description(""),

    Option("mon_cluster_log_file_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("info")
    .set_description(""),

    Option("mon_cluster_log_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_description(""),

    Option("mon_cluster_log_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description(""),

    Option("mon_cluster_log_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("12201")
    .set_description(""),

    Option("enable_experimental_unrecoverable_data_corrupting_features", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("plugin_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_PKGLIBDIR)
    .set_description("")
    .set_safe(),

    Option("xio_trace_mempool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("xio_trace_msgcnt", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("xio_trace_xcon", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("xio_queue_depth", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("xio_mp_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("xio_mp_max_64", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("xio_mp_max_256", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description(""),

    Option("xio_mp_max_1k", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description(""),

    Option("xio_mp_max_page", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("xio_mp_max_hint", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("xio_portal_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("xio_max_conns_per_portal", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("xio_transport_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rdma")
    .set_description(""),

    Option("xio_max_send_inline", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("compressor_zlib_isal", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("compressor_zlib_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("async_compressor_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("async_compressor_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("snappy")
    .set_description(""),

    Option("async_compressor_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("async_compressor_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("async_compressor_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("plugin_crypto_accelerator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("crypto_isal")
    .set_description(""),

    Option("mempool_debug", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("keyfile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("keyring", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(
      "/etc/ceph/$cluster.$name.keyring,/etc/ceph/$cluster.keyring,"
      "/etc/ceph/keyring,/etc/ceph/keyring.bin," 
  #if defined(__FreeBSD)
      "/usr/local/etc/ceph/$cluster.$name.keyring,"
      "/usr/local/etc/ceph/$cluster.keyring,"
      "/usr/local/etc/ceph/keyring,/usr/local/etc/ceph/keyring.bin," 
  #endif
    )
    .set_description(""),

    Option("heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("heartbeat_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("heartbeat_inject_failure", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("perf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("async+posix")
    .set_description("")
    .set_safe(),

    Option("ms_public_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_cluster_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_tcp_nodelay", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_tcp_rcvbuf", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("ms_tcp_prefetch_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("ms_initial_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.2)
    .set_description(""),

    Option("ms_max_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(15.0)
    .set_description(""),

    Option("ms_crc_data", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_crc_header", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_die_on_bad_msg", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_die_on_unhandled_msg", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_die_on_old_message", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_die_on_skipped_message", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_dispatch_throttle_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100 << 20)
    .set_description(""),

    Option("ms_bind_ipv6", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_bind_port_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6800)
    .set_description(""),

    Option("ms_bind_port_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7300)
    .set_description(""),

    Option("ms_bind_retry_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  #if !defined(__FreeBSD__)
    .set_default(3)
  #else
    // FreeBSD does not use SO_REAUSEADDR so allow for a bit more time per default
    .set_default(6)
  #endif
    .set_description(""),

    Option("ms_bind_retry_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  #if !defined(__FreeBSD__)
    .set_default(5)
  #else
    // FreeBSD does not use SO_REAUSEADDR so allow for a bit more time per default
    .set_default(6)
  #endif
    .set_description(""),

    Option("ms_bind_before_connect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_tcp_listen_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("ms_rwthread_stack_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description(""),

    Option("ms_tcp_read_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(900)
    .set_description(""),

    Option("ms_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16777216)
    .set_description(""),

    Option("ms_pq_min_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("ms_inject_socket_failures", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("ms_inject_delay_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("")
    .set_safe(),

    Option("ms_inject_delay_msg_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("ms_inject_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("ms_inject_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("ms_inject_internal_delays", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("ms_dump_on_send", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_dump_corrupt_message_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("ms_async_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("ms_async_max_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("ms_async_set_affinity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_async_affinity_cores", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_async_rdma_device_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_async_rdma_enable_hugepage", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_async_rdma_buffer_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128_K)
    .set_description(""),

    Option("ms_async_rdma_send_buffers", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_K)
    .set_description(""),

    Option("ms_async_rdma_receive_buffers", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32768)
    .set_description(""),

    Option("ms_async_rdma_receive_queue_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("ms_async_rdma_port_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("ms_async_rdma_polling_us", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("ms_async_rdma_local_gid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_async_rdma_roce_ver", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("ms_async_rdma_sl", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("ms_async_rdma_dscp", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(96)
    .set_description(""),

    Option("ms_dpdk_port_id", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("ms_dpdk_coremask", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("1")
    .set_description("")
    .set_safe(),

    Option("ms_dpdk_memory_channel", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("4")
    .set_description(""),

    Option("ms_dpdk_hugepages", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_dpdk_pmd", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_dpdk_host_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("")
    .set_safe(),

    Option("ms_dpdk_gateway_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("")
    .set_safe(),

    Option("ms_dpdk_netmask_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("")
    .set_safe(),

    Option("ms_dpdk_lro", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_dpdk_hw_flow_control", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_dpdk_hw_queue_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("ms_dpdk_debug_allow_loopback", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_dpdk_rx_buffer_count_per_core", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description(""),

    Option("inject_early_sigterm", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/mon/$cluster-$id")
    .set_description(""),

    Option("mon_initial_members", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("mon_compact_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_compact_on_bootstrap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_compact_on_trim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mon_cpu_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("mon_osd_mapping_pgs_per_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("mon_osd_max_creating_pgs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("mon_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_session_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("mon_subscribe_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("mon_delta_reset_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mon_osd_laggy_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("mon_osd_laggy_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .set_description(""),

    Option("mon_osd_laggy_max_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("mon_osd_adjust_heartbeat_grace", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_adjust_down_out_interval", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_auto_mark_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_osd_auto_mark_auto_out_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_auto_mark_new_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_destroyed_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("mon_osd_down_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("mon_osd_down_out_subtree_limit", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rack")
    .set_description(""),

    Option("mon_osd_min_up_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .set_description(""),

    Option("mon_osd_min_in_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.75)
    .set_description(""),

    Option("mon_osd_warn_op_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("mon_osd_err_op_age_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("mon_osd_max_split_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("mon_osd_prime_pg_temp", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_osd_prime_pg_temp_max_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.5)
    .set_description(""),

    Option("mon_osd_prime_pg_temp_max_estimate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.25)
    .set_description(""),

    Option("mon_osd_pool_ec_fast_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_stat_smooth_intervals", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6)
    .set_description(""),

    Option("mon_election_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_lease", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_lease_renew_interval_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.6)
    .set_description(""),

    Option("mon_lease_ack_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description(""),

    Option("mon_accept_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description(""),

    Option("mon_clock_drift_allowed", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.050)
    .set_description(""),

    Option("mon_clock_drift_warn_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_timecheck_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(300.0)
    .set_description(""),

    Option("mon_timecheck_skew_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("mon_pg_stuck_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mon_pg_min_inactive", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("mon_pg_warn_min_per_osd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mon_pg_warn_max_per_osd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("mon_pg_warn_max_object_skew", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("mon_pg_warn_min_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mon_pg_warn_min_pool_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("mon_pg_check_down_all_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.5)
    .set_description(""),

    Option("mon_cache_target_full_warn_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.66)
    .set_description(""),

    Option("mon_osd_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.95)
    .set_description(""),

    Option("mon_osd_backfillfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.90)
    .set_description(""),

    Option("mon_osd_nearfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.85)
    .set_description(""),

    Option("mon_osd_initial_require_min_compat_client", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("jewel")
    .set_description(""),

    Option("mon_allow_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_fake_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_globalid_prealloc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mon_osd_report_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(900)
    .set_description(""),

    Option("mon_force_standby_active", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_warn_on_legacy_crush_tunables", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_crush_min_required_version", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("firefly")
    .set_description(""),

    Option("mon_warn_on_crush_straw_calc_version_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_warn_on_osd_down_out_interval_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_warn_on_cache_pools_without_hit_sets", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_warn_on_pool_no_app", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Enable POOL_APP_NOT_ENABLED health check"),

    Option("mon_min_osdmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("mon_max_pgmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("mon_max_log_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("mon_max_mdsmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("mon_max_osd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mon_probe_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description(""),

    Option("mon_client_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100ul << 20)
    .set_description(""),

    Option("mon_mgr_proxy_client_bytes_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .set_description(""),

    Option("mon_log_max_summary", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("mon_daemon_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(400ul << 20)
    .set_description(""),

    Option("mon_max_log_entries_per_event", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("mon_reweight_min_pgs_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mon_reweight_min_bytes_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description(""),

    Option("mon_reweight_max_osds", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("mon_reweight_max_change", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .set_description(""),

    Option("mon_health_data_update_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .set_description(""),

    Option("mon_health_to_clog", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mon_health_to_clog_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("mon_health_to_clog_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .set_description(""),

    Option("mon_health_preluminous_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Include health warnings in preluminous JSON fields"),

    Option("mon_health_preluminous_compat_warning", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Warn about the health JSON format change in preluminous JSON fields"),

    Option("mon_health_max_detail", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("mon_health_log_update_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .set_description("Minimum time in seconds between log messages about "
                     "each health check")
    .set_min(0),

    Option("mon_data_avail_crit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_data_avail_warn", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mon_data_size_warn", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(15_G)
    .set_description(""),

    Option("mon_warn_not_scrubbed", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mon_warn_not_deep_scrubbed", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mon_scrub_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("mon_scrub_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_min)
    .set_description(""),

    Option("mon_scrub_max_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("mon_scrub_inject_crc_mismatch", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .set_description(""),

    Option("mon_scrub_inject_missing_keys", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .set_description(""),

    Option("mon_config_key_max_entry_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("mon_sync_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .set_description(""),

    Option("mon_sync_max_payload_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1048576)
    .set_description(""),

    Option("mon_sync_debug", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_inject_sync_get_chunk_delay", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mon_osd_min_down_reporters", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("mon_osd_reporter_subtree_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("host")
    .set_description(""),

    Option("mon_osd_force_trim_to", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mon_mds_force_trim_to", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mon_mds_skip_sanity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_deprecated_as_obsolete", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_dump_transactions", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_dump_json", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_dump_location", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("/var/log/ceph/$cluster-$name.tdump")
    .set_description(""),

    Option("mon_debug_no_require_mimic", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_no_require_bluestore_for_ec_overwrites", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_debug_no_initial_persistent_features", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_inject_transaction_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(10.0)
    .set_description(""),

    Option("mon_inject_transaction_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mon_sync_provider_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mon_sync_requester_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mon_force_quorum_join", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mon_keyvaluedb", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb")
    .set_description(""),

    Option("mon_debug_unsafe_allow_tier_with_nonempty_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_osd_blacklist_default_expire", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("mon_osd_crush_smoke_test", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("paxos_stash_full_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .set_description(""),

    Option("paxos_max_join_drift", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("paxos_propose_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("paxos_min_wait", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .set_description(""),

    Option("paxos_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("paxos_trim_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .set_description(""),

    Option("paxos_trim_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("paxos_service_trim_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .set_description(""),

    Option("paxos_service_trim_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("paxos_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("auth_cluster_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx")
    .set_description(""),

    Option("auth_service_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx")
    .set_description(""),

    Option("auth_client_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx, none")
    .set_description(""),

    Option("auth_supported", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("max_rotating_auth_attempts", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("cephx_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_cluster_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_service_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_sign_messages", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("auth_mon_ticket_ttl", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(12_hr)
    .set_description(""),

    Option("auth_service_ticket_ttl", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("auth_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mon_client_hunt_parallel", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("mon_client_hunt_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3.0)
    .set_description(""),

    Option("mon_client_ping_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("mon_client_ping_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("mon_client_hunt_interval_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description(""),

    Option("mon_client_hunt_interval_min_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("mon_client_hunt_interval_max_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("mon_client_max_log_entries_per_message", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("mon_max_pool_pg_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("mon_pool_quota_warn_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mon_pool_quota_crit_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("crush_location", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("crush_location_hook", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("crush_location_hook_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("objecter_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description(""),

    Option("objecter_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("objecter_inflight_op_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description(""),

    Option("objecter_inflight_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("objecter_completion_locks_per_session", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("objecter_inject_no_watch_ping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("objecter_retry_writes_after_first_reply", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("objecter_debug_inject_relock_delay", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filer_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("filer_max_truncate_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("journaler_write_head_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description(""),

    Option("journaler_prefetch_periods", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("journaler_prezero_periods", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_check_max_object_name_len_on_startup", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_max_backfills", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_min_recovery_priority", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_backfill_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("osd_recovery_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("osd_agent_max_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("osd_agent_max_low_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_agent_min_evict_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.1)
    .set_description(""),

    Option("osd_agent_quantize_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.1)
    .set_description(""),

    Option("osd_agent_delay_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description(""),

    Option("osd_find_best_info_ignore_history_les", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_agent_hist_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("osd_agent_slop", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.02)
    .set_description(""),

    Option("osd_uuid", Option::TYPE_UUID, Option::LEVEL_ADVANCED)
    .set_default(uuid_d())
    .set_description(""),

    Option("osd_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/osd/$cluster-$id")
    .set_description(""),

    Option("osd_journal", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/osd/$cluster-$id/journal")
    .set_description(""),

    Option("osd_journal_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5120)
    .set_description(""),

    Option("osd_journal_flush_on_shutdown", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_os_flags", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_max_write_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(90)
    .set_description(""),

    Option("osd_max_pgls", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("osd_client_message_size_cap", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500_M)
    .set_description(""),

    Option("osd_client_message_cap", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_pg_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6)
    .set_description(""),

    Option("osd_pgp_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6)
    .set_description(""),

    Option("osd_crush_update_weight_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_crush_chooseleaf_type", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_pool_use_gmt_hitset", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_crush_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_class_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_crush_initial_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("osd_pool_default_crush_rule", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("osd_pool_erasure_code_stripe_unit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("osd_pool_default_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("osd_pool_default_min_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_pool_default_pg_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description(""),

    Option("osd_pool_default_pgp_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description(""),

    Option("osd_pool_default_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("replicated")
    .set_description(""),

    Option("osd_pool_default_erasure_code_profile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("plugin=jerasure technique=reed_sol_van k=2 m=1")
    .set_description(""),

    Option("osd_erasure_code_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("jerasure lrc"
  #ifdef HAVE_BETTER_YASM_ELF64
         " isa"
  #endif
        )
    .set_description(""),

    Option("osd_allow_recovery_below_min_size", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_pool_default_flags", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_pool_default_flag_hashpspool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_pool_default_flag_nodelete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_pool_default_flag_nopgchange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_pool_default_flag_nosizechange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_pool_default_hit_set_bloom_fpp", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
    .set_description(""),

    Option("osd_pool_default_cache_target_dirty_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.4)
    .set_description(""),

    Option("osd_pool_default_cache_target_dirty_high_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.6)
    .set_description(""),

    Option("osd_pool_default_cache_target_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.8)
    .set_description(""),

    Option("osd_pool_default_cache_min_flush_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_pool_default_cache_min_evict_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_pool_default_cache_max_evict_check_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_hit_set_min_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("osd_hit_set_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100000)
    .set_description(""),

    Option("osd_hit_set_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".ceph-internal")
    .set_description(""),

    Option("osd_tier_promote_max_objects_sec", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .set_description(""),

    Option("osd_tier_promote_max_bytes_sec", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5_M)
    .set_description(""),

    Option("osd_tier_default_cache_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("writeback")
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1200)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("bloom")
    .set_description(""),

    Option("osd_tier_default_cache_min_read_recency_for_promote", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_tier_default_cache_min_write_recency_for_promote", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_grade_decay_rate", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_search_last_n", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_objecter_finishers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_map_dedup", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_map_max_advance", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description(""),

    Option("osd_map_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("osd_map_message_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description(""),

    Option("osd_map_share_max_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description(""),

    Option("osd_inject_bad_map_crc_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_inject_failure_on_pg_removal", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_max_markdown_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("osd_max_markdown_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_peering_wq_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_peering_wq_batch_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_op_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4194304)
    .set_description(""),

    Option("osd_op_pq_min_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("osd_disk_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_disk_thread_ioprio_class", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("osd_disk_thread_ioprio_priority", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("osd_recover_clone_overlap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_op_num_threads_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_op_num_threads_per_shard_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_op_num_threads_per_shard_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_op_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_op_num_shards_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_op_num_shards_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description(""),

    Option("osd_skip_data_digest", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_op_queue", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("wpq")
    .set_enum_allowed( { "wpq", "prioritized", "mclock_opclass", "mclock_client", "debug_random" } )
    .set_description("which operation queue algorithm to use")
    .set_long_description("which operation queue algorithm to use; mclock_opclass and mclock_client are currently experimental")
    .add_see_also("osd_op_queue_cut_off"),

    Option("osd_op_queue_cut_off", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("low")
    .set_enum_allowed( { "low", "high", "debug_random" } )
    .set_description("the threshold between high priority ops and low priority ops")
    .set_long_description("the threshold between high priority ops that use strict priority ordering and low priority ops that use a fairness algorithm that may or may not incorporate priority")
    .add_see_also("osd_op_queue"),

    Option("osd_op_queue_mclock_client_op_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1000.0)
    .set_description("mclock reservation of client operator requests")
    .set_long_description("mclock reservation of client operator requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the reservation")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_client_op_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock weight of client operator requests")
    .set_long_description("mclock weight of client operator requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the weight")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_client_op_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit of client operator requests")
    .set_long_description("mclock limit of client operator requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the limit")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_osd_subop_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1000.0)
    .set_description("mclock reservation of osd sub-operation requests")
    .set_long_description("mclock reservation of osd sub-operation requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the reservation")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_osd_subop_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock weight of osd sub-operation requests")
    .set_long_description("mclock weight of osd sub-operation requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the weight")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_osd_subop_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit of osd sub-operation requests")
    .set_long_description("mclock limit of osd sub-operation requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the limit")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_snap_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock reservation of snaptrim requests")
    .set_long_description("mclock reservation of snaptrim requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the reservation")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_snap_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("mclock weight of snaptrim requests")
    .set_long_description("mclock weight of snaptrim requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the weight")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_snap_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.001)
    .set_description("")
    .set_description("mclock limit of snaptrim requests")
    .set_long_description("mclock limit of snaptrim requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the limit")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_recov_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock reservation of recovery requests")
    .set_long_description("mclock reservation of recovery requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the reservation")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_recov_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("mclock weight of recovery requests")
    .set_long_description("mclock weight of recovery requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the weight")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_recov_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.001)
    .set_description("mclock limit of recovery requests")
    .set_long_description("mclock limit of recovery requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the limit")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_scrub_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock reservation of scrub requests")
    .set_long_description("mclock reservation of scrub requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the reservation")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_wgt")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_scrub_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("mclock weight of scrub requests")
    .set_long_description("mclock weight of scrub requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the weight")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_lim"),

    Option("osd_op_queue_mclock_scrub_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.001)
    .set_description("mclock weight of limit requests")
    .set_long_description("mclock weight of limit requests when osd_op_queue is either 'mclock_opclass' or 'mclock_client'; higher values increase the limit")
    .add_see_also("osd_op_queue")
    .add_see_also("osd_op_queue_mclock_client_op_res")
    .add_see_also("osd_op_queue_mclock_client_op_wgt")
    .add_see_also("osd_op_queue_mclock_client_op_lim")
    .add_see_also("osd_op_queue_mclock_osd_subop_res")
    .add_see_also("osd_op_queue_mclock_osd_subop_wgt")
    .add_see_also("osd_op_queue_mclock_osd_subop_lim")
    .add_see_also("osd_op_queue_mclock_snap_res")
    .add_see_also("osd_op_queue_mclock_snap_wgt")
    .add_see_also("osd_op_queue_mclock_snap_lim")
    .add_see_also("osd_op_queue_mclock_recov_res")
    .add_see_also("osd_op_queue_mclock_recov_wgt")
    .add_see_also("osd_op_queue_mclock_recov_lim")
    .add_see_also("osd_op_queue_mclock_scrub_res")
    .add_see_also("osd_op_queue_mclock_scrub_wgt"),

    Option("osd_ignore_stale_divergent_priors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_read_ec_check_for_errors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_recover_clone_overlap_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_backfill_scan_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description(""),

    Option("osd_backfill_scan_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("osd_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description(""),

    Option("osd_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(150)
    .set_description(""),

    Option("osd_recovery_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_recovery_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("osd_recovery_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next recovery or backfill op"),

    Option("osd_recovery_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.1)
    .set_description("Time in seconds to sleep before next recovery or backfill op for HDDs"),

    Option("osd_recovery_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next recovery or backfill op for SSDs"),

    Option("osd_recovery_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.025)
    .set_description("Time in seconds to sleep before next recovery or backfill op when data is on HDD and journal is on SSD"),

    Option("osd_snap_trim_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_scrub_invalid_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_remove_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("osd_remove_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10_hr)
    .set_description(""),

    Option("osd_command_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10*60)
    .set_description(""),

    Option("osd_command_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15*60)
    .set_description(""),

    Option("osd_heartbeat_addr", Option::TYPE_ADDR, Option::LEVEL_ADVANCED)
    .set_default(entity_addr_t())
    .set_description(""),

    Option("osd_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6)
    .set_description(""),

    Option("osd_heartbeat_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_heartbeat_min_peers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_heartbeat_use_min_delay_socket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_heartbeat_min_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2000)
    .set_description(""),

    Option("osd_pg_max_concurrent_snap_trims", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_max_trimming_pgs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_heartbeat_min_healthy_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.33)
    .set_description(""),

    Option("osd_mon_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_mon_report_interval_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("osd_mon_report_interval_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_mon_report_max_in_flight", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("osd_beacon_report_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("osd_pg_stat_report_interval_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("osd_mon_ack_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("osd_stats_ack_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description(""),

    Option("osd_stats_ack_timeout_decay", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.9)
    .set_description(""),

    Option("osd_default_data_pool_replay_window", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(45)
    .set_description(""),

    Option("osd_auto_mark_unfound_lost", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_recovery_delay_start", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_recovery_max_active", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("osd_recovery_max_single_start", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_recovery_max_chunk", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8<<20)
    .set_description(""),

    Option("osd_recovery_max_omap_entries_per_chunk", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64000)
    .set_description(""),

    Option("osd_copyfrom_max_chunk", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8<<20)
    .set_description(""),

    Option("osd_push_per_object_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("osd_max_push_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8<<20)
    .set_description(""),

    Option("osd_max_push_objects", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_recovery_forget_lost_objects", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_max_scrubs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_scrub_during_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_scrub_begin_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_scrub_end_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(24)
    .set_description(""),

    Option("osd_scrub_load_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description(""),

    Option("osd_scrub_min_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("osd_scrub_max_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(7_day)
    .set_description(""),

    Option("osd_scrub_interval_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description(""),

    Option("osd_scrub_backoff_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.66)
    .set_description(""),

    Option("osd_scrub_chunk_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_scrub_chunk_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .set_description(""),

    Option("osd_scrub_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_scrub_auto_repair", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_scrub_auto_repair_num_errors", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_deep_scrub_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(7_day)
    .set_description(""),

    Option("osd_deep_scrub_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.15)
    .set_description(""),

    Option("osd_deep_scrub_stride", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(524288)
    .set_description(""),

    Option("osd_deep_scrub_update_digest_min_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2_hr)
    .set_description(""),

    Option("osd_class_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_LIBDIR "/rados-classes")
    .set_description(""),

    Option("osd_open_classes_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_class_load_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops " "rbd refcount replica_log rgw statelog timeindex user version")
    .set_description(""),

    Option("osd_class_default_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops " "rbd refcount replica_log rgw statelog timeindex user version")
    .set_description(""),

    Option("osd_check_for_log_corruption", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_use_stale_snap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_rollback_to_cluster_snap", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("osd_default_notify_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_kill_backfill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_pg_epoch_persisted_max_stale", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description(""),

    Option("osd_min_pg_log_entries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1500)
    .set_description("minimum number of entries to maintain in the PG log")
    .add_service("osd")
    .add_see_also("osd_max_pg_log_entries")
    .add_see_also("osd_pg_log_dups_tracked"),

    Option("osd_max_pg_log_entries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("maximum number of entries to maintain in the PG log when degraded before we trim")
    .add_service("osd")
    .add_see_also("osd_min_pg_log_entries")
    .add_see_also("osd_pg_log_dups_tracked"),

    Option("osd_pg_log_dups_tracked", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3000)
    .set_description("how many versions back to track in order to detect duplicate ops; this is combined with both the regular pg log entries and additional minimal dup detection entries")
    .add_service("osd")
    .add_see_also("osd_min_pg_log_entries")
    .add_see_also("osd_max_pg_log_entries"),

    Option("osd_force_recovery_pg_log_entries_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.3)
    .set_description(""),

    Option("osd_pg_log_trim_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_op_complaint_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_command_max_records", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(256)
    .set_description(""),

    Option("osd_max_pg_blocked_by", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description(""),

    Option("osd_op_log_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_verify_sparse_read_holes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_backoff_on_unfound", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_backoff_on_degraded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_backoff_on_down", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_backoff_on_peering", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_crash_on_ignored_backoff", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_inject_dispatch_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_inject_dispatch_delay_duration", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.1)
    .set_description(""),

    Option("osd_debug_drop_ping_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_drop_ping_duration", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_op_order", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_verify_missing_on_start", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_scrub_chance_rewrite_digest", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_verify_snaps_on_info", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_verify_stray_on_activate", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_skip_full_check_in_backfill_reservation", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_reject_backfill_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_inject_copyfrom_error", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_misdirected_ops", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_skip_full_check_in_recovery", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_random_push_read_error", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("osd_debug_verify_cached_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_num_op_tracker_shard", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("osd_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_op_history_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("osd_op_history_slow_op_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_op_history_slow_op_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("osd_target_transaction_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_failsafe_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.97)
    .set_description(""),

    Option("osd_fast_fail_on_connection_refused", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_pg_object_context_cache_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description(""),

    Option("osd_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_function_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_fast_info", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_debug_pg_log_writeout", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("osd_loop_before_reset_tphandle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description(""),

    Option("threadpool_default_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("threadpool_empty_queue_max_wait", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("leveldb_log_to_ceph_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("leveldb_write_buffer_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description(""),

    Option("leveldb_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_description(""),

    Option("leveldb_block_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("leveldb_bloom_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("leveldb_max_open_files", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("leveldb_compression", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("leveldb_paranoid", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("leveldb_log", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/dev/null")
    .set_description(""),

    Option("leveldb_compact_on_mount", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("kinetic_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("kinetic_port", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8123)
    .set_description(""),

    Option("kinetic_user_id", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("kinetic_hmac_key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("asdfasdf")
    .set_description(""),

    Option("kinetic_use_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_separate_wal_dir", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_db_paths", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("")
    .set_safe(),

    Option("rocksdb_log_to_ceph_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rocksdb_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_description(""),

    Option("rocksdb_cache_row_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rocksdb_cache_shard_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("rocksdb_cache_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("lru")
    .set_description(""),

    Option("rocksdb_block_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_description(""),

    Option("rocksdb_perf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_collect_compaction_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_collect_extended_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_collect_memory_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_enable_rmrange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rocksdb_bloom_bits_per_key", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description("Number of bits per key to use for RocksDB's bloom filters.")
    .set_long_description("RocksDB bloom filters can be used to quickly answer the question of whether or not a key may exist or definitely does not exist in a given RocksDB SST file without having to read all keys into memory.  Using a higher bit value decreases the likelihood of false positives at the expense of additional disk space and memory consumption when the filter is loaded into RAM.  The current default value of 20 was found to provide significant performance gains when getattr calls are made (such as during new object creation in bluestore) without significant memory overhead or cache pollution when combined with rocksdb partitioned index filters.  See: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters for more information."),

    Option("rocksdb_cache_index_and_filter_blocks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Whether to cache indices and filters in block cache")
    .set_long_description("By default RocksDB will load an SST file's index and bloom filters into memory when it is opened and remove them from memory when an SST file is closed.  Thus, memory consumption by indices and bloom filters is directly tied to the number of concurrent SST files allowed to be kept open.  This option instead stores cached indicies and filters in the block cache where they directly compete with other cached data.  By default we set this option to true to better account for and bound rocksdb memory usage and keep filters in memory even when an SST file is closed."),

    Option("rocksdb_cache_index_and_filter_blocks_with_high_priority", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Whether to cache indices and filters in the block cache with high priority")
    .set_long_description("A downside of setting rocksdb_cache_index_and_filter_blocks to true is that regular data can push indices and filters out of memory.  Setting this option to true means they are cached with higher priority than other data and should typically stay in the block cache."),

    Option("rocksdb_pin_l0_filter_and_index_blocks_in_cache", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Whether to pin Level 0 indices and bloom filters in the block cache")
    .set_long_description("A downside of setting rocksdb_cache_index_and_filter_blocks to true is that regular data can push indices and filters out of memory.  Setting this option to true means that level 0 SST files will always have their indices and filters pinned in the block cache."),

    Option("rocksdb_index_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("binary_search")
    .set_description("Type of index for SST files: binary_search, hash_search, two_level")
    .set_long_description("This option controls the table index type.  binary_search is a space efficient index block that is optimized for block-search-based index. hash_search may improve prefix lookup performance at the expense of higher disk and memory usage and potentially slower compactions.  two_level is an experimental index type that uses two binary search indexes and works in conjunction with partition filters.  See: http://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html"),

    Option("rocksdb_partition_filters", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("(experimental) partition SST index/filters into smaller blocks")
    .set_long_description("This is an experimental option for rocksdb that works in conjunction with two_level indices to avoid having to keep the entire filter/index in cache when cache_index_and_filter_blocks is true.  The idea is to keep a much smaller top-level index in heap/cache and then opportunistically cache the lower level indices.  See: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters"),

    Option("rocksdb_metadata_block_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(4096)
    .set_description("The block size for index partitions. (0 = rocksdb default)"),

    Option("mon_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("write_buffer_size=33554432,compression=kNoCompression")
    .set_description(""),

    Option("osd_client_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(63)
    .set_description(""),

    Option("osd_recovery_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("osd_snap_trim_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_snap_trim_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1<<20)
    .set_description(""),

    Option("osd_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_scrub_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50<<20)
    .set_description(""),

    Option("osd_requested_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description(""),

    Option("osd_recovery_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_recovery_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20<<20)
    .set_description(""),

    Option("osd_recovery_op_warn_multiple", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description(""),

    Option("osd_mon_shutdown_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_shutdown_pgref_assert", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_max_object_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_description(""),

    Option("osd_max_object_name_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2048)
    .set_description(""),

    Option("osd_max_object_namespace_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(256)
    .set_description(""),

    Option("osd_max_attr_name_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_max_attr_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("osd_max_omap_entries_per_request", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(131072)
    .set_description(""),

    Option("osd_max_omap_bytes_per_request", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1<<30)
    .set_description(""),

    Option("osd_objectstore", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("filestore")
    .set_description(""),

    Option("osd_objectstore_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_objectstore_fuse", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_bench_small_size_max_iops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_bench_large_size_max_throughput", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100 << 20)
    .set_description(""),

    Option("osd_bench_max_block_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64 << 20)
    .set_description(""),

    Option("osd_bench_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("osd_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osdc_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_discard_disconnected_ops", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("memstore_device_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description(""),

    Option("memstore_page_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("memstore_page_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64 << 10)
    .set_description(""),

    Option("objectstore_blackhole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    // --------------------------
    // bluestore

    Option("bdev_debug_inflight_ios", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bdev_inject_crash", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("bdev_inject_crash_flush_delay", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2)
    .set_description(""),

    Option("bdev_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("bdev_aio_poll_ms", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .set_description(""),

    Option("bdev_aio_max_queue_depth", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("bdev_aio_reap_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description(""),

    Option("bdev_block_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("bdev_debug_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bdev_debug_aio_suicide_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(60.0)
    .set_description(""),

    Option("bdev_nvme_unbind_from_kernel", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bdev_nvme_retry_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("bluefs_alloc_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1048576)
    .set_description(""),

    Option("bluefs_max_prefetch", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1048576)
    .set_description(""),

    Option("bluefs_min_log_runway", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1048576)
    .set_description(""),

    Option("bluefs_max_log_runway", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4194304)
    .set_description(""),

    Option("bluefs_log_compact_min_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description(""),

    Option("bluefs_log_compact_min_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16*1048576)
    .set_description(""),

    Option("bluefs_min_flush_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(524288)
    .set_description(""),

    Option("bluefs_compact_log_sync", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_buffered_io", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_sync_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_allocator", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("stupid")
    .set_description(""),

    Option("bluefs_preextend_wal_files", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluestore_bluefs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_tag("mkfs")
    .set_description("Use BlueFS to back rocksdb")
    .set_long_description("BlueFS allows rocksdb to share the same physical device(s) as the rest of BlueStore.  It should be used in all cases unless testing/developing an alternative metadata database for BlueStore."),

    Option("bluestore_bluefs_env_mirror", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_tag("mkfs")
    .set_description("Mirror bluefs data to file system for testing/validation"),

    Option("bluestore_bluefs_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("minimum disk space allocated to BlueFS (e.g., at mkfs)"),

    Option("bluestore_bluefs_min_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.02)
    .set_description("Minimum fraction of free space devoted to BlueFS"),

    Option("bluestore_bluefs_max_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.90)
    .set_description("Maximum fraction of free storage devoted to BlueFS"),

    Option("bluestore_bluefs_gift_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.02)
    .set_description("Maximum fraction of free space to give to BlueFS at once"),

    Option("bluestore_bluefs_reclaim_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.20)
    .set_description("Maximum fraction of free space to reclaim from BlueFS at once"),

    Option("bluestore_bluefs_balance_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("How frequently (in seconds) to balance free space between BlueFS and BlueStore"),

    Option("bluestore_spdk_mem", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(512)
    .set_description(""),

    Option("bluestore_spdk_coremask", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("0x3")
    .set_description(""),

    Option("bluestore_spdk_max_io_completion", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("bluestore_block_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .add_tag("mkfs")
    .set_description("Path to block device/file"),

    Option("bluestore_block_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10_G)
    .add_tag("mkfs")
    .set_description("Size of file to create for backing bluestore"),

    Option("bluestore_block_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_tag("mkfs")
    .set_description("Create bluestore_block_path if it doesn't exist")
    .add_see_also("bluestore_block_path").add_see_also("bluestore_block_size"),

    Option("bluestore_block_db_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .add_tag("mkfs")
    .set_description("Path for db block device"),

    Option("bluestore_block_db_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .add_tag("mkfs")
    .set_description("Size of file to create for bluestore_block_db_path"),

    Option("bluestore_block_db_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_tag("mkfs")
    .set_description("Create bluestore_block_db_path if it doesn't exist")
    .add_see_also("bluestore_block_db_path")
    .add_see_also("bluestore_block_db_size"),

    Option("bluestore_block_wal_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .add_tag("mkfs")
    .set_description("Path to block device/file backing bluefs wal"),

    Option("bluestore_block_wal_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(96_M)
    .add_tag("mkfs")
    .set_description("Size of file to create for bluestore_block_wal_path"),

    Option("bluestore_block_wal_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_tag("mkfs")
    .set_description("Create bluestore_block_wal_path if it doesn't exist")
    .add_see_also("bluestore_block_wal_path")
    .add_see_also("bluestore_block_wal_size"),

    Option("bluestore_block_preallocate_file", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_tag("mkfs")
    .set_description("Preallocate file created via bluestore_block*_create"),

    Option("bluestore_csum_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("crc32c")
    .set_enum_allowed({"none", "crc32c", "crc32c_16", "crc32c_8", "xxhash32", "xxhash64"})
    .set_safe()
    .set_description("Default checksum algorithm to use")
    .set_long_description("crc32c, xxhash32, and xxhash64 are available.  The _16 and _8 variants use only a subset of the bits for more compact (but less reliable) checksumming."),

    Option("bluestore_min_alloc_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .add_tag("mkfs")
    .set_description("Minimum allocation size to allocate for an object")
    .set_long_description("A smaller allocation size generally means less data is read and then rewritten when a copy-on-write operation is triggered (e.g., when writing to something that was recently snapshotted).  Similarly, less data is journaled before performing an overwrite (writes smaller than min_alloc_size must first pass through the BlueStore journal).  Larger values of min_alloc_size reduce the amount of metadata required to describe the on-disk layout and reduce overall fragmentation."),

    Option("bluestore_min_alloc_size_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .add_tag("mkfs")
    .set_description("Default min_alloc_size value for rotational media"),

    Option("bluestore_min_alloc_size_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16_K)
    .add_tag("mkfs")
    .set_description("Default min_alloc_size value for non-rotational (solid state)  media"),

    Option("bluestore_max_alloc_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .add_tag("mkfs")
    .set_description("Maximum size of a single allocation (0 for no max)"),

    Option("bluestore_prefer_deferred_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Writes smaller than this size will be written to the journal and then asynchronously written to the device.  This can be beneficial when using rotational media where seeks are expensive, and is helpful both with and without solid state journal/wal devices."),

    Option("bluestore_prefer_deferred_size_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32768)
    .set_safe()
    .set_description("Default bluestore_prefer_deferred_size for rotational media"),

    Option("bluestore_prefer_deferred_size_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Default bluestore_prefer_deferred_size for non-rotational (solid state) media"),

    Option("bluestore_compression_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("none")
    .set_enum_allowed({"none", "passive", "aggressive", "force"})
    .set_safe()
    .set_description("Default policy for using compression when pool does not specify")
    .set_long_description("'none' means never use compression.  'passive' means use compression when clients hint that data is compressible.  'aggressive' means use compression unless clients hint that data is not compressible.  This option is used when the per-pool property for the compression mode is not present."),

    Option("bluestore_compression_algorithm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("snappy")
    .set_enum_allowed({"", "snappy", "zlib", "zstd", "lz4"})
    .set_safe()
    .set_description("Default compression algorithm to use when writing object data")
    .set_long_description("This controls the default compressor to use (if any) if the per-pool property is not set.  Note that zstd is *not* recommended for bluestore due to high CPU overhead when compressing small amounts of data."),

    Option("bluestore_compression_min_blob_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Chunks smaller than this are never compressed"),

    Option("bluestore_compression_min_blob_size_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128_K)
    .set_safe()
    .set_description("Default value of bluestore_compression_min_blob_size for rotational media"),

    Option("bluestore_compression_min_blob_size_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8_K)
    .set_safe()
    .set_description("Default value of bluestore_compression_min_blob_size for non-rotational (solid state) media"),

    Option("bluestore_compression_max_blob_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Chunks larger than this are broken into smaller chunks before being compressed"),

    Option("bluestore_compression_max_blob_size_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_safe()
    .set_description("Default value of bluestore_compression_max_blob_size for rotational media"),

    Option("bluestore_compression_max_blob_size_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_safe()
    .set_description("Default value of bluestore_compression_max_blob_size for non-rotational (solid state) media"),

    Option("bluestore_gc_enable_blob_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_safe()
    .set_description(""),

    Option("bluestore_gc_enable_total_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_safe()
    .set_description(""),

    Option("bluestore_max_blob_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_safe()
    .set_description(""),

    Option("bluestore_max_blob_size_hdd", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(512_K)
    .set_safe()
    .set_description(""),

    Option("bluestore_max_blob_size_ssd", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_safe()
    .set_description(""),

    Option("bluestore_compression_required_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.875)
    .set_safe()
    .set_description("Compression ratio required to store compressed data")
    .set_long_description("If we compress data and get less than this we discard the result and store the original uncompressed data."),

    Option("bluestore_extent_map_shard_max_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(1200)
    .set_description("Max size (bytes) for a single extent map shard before splitting"),

    Option("bluestore_extent_map_shard_target_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(500)
    .set_description("Target size (bytes) for a single extent map shard"),

    Option("bluestore_extent_map_shard_min_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(150)
    .set_description("Min size (bytes) for a single extent map shard before merging"),

    Option("bluestore_extent_map_shard_target_size_slop", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.2)
    .set_description("Ratio above/below target for a shard when trying to align to an existing extent or blob boundary"),

    Option("bluestore_extent_map_inline_shard_prealloc_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(256)
    .set_description("Preallocated buffer for inline shards"),

    Option("bluestore_cache_trim_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.2)
    .set_description("How frequently we trim the bluestore cache"),

    Option("bluestore_cache_trim_max_skip_pinned", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(64)
    .set_description("Max pinned cache entries we consider before giving up"),

    Option("bluestore_cache_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("2q")
    .set_enum_allowed({"2q", "lru"})
    .set_description("Cache replacement algorithm"),

    Option("bluestore_2q_cache_kin_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.5)
    .set_description("2Q paper suggests .5"),

    Option("bluestore_2q_cache_kout_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.5)
    .set_description("2Q paper suggests .5"),

    Option("bluestore_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Cache size (in bytes) for BlueStore")
    .set_long_description("This includes data and metadata cached by BlueStore as well as memory devoted to rocksdb's cache(s)."),

    Option("bluestore_cache_size_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("Default bluestore_cache_size for rotational media"),

    Option("bluestore_cache_size_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3_G)
    .set_description("Default bluestore_cache_size for non-rotational (solid state) media"),

    Option("bluestore_cache_meta_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.01)
    .set_description("Ratio of bluestore cache to devote to metadata"),

    Option("bluestore_cache_kv_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.99)
    .set_description("Ratio of bluestore cache to devote to kv database (rocksdb)"),

    Option("bluestore_cache_kv_max", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512_M)
    .set_description("Max memory (bytes) to devote to kv database (rocksdb)"),

    Option("bluestore_kvbackend", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("rocksdb")
    .add_tag("mkfs")
    .set_description("Key value database to use for bluestore"),

    Option("bluestore_allocator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("stupid")
    .set_enum_allowed({"bitmap", "stupid"})
    .set_description("Allocator policy"),

    Option("bluestore_freelist_blocks_per_key", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(128)
    .set_description("Block (and bits) per database key"),

    Option("bluestore_bitmapallocator_blocks_per_zone", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description(""),

    Option("bluestore_bitmapallocator_span_size", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description(""),

    Option("bluestore_max_deferred_txc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Max transactions with deferred writes that can accumulate before we force flush deferred writes"),

    Option("bluestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("compression=kNoCompression,max_write_buffer_number=4,min_write_buffer_number_to_merge=1,recycle_log_file_num=4,write_buffer_size=268435456,writable_file_max_buffer_size=0,compaction_readahead_size=2097152")
    .set_description("Rocksdb options"),

    Option("bluestore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run fsck at mount"),

    Option("bluestore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Run deep fsck at mount"),

    Option("bluestore_fsck_on_umount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run fsck at umount"),

    Option("bluestore_fsck_on_umount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Run deep fsck at umount"),

    Option("bluestore_fsck_on_mkfs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Run fsck after mkfs"),

    Option("bluestore_fsck_on_mkfs_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run deep fsck after mkfs"),

    Option("bluestore_sync_submit_transaction", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Try to submit metadata transaction to rocksdb in queuing thread context"),

    Option("bluestore_throttle_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_safe()
    .set_description("Maximum bytes in flight before we throttle IO submission"),

    Option("bluestore_throttle_deferred_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_safe()
    .set_description("Maximum bytes for deferred writes before we throttle IO submission"),

    Option("bluestore_throttle_cost_per_io", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Overhead added to transaction cost (in bytes) for each IO"),

  Option("bluestore_throttle_cost_per_io_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(670000)
    .set_safe()
    .set_description("Default bluestore_throttle_cost_per_io for rotational media"),

    Option("bluestore_throttle_cost_per_io_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4000)
    .set_safe()
    .set_description("Default bluestore_throttle_cost_per_io for non-rotation (solid state) media"),


    Option("bluestore_deferred_batch_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_safe()
    .set_description("Max number of deferred writes before we flush the deferred write queue"),

    Option("bluestore_deferred_batch_ops_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_safe()
    .set_description("Default bluestore_deferred_batch_ops for rotational media"),

    Option("bluestore_deferred_batch_ops_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_safe()
    .set_description("Default bluestore_deferred_batch_ops for non-rotational (solid state) media"),

    Option("bluestore_nid_prealloc", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description("Number of unique object ids to preallocate at a time"),

    Option("bluestore_blobid_prealloc", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10240)
    .set_description("Number of unique blob ids to preallocate at a time"),

    Option("bluestore_clone_cow", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_safe()
    .set_description("Use copy-on-write when cloning objects (versus reading and rewriting them at clone time)"),

    Option("bluestore_default_buffered_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_safe()
    .set_description("Cache read results by default (unless hinted NOCACHE or WONTNEED)"),

    Option("bluestore_default_buffered_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_safe()
    .set_description("Cache writes by default (unless hinted NOCACHE or WONTNEED)"),

    Option("bluestore_debug_misc", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_no_reuse_blocks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_small_allocations", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("bluestore_debug_freelist", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_prefill", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("simulate fragmentation"),

    Option("bluestore_debug_prefragment_max", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1048576)
    .set_description(""),

    Option("bluestore_debug_inject_read_err", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_randomize_serial_transaction", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("bluestore_debug_omit_block_device_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_fsck_abort", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_omit_kv_commit", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_permit_any_bdev_label", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_shard_finishers", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_random_read_err", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    // -----------------------------------------
    // kstore

    Option("kstore_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("kstore_max_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_description(""),

    Option("kstore_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb")
    .set_description(""),

    Option("kstore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("compression=kNoCompression")
    .set_description(""),

    Option("kstore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("kstore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("kstore_nid_prealloc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("kstore_sync_transaction", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("kstore_sync_submit_transaction", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("kstore_onode_map_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("kstore_default_stripe_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    // ---------------------
    // filestore

    Option("filestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("filestore_omap_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb")
    .set_description(""),

    Option("filestore_omap_backend_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("filestore_wbthrottle_enable", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_bytes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(41943040)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_bytes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(419430400)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_bytes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(41943040)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_bytes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(419430400)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description(""),

    Option("filestore_wbthrottle_btrfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description(""),

    Option("filestore_wbthrottle_xfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description(""),

    Option("filestore_odsync_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_index_retry_probability", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_debug_inject_read_err", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_debug_random_read_err", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_debug_omap_check", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_omap_header_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("filestore_max_inline_xattr_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_xfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_btrfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2048)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_other", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("filestore_max_inline_xattrs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_inline_xattrs_xfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("filestore_max_inline_xattrs_btrfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("filestore_max_inline_xattrs_other", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("filestore_max_xattr_value_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_xattr_value_size_xfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64<<10)
    .set_description(""),

    Option("filestore_max_xattr_value_size_btrfs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64<<10)
    .set_description(""),

    Option("filestore_max_xattr_value_size_other", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1<<10)
    .set_description(""),

    Option("filestore_sloppy_crc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_sloppy_crc_block_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("filestore_max_alloc_hint_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1ULL << 20)
    .set_description(""),

    Option("filestore_max_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("filestore_min_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.01)
    .set_description(""),

    Option("filestore_btrfs_snap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_btrfs_clone_range", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_zfs_snap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_fsync_flushes_journal_data", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_fiemap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_punch_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_seek_data_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_splice", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_fadvise", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_collect_device_partition_information", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_xfs_extsize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_journal_parallel", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_journal_writeahead", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_journal_trailing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_queue_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("filestore_queue_max_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100 << 20)
    .set_description(""),

    Option("filestore_caller_concurrency", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("filestore_expected_throughput_bytes", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(200 << 20)
    .set_description(""),

    Option("filestore_expected_throughput_ops", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description(""),

    Option("filestore_queue_max_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_high_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.3)
    .set_description(""),

    Option("filestore_queue_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.9)
    .set_description(""),

    Option("filestore_op_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("filestore_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("filestore_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(180)
    .set_description(""),

    Option("filestore_commit_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("filestore_fiemap_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("filestore_merge_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("filestore_split_multiple", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("filestore_split_rand_factor", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("filestore_update_to", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("filestore_blackhole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("filestore_fd_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("filestore_fd_cache_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description(""),

    Option("filestore_ondisk_finisher_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("filestore_apply_finisher_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("filestore_dump_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("filestore_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_inject_stall", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_fail_eio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("filestore_debug_verify_split", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("journal_dio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("journal_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("journal_force_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("journal_block_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("journal_max_corrupt_search", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10<<20)
    .set_description(""),

    Option("journal_block_align", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("journal_write_header_frequency", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("journal_max_write_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10 << 20)
    .set_description(""),

    Option("journal_max_write_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("journal_throttle_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.6)
    .set_description(""),

    Option("journal_throttle_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.9)
    .set_description(""),

    Option("journal_throttle_high_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("journal_throttle_max_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("journal_align_min_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64 << 10)
    .set_description(""),

    Option("journal_replay_from", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("journal_zero_on_create", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("journal_ignore_corruption", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("journal_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("fio_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/tmp/fio")
    .set_description(""),

    Option("rados_mon_op_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rados_osd_op_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rados_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("nss_db_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("mgr_module_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_PKGLIBDIR "/mgr")
    .set_description(""),

    Option("mgr_initial_modules", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("restful status")
    .set_description(""),

    Option("mgr_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/mgr/$cluster-$id")
    .set_description(""),

    Option("mgr_tick_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("mgr_stats_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mgr_client_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128*1048576)
    .set_description(""),

    Option("mgr_client_messages", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("mgr_osd_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512*1048576)
    .set_description(""),

    Option("mgr_osd_messages", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description(""),

    Option("mgr_mds_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128*1048576)
    .set_description(""),

    Option("mgr_mds_messages", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("mgr_mon_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128*1048576)
    .set_description(""),

    Option("mgr_mon_messages", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("mgr_connect_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("mgr_service_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .set_description(""),

    Option("mon_mgr_digest_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mon_mgr_beacon_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mon_mgr_inactive_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mon_mgr_mkfs_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mutex_perf_counter", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("throttler_perf_counter", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("event_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("internal_safe_to_start_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("debug_deliberately_leak_memory", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),
  });
}

std::vector<Option> get_rgw_options() {
  return std::vector<Option>({
    Option("rgw_acl_grants_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("rgw_max_chunk_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4_M)
    .set_description(""),

    Option("rgw_put_obj_min_window_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16_M)
    .set_description(""),

    Option("rgw_put_obj_max_window_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_description(""),

    Option("rgw_max_put_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5_G)
    .set_description(""),

    Option("rgw_max_put_param_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description(""),

    Option("rgw_override_bucket_index_max_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rgw_bucket_index_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description(""),

    Option("rgw_enable_quota_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_enable_gc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_enable_lc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/radosgw/$cluster-$id")
    .set_description(""),

    Option("rgw_enable_apis", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("s3, s3website, swift, swift_auth, admin")
    .set_description(""),

    Option("rgw_cache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_cache_lru_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("rgw_socket_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_dns_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_dns_s3website_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_content_length_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_lifecycle_work_time", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("00:00-06:00")
    .set_description(""),

    Option("rgw_lc_lock_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("rgw_lc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("rgw_lc_debug_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .set_description(""),

    Option("rgw_mp_lock_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("rgw_script_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_request_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_swift_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_swift_url_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("swift")
    .set_description(""),

    Option("rgw_swift_auth_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_swift_auth_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("auth")
    .set_description(""),

    Option("rgw_swift_tenant_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_swift_account_in_url", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_swift_enforce_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_keystone_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_token", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_admin_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_barbican_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_barbican_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_barbican_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_barbican_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_barbican_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_api_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("rgw_keystone_accepted_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("Member, admin")
    .set_description(""),

    Option("rgw_keystone_accepted_admin_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_keystone_token_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("rgw_keystone_revocation_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15_min)
    .set_description(""),

    Option("rgw_keystone_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_keystone_implicit_tenants", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_cross_domain_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("<allow-access-from domain=\"*\" secure=\"false\" />")
    .set_description(""),

    Option("rgw_healthcheck_disabling_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_s3_auth_use_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_s3_auth_use_keystone", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_s3_auth_aws4_force_boto2_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_barbican_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_ldap_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ldaps://<ldap.your.domain>")
    .set_description(""),

    Option("rgw_ldap_binddn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("uid=admin,cn=users,dc=example,dc=com")
    .set_description(""),

    Option("rgw_ldap_searchdn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cn=users,cn=accounts,dc=example,dc=com")
    .set_description(""),

    Option("rgw_ldap_dnattr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("uid")
    .set_description(""),

    Option("rgw_ldap_secret", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/etc/openldap/secret")
    .set_description(""),

    Option("rgw_s3_auth_use_ldap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_ldap_searchfilter", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_admin_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("admin")
    .set_description(""),

    Option("rgw_enforce_swift_acls", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_swift_token_expiration", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("rgw_print_continue", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_print_prohibited_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_remote_addr_param", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("REMOTE_ADDR")
    .set_description(""),

    Option("rgw_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10*60)
    .set_description(""),

    Option("rgw_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rgw_thread_pool_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("rgw_num_control_oids", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description(""),

    Option("rgw_num_rados_handles", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("rgw_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_nfs_lru_lanes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("rgw_nfs_lru_lane_hiwat", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(911)
    .set_description(""),

    Option("rgw_nfs_fhcache_partitions", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("rgw_nfs_fhcache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2017)
    .set_description(""),

    Option("rgw_nfs_namespace_expire_secs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_min(1)
    .set_description(""),

    Option("rgw_nfs_max_gc", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_min(1)
    .set_description(""),

    Option("rgw_nfs_write_completion_interval_s", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("rgw_zone", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_zone_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description(""),

    Option("rgw_default_zone_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.zone")
    .set_description(""),

    Option("rgw_region", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_region_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description(""),

    Option("rgw_default_region_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.region")
    .set_description(""),

    Option("rgw_zonegroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_zonegroup_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description(""),

    Option("rgw_default_zonegroup_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.zonegroup")
    .set_description(""),

    Option("rgw_realm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_realm_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description(""),

    Option("rgw_default_realm_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.realm")
    .set_description(""),

    Option("rgw_period_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description(""),

    Option("rgw_period_latest_epoch_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".latest_epoch")
    .set_description(""),

    Option("rgw_log_nonexistent_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_log_object_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("%Y-%m-%d-%H-%i-%n")
    .set_description(""),

    Option("rgw_log_object_name_utc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_usage_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("rgw_usage_max_user_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description(""),

    Option("rgw_enable_ops_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_enable_usage_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_ops_log_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_ops_log_socket_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_ops_log_data_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5 << 20)
    .set_description(""),

    Option("rgw_fcgi_socket_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("rgw_usage_log_flush_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("rgw_usage_log_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("rgw_intent_log_object_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("%Y-%m-%d-%i-%n")
    .set_description(""),

    Option("rgw_intent_log_object_name_utc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_init_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("rgw_mime_types_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/etc/mime.types")
    .set_description(""),

    Option("rgw_gc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("rgw_gc_obj_min_wait", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2_hr)
    .set_description(""),

    Option("rgw_gc_processor_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("rgw_gc_processor_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("rgw_s3_success_create_obj_status", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rgw_resolve_cname", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_obj_stripe_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4 << 20)
    .set_description(""),

    Option("rgw_extended_http_attrs", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_exit_timeout_secs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description(""),

    Option("rgw_get_obj_window_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16 << 20)
    .set_description(""),

    Option("rgw_get_obj_max_req_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4 << 20)
    .set_description(""),

    Option("rgw_relaxed_s3_bucket_names", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_defer_to_bucket_acls", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_list_buckets_max_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_md_log_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description(""),

    Option("rgw_num_zone_opstate_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("rgw_opstate_ratelimit_sec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("rgw_curl_wait_timeout_ms", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_copy_obj_progress", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_copy_obj_progress_every_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description(""),

    Option("rgw_obj_tombstone_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_data_log_window", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("rgw_data_log_changes_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_data_log_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description(""),

    Option("rgw_data_log_obj_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("data_log")
    .set_description(""),

    Option("rgw_replica_log_obj_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("replica_log")
    .set_description(""),

    Option("rgw_bucket_quota_ttl", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("rgw_bucket_quota_soft_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.95)
    .set_description(""),

    Option("rgw_bucket_quota_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("rgw_bucket_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("rgw_bucket_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("rgw_expose_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_frontends", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("civetweb port=7480")
    .set_description(""),

    Option("rgw_user_quota_bucket_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(180)
    .set_description(""),

    Option("rgw_user_quota_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("rgw_user_quota_sync_idle_users", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_user_quota_sync_wait_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description(""),

    Option("rgw_user_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("rgw_user_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("rgw_multipart_min_part_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_M)
    .set_description(""),

    Option("rgw_multipart_part_upload_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("rgw_max_slo_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_olh_pending_timeout_sec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description(""),

    Option("rgw_user_max_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_objexp_gc_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10_min)
    .set_description(""),

    Option("rgw_objexp_time_step", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("rgw_objexp_hints_num_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(127)
    .set_description(""),

    Option("rgw_objexp_chunk_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("rgw_enable_static_website", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_log_http_headers", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_num_async_rados_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("rgw_md_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description(""),

    Option("rgw_run_sync_thread", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_sync_lease_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description(""),

    Option("rgw_sync_log_trim_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1200)
    .set_description(""),

    Option("rgw_sync_data_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("rgw_sync_meta_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("rgw_sync_trace_history_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("rgw_sync_trace_per_node_log_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("rgw_sync_trace_servicemap_update_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("rgw_period_push_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description(""),

    Option("rgw_period_push_interval_max", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("rgw_safe_max_objects_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100*1024)
    .set_description(""),

    Option("rgw_shard_warning_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(90)
    .set_description(""),

    Option("rgw_swift_versioning_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_swift_custom_header", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_swift_need_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_reshard_num_logs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description(""),

    Option("rgw_reshard_bucket_lock_duration", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description(""),

    Option("rgw_crypt_require_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_crypt_default_encryption_key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_crypt_s3_kms_encryption_keys", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_crypt_suppress_logs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_list_bucket_min_readahead", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("rgw_rest_getusage_op_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_torrent_flag", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("rgw_torrent_tracker", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_torrent_createby", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_torrent_comment", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_torrent_encoding", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_data_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description("data changes notification interval to followers"),

    Option("rgw_torrent_origin", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_torrent_sha_unit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512*1024)
    .set_description(""),

    Option("rgw_dynamic_resharding", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rgw_max_objs_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100000)
    .set_description(""),

    Option("rgw_reshard_thread_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10_min)
    .set_description(""),
  });
}

static std::vector<Option> get_rbd_options() {
  return std::vector<Option>({
    Option("rbd_default_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rbd")
    .set_description("default pool for storing new images")
    .set_validator([](std::string *value, std::string *error_message){
      boost::regex pattern("^[^@/]+$");
      if (!boost::regex_match (*value, pattern)) {
        *value = "rbd";
        *error_message = "invalid RBD default pool, resetting to 'rbd'";
      }
      return 0;
    }),

    Option("rbd_default_data_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("default pool for storing data blocks for new images")
    .set_validator([](std::string *value, std::string *error_message){
      boost::regex pattern("^[^@/]*$");
      if (!boost::regex_match (*value, pattern)) {
        *value = "";
        *error_message = "ignoring invalid RBD data pool";
      }
      return 0;
    }),

    Option("rbd_default_features", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("layering,exclusive-lock,object-map,fast-diff,deep-flatten")
    .set_description("default v2 image features for new images")
    .set_long_description(
        "RBD features are only applicable for v2 images. This setting accepts "
        "either an integer bitmask value or comma-delimited string of RBD "
        "feature names. This setting is always internally stored as an integer "
        "bitmask value. The mapping between feature bitmask value and feature "
        "name is as follows: +1 -> layering, +2 -> striping, "
        "+4 -> exclusive-lock, +8 -> object-map, +16 -> fast-diff, "
        "+32 -> deep-flatten, +64 -> journaling, +128 -> data-pool")
    .set_safe()
    .set_validator([](std::string *value, std::string *error_message){
      static const std::map<std::string, uint64_t> FEATURE_MAP = {
        {RBD_FEATURE_NAME_LAYERING, RBD_FEATURE_LAYERING},
        {RBD_FEATURE_NAME_STRIPINGV2, RBD_FEATURE_STRIPINGV2},
        {RBD_FEATURE_NAME_EXCLUSIVE_LOCK, RBD_FEATURE_EXCLUSIVE_LOCK},
        {RBD_FEATURE_NAME_OBJECT_MAP, RBD_FEATURE_OBJECT_MAP},
        {RBD_FEATURE_NAME_FAST_DIFF, RBD_FEATURE_FAST_DIFF},
        {RBD_FEATURE_NAME_DEEP_FLATTEN, RBD_FEATURE_DEEP_FLATTEN},
        {RBD_FEATURE_NAME_JOURNALING, RBD_FEATURE_JOURNALING},
        {RBD_FEATURE_NAME_DATA_POOL, RBD_FEATURE_DATA_POOL},
      };
      static_assert((RBD_FEATURE_DATA_POOL << 1) > RBD_FEATURES_ALL,
                    "new RBD feature added");

      // convert user-friendly comma delimited feature name list to a bitmask
      // that is used by the librbd API
      uint64_t features = 0;
      error_message->clear();

      try {
        features = boost::lexical_cast<decltype(features)>(*value);

        uint64_t unsupported_features = (features & ~RBD_FEATURES_ALL);
        if (unsupported_features != 0ull) {
          features &= RBD_FEATURES_ALL;

          std::stringstream ss;
          ss << "ignoring unknown feature mask 0x"
             << std::hex << unsupported_features;
          *error_message = ss.str();
        }
      } catch (const boost::bad_lexical_cast& ) {
        int r = 0;
        std::vector<std::string> feature_names;
        boost::split(feature_names, *value, boost::is_any_of(","));
        for (auto feature_name: feature_names) {
          boost::trim(feature_name);
          auto feature_it = FEATURE_MAP.find(feature_name);
          if (feature_it != FEATURE_MAP.end()) {
            features += feature_it->second;
          } else {
            if (!error_message->empty()) {
              *error_message += ", ";
            }
            *error_message += "ignoring unknown feature " + feature_name;
            r = -EINVAL;
          }
        }

        if (features == 0 && r == -EINVAL) {
          features = RBD_FEATURES_DEFAULT;
        }
      }
      *value = stringify(features);
      return 0;
    }),

    Option("rbd_op_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("number of threads to utilize for internal processing"),

    Option("rbd_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("time in seconds for detecting a hung thread"),

    Option("rbd_non_blocking_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("process AIO ops from a dispatch thread to prevent blocking"),

    Option("rbd_cache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to enable caching (writeback unless rbd_cache_max_dirty is 0)"),

    Option("rbd_cache_writethrough_until_flush", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to make writeback caching writethrough until "
                     "flush is called, to be sure the user of librbd will send "
                     "flushes so that writeback is safe"),

    Option("rbd_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32<<20)
    .set_description("cache size in bytes"),

    Option("rbd_cache_max_dirty", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(24<<20)
    .set_description("dirty limit in bytes - set to 0 for write-through caching"),

    Option("rbd_cache_target_dirty", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16<<20)
    .set_description("target dirty limit in bytes"),

    Option("rbd_cache_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("seconds in cache before writeback starts"),

    Option("rbd_cache_max_dirty_object", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("dirty limit for objects - set to 0 for auto calculate from rbd_cache_size"),

    Option("rbd_cache_block_writes_upfront", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("whether to block writes to the cache before the aio_write call completes"),

    Option("rbd_concurrent_management_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_min(1)
    .set_description("how many operations can be in flight for a management operation like deleting or resizing an image"),

    Option("rbd_balance_snap_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("distribute snap read requests to random OSD"),

    Option("rbd_localize_snap_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("localize snap read requests to closest OSD"),

    Option("rbd_balance_parent_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("distribute parent read requests to random OSD"),

    Option("rbd_localize_parent_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("localize parent requests to closest OSD"),

    Option("rbd_readahead_trigger_requests", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("number of sequential requests necessary to trigger readahead"),

    Option("rbd_readahead_max_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_description("set to 0 to disable readahead"),

    Option("rbd_readahead_disable_after_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50_M)
    .set_description("how many bytes are read in total before readahead is disabled"),

    Option("rbd_clone_copy_on_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("copy-up parent image blocks to clone upon read request"),

    Option("rbd_blacklist_on_break_lock", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to blacklist clients whose lock was broken"),

    Option("rbd_blacklist_expire_seconds", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("number of seconds to blacklist - set to 0 for OSD default"),

    Option("rbd_request_timed_out_seconds", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("number of seconds before maintenance request times out"),

    Option("rbd_skip_partial_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("when trying to discard a range inside an object, set to true to skip zeroing the range"),

    Option("rbd_enable_alloc_hint", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("when writing a object, it will issue a hint to osd backend to indicate the expected size object need"),

    Option("rbd_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("true if LTTng-UST tracepoints should be enabled"),

    Option("rbd_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("create a blkin trace for all RBD requests"),

    Option("rbd_validate_pool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("validate empty pools for RBD compatibility"),

    Option("rbd_validate_names", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("validate new image names for RBD compatibility"),

    Option("rbd_auto_exclusive_lock_until_manual_request", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("automatically acquire/release exclusive lock until it is explicitly requested"),

    Option("rbd_mirroring_resync_after_disconnect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("automatically start image resync after mirroring is disconnected due to being laggy"),

    Option("rbd_mirroring_replay_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("time-delay in seconds for rbd-mirror asynchronous replication"),

    Option("rbd_default_format", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("default image format for new images"),

    Option("rbd_default_order", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(22)
    .set_description("default order (data block object size) for new images"),

    Option("rbd_default_stripe_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default stripe count for new images"),

    Option("rbd_default_stripe_unit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default stripe width for new images"),

    Option("rbd_default_map_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("default krbd map options"),

    Option("rbd_journal_order", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_min(12)
    .set_default(24)
    .set_description("default order (object size) for journal data objects"),

    Option("rbd_journal_splay_width", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("number of active journal objects"),

    Option("rbd_journal_commit_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("commit time interval, seconds"),

    Option("rbd_journal_object_flush_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of pending commits per journal object"),

    Option("rbd_journal_object_flush_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of pending bytes per journal object"),

    Option("rbd_journal_object_flush_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum age (in seconds) for pending commits"),

    Option("rbd_journal_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("pool for journal objects"),

    Option("rbd_journal_max_payload_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16384)
    .set_description("maximum journal payload size before splitting"),

    Option("rbd_journal_max_concurrent_object_sets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of object sets a journal client can be behind before it is automatically unregistered"),
  });
}

static std::vector<Option> get_rbd_mirror_options() {
  return std::vector<Option>({
    Option("rbd_mirror_journal_commit_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("commit time interval, seconds"),

    Option("rbd_mirror_journal_poll_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("maximum age (in seconds) between successive journal polls"),

    Option("rbd_mirror_journal_max_fetch_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32768)
    .set_description("maximum bytes to read from each journal data object per fetch"),

    Option("rbd_mirror_sync_point_update_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("number of seconds between each update of the image sync point object number"),

    Option("rbd_mirror_concurrent_image_syncs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("maximum number of image syncs in parallel"),

    Option("rbd_mirror_pool_replayers_refresh_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("interval to refresh peers in rbd-mirror daemon"),

    Option("rbd_mirror_delete_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("interval to check and retry the failed requests in deleter"),

    Option("rbd_mirror_image_state_check_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_min(1)
    .set_description("interval to get images from pool watcher and set sources in replayer"),

    Option("rbd_mirror_leader_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_min(1)
    .set_description("interval (in seconds) between mirror leader heartbeats"),

    Option("rbd_mirror_leader_max_missed_heartbeats", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("number of missed heartbeats for non-lock owner to attempt to acquire lock"),

    Option("rbd_mirror_leader_max_acquire_attempts_before_break", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("number of failed attempts to acquire lock after missing heartbeats before breaking lock"),
  });
}

std::vector<Option> get_mds_options() {
  return std::vector<Option>({
    Option("mds_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/mds/$cluster-$id")
    .set_description(""),

    Option("mds_max_file_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1ULL << 40)
    .set_description(""),

    Option("mds_max_xattr_pairs_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64 << 10)
    .set_description(""),

    Option("mds_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of inodes in MDS cache (<=0 is unlimited)")
    .set_long_description("This tunable is no longer recommended. Use mds_cache_memory_limit."),

    Option("mds_cache_memory_limit", Option::TYPE_UINT, Option::LEVEL_BASIC)
    .set_default(1*(1LL<<30))
    .set_description("target maximum memory usage of MDS cache")
    .set_long_description("This sets a target maximum memory usage of the MDS cache and is the primary tunable to limit the MDS memory usage. The MDS will try to stay under a reservation of this limit (by default 95%; 1 - mds_cache_reservation) by trimming unused metadata in its cache and recalling cached items in the client caches. It is possible for the MDS to exceed this limit due to slow recall from clients. The mds_health_cache_threshold (150%) sets a cache full threshold for when the MDS signals a cluster health warning."),

    Option("mds_cache_reservation", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
    .set_description("amount of memory to reserve"),

    Option("mds_health_cache_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5)
    .set_description("threshold for cache size to generate health warning"),

    Option("mds_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.7)
    .set_description(""),

    Option("mds_max_file_recover", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description(""),

    Option("mds_dir_max_commit_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mds_dir_keys_per_op", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16384)
    .set_description(""),

    Option("mds_decay_halflife", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_beacon_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("mds_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description(""),

    Option("mds_enforce_unique_name", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_blacklist_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(24.0*60.0)
    .set_description(""),

    Option("mds_session_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mds_session_blacklist_on_timeout", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_session_blacklist_on_evict", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_sessionmap_keys_per_op", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("mds_revoke_cap_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mds_recall_state_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description(""),

    Option("mds_freeze_tree_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mds_session_autoclose", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description(""),

    Option("mds_health_summarize_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mds_reconnect_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(45)
    .set_description(""),

    Option("mds_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_dirstat_min_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("mds_scatter_nudge_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_client_prealloc_inos", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("mds_early_reply", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_default_dir_hash", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(CEPH_STR_HASH_RJENKINS)
    .set_description(""),

    Option("mds_log_pause", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_log_skip_corrupt_events", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_log_max_events", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("mds_log_events_per_segment", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),

    Option("mds_log_segment_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_log_max_segments", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mds_log_max_expiring", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("mds_bal_export_pin", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_bal_sample_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3.0)
    .set_description(""),

    Option("mds_bal_replicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(8000)
    .set_description(""),

    Option("mds_bal_unreplicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_bal_frag", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_bal_split_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mds_bal_split_rd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(25000)
    .set_description(""),

    Option("mds_bal_split_wr", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mds_bal_split_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description(""),

    Option("mds_bal_merge_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("mds_bal_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("mds_bal_fragment_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_bal_fragment_size_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000*10)
    .set_description(""),

    Option("mds_bal_fragment_fast_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5)
    .set_description(""),

    Option("mds_bal_idle_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_bal_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("mds_bal_max_until", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("mds_bal_mode", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_bal_min_rebalance", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.1)
    .set_description(""),

    Option("mds_bal_min_start", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.2)
    .set_description(""),

    Option("mds_bal_need_min", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.8)
    .set_description(""),

    Option("mds_bal_need_max", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.2)
    .set_description(""),

    Option("mds_bal_midchunk", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .set_description(""),

    Option("mds_bal_minchunk", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.001)
    .set_description(""),

    Option("mds_bal_target_decay", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("mds_replay_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("mds_shutdown_check", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_thrash_exports", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_thrash_fragments", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_dump_cache_on_map", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_dump_cache_after_rejoin", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_verify_scatter", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_debug_scatterstat", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_debug_frag", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_debug_auth_pins", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_debug_subtrees", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_kill_mdstable_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_export_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_import_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_link_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_rename_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_openc_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_journal_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_journal_expire_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_kill_journal_replay_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_journal_format", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("mds_kill_create_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_inject_traceless_reply_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_wipe_sessions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_wipe_ino_prealloc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_skip_ino", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_standby_for_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("mds_standby_for_rank", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("mds_standby_for_fscid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("mds_standby_replay", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("mds_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("mds_op_history_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description(""),

    Option("mds_op_complaint_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("mds_op_log_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_snap_min_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_snap_max_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4294967294)
    .set_description(""),

    Option("mds_snap_rstat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("mds_verify_backtrace", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("mds_max_completed_flushes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100000)
    .set_description(""),

    Option("mds_max_completed_requests", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100000)
    .set_description(""),

    Option("mds_action_on_write_error", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("mds_mon_shutdown_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_max_purge_files", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description(""),

    Option("mds_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description(""),

    Option("mds_max_purge_ops_per_pg", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description(""),

    Option("mds_purge_queue_busy_flush_period", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("mds_root_ino_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_root_ino_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("mds_max_scrub_ops_in_progress", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("mds_damage_table_max_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description(""),

    Option("mds_client_writeable_range_max_inc_objs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description(""),
  });
}

std::vector<Option> get_mds_client_options() {
  return std::vector<Option>({
    Option("client_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16384)
    .set_description(""),

    Option("client_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.75)
    .set_description(""),

    Option("client_use_random_mds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("client_mount_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(300.0)
    .set_description(""),

    Option("client_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description(""),

    Option("client_trace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),


    Option("client_readahead_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128*1024)
    .set_description(""),

    Option("client_readahead_max_bytes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("client_readahead_max_periods", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("client_reconnect_stale", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("client_snapdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".snap")
    .set_description(""),

    Option("client_mountpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/")
    .set_description(""),

    Option("client_mount_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("client_mount_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("client_notify_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_client_watch_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description(""),

    Option("client_caps_release_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("client_quota_df", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_oc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_oc_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(200_M)
    .set_description(""),

    Option("client_oc_max_dirty", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description(""),

    Option("client_oc_target_dirty", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description(""),

    Option("client_oc_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description(""),

    Option("client_oc_max_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("client_debug_getattr_caps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_debug_force_sync_read", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_debug_inject_tick_delay", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("client_max_inline_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description(""),

    Option("client_inject_release_failure", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_inject_fixed_oldest_tid", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_metadata", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("client_acl_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("client_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_dirsize_rbytes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_use_invalidate_cb", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_disable_pagecache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("fuse_allow_other", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_default_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("fuse_big_writes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("big_writes is deprecated in libfuse 3.0.0"),

    Option("fuse_atomic_o_trunc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_debug", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("fuse_multithreaded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_require_active_mds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_syncfs_on_mksnap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("fuse_set_user_groups", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("check for ceph-fuse to consider supplementary groups for permissions"),

    Option("client_try_dentry_invalidate", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_die_on_failed_remount", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_check_pool_perm", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("client_use_faked_inos", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("client_mds_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),
  });
}


static std::vector<Option> build_options()
{
  std::vector<Option> result = get_global_options();

  auto ingest = [&result](std::vector<Option>&& options, const char* svc) {
    for (const auto &o_in : options) {
      Option o(o_in);
      o.add_service(svc);
      result.push_back(o);
    }
  };

  ingest(get_rgw_options(), "rgw");
  ingest(get_rbd_options(), "rbd");
  ingest(get_rbd_mirror_options(), "rbd-mirror");
  ingest(get_mds_options(), "mds");
  ingest(get_mds_client_options(), "mds_client");

  return result;
}

const std::vector<Option> ceph_options = build_options();
