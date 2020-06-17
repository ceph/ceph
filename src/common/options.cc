// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "options.h"
#include "common/Formatter.h"

// Helpers for validators
#include "include/stringify.h"
#include "include/common_fwd.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <regex>

// Definitions for enums
#include "common/perf_counters.h"

// rbd feature validation
#include "librbd/Features.h"

using std::ostream;
using std::ostringstream;

using ceph::Formatter;
using ceph::parse_timespan;

namespace {
class printer : public boost::static_visitor<> {
  ostream& out;
public:
  explicit printer(ostream& os)
    : out(os) {}
  template<typename T>
  void operator()(const T& v) const {
    out << v;
  }
  void operator()(boost::blank blank) const {
    return;
  }
  void operator()(bool v) const {
    out << (v ? "true" : "false");
  }
  void operator()(double v) const {
    out << std::fixed << v << std::defaultfloat;
  }
  void operator()(const Option::size_t& v) const {
    out << v.value;
  }
  void operator()(const std::chrono::seconds v) const {
    out << v.count();
  }
};
}

ostream& operator<<(ostream& os, const Option::value_t& v) {
  printer p{os};
  v.apply_visitor(p);
  return os;
}

void Option::dump_value(const char *field_name,
    const Option::value_t &v, Formatter *f) const
{
  if (boost::get<boost::blank>(&v)) {
    // This should be nil but Formatter doesn't allow it.
    f->dump_string(field_name, "");
    return;
  }
  switch (type) {
  case TYPE_INT:
    f->dump_int(field_name, boost::get<int64_t>(v)); break;
  case TYPE_UINT:
    f->dump_unsigned(field_name, boost::get<uint64_t>(v)); break;
  case TYPE_STR:
    f->dump_string(field_name, boost::get<std::string>(v)); break;
  case TYPE_FLOAT:
    f->dump_float(field_name, boost::get<double>(v)); break;
  case TYPE_BOOL:
    f->dump_bool(field_name, boost::get<bool>(v)); break;
  default:
    f->dump_stream(field_name) << v; break;
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

int Option::parse_value(
  const std::string& raw_val,
  value_t *out,
  std::string *error_message,
  std::string *normalized_value) const
{
  std::string val = raw_val;

  int r = pre_validate(&val, error_message);
  if (r != 0) {
    return r;
  }

  if (type == Option::TYPE_INT) {
    int64_t f = strict_si_cast<int64_t>(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = f;
  } else if (type == Option::TYPE_UINT) {
    uint64_t f = strict_si_cast<uint64_t>(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = f;
  } else if (type == Option::TYPE_STR) {
    *out = val;
  } else if (type == Option::TYPE_FLOAT) {
    double f = strict_strtod(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    } else {
      *out = f;
    }
  } else if (type == Option::TYPE_BOOL) {
    if (strcasecmp(val.c_str(), "false") == 0) {
      *out = false;
    } else if (strcasecmp(val.c_str(), "true") == 0) {
      *out = true;
    } else {
      int b = strict_strtol(val.c_str(), 10, error_message);
      if (!error_message->empty()) {
	return -EINVAL;
      }
      *out = (bool)!!b;
    }
  } else if (type == Option::TYPE_ADDR) {
    entity_addr_t addr;
    if (!addr.parse(val.c_str())){
      return -EINVAL;
    }
    *out = addr;
  } else if (type == Option::TYPE_ADDRVEC) {
    entity_addrvec_t addr;
    if (!addr.parse(val.c_str())){
      return -EINVAL;
    }
    *out = addr;
  } else if (type == Option::TYPE_UUID) {
    uuid_d uuid;
    if (!uuid.parse(val.c_str())) {
      return -EINVAL;
    }
    *out = uuid;
  } else if (type == Option::TYPE_SIZE) {
    Option::size_t sz{strict_iecstrtoll(val.c_str(), error_message)};
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = sz;
  } else if (type == Option::TYPE_SECS) {
    try {
      *out = parse_timespan(val);
    } catch (const std::invalid_argument& e) {
      *error_message = e.what();
      return -EINVAL;
    }
  } else {
    ceph_abort();
  }

  r = validate(*out, error_message);
  if (r != 0) {
    return r;
  }

  if (normalized_value) {
    *normalized_value = to_str(*out);
  }
  return 0;
}

void Option::dump(Formatter *f) const
{
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

  f->dump_bool("can_update_at_runtime", can_update_at_runtime());

  f->open_array_section("flags");
  if (has_flag(FLAG_RUNTIME)) {
    f->dump_string("option", "runtime");
  }
  if (has_flag(FLAG_NO_MON_UPDATE)) {
    f->dump_string("option", "no_mon_update");
  }
  if (has_flag(FLAG_STARTUP)) {
    f->dump_string("option", "startup");
  }
  if (has_flag(FLAG_CLUSTER_CREATE)) {
    f->dump_string("option", "cluster_create");
  }
  if (has_flag(FLAG_CREATE)) {
    f->dump_string("option", "create");
  }
  f->close_section();
}

std::string Option::to_str(const Option::value_t& v)
{
  return stringify(v);
}

void Option::print(ostream *out) const
{
  *out << name << " - " << desc << "\n";
  *out << "  (" << type_to_str(type) << ", " << level_to_str(level) << ")\n";
  if (!boost::get<boost::blank>(&daemon_value)) {
    *out << "  Default (non-daemon): " << stringify(value) << "\n";
    *out << "  Default (daemon): " << stringify(daemon_value) << "\n";
  } else {
    *out << "  Default: " << stringify(value) << "\n";
  }
  if (!enum_allowed.empty()) {
    *out << "  Possible values: ";
    for (auto& i : enum_allowed) {
      *out << " " << stringify(i);
    }
    *out << "\n";
  }
  if (!boost::get<boost::blank>(&min)) {
    *out << "  Minimum: " << stringify(min) << "\n"
	 << "  Maximum: " << stringify(max) << "\n";
  }
  *out << "  Can update at runtime: "
       << (can_update_at_runtime() ? "true" : "false") << "\n";
  if (!services.empty()) {
    *out << "  Services: " << services << "\n";
  }
  if (!tags.empty()) {
    *out << "  Tags: " << tags << "\n";
  }
  if (!see_also.empty()) {
    *out << "  See also: " << see_also << "\n";
  }

  if (long_desc.size()) {
    *out << "\n" << long_desc << "\n";
  }
}

constexpr unsigned long long operator"" _min (unsigned long long min) {
  return min * 60;
}
constexpr unsigned long long operator"" _hr (unsigned long long hr) {
  return hr * 60 * 60;
}
constexpr unsigned long long operator"" _day (unsigned long long day) {
  return day * 24 * 60 * 60;
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
constexpr unsigned long long operator"" _T (unsigned long long n) {
  return n << 40;
}

std::vector<Option> get_global_options() {
  return std::vector<Option>({
    Option("host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("local hostname")
    .set_long_description("if blank, ceph assumes the short hostname (hostname -s)")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("common")
    .add_tag("network"),

    Option("fsid", Option::TYPE_UUID, Option::LEVEL_BASIC)
    .set_description("cluster fsid (uuid)")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("service"),

    Option("public_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("public-facing address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"}),

    Option("public_addrv", Option::TYPE_ADDRVEC, Option::LEVEL_BASIC)
    .set_description("public-facing address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"}),

    Option("public_bind_addr", Option::TYPE_ADDR, Option::LEVEL_ADVANCED)
    .set_default(entity_addr_t())
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mon")
    .set_description(""),

    Option("cluster_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("cluster-facing address to bind to")
    .add_service("osd")
    .set_flag(Option::FLAG_STARTUP)
    .add_tag("network"),

    Option("public_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service({"mon", "mds", "osd", "mgr"})
    .set_flag(Option::FLAG_STARTUP)
    .add_tag("network")
    .set_description("Network(s) from which to choose a public address to bind to"),

    Option("public_network_interface", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service({"mon", "mds", "osd", "mgr"})
    .add_tag("network")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Interface name(s) from which to choose an address from a public_network to bind to; public_network must also be specified.")
    .add_see_also("public_network"),

    Option("cluster_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service("osd")
    .set_flag(Option::FLAG_STARTUP)
    .add_tag("network")
    .set_description("Network(s) from which to choose a cluster address to bind to"),

    Option("cluster_network_interface", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service({"mon", "mds", "osd", "mgr"})
    .set_flag(Option::FLAG_STARTUP)
    .add_tag("network")
    .set_description("Interface name(s) from which to choose an address from a cluster_network to bind to; cluster_network must also be specified.")
    .add_see_also("cluster_network"),

    Option("monmap", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to MonMap file")
    .set_long_description("This option is normally used during mkfs, but can also "
  			"be used to identify which monitors to connect to.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mon")
    .set_flag(Option::FLAG_CREATE),

    Option("mon_host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("list of hosts or addresses to search for a monitor")
    .set_long_description("This is a comma, whitespace, or semicolon separated "
  			"list of IP addresses or hostnames. Hostnames are "
  			"resolved via DNS and all A or AAAA records are "
  			"included in the search list.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("mon_dns_srv_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ceph-mon")
    .set_description("name of DNS SRV record to check for monitor addresses")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("network")
    .add_see_also("mon_host"),

    Option("container_image", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("container image (used by cephadm orchestrator)")
    .set_flag(Option::FLAG_STARTUP)
    .set_default("docker.io/ceph/daemon-base:latest-master-devel"),

    Option("no_config_file", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("config")
    .set_description("signal that we don't require a config file to be present")
    .set_long_description("When specified, we won't be looking for a "
			  "configuration file, and will instead expect that "
			  "whatever options or values are required for us to "
			  "work will be passed as arguments."),

    // lockdep
    Option("lockdep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable lockdep lock dependency analyzer")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("lockdep_force_backtrace", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("always gather current backtrace at every lock")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_see_also("lockdep"),

    Option("run_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/run/ceph")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("path for the 'run' directory for storing pid and socket files")
    .add_service("common")
    .add_see_also("admin_socket"),

    Option("admin_socket", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_daemon_default("$run_dir/$cluster-$name.asok")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("path for the runtime control socket file, used by the 'ceph daemon' command")
    .add_service("common"),

    Option("admin_socket_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("file mode to set for the admin socket file, e.g, '0755'")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_see_also("admin_socket"),

    // daemon
    Option("daemonize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_daemon_default(true)
    .set_description("whether to daemonize (background) after startup")
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"pid_file", "chdir"}),

    Option("setuser", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("uid or user name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("setgroup"),

    Option("setgroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("gid or group name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("setuser"),

    Option("setuser_match_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("if set, setuser/setgroup is condition on this path matching ownership")
    .set_long_description("If setuser or setgroup are specified, and this option is non-empty, then the uid/gid of the daemon will only be changed if the file or directory specified by this option has a matching uid and/or gid.  This exists primarily to allow switching to user ceph for OSDs to be conditional on whether the osd data contents have also been chowned after an upgrade.  This is normally specified by the systemd unit file.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"setuser", "setgroup"}),

    Option("pid_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("path to write a pid file (if any)")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    Option("chdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to chdir(2) to after daemonizing")
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also("daemonize"),

    Option("fatal_signal_handlers", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("whether to register signal handlers for SIGABRT etc that dump a stack trace")
    .set_long_description("This is normally true for daemons and values for libraries.")
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    Option("crash_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_default("/var/lib/ceph/crash")
    .set_description("Directory where crash reports are archived"),

    // restapi
    Option("restapi_log_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("restapi_base_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("erasure_code_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_PKGLIBDIR"/erasure-code")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("directory where erasure-code plugins can be found")
    .add_service({"mon", "osd"}),

    // logging
    Option("log_file", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_daemon_default("/var/log/ceph/$cluster-$name.log")
    .set_description("path to log file")
    .add_see_also({"log_to_file",
		   "log_to_stderr",
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

    Option("log_to_file", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_description("send log lines to a file")
    .add_see_also("log_file"),

    Option("log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_daemon_default(false)
    .set_description("send log lines to stderr"),

    Option("err_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_daemon_default(true)
    .set_description("send critical error log lines to stderr"),

    Option("log_stderr_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("String to prefix log messages with when sent to stderr")
    .set_long_description("This is useful in container environments when combined with mon_cluster_log_to_stderr.  The mon log prefixes each line with the channel name (e.g., 'default', 'audit'), while log_stderr_prefix can be set to 'debug '.")
    .add_see_also("mon_cluster_log_to_stderr"),

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

    Option("log_coarse_timestamps", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("timestamp log entries from coarse system clock "
		     "to improve performance")
    .add_service("common")
    .add_tag("performance")
    .add_tag("service"),


    // unmodified
    Option("clog_to_monitors", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=true")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Make daemons send cluster log messages to monitors"),

    Option("clog_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Make daemons send cluster log messages to syslog"),

    Option("clog_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("info")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Syslog level for cluster log messages")
    .add_see_also("clog_to_syslog"),

    Option("clog_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=daemon audit=local0")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Syslog facility for cluster log messages")
    .add_see_also("clog_to_syslog"),

    Option("clog_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Make daemons send cluster log to graylog"),

    Option("clog_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Graylog host to cluster log messages")
    .add_see_also("clog_to_graylog"),

    Option("clog_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("12201")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Graylog port number for cluster log messages")
    .add_see_also("clog_to_graylog"),

    Option("mon_cluster_log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Make monitor send cluster log messages to stderr (prefixed by channel)")
    .add_see_also("log_stderr_prefix"),

    Option("mon_cluster_log_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Make monitor send cluster log messages to syslog"),

    Option("mon_cluster_log_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("info")
    .add_service("mon")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Syslog level for cluster log messages")
    .add_see_also("mon_cluster_log_to_syslog"),

    Option("mon_cluster_log_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("daemon")
    .add_service("mon")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Syslog facility for cluster log messages")
    .add_see_also("mon_cluster_log_to_syslog"),

    Option("mon_cluster_log_to_file", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Make monitor send cluster log messages to file")
    .add_see_also("mon_cluster_log_file"),

    Option("mon_cluster_log_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default=/var/log/ceph/$cluster.$channel.log cluster=/var/log/ceph/$cluster.log")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("File(s) to write cluster log to")
    .set_long_description("This can either be a simple file name to receive all messages, or a list of key/value pairs where the key is the log channel and the value is the filename, which may include $cluster and $channel metavariables")
    .add_see_also("mon_cluster_log_to_file"),

    Option("mon_cluster_log_file_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("debug")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Lowest level to include is cluster log file")
    .add_see_also("mon_cluster_log_file"),

    Option("mon_cluster_log_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Make monitor send cluster log to graylog"),

    Option("mon_cluster_log_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Graylog host for cluster log messages")
    .add_see_also("mon_cluster_log_to_graylog"),

    Option("mon_cluster_log_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("12201")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Graylog port for cluster log messages")
    .add_see_also("mon_cluster_log_to_graylog"),

    Option("enable_experimental_unrecoverable_data_corrupting_features", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_RUNTIME)
    .set_default("")
    .set_description("Enable named (or all with '*') experimental features that may be untested, dangerous, and/or cause permanent data loss"),

    Option("plugin_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_PKGLIBDIR)
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "osd"})
    .set_description("Base directory for dynamically loaded plugins"),

    // Compressor
    Option("compressor_zlib_isal", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use Intel ISA-L accelerated zlib implementation if available"),

    Option("compressor_zlib_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Zlib compression level to use"),

    Option("compressor_zlib_winsize", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-15)
    .set_min_max(-15,32)
    .set_description("Zlib compression winsize to use"),

    Option("compressor_zstd_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Zstd compression level to use"),

    Option("qat_compressor_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable Intel QAT acceleration support for compression if available"),

    Option("plugin_crypto_accelerator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("crypto_isal")
    .set_description("Crypto accelerator library to use"),

    Option("openssl_engine_opts", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Use engine for specific openssl algorithm")
    .set_long_description("Pass opts in this way: engine_id=engine1,dynamic_path=/some/path/engine1.so,default_algorithms=DIGESTS:engine_id=engine2,dynamic_path=/some/path/engine2.so,default_algorithms=CIPHERS,other_ctrl=other_value"),

    Option("mempool_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description(""),

    Option("thp", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("enable transparent huge page (THP) support")
    .set_long_description("Ceph is known to suffer from memory fragmentation due to THP use. This is indicated by RSS usage above configured memory targets. Enabling THP is currently discouraged until selective use of THP by Ceph is implemented."),

    Option("key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Authentication key")
    .set_long_description("A CephX authentication key, base64 encoded.  It normally looks something like 'AQAtut9ZdMbNJBAAHz6yBAWyJyz2yYRyeMWDag=='.")
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_see_also("keyfile")
    .add_see_also("keyring"),

    Option("keyfile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Path to a file containing a key")
    .set_long_description("The file should contain a CephX authentication key and optionally a trailing newline, but nothing else.")
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_see_also("key"),

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
    .set_description("Path to a keyring file.")
    .set_long_description("A keyring file is an INI-style formatted file where the section names are client or daemon names (e.g., 'osd.0') and each section contains a 'key' property with CephX authentication key as the value.")
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_see_also("key")
    .add_see_also("keyfile"),

    Option("heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Frequency of internal heartbeat checks (seconds)"),

    Option("heartbeat_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("File to touch on successful internal heartbeat")
    .set_long_description("If set, this file will be touched every time an internal heartbeat check succeeds.")
    .add_see_also("heartbeat_interval"),

    Option("heartbeat_inject_failure", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("perf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable internal performance metrics")
    .set_long_description("If enabled, collect and expose internal health metrics"),

    Option("ms_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_default("async+posix")
    .set_description("Messenger implementation to use for network communication"),

    Option("ms_public_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Messenger implementation to use for the public network")
    .set_long_description("If not specified, use ms_type")
    .add_see_also("ms_type"),

    Option("ms_cluster_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Messenger implementation to use for the internal cluster network")
    .set_long_description("If not specified, use ms_type")
    .add_see_also("ms_type"),

    Option("ms_mon_cluster_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Connection modes (crc, secure) for intra-mon connections in order of preference")
    .add_see_also("ms_mon_service_mode")
    .add_see_also("ms_mon_client_mode")
    .add_see_also("ms_service_mode")
    .add_see_also("ms_cluster_mode")
    .add_see_also("ms_client_mode"),

    Option("ms_mon_service_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Allowed connection modes (crc, secure) for connections to mons")
    .add_see_also("ms_service_mode")
    .add_see_also("ms_mon_cluster_mode")
    .add_see_also("ms_mon_client_mode")
    .add_see_also("ms_cluster_mode")
    .add_see_also("ms_client_mode"),

    Option("ms_mon_client_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Connection modes (crc, secure) for connections from clients to monitors in order of preference")
    .add_see_also("ms_mon_service_mode")
    .add_see_also("ms_mon_cluster_mode")
    .add_see_also("ms_service_mode")
    .add_see_also("ms_cluster_mode")
    .add_see_also("ms_client_mode"),

    Option("ms_cluster_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Connection modes (crc, secure) for intra-cluster connections in order of preference")
    .add_see_also("ms_service_mode")
    .add_see_also("ms_client_mode"),

    Option("ms_service_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Allowed connection modes (crc, secure) for connections to daemons")
    .add_see_also("ms_cluster_mode")
    .add_see_also("ms_client_mode"),

    Option("ms_client_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .set_description("Connection modes (crc, secure) for connections from clients in order of preference")
    .add_see_also("ms_cluster_mode")
    .add_see_also("ms_service_mode"),

    Option("ms_learn_addr_from_peer", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Learn address from what IP our first peer thinks we connect from")
    .set_long_description("Use the IP address our first peer (usually a monitor) sees that we are connecting from.  This is useful if a client is behind some sort of NAT and we want to see it identified by its local (not NATed) address."),

    Option("ms_tcp_nodelay", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Disable Nagle's algorithm and send queued network traffic immediately"),

    Option("ms_tcp_rcvbuf", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Size of TCP socket receive buffer"),

    Option("ms_tcp_prefetch_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_description("Maximum amount of data to prefetch out of the socket receive buffer"),

    Option("ms_initial_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.2)
    .set_description("Initial backoff after a network error is detected (seconds)"),

    Option("ms_max_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(15.0)
    .set_description("Maximum backoff after a network error before retrying (seconds)")
    .add_see_also("ms_initial_backoff"),

    Option("ms_crc_data", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Set and/or verify crc32c checksum on data payload sent over network"),

    Option("ms_crc_header", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Set and/or verify crc32c checksum on header payload sent over network"),

    Option("ms_die_on_bad_msg", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Induce a daemon crash/exit when a bad network message is received"),

    Option("ms_die_on_unhandled_msg", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Induce a daemon crash/exit when an unrecognized message is received"),

    Option("ms_die_on_old_message", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Induce a daemon crash/exit when a old, undecodable message is received"),

    Option("ms_die_on_skipped_message", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Induce a daemon crash/exit if sender skips a message sequence number"),

    Option("ms_die_on_bug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Induce a crash/exit on various bugs (for testing purposes)"),

    Option("ms_dispatch_throttle_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description("Limit messages that are read off the network but still being processed"),

    Option("ms_bind_ipv4", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Bind servers to IPv4 address(es)")
    .add_see_also("ms_bind_ipv6"),

    Option("ms_bind_ipv6", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Bind servers to IPv6 address(es)")
    .add_see_also("ms_bind_ipv4"),

    Option("ms_bind_prefer_ipv4", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Prefer IPV4 over IPV6 address(es)"),

    Option("ms_bind_msgr1", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Bind servers to msgr1 (legacy) protocol address(es)")
    .add_see_also("ms_bind_msgr2"),

    Option("ms_bind_msgr2", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Bind servers to msgr2 (nautilus+) protocol address(es)")
    .add_see_also("ms_bind_msgr1"),

    Option("ms_bind_port_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(6800)
    .set_description("Lowest port number to bind daemon(s) to"),

    Option("ms_bind_port_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7300)
    .set_description("Highest port number to bind daemon(s) to"),

    Option("ms_bind_retry_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  #if !defined(__FreeBSD__)
    .set_default(3)
  #else
    // FreeBSD does not use SO_REAUSEADDR so allow for a bit more time per default
    .set_default(6)
  #endif
    .set_description("Number of attempts to make while bind(2)ing to a port"),

    Option("ms_bind_retry_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  #if !defined(__FreeBSD__)
    .set_default(5)
  #else
    // FreeBSD does not use SO_REAUSEADDR so allow for a bit more time per default
    .set_default(6)
  #endif
    .set_description("Delay between bind(2) attempts (seconds)"),

    Option("ms_bind_before_connect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Call bind(2) on client sockets"),

    Option("ms_tcp_listen_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description("Size of queue of incoming connections for accept(2)"),


    Option("ms_connection_ready_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Time before we declare a not yet ready connection as dead (seconds)"),

    Option("ms_connection_idle_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(900)
    .set_description("Time before an idle connection is closed (seconds)"),

    Option("ms_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(16777216)
    .set_description(""),

    Option("ms_pq_min_cost", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(65536)
    .set_description(""),

    Option("ms_inject_socket_failures", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Inject a socket failure every Nth socket operation"),

    Option("ms_inject_delay_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("Entity type to inject delays for")
    .set_flag(Option::FLAG_RUNTIME),

    Option("ms_inject_delay_msg_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("Message type to inject delays for"),

    Option("ms_inject_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description("Max delay to inject"),

    Option("ms_inject_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("ms_inject_internal_delays", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Inject various internal delays to induce races (seconds)"),

    Option("ms_blackhole_osd", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_blackhole_mon", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_blackhole_mds", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_blackhole_mgr", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_blackhole_client", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("ms_dump_on_send", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Hexdump message to debug log on message send"),

    Option("ms_dump_corrupt_message_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Log level at which to hexdump corrupt messages we receive"),

    Option("ms_async_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min_max(1, 24)
    .set_description("Threadpool size for AsyncMessenger (ms_type=async)"),

    Option("ms_async_max_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Maximum threadpool size of AsyncMessenger")
    .add_see_also("ms_async_op_threads"),

    Option("ms_async_rdma_device_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_async_rdma_enable_hugepage", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_async_rdma_buffer_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
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

    Option("ms_async_rdma_support_srq", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("ms_async_rdma_port_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("ms_async_rdma_polling_us", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("ms_async_rdma_gid_idx", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("use gid_idx to select GID for choosing RoCEv1 or RoCEv2"),

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

    Option("ms_max_accept_failures", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("The maximum number of consecutive failed accept() calls before "
                     "considering the daemon is misconfigured and abort it."),

    Option("ms_async_rdma_cm", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("ms_async_rdma_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ib")
    .set_description(""),

    Option("ms_dpdk_port_id", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("ms_dpdk_coremask", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("0xF")         //begin with 0x for the string
    .set_description("")
    .add_see_also("ms_async_op_threads"),

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
    .set_description(""),

    Option("ms_dpdk_gateway_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("ms_dpdk_netmask_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

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
    .set_description("send ourselves a SIGTERM early during startup"),

    // MON
    Option("mon_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("enable/disable MON op tracking"),

    Option("mon_op_complaint_time", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(30)
    .add_service("mon")
    .set_description("time after which to consider a monitor operation blocked "
                     "after no updates"),

    Option("mon_op_log_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("max number of slow ops to display"),

    Option("mon_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .add_service("mon")
    .set_description("max number of completed ops to track"),

    Option("mon_op_history_duration", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(600)
    .add_service("mon")
    .set_description("expiration time in seconds of historical MON OPS"),

    Option("mon_op_history_slow_op_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .add_service("mon")
    .set_description("max number of slow historical MON OPS to keep"),

    Option("mon_op_history_slow_op_threshold", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mon")
    .set_description("duration of an op to be considered as a historical slow op"),

    Option("mon_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_default("/var/lib/ceph/mon/$cluster-$id")
    .add_service("mon")
    .set_description("path to mon database"),

    Option("mon_initial_members", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .add_service("mon")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description(""),

    Option("mon_compact_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description(""),

    Option("mon_compact_on_bootstrap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description(""),

    Option("mon_compact_on_trim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description(""),

    /* -- mon: osdmap prune (begin) -- */
    Option("mon_osdmap_full_prune_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("enables pruning full osdmap versions when we go over a given number of maps")
    .add_see_also("mon_osdmap_full_prune_min")
    .add_see_also("mon_osdmap_full_prune_interval")
    .add_see_also("mon_osdmap_full_prune_txsize"),

    Option("mon_osdmap_full_prune_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .add_service("mon")
    .set_description("minimum number of versions in the store to trigger full map pruning")
    .add_see_also("mon_osdmap_full_prune_enabled")
    .add_see_also("mon_osdmap_full_prune_interval")
    .add_see_also("mon_osdmap_full_prune_txsize"),

    Option("mon_osdmap_full_prune_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mon")
    .set_description("interval between maps that will not be pruned; maps in the middle will be pruned.")
    .add_see_also("mon_osdmap_full_prune_enabled")
    .add_see_also("mon_osdmap_full_prune_interval")
    .add_see_also("mon_osdmap_full_prune_txsize"),

    Option("mon_osdmap_full_prune_txsize", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .add_service("mon")
    .set_description("number of maps we will prune per iteration")
    .add_see_also("mon_osdmap_full_prune_enabled")
    .add_see_also("mon_osdmap_full_prune_interval")
    .add_see_also("mon_osdmap_full_prune_txsize"),
    /* -- mon: osdmap prune (end) -- */

    Option("mon_osd_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description("maximum number of OSDMaps to cache in memory"),

    Option("mon_osd_cache_size_min", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .add_service("mon")
    .set_description("The minimum amount of bytes to be kept mapped in memory for osd monitor caches."),

    Option("mon_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_default(2_G)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("The amount of bytes pertaining to osd monitor caches and kv cache to be kept mapped in memory with cache auto-tuning enabled"),

    Option("mon_memory_autotune", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("Autotune the cache memory being used for osd monitors and kv database"),

    Option("mon_cpu_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .add_service("mon")
    .set_description("worker threads for CPU intensive background work"),

    Option("mon_osd_mapping_pgs_per_chunk", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(4096)
    .add_service("mon")
    .set_description("granularity of PG placement calculation background work"),

    Option("mon_clean_pg_upmaps_per_chunk", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(256)
    .add_service("mon")
    .set_description("granularity of PG upmap validation background work"),

    Option("mon_osd_max_creating_pgs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .add_service("mon")
    .set_description("maximum number of PGs the mon will create at once"),

    Option("mon_osd_max_initial_pgs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .add_service("mon")
    .set_description("maximum number of PGs a pool will created with")
    .set_long_description("If the user specifies more PGs than this, the cluster will subsequently split PGs after the pool is created in order to reach the target."),

    Option("mon_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("interval for internal mon background checks"),

    Option("mon_session_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .add_service("mon")
    .set_description("close inactive mon client connections after this many seconds"),

    Option("mon_subscribe_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1_day)
    .add_service("mon")
    .set_description("subscribe interval for pre-jewel clients"),

    Option("mon_delta_reset_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mon")
    .add_service("mon")
    .set_description("window duration for rate calculations in 'ceph status'"),

    Option("mon_osd_laggy_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .add_service("mon")
    .set_description("halflife of OSD 'lagginess' factor"),

    Option("mon_osd_laggy_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .set_min_max(0.0, 1.0)
    .add_service("mon")
    .set_description("how heavily to weight OSD marking itself back up in overall laggy_probability")
    .set_long_description("1.0 means that an OSD marking itself back up (because it was marked down but not actually dead) means a 100% laggy_probability; 0.0 effectively disables tracking of laggy_probability."),

    Option("mon_osd_laggy_max_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .add_service("mon")
    .set_description("cap value for period for OSD to be marked for laggy_interval calculation"),

    Option("mon_osd_adjust_heartbeat_grace", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("increase OSD heartbeat grace if peers appear to be laggy")
    .set_long_description("If an OSD is marked down but then marks itself back up, it implies it wasn't actually down but was unable to respond to heartbeats.  If this option is true, we can use the laggy_probability and laggy_interval values calculated to model this situation to increase the heartbeat grace period for this OSD so that it isn't marked down again.  laggy_probability is an estimated probability that the given OSD is down because it is laggy (not actually down), and laggy_interval is an estiate on how long it stays down when it is laggy.")
    .add_see_also("mon_osd_laggy_halflife")
    .add_see_also("mon_osd_laggy_weight")
    .add_see_also("mon_osd_laggy_max_interval"),

    Option("mon_osd_adjust_down_out_interval", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("increase the mon_osd_down_out_interval if an OSD appears to be laggy")
    .add_see_also("mon_osd_adjust_heartbeat_grace"),

    Option("mon_osd_auto_mark_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("mark any OSD that comes up 'in'"),

    Option("mon_osd_auto_mark_auto_out_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("mark any OSD that comes up that was automatically marked 'out' back 'in'")
    .add_see_also("mon_osd_down_out_interval"),

    Option("mon_osd_auto_mark_new_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("mark any new OSD that comes up 'in'"),

    Option("mon_osd_destroyed_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .add_service("mon")
    .set_description("mark any OSD 'out' that has been 'destroy'ed for this long (seconds)"),

    Option("mon_osd_down_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .add_service("mon")
    .set_description("mark any OSD 'out' that has been 'down' for this long (seconds)"),

    Option("mon_osd_down_out_subtree_limit", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rack")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .set_description("do not automatically mark OSDs 'out' if an entire subtree of this size is down")
    .add_see_also("mon_osd_down_out_interval"),

    Option("mon_osd_min_up_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.3)
    .add_service("mon")
    .set_description("do not automatically mark OSDs 'out' if fewer than this many OSDs are 'up'")
    .add_see_also("mon_osd_down_out_interval"),

    Option("mon_osd_min_in_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.75)
    .add_service("mon")
    .set_description("do not automatically mark OSDs 'out' if fewer than this many OSDs are 'in'")
    .add_see_also("mon_osd_down_out_interval"),

    Option("mon_osd_warn_op_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .add_service("mgr")
    .set_description("issue REQUEST_SLOW health warning if OSD ops are slower than this age (seconds)"),

    Option("mon_osd_err_op_age_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .add_service("mgr")
    .set_description("issue REQUEST_STUCK health error if OSD ops are slower than is age (seconds)"),

    Option("mon_osd_prime_pg_temp", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_service("mon")
    .set_description("minimize peering work by priming pg_temp values after a map change"),

    Option("mon_osd_prime_pg_temp_max_time", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.5)
    .add_service("mon")
    .set_description("maximum time to spend precalculating PG mappings on map change (seconds)"),

    Option("mon_osd_prime_pg_temp_max_estimate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.25)
    .add_service("mon")
    .set_description("calculate all PG mappings if estimated fraction of PGs that change is above this amount"),

    Option("mon_stat_smooth_intervals", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(6)
    .set_min(1)
    .add_service("mgr")
    .set_description("number of PGMaps stats over which we calc the average read/write throughput of the whole cluster"),

    Option("mon_election_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("maximum time for a mon election (seconds)"),

    Option("mon_lease", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("lease interval between quorum monitors (seconds)")
    .set_long_description("This setting controls how sensitive your mon quorum is to intermittent network issues or other failures."),

    Option("mon_lease_renew_interval_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.6)
    .set_min_max((double)0.0, (double).9999999)
    .add_service("mon")
    .set_description("multiple of mon_lease for the lease renewal interval")
    .set_long_description("Leases must be renewed before they time out.  A smaller value means frequent renewals, while a value close to 1 makes a lease expiration more likely.")
    .add_see_also("mon_lease"),

    Option("mon_lease_ack_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_min_max(1.0001, 100.0)
    .add_service("mon")
    .set_description("multiple of mon_lease for the lease ack interval before calling new election")
    .add_see_also("mon_lease"),

    Option("mon_accept_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .add_service("mon")
    .set_description("multiple of mon_lease for follower mons to accept proposed state changes before calling a new election")
    .add_see_also("mon_lease"),

    Option("mon_clock_drift_allowed", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.050)
    .add_service("mon")
    .set_description("allowed clock drift (in seconds) between mons before issuing a health warning"),

    Option("mon_clock_drift_warn_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("exponential backoff factor for logging clock drift warnings in the cluster log"),

    Option("mon_timecheck_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(300.0)
    .add_service("mon")
    .set_description("frequency of clock synchronization checks between monitors (seconds)"),

    Option("mon_timecheck_skew_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .add_service("mon")
    .set_description("frequency of clock synchronization (re)checks between monitors while clocks are believed to be skewed (seconds)")
    .add_see_also("mon_timecheck_interval"),

    Option("mon_pg_stuck_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("number of seconds after which pgs can be considered stuck inactive, unclean, etc")
    .set_long_description("see doc/control.rst under dump_stuck for more info")
    .add_service("mgr"),

    Option("mon_pg_warn_min_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .add_service("mgr")
    .set_description("minimal number PGs per (in) osd before we warn the admin"),

    Option("mon_max_pg_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_min(1)
    .set_default(250)
    .add_service("mgr")
    .set_description("Max number of PGs per OSD the cluster will allow")
    .set_long_description("If the number of PGs per OSD exceeds this, a "
        "health warning will be visible in `ceph status`.  This is also used "
        "in automated PG management, as the threshold at which some pools' "
        "pg_num may be shrunk in order to enable increasing the pg_num of "
        "others."),

    Option("mon_target_pg_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_min(1)
    .set_default(100)
    .set_description("Automated PG management creates this many PGs per OSD")
    .set_long_description("When creating pools, the automated PG management "
        "logic will attempt to reach this target.  In some circumstances, it "
        "may exceed this target, up to the ``mon_max_pg_per_osd`` limit. "
        "Conversely, a lower number of PGs per OSD may be created if the "
        "cluster is not yet fully utilised"),

    Option("mon_pg_warn_max_object_skew", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description("max skew few average in objects per pg")
    .add_service("mgr"),

    Option("mon_pg_warn_min_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("do not warn below this object #")
    .add_service("mgr"),

    Option("mon_pg_warn_min_pool_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("do not warn on pools below this object #")
    .add_service("mgr"),

    Option("mon_pg_check_down_all_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.5)
    .set_description("threshold of down osds after which we check all pgs")
    .add_service("mgr"),

    Option("mon_cache_target_full_warn_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.66)
    .add_service("mgr")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("issue CACHE_POOL_NEAR_FULL health warning when cache pool utilization exceeds this ratio of usable space"),

    Option("mon_osd_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.95)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("full ratio of OSDs to be set during initial creation of the cluster"),

    Option("mon_osd_backfillfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.90)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description(""),

    Option("mon_osd_nearfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.85)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("nearfull ratio for OSDs to be set during initial creation of cluster"),

    Option("mon_osd_initial_require_min_compat_client", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("luminous")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description(""),

    Option("mon_allow_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("allow pool deletions"),

    Option("mon_fake_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("fake pool deletions by renaming the rados pool"),

    Option("mon_globalid_prealloc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .add_service("mon")
    .set_description("number of globalid values to preallocate")
    .set_long_description("This setting caps how many new clients can authenticate with the cluster before the monitors have to perform a write to preallocate more.  Large values burn through the 64-bit ID space more quickly."),

    Option("mon_osd_report_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(900)
    .add_service("mon")
    .set_description("time before OSDs who do not report to the mons are marked down (seconds)"),

    Option("mon_warn_on_msgr2_not_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("issue MON_MSGR2_NOT_ENABLED health warning if monitors are all running Nautilus but not all binding to a msgr2 port")
    .add_see_also("ms_bind_msgr2"),

    Option("mon_warn_on_legacy_crush_tunables", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mgr")
    .set_description("issue OLD_CRUSH_TUNABLES health warning if CRUSH tunables are older than mon_crush_min_required_version")
    .add_see_also("mon_crush_min_required_version"),

    Option("mon_crush_min_required_version", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("hammer")
    .add_service("mgr")
    .set_description("minimum ceph release to use for mon_warn_on_legacy_crush_tunables")
    .add_see_also("mon_warn_on_legacy_crush_tunables"),

    Option("mon_warn_on_crush_straw_calc_version_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mgr")
    .set_description("issue OLD_CRUSH_STRAW_CALC_VERSION health warning if the CRUSH map's straw_calc_version is zero"),

    Option("mon_warn_on_osd_down_out_interval_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mgr")
    .set_description("issue OSD_NO_DOWN_OUT_INTERVAL health warning if mon_osd_down_out_interval is zero")
    .set_long_description("Having mon_osd_down_out_interval set to 0 means that down OSDs are not marked out automatically and the cluster does not heal itself without administrator intervention.")
    .add_see_also("mon_osd_down_out_interval"),

    Option("mon_warn_on_cache_pools_without_hit_sets", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mgr")
    .set_description("issue CACHE_POOL_NO_HIT_SET health warning for cache pools that do not have hit sets configured"),

    Option("mon_warn_on_pool_no_app", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_service("mgr")
    .set_description("issue POOL_APP_NOT_ENABLED health warning if pool has not application enabled"),

    Option("mon_warn_on_pool_pg_num_not_power_of_two", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_service("mon")
    .set_description("issue POOL_PG_NUM_NOT_POWER_OF_TWO warning if pool has a non-power-of-two pg_num value"),

    Option("mon_warn_on_pool_no_redundancy", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("Issue a health warning if any pool is configured with no replicas")
    .add_see_also("osd_pool_default_size")
    .add_see_also("osd_pool_default_min_size"),

    Option("mon_allow_pool_size_one", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("allow configuring pool with no replicas"),

    Option("mon_warn_on_misplaced", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mgr")
    .set_description("Issue a health warning if there are misplaced objects"),

    Option("mon_warn_on_too_few_osds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mgr")
    .set_description("Issue a health warning if there are fewer OSDs than osd_pool_default_size"),

    Option("mon_warn_on_slow_ping_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .add_service("mgr")
    .set_description("Override mon_warn_on_slow_ping_ratio with specified threshold in milliseconds")
    .add_see_also("mon_warn_on_slow_ping_ratio"),

    Option("mon_warn_on_slow_ping_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
    .add_service("mgr")
    .set_description("Issue a health warning if heartbeat ping longer than percentage of osd_heartbeat_grace")
    .add_see_also("osd_heartbeat_grace")
    .add_see_also("mon_warn_on_slow_ping_time"),

    Option("mon_max_snap_prune_per_epoch", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .add_service("mon")
    .set_description("max number of pruned snaps we will process in a single OSDMap epoch"),

    Option("mon_min_osdmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description("min number of OSDMaps to store"),

    Option("mon_max_log_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description("max number of past cluster log epochs to store"),

    Option("mon_max_mdsmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description("max number of FSMaps/MDSMaps to store"),

    Option("mon_max_mgrmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description("max number of MgrMaps to store"),

    Option("mon_max_osd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .add_service("mon")
    .set_description("max number of OSDs in a cluster"),

    Option("mon_probe_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .add_service("mon")
    .set_description("timeout for querying other mons during bootstrap pre-election phase (seconds)"),

    Option("mon_client_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100ul << 20)
    .add_service("mon")
    .set_description("max bytes of outstanding client messages mon will read off the network"),

    Option("mon_daemon_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(400ul << 20)
    .add_service("mon")
    .set_description("max bytes of outstanding mon messages mon will read off the network"),

    Option("mon_mgr_proxy_client_bytes_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.3)
    .add_service("mon")
    .set_description("ratio of mon_client_bytes that can be consumed by "
                     "proxied mgr commands before we error out to client"),

    Option("mon_log_max_summary", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .add_service("mon")
    .set_description("number of recent cluster log messages to retain"),

    Option("mon_max_log_entries_per_event", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .add_service("mon")
    .set_description("max cluster log entries per paxos event"),

    Option("mon_reweight_min_pgs_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mgr")
    .set_description(""),

    Option("mon_reweight_min_bytes_per_osd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .add_service("mgr")
    .set_description(""),

    Option("mon_reweight_max_osds", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .add_service("mgr")
    .set_description(""),

    Option("mon_reweight_max_change", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .add_service("mgr")
    .set_description(""),

    Option("mon_health_to_clog", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("log monitor health to cluster log"),

    Option("mon_health_to_clog_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .add_service("mon")
    .set_description("frequency to log monitor health to cluster log")
    .add_see_also("mon_health_to_clog"),

    Option("mon_health_to_clog_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(60.0)
    .add_service("mon")
    .set_description(""),

    Option("mon_health_max_detail", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .add_service("mon")
    .set_description("max detailed pgs to report in health detail"),

    Option("mon_health_log_update_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .add_service("mon")
    .set_description("minimum time in seconds between log messages about "
                     "each health check")
    .set_min(0),

    Option("mon_data_avail_crit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("issue MON_DISK_CRIT health error when mon available space below this percentage"),

    Option("mon_data_avail_warn", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .add_service("mon")
    .set_description("issue MON_DISK_LOW health warning when mon available space below this percentage"),

    Option("mon_data_size_warn", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(15_G)
    .add_service("mon")
    .set_description("issue MON_DISK_BIG health warning when mon database is above this size"),

    Option("mon_warn_pg_not_scrubbed_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_min(0)
    .set_description("Percentage of the scrub max interval past the scrub max interval to warn")
    .set_long_description("")
    .add_see_also("osd_scrub_max_interval"),

    Option("mon_warn_pg_not_deep_scrubbed_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.75)
    .set_min(0)
    .set_description("Percentage of the deep scrub interval past the deep scrub interval to warn")
    .set_long_description("")
    .add_see_also("osd_deep_scrub_interval"),

    Option("mon_scrub_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .add_service("mon")
    .set_description("frequency for scrubbing mon database"),

    Option("mon_scrub_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_min)
    .add_service("mon")
    .set_description("timeout to restart scrub of mon quorum participant does not respond for the latest chunk"),

    Option("mon_scrub_max_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .add_service("mon")
    .set_description("max keys per on scrub chunk/step"),

    Option("mon_scrub_inject_crc_mismatch", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("mon")
    .set_description("probability for injecting crc mismatches into mon scrub"),

    Option("mon_scrub_inject_missing_keys", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("mon")
    .set_description("probability for injecting missing keys into mon scrub"),

    Option("mon_config_key_max_entry_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .add_service("mon")
    .set_description("Defines the number of bytes allowed to be held in a "
		     "single config-key entry"),

    Option("mon_sync_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .add_service("mon")
    .set_description("timeout before canceling sync if syncing mon does not respond"),

    Option("mon_sync_max_payload_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .add_service("mon")
    .set_description("target max message payload for mon sync"),

    Option("mon_sync_max_payload_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2000)
    .add_service("mon")
    .set_description("target max keys in message payload for mon sync"),

    Option("mon_sync_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("enable extra debugging during mon sync"),

    Option("mon_inject_sync_get_chunk_delay", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("inject delay during sync (seconds)"),

    Option("mon_osd_min_down_reporters", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .add_service("mon")
    .set_description("number of OSDs from different subtrees who need to report a down OSD for it to count")
    .add_see_also("mon_osd_reporter_subtree_level"),

    Option("mon_osd_reporter_subtree_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("host")
    .add_service("mon")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("in which level of parent bucket the reporters are counted"),

    Option("mon_osd_snap_trim_queue_warn_on", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32768)
    .add_service("mon")
    .set_description("Warn when snap trim queue is that large (or larger).")
    .set_long_description("Warn when snap trim queue length for at least one PG crosses this value, as this is indicator of snap trimmer not keeping up, wasting disk space"),

    Option("mon_osd_force_trim_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("force mons to trim osdmaps through this epoch"),

    Option("mon_mds_force_trim_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("force mons to trim mdsmaps/fsmaps through this epoch"),

    Option("mon_mds_skip_sanity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("skip sanity checks on fsmap/mdsmap"),

    Option("mon_debug_extra_checks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("Enable some additional monitor checks")
    .set_long_description(
        "Enable some additional monitor checks that would be too expensive "
        "to run on production systems, or would only be relevant while "
        "testing or debugging."),

    Option("mon_debug_block_osdmap_trim", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("Block OSDMap trimming while the option is enabled.")
    .set_long_description(
        "Blocking OSDMap trimming may be quite helpful to easily reproduce "
        "states in which the monitor keeps (hundreds of) thousands of "
        "osdmaps."),

    Option("mon_debug_deprecated_as_obsolete", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("treat deprecated mon commands as obsolete"),

    Option("mon_debug_dump_transactions", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("dump paxos transactions to log")
    .add_see_also("mon_debug_dump_location"),

    Option("mon_debug_dump_json", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("dump paxos transasctions to log as json")
    .add_see_also("mon_debug_dump_transactions"),

    Option("mon_debug_dump_location", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("/var/log/ceph/$cluster-$name.tdump")
    .add_service("mon")
    .set_description("file to dump paxos transactions to")
    .add_see_also("mon_debug_dump_transactions"),

    Option("mon_debug_no_require_octopus", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("do not set octopus feature for new mon clusters"),

    Option("mon_debug_no_require_pacific", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("do not set pacific feature for new mon clusters"),

    Option("mon_debug_no_require_bluestore_for_ec_overwrites", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description("do not require bluestore OSDs to enable EC overwrites on a rados pool"),

    Option("mon_debug_no_initial_persistent_features", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("do not set any monmap features for new mon clusters"),

    Option("mon_inject_transaction_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(10.0)
    .add_service("mon")
    .set_description("max duration of injected delay in paxos"),

    Option("mon_inject_transaction_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("probability of injecting a delay in paxos"),

    Option("mon_inject_pg_merge_bounce_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("probability of failing and reverting a pg_num decrement"),

    Option("mon_sync_provider_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("kill mon sync requester at specific point"),

    Option("mon_sync_requester_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description("kill mon sync requestor at specific point"),

    Option("mon_force_quorum_join", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon")
    .set_description("force mon to rejoin quorum even though it was just removed"),

    Option("mon_keyvaluedb", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb")
    .set_enum_allowed({"leveldb", "rocksdb"})
    .set_flag(Option::FLAG_CREATE)
    .add_service("mon")
    .set_description("database backend to use for the mon database"),

    Option("mon_debug_unsafe_allow_tier_with_nonempty_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon")
    .set_description(""),

    Option("mon_osd_blacklist_default_expire", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .add_service("mon")
    .set_description("Duration in seconds that blacklist entries for clients "
                     "remain in the OSD map"),

    Option("mon_mds_blacklist_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1_day)
    .set_min(1_hr)
    .add_service("mon")
    .set_description("Duration in seconds that blacklist entries for MDS "
                     "daemons remain in the OSD map")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mon_mgr_blacklist_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1_day)
    .set_min(1_hr)
    .add_service("mon")
    .set_description("Duration in seconds that blacklist entries for mgr "
                     "daemons remain in the OSD map")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mon_osd_crush_smoke_test", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon")
    .set_description("perform a smoke test on any new CRUSH map before accepting changes"),

    Option("mon_smart_report_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("mon")
    .set_description("Timeout (in seconds) for smarctl to run, default is set to 5"),


    // PAXOS

    Option("paxos_stash_full_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .add_service("mon")
    .set_description(""),

    Option("paxos_max_join_drift", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mon")
    .set_description(""),

    Option("paxos_propose_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .add_service("mon")
    .set_description(""),

    Option("paxos_min_wait", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .add_service("mon")
    .set_description(""),

    Option("paxos_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description(""),

    Option("paxos_trim_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .add_service("mon")
    .set_description(""),

    Option("paxos_trim_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description(""),

    Option("paxos_service_trim_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .add_service("mon")
    .set_description(""),

    Option("paxos_service_trim_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon")
    .set_description(""),

    Option("paxos_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon")
    .set_description(""),


    // AUTH

    Option("auth_cluster_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx")
    .set_description("authentication methods required by the cluster"),

    Option("auth_service_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx")
    .set_description("authentication methods required by service daemons"),

    Option("auth_client_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephx, none")
    .set_flag(Option::FLAG_MINIMAL_CONF)
    .set_description("authentication methods allowed by clients"),

    Option("auth_supported", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("authentication methods required (deprecated)"),

    Option("max_rotating_auth_attempts", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("number of attempts to initialize rotating keys before giving up"),

    Option("rotating_keys_bootstrap_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("timeout for obtaining rotating keys during bootstrap phase (seconds)"),

    Option("rotating_keys_renewal_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("timeout for updating rotating keys (seconds)"),

    Option("cephx_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Cephx version required (1 = pre-mimic, 2 = mimic+)"),

    Option("cephx_cluster_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_cluster_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Cephx version required by the cluster from clients (1 = pre-mimic, 2 = mimic+)"),

    Option("cephx_service_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("cephx_service_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Cephx version required from ceph services (1 = pre-mimic, 2 = mimic+)"),

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
    .set_default(3)
    .set_description(""),

    Option("mon_client_hunt_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3.0)
    .set_description(""),

    Option("mon_client_log_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("How frequently we send queued cluster log messages to mon"),

    Option("mon_client_ping_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description(""),

    Option("mon_client_ping_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description(""),

    Option("mon_client_hunt_interval_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5)
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

    Option("mon_client_directed_command_retry", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2)
    .set_description("Number of times to try sending a comamnd directed at a specific monitor"),

    Option("mon_max_pool_pg_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("mon_pool_quota_warn_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("percent of quota at which to issue warnings")
    .add_service("mgr"),

    Option("mon_pool_quota_crit_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("percent of quota at which to issue errors")
    .add_service("mgr"),

    Option("crush_location", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("crush_location_hook", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("crush_location_hook_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("objecter_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5.0)
    .set_description(""),

    Option("objecter_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description("Seconds before in-flight op is considered 'laggy' and we query mon for the latest OSDMap"),

    Option("objecter_inflight_op_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description("Max in-flight data in bytes (both directions)"),

    Option("objecter_inflight_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("Max in-flight operations"),

    Option("objecter_completion_locks_per_session", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(32)
    .set_description(""),

    Option("objecter_inject_no_watch_ping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("objecter_retry_writes_after_first_reply", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("objecter_debug_inject_relock_delay", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filer_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Max in-flight operations for purging a striped range (e.g., MDS journal)"),

    Option("filer_max_truncate_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description("Max in-flight operations for truncating/deleting a striped sequence (e.g., MDS journal)"),

    Option("journaler_write_head_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description("Interval in seconds between journal header updates (to help bound replay time)"),

    // * journal object size
    Option("journaler_prefetch_periods", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_min(2)			// we need at least 2 periods to make progress.
    .set_description("Number of striping periods to prefetch while reading MDS journal"),

    // * journal object size
    Option("journaler_prezero_periods", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    // we need to zero at least two periods, minimum, to ensure that we
    // have a full empty object/period in front of us.
    .set_min(2)
    .set_description("Number of striping periods to zero head of MDS journal write position"),

    // -- OSD --
    Option("osd_calc_pg_upmaps_aggressively", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("try to calculate PG upmaps more aggressively, e.g., "
                     "by doing a fairly exhaustive search of existing PGs "
                     "that can be unmapped or upmapped"),

    Option("osd_calc_pg_upmaps_local_fallback_retries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum number of PGs we can attempt to unmap or upmap "
                     "for a specific overfull or underfull osd per iteration "),

    Option("osd_numa_prefer_iface", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("prefer IP on network interface on same numa node as storage")
    .add_see_also("osd_numa_auto_affinity"),

    Option("osd_numa_auto_affinity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("automatically set affinity to numa node when storage and network match"),

    Option("osd_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("set affinity to a numa node (-1 for none)")
    .add_see_also("osd_numa_auto_affinity"),

    Option("osd_smart_report_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Timeout (in seconds) for smarctl to run, default is set to 5"),

    Option("osd_check_max_object_name_len_on_startup", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("osd_max_backfills", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Maximum number of concurrent local and remote backfills or recoveries per OSD ")
    .set_long_description("There can be osd_max_backfills local reservations AND the same remote reservations per OSD. So a value of 1 lets this OSD participate as 1 PG primary in recovery and 1 shard of another recovering PG."),

    Option("osd_min_recovery_priority", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Minimum priority below which recovery is not performed")
    .set_long_description("The purpose here is to prevent the cluster from doing *any* lower priority work (e.g., rebalancing) below this threshold and focus solely on higher priority work (e.g., replicating degraded objects)."),

    Option("osd_backfill_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description("how frequently to retry backfill reservations after being denied (e.g., due to a full OSD)"),

    Option("osd_recovery_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0)
    .set_description("how frequently to retry recovery reservations after being denied (e.g., due to a full OSD)"),

    Option("osd_agent_max_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("maximum concurrent tiering operations for tiering agent"),

    Option("osd_agent_max_low_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("maximum concurrent low-priority tiering operations for tiering agent"),

    Option("osd_agent_min_evict_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.1)
    .set_min_max(0.0, .99)
    .set_description("minimum effort to expend evicting clean objects"),

    Option("osd_agent_quantize_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.1)
    .set_description("size of quantize unit for eviction effort"),

    Option("osd_agent_delay_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description("how long agent should sleep if it has no work to do"),

    Option("osd_find_best_info_ignore_history_les", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("ignore last_epoch_started value when peering AND PROBABLY LOSE DATA")
    .set_long_description("THIS IS AN EXTREMELY DANGEROUS OPTION THAT SHOULD ONLY BE USED AT THE DIRECTION OF A DEVELOPER.  It makes peering ignore the last_epoch_started value when peering, which can allow the OSD to believe an OSD has an authoritative view of a PG's contents even when it is in fact old and stale, typically leading to data loss (by believing a stale PG is up to date)."),

    Option("osd_agent_hist_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("halflife of agent atime and temp histograms"),

    Option("osd_agent_slop", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.02)
    .set_description("slop factor to avoid switching tiering flush and eviction mode"),

    Option("osd_uuid", Option::TYPE_UUID, Option::LEVEL_ADVANCED)
    .set_default(uuid_d())
    .set_flag(Option::FLAG_CREATE)
    .set_description("uuid label for a new OSD"),

    Option("osd_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/osd/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description("path to OSD data"),

    Option("osd_journal", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/osd/$cluster-$id/journal")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description("path to OSD journal (when FileStore backend is in use)"),

    Option("osd_journal_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5120)
    .set_flag(Option::FLAG_CREATE)
    .set_description("size of FileStore journal (in MiB)"),

    Option("osd_journal_flush_on_shutdown", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("flush FileStore journal contents during clean OSD shutdown"),

    Option("osd_os_flags", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("flags to skip filestore omap or journal initialization"),

    Option("osd_max_write_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_min(4)
    .set_default(90)
    .set_description("Maximum size of a RADOS write operation in megabytes")
    .set_long_description("This setting prevents clients from doing "
        "very large writes to RADOS.  If you set this to a value "
        "below what clients expect, they will receive an error "
        "when attempting to write to the cluster."),

    Option("osd_max_pgls", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("maximum number of results when listing objects in a pool"),

    Option("osd_client_message_size_cap", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(500_M)
    .set_description("maximum memory to devote to in-flight client requests")
    .set_long_description("If this value is exceeded, the OSD will not read any new client data off of the network until memory is freed."),

    Option("osd_client_message_cap", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("maximum number of in-flight client requests"),

    Option("osd_crush_update_weight_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("update CRUSH weight-set weights when updating weights")
    .set_long_description("If this setting is true, we will update the weight-set weights when adjusting an item's weight, effectively making changes take effect immediately, and discarding any previous optimization in the weight-set value.  Setting this value to false will leave it to the balancer to (slowly, presumably) adjust weights to approach the new target value."),

    Option("osd_crush_chooseleaf_type", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .set_description("default chooseleaf type for osdmaptool --create"),

    Option("osd_pool_use_gmt_hitset", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("use UTC for hitset timestamps")
    .set_long_description("This setting only exists for compatibility with hammer (and older) clusters."),

    Option("osd_crush_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("update OSD CRUSH location on startup"),

    Option("osd_class_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("set OSD device class on startup"),

    Option("osd_crush_initial_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("if >= 0, initial CRUSH weight for newly created OSDs")
    .set_long_description("If this value is negative, the size of the OSD in TiB is used."),

    Option("osd_pool_default_ec_fast_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("set ec_fast_read for new erasure-coded pools")
    .add_service("mon"),

    Option("osd_pool_default_crush_rule", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("CRUSH rule for newly created pools")
    .add_service("mon"),

    Option("osd_pool_erasure_code_stripe_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_description("the amount of data (in bytes) in a data chunk, per stripe")
    .add_service("mon"),

    Option("osd_pool_default_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min_max(0, 10)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("the number of copies of an object for new replicated pools")
    .add_service("mon"),

    Option("osd_pool_default_min_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_min_max(0, 255)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("the minimal number of copies allowed to write to a degraded pool for new replicated pools")
    .set_long_description("0 means no specific default; ceph will use size-size/2")
    .add_see_also("osd_pool_default_size")
    .add_service("mon"),

    Option("osd_pool_default_pg_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("number of PGs for new pools")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("osd_pool_default_pgp_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("number of PGs for placement purposes (0 to match pg_num)")
    .add_see_also("osd_pool_default_pg_num")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("osd_pool_default_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("replicated")
    .set_enum_allowed({"replicated", "erasure"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("default type of pool to create")
    .add_service("mon"),

    Option("osd_pool_default_erasure_code_profile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("plugin=jerasure technique=reed_sol_van k=2 m=2")
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("default erasure code profile for new erasure-coded pools")
    .add_service("mon"),

    Option("osd_erasure_code_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("jerasure lrc"
  #if defined(HAVE_BETTER_YASM_ELF64) || defined(HAVE_ARMV8_SIMD)
         " isa"
  #endif
        )
    .set_flag(Option::FLAG_STARTUP)
    .set_description("erasure code plugins to load")
    .add_service("mon")
    .add_service("osd"),

    Option("osd_allow_recovery_below_min_size", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("allow replicated pools to recover with < min_size active members")
    .add_service("osd"),

    Option("osd_pool_default_flags", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("(integer) flags to set on new pools")
    .add_service("mon"),

    Option("osd_pool_default_flag_hashpspool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("set hashpspool (better hashing scheme) flag on new pools")
    .add_service("mon"),

    Option("osd_pool_default_flag_nodelete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("set nodelete flag on new pools")
    .add_service("mon"),

    Option("osd_pool_default_flag_nopgchange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("set nopgchange flag on new pools")
    .add_service("mon"),

    Option("osd_pool_default_flag_nosizechange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("set nosizechange flag on new pools")
    .add_service("mon"),

    Option("osd_pool_default_hit_set_bloom_fpp", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
    .set_description("")
    .add_see_also("osd_tier_default_cache_hit_set_type")
    .add_service("mon"),

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

    Option("osd_pool_default_pg_autoscale_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("on")
    .set_flag(Option::FLAG_RUNTIME)
    .set_enum_allowed({"off", "warn", "on"})
    .set_description("Default PG autoscaling behavior for new pools"),

    Option("osd_pool_default_read_lease_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.8)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default read_lease_ratio for a pool, as a multiple of osd_heartbeat_grace")
    .set_long_description("This should be <= 1.0 so that the read lease will have expired by the time we decide to mark a peer OSD down.")
    .add_see_also("osd_heartbeat_grace"),

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

    Option("osd_tier_promote_max_bytes_sec", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5_M)
    .set_description(""),

    Option("osd_tier_default_cache_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("writeback")
    .set_enum_allowed({"none", "writeback", "forward",
	               "readonly", "readforward", "readproxy", "proxy"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_period", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1200)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("bloom")
    .set_enum_allowed({"bloom", "explicit_hash", "explicit_object"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description(""),

    Option("osd_tier_default_cache_min_read_recency_for_promote", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("number of recent HitSets the object must appear in to be promoted (on read)"),

    Option("osd_tier_default_cache_min_write_recency_for_promote", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("number of recent HitSets the object must appear in to be promoted (on write)"),

    Option("osd_tier_default_cache_hit_set_grade_decay_rate", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_tier_default_cache_hit_set_search_last_n", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_objecter_finishers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_flag(Option::FLAG_STARTUP)
    .set_description(""),

    Option("osd_map_dedup", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_map_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description(""),

    Option("osd_map_message_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description("maximum number of OSDMaps to include in a single message"),

    Option("osd_map_message_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(10_M)
    .set_description("maximum number of bytes worth of OSDMaps to include in a single message"),

    Option("osd_map_share_max_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40)
    .set_description(""),

    Option("osd_pg_epoch_max_lag_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.0)
    .set_description("Max multiple of the map cache that PGs can lag before we throttle map injest")
    .add_see_also("osd_map_cache_size"),

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

    Option("osd_op_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4194304)
    .set_description(""),

    Option("osd_op_pq_min_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    Option("osd_recover_clone_overlap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_num_cache_shards", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("The number of cache shards to use in the object store."),

    Option("osd_op_num_threads_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP)
    .set_description(""),

    Option("osd_op_num_threads_per_shard_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("")
    .add_see_also("osd_op_num_threads_per_shard"),

    Option("osd_op_num_threads_per_shard_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("")
    .add_see_also("osd_op_num_threads_per_shard"),

    Option("osd_op_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP)
    .set_description(""),

    Option("osd_op_num_shards_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("")
    .add_see_also("osd_op_num_shards"),

    Option("osd_op_num_shards_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("")
    .add_see_also("osd_op_num_shards"),

    Option("osd_skip_data_digest", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Do not store full-object checksums if the backend (bluestore) does its own checksums.  Only usable with all BlueStore OSDs."),

    Option("osd_op_queue", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("wpq")
    .set_enum_allowed( { "wpq", "prioritized",
	  "mclock_opclass", "mclock_client", "mclock_scheduler",
	  "debug_random" } )
    .set_description("which operation priority queue algorithm to use")
    .set_long_description("which operation priority queue algorithm to use; "
			  "mclock_opclass mclock_client, and "
			  "mclock_client_profile are currently experimental")
    .add_see_also("osd_op_queue_cut_off"),

    Option("osd_op_queue_cut_off", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("high")
    .set_enum_allowed( { "low", "high", "debug_random" } )
    .set_description("the threshold between high priority ops and low priority ops")
    .set_long_description("the threshold between high priority ops that use strict priority ordering and low priority ops that use a fairness algorithm that may or may not incorporate priority")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_client_res", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO proportion reserved for each client (default)")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_client_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO share for each client (default) over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_client_lim", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(999999)
    .set_description("IO limit for each client (default) over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_recovery_res", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO proportion reserved for background recovery (default)")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_recovery_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO share for each background recovery over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_recovery_lim", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(999999)
    .set_description("IO limit for background recovery over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_best_effort_res", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO proportion reserved for background best_effort (default)")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_best_effort_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("IO share for each background best_effort over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_background_best_effort_lim", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(999999)
    .set_description("IO limit for background best_effort over reservation")
    .set_long_description("Only considered for osd_op_queue = mClockScheduler")
    .add_see_also("osd_op_queue"),

    Option("osd_mclock_scheduler_anticipation_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock anticipation timeout in seconds")
    .set_long_description("the amount of time that mclock waits until the unused resource is forfeited"),

    Option("osd_ignore_stale_divergent_priors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_read_ec_check_for_errors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    // Only use clone_overlap for recovery if there are fewer than
    // osd_recover_clone_overlap_limit entries in the overlap set
    Option("osd_recover_clone_overlap_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("")
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_debug_feed_pullee", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .set_description("Feed a pullee, and force primary to pull "
                     "a currently missing object from it"),

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

    Option("osd_recovery_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next recovery or backfill op"),

    Option("osd_recovery_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.1)
    .set_description("Time in seconds to sleep before next recovery or backfill op for HDDs"),

    Option("osd_recovery_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next recovery or backfill op for SSDs")
    .add_see_also("osd_recovery_sleep"),

    Option("osd_recovery_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.025)
    .set_description("Time in seconds to sleep before next recovery or backfill op when data is on HDD and journal is on SSD")
    .add_see_also("osd_recovery_sleep"),

    Option("osd_snap_trim_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next snap trim (overrides values below)"),

    Option("osd_snap_trim_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Time in seconds to sleep before next snap trim for HDDs"),

    Option("osd_snap_trim_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next snap trim for SSDs"),

    Option("osd_snap_trim_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("Time in seconds to sleep before next snap trim when data is on HDD and journal is on SSD"),

    Option("osd_scrub_invalid_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_command_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10_min)
    .set_description(""),

    Option("osd_command_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15_min)
    .set_description(""),

    Option("osd_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(6)
    .set_min_max(1, 60)
    .set_description("Interval (in seconds) between peer pings"),

    Option("osd_heartbeat_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description(""),

    Option("osd_heartbeat_stale", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description("Interval (in seconds) we mark an unresponsive heartbeat peer as stale.")
    .set_long_description("Automatically mark unresponsive heartbeat sessions as stale and tear them down. "
		          "The primary benefit is that OSD doesn't need to keep a flood of "
			  "blocked heartbeat messages around in memory."),

    Option("osd_heartbeat_min_peers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_heartbeat_use_min_delay_socket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_heartbeat_min_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(2000)
    .set_description("Minimum heartbeat packet size in bytes. Will add dummy payload if heartbeat packet is smaller than this."),

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

    Option("osd_mon_heartbeat_stat_stale", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description("Stop reporting on heartbeat ping times not updated for this many seconds.")
    .set_long_description("Stop reporting on old heartbeat information unless this is set to zero"),

    Option("osd_mon_report_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Frequency of OSD reports to mon for peer failures, fullness status changes"),

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

    Option("osd_max_snap_prune_intervals_per_epoch", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(512)
    .set_description("Max number of snap intervals to report to mgr in pg_stat_t"),

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
    .set_default(0)
    .set_description("Number of simultaneous active recovery operations per OSD (overrides _ssd and _hdd if non-zero)")
    .add_see_also("osd_recovery_max_active_hdd")
    .add_see_also("osd_recovery_max_active_ssd"),

    Option("osd_recovery_max_active_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("Number of simultaneous active recovery oeprations per OSD (for rotational devices)")
    .add_see_also("osd_recovery_max_active")
    .add_see_also("osd_recovery_max_active_ssd"),

    Option("osd_recovery_max_active_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Number of simultaneous active recovery oeprations per OSD (for non-rotational solid state devices)")
    .add_see_also("osd_recovery_max_active")
    .add_see_also("osd_recovery_max_active_hdd"),

    Option("osd_recovery_max_single_start", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description(""),

    Option("osd_recovery_max_chunk", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description(""),

    Option("osd_recovery_max_omap_entries_per_chunk", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8096)
    .set_description(""),

    Option("osd_copyfrom_max_chunk", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description(""),

    Option("osd_push_per_object_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description(""),

    Option("osd_max_push_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8<<20)
    .set_description(""),

    Option("osd_max_push_objects", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description(""),

    Option("osd_max_scrubs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Maximum concurrent scrubs on a single OSD"),

    Option("osd_scrub_during_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Allow scrubbing when PGs on the OSD are undergoing recovery"),

    Option("osd_repair_during_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Allow requested repairing when PGs on the OSD are undergoing recovery"),

    Option("osd_scrub_begin_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Restrict scrubbing to this hour of the day or later")
    .add_see_also("osd_scrub_end_hour"),

    Option("osd_scrub_end_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(24)
    .set_description("Restrict scrubbing to hours of the day earlier than this")
    .add_see_also("osd_scrub_begin_hour"),

    Option("osd_scrub_begin_week_day", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Restrict scrubbing to this day of the week or later")
    .set_long_description("0 or 7 = Sunday, 1 = Monday, etc.")
    .add_see_also("osd_scrub_end_week_day"),

    Option("osd_scrub_end_week_day", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7)
    .set_description("Restrict scrubbing to days of the week earlier than this")
    .set_long_description("0 or 7 = Sunday, 1 = Monday, etc.")
    .add_see_also("osd_scrub_begin_week_day"),

    Option("osd_scrub_load_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description("Allow scrubbing when system load divided by number of CPUs is below this value"),

    Option("osd_scrub_min_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description("Scrub each PG no more often than this interval")
    .add_see_also("osd_scrub_max_interval"),

    Option("osd_scrub_max_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(7_day)
    .set_description("Scrub each PG no less often than this interval")
    .add_see_also("osd_scrub_min_interval"),

    Option("osd_scrub_interval_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description("Ratio of scrub interval to randomly vary")
    .set_long_description("This prevents a scrub 'stampede' by randomly varying the scrub intervals so that they are soon uniformly distributed over the week")
    .add_see_also("osd_scrub_min_interval"),

    Option("osd_scrub_backoff_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.66)
    .set_long_description("This is the precentage of ticks that do NOT schedule scrubs, 66% means that 1 out of 3 ticks will schedule scrubs")
    .set_description("Backoff ratio for scheduling scrubs"),

    Option("osd_scrub_chunk_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Minimum number of objects to scrub in a single chunk")
    .add_see_also("osd_scrub_chunk_max"),

    Option("osd_scrub_chunk_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .set_description("Maximum number of objects to scrub in a single chunk")
    .add_see_also("osd_scrub_chunk_min"),

    Option("osd_scrub_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Duration to inject a delay during scrubbing"),

    Option("osd_scrub_extended_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Duration to inject a delay during scrubbing out of scrubbing hours")
    .add_see_also("osd_scrub_begin_hour")
    .add_see_also("osd_scrub_end_hour")
    .add_see_also("osd_scrub_begin_week_day")
    .add_see_also("osd_scrub_end_week_day"),

    Option("osd_scrub_auto_repair", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Automatically repair damaged objects detected during scrub"),

    Option("osd_scrub_auto_repair_num_errors", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Maximum number of detected errors to automatically repair")
    .add_see_also("osd_scrub_auto_repair"),

    Option("osd_scrub_max_preemptions", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Set the maximum number of times we will preempt a deep scrub due to a client operation before blocking client IO to complete the scrub"),

    Option("osd_deep_scrub_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(7_day)
    .set_description("Deep scrub each PG (i.e., verify data checksums) at least this often"),

    Option("osd_deep_scrub_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.15)
    .set_description("Scrubs will randomly become deep scrubs at this rate (0.15 -> 15% of scrubs are deep)")
    .set_long_description("This prevents a deep scrub 'stampede' by spreading deep scrubs so they are uniformly distributed over the week"),

    Option("osd_deep_scrub_stride", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_description("Number of bytes to read from an object at a time during deep scrub"),

    Option("osd_deep_scrub_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("Number of keys to read from an object at a time during deep scrub"),

    Option("osd_deep_scrub_update_digest_min_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2_hr)
    .set_description("Update overall object digest only if object was last modified longer ago than this"),

    Option("osd_deep_scrub_large_omap_object_key_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(200000)
    .set_description("Warn when we encounter an object with more omap keys than this")
    .add_service("osd")
    .add_see_also("osd_deep_scrub_large_omap_object_value_sum_threshold"),

    Option("osd_deep_scrub_large_omap_object_value_sum_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("Warn when we encounter an object with more omap key bytes than this")
    .add_service("osd")
    .add_see_also("osd_deep_scrub_large_omap_object_key_threshold"),

    Option("osd_class_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_LIBDIR "/rados-classes")
    .set_description(""),

    Option("osd_open_classes_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_class_load_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops " "otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap queue 2pc_queue")
    .set_description(""),

    Option("osd_class_default_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops " "otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap queue 2pc_queue")
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

    Option("osd_target_pg_log_entries_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3000 * 100)
    .set_description("target number of PG entries total on an OSD")
    .add_see_also("osd_max_pg_log_entries")
    .add_see_also("osd_min_pg_log_entries"),

    Option("osd_min_pg_log_entries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(250)
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

    Option("osd_object_clean_region_max_num_intervals", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("number of intervals in clean_offsets")
    .set_long_description("partial recovery uses multiple intervals to record the clean part of the object"
        "when the number of intervals is greater than osd_object_clean_region_max_num_intervals, minimum interval will be trimmed"
        "(0 will recovery the entire object data interval)")
    .add_service("osd"),

    Option("osd_force_recovery_pg_log_entries_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.3)
    .set_description(""),

    Option("osd_pg_log_trim_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_force_auth_primary_missing_objects", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("Approximate missing objects above which to force auth_log_shard to be primary temporarily"),

    Option("osd_async_recovery_min_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("A mixture measure of number of current log entries difference "
                     "and historical missing objects,  above which we switch to use "
                     "asynchronous recovery when appropriate"),

    Option("osd_max_pg_per_osd_hard_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min(1)
    .set_description("Maximum number of PG per OSD, a factor of 'mon_max_pg_per_osd'")
    .set_long_description("OSD will refuse to instantiate PG if the number of PG it serves exceeds this number.")
    .add_see_also("mon_max_pg_per_osd"),

    Option("osd_pg_log_trim_max", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("maximum number of entries to remove at once from the PG log")
    .add_service("osd")
    .add_see_also("osd_min_pg_log_entries")
    .add_see_also("osd_max_pg_log_entries"),

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

    Option("osd_backoff_on_unfound", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("osd_backoff_on_degraded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_backoff_on_peering", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_debug_shutdown", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Turn up debug levels during shutdown"),

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

    Option("osd_debug_verify_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
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

    Option("osd_debug_deep_scrub_sleep", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Inject an expensive sleep during deep scrub IO to make it easier to induce preemption"),

    Option("osd_debug_no_acting_change", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),
    Option("osd_debug_no_purge_strays", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_pretend_recovery_active", Option::TYPE_BOOL, Option::LEVEL_DEV)
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

    Option("osd_delete_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next removal transaction (overrides values below)"),

    Option("osd_delete_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Time in seconds to sleep before next removal transaction for HDDs"),

    Option("osd_delete_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Time in seconds to sleep before next removal transaction for SSDs"),

    Option("osd_delete_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("Time in seconds to sleep before next removal transaction when data is on HDD and journal is on SSD"),

    Option("osd_failsafe_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.97)
    .set_description(""),

    Option("osd_fast_shutdown", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Fast, immediate shutdown")
    .set_long_description("Setting this to false makes the OSD do a slower teardown of all state when it receives a SIGINT or SIGTERM or when shutting down for any other reason.  That slow shutdown is primarilyy useful for doing memory leak checking with valgrind."),

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

    Option("leveldb_write_buffer_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description(""),

    Option("leveldb_cache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_description(""),

    Option("leveldb_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
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

    Option("rocksdb_log_to_ceph_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("rocksdb_cache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_M)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description(""),

    Option("rocksdb_cache_row_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description(""),

    Option("rocksdb_cache_shard_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description(""),

    Option("rocksdb_cache_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("binned_lru")
    .set_description(""),

    Option("rocksdb_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
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

    Option("rocksdb_delete_range_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1048576)
    .set_description("The number of keys required to invoke DeleteRange when deleting muliple keys."),

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
    .set_default(false)
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

    Option("rocksdb_metadata_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K)
    .set_description("The block size for index partitions. (0 = rocksdb default)"),

    Option("mon_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("write_buffer_size=33554432,"
		 "compression=kNoCompression,"
		 "level_compaction_dynamic_level_bytes=true")
    .set_description(""),

    Option("osd_client_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(63)
    .set_description(""),

    Option("osd_recovery_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("Priority to use for recovery operations if not specified for the pool"),

    Option("osd_peering_op_priority", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(255)
    .set_description(""),

    Option("osd_snap_trim_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_snap_trim_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1<<20)
    .set_description(""),

    Option("osd_pg_delete_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description(""),

    Option("osd_pg_delete_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1<<20)
    .set_description(""),

    Option("osd_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Priority for scrub operations in work queue"),

    Option("osd_scrub_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(50<<20)
    .set_description("Cost for scrub operations in work queue"),

    Option("osd_requested_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description(""),

    Option("osd_recovery_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Priority of recovery in the work queue")
    .set_long_description("Not related to a pool's recovery_priority"),

    Option("osd_recovery_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
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

    Option("osd_max_object_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
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
    .set_default(1024)
    .set_description(""),

    Option("osd_max_omap_bytes_per_request", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description(""),

    Option("osd_max_write_op_reply_len", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Max size of the per-op payload for requests with the RETURNVEC flag set")
    .set_long_description("This value caps the amount of data (per op; a request may have many ops) that will be sent back to the client and recorded in the PG log."),

    Option("osd_objectstore", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("bluestore")
    .set_enum_allowed({"bluestore", "filestore", "memstore", "kstore"})
    .set_flag(Option::FLAG_CREATE)
    .set_description("backend type for an OSD (like filestore or bluestore)"),

    Option("osd_objectstore_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_objectstore_fuse", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("osd_bench_small_size_max_iops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description(""),

    Option("osd_bench_large_size_max_throughput", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description(""),

    Option("osd_bench_max_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M)
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

    Option("osd_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_default(4_G)
    .set_min(896_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also("bluestore_cache_autotune")
    .add_see_also("osd_memory_cache_min")
    .add_see_also("osd_memory_base")
    .set_description("When tcmalloc and cache autotuning is enabled, try to keep this many bytes mapped in memory.")
    .set_long_description("The minimum value must be at least equal to osd_memory_base + osd_memory_cache_min."),

    Option("osd_memory_target_cgroup_limit_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.8)
    .set_min_max(0.0, 1.0)
    .add_see_also("osd_memory_target")
    .set_description("Set the default value for osd_memory_target to the cgroup memory limit (if set) times this value")
    .set_long_description("A value of 0 disables this feature."),

    Option("osd_memory_base", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(768_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also("bluestore_cache_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the minimum amount of memory in bytes the OSD will need."),

    Option("osd_memory_expected_fragmentation", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.15)
    .set_min_max(0.0, 1.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also("bluestore_cache_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the percent of memory fragmentation."),

    Option("osd_memory_cache_min", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .set_min(128_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also("bluestore_cache_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, set the minimum amount of memory used for caches."),

    Option("osd_memory_cache_resize_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1)
    .add_see_also("bluestore_cache_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, wait this many seconds between resizing caches."),

    Option("memstore_device_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description(""),

    Option("memstore_page_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("memstore_page_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description(""),

    Option("memstore_debug_omit_block_device_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_see_also("bluestore_debug_omit_block_device_write")
    .set_description("write metadata only"),

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

    Option("bdev_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_description(""),

    Option("bdev_debug_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bdev_debug_aio_suicide_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(60.0)
    .set_description(""),

    Option("bdev_debug_aio_log_age", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5.0)
    .set_description(""),

    Option("bdev_nvme_unbind_from_kernel", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bdev_nvme_retry_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description(""),

    Option("bdev_enable_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bdev_async_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description("Allocation unit size for DB and WAL devices"),

    Option("bluefs_shared_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description("Allocation unit size for primary/shared device"),

    Option("bluefs_max_prefetch", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description(""),

    Option("bluefs_min_log_runway", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description(""),

    Option("bluefs_max_log_runway", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4194304)
    .set_description(""),

    Option("bluefs_log_compact_min_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description(""),

    Option("bluefs_log_compact_min_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_M)
    .set_description(""),

    Option("bluefs_min_flush_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_description(""),

    Option("bluefs_compact_log_sync", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_buffered_io", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enabled buffered IO for bluefs reads.")
    .set_long_description("When this option is enabled, bluefs will in some cases perform buffered reads.  This allows the kernel page cache to act as a secondary cache for things like RocksDB compaction.  For example, if the rocksdb block cache isn't large enough to hold blocks from the compressed SST files itself, they can be read from page cache instead of from the disk.  This option previously was enabled by default, however in some test cases it appears to cause excessive swap utilization by the linux kernel and a large negative performance impact after several hours of run time.  Please exercise caution when enabling."),

    Option("bluefs_sync_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluefs_allocator", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("hybrid")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid"})
    .set_description(""),

    Option("bluefs_log_replay_check_allocations", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
      .set_default(true)
      .set_description("Enables checks for allocations consistency during log replay"),

    Option("bluestore_bluefs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Use BlueFS to back rocksdb")
    .set_long_description("BlueFS allows rocksdb to share the same physical device(s) as the rest of BlueStore.  It should be used in all cases unless testing/developing an alternative metadata database for BlueStore."),

    Option("bluestore_bluefs_env_mirror", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Mirror bluefs data to file system for testing/validation"),

    Option("bluestore_bluefs_min", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("minimum disk space allocated to BlueFS (e.g., at mkfs)"),

    Option("bluestore_bluefs_min_free", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("minimum free space allocated to BlueFS"),

    Option("bluestore_bluefs_max_free", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(10_G)
    .set_description("Maximum free space allocated to BlueFS"),

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

    Option("bluestore_bluefs_alloc_failure_dump_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("How frequently (in seconds) to dump allocator on"
      "BlueFS space allocation failure"),

    Option("bluestore_bluefs_db_compatibility", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Sync db with legacy bluefs extents info")
    .set_long_description("Enforces db sync with legacy bluefs extents information on close."
                          " Enables downgrades to pre-nautilus releases"),

    Option("bluestore_spdk_mem", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(512)
    .set_description("Amount of dpdk memory size in MB")
    .set_long_description("If running multiple SPDK instances per node, you must specify the amount of dpdk memory size in MB each instance will use, to make sure each instance uses its own dpdk memory"),

    Option("bluestore_spdk_coremask", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("0x1")
    .set_description("A hexadecimal bit mask of the cores to run on. Note the core numbering can change between platforms and should be determined beforehand"),

    Option("bluestore_spdk_max_io_completion", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Maximal I/Os to be batched completed while checking queue pair completions, 0 means let spdk library determine it"),

    Option("bluestore_spdk_io_sleep", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(5)
    .set_description("Time period to wait if there is no completed I/O from polling"),

    Option("bluestore_block_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_flag(Option::FLAG_CREATE)
    .set_description("Path to block device/file"),

    Option("bluestore_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(100_G)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Size of file to create for backing bluestore"),

    Option("bluestore_block_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Create bluestore_block_path if it doesn't exist")
    .add_see_also("bluestore_block_path").add_see_also("bluestore_block_size"),

    Option("bluestore_block_db_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_flag(Option::FLAG_CREATE)
    .set_description("Path for db block device"),

    Option("bluestore_block_db_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Size of file to create for bluestore_block_db_path"),

    Option("bluestore_block_db_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Create bluestore_block_db_path if it doesn't exist")
    .add_see_also("bluestore_block_db_path")
    .add_see_also("bluestore_block_db_size"),

    Option("bluestore_block_wal_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_flag(Option::FLAG_CREATE)
    .set_description("Path to block device/file backing bluefs wal"),

    Option("bluestore_block_wal_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(96_M)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Size of file to create for bluestore_block_wal_path"),

    Option("bluestore_block_wal_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Create bluestore_block_wal_path if it doesn't exist")
    .add_see_also("bluestore_block_wal_path")
    .add_see_also("bluestore_block_wal_size"),

    Option("bluestore_block_preallocate_file", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Preallocate file created via bluestore_block*_create"),

    Option("bluestore_ignore_data_csum", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Ignore checksum errors on read and do not generate an EIO error"),

    Option("bluestore_csum_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("crc32c")
    .set_enum_allowed({"none", "crc32c", "crc32c_16", "crc32c_8", "xxhash32", "xxhash64"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default checksum algorithm to use")
    .set_long_description("crc32c, xxhash32, and xxhash64 are available.  The _16 and _8 variants use only a subset of the bits for more compact (but less reliable) checksumming."),

    Option("bluestore_retry_disk_reads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min_max(0, 255)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Number of read retries on checksum validation error")
    .set_long_description("Retries to read data from the disk this many times when checksum validation fails to handle spurious read errors gracefully."),

    Option("bluestore_min_alloc_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Minimum allocation size to allocate for an object")
    .set_long_description("A smaller allocation size generally means less data is read and then rewritten when a copy-on-write operation is triggered (e.g., when writing to something that was recently snapshotted).  Similarly, less data is journaled before performing an overwrite (writes smaller than min_alloc_size must first pass through the BlueStore journal).  Larger values of min_alloc_size reduce the amount of metadata required to describe the on-disk layout and reduce overall fragmentation."),

    Option("bluestore_min_alloc_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Default min_alloc_size value for rotational media")
    .add_see_also("bluestore_min_alloc_size"),

    Option("bluestore_min_alloc_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Default min_alloc_size value for non-rotational (solid state)  media")
    .add_see_also("bluestore_min_alloc_size"),

    Option("bluestore_max_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_CREATE)
    .set_description("Maximum size of a single allocation (0 for no max)"),

    Option("bluestore_prefer_deferred_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Writes smaller than this size will be written to the journal and then asynchronously written to the device.  This can be beneficial when using rotational media where seeks are expensive, and is helpful both with and without solid state journal/wal devices."),

    Option("bluestore_prefer_deferred_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_prefer_deferred_size for rotational media")
    .add_see_also("bluestore_prefer_deferred_size"),

    Option("bluestore_prefer_deferred_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_prefer_deferred_size for non-rotational (solid state) media")
    .add_see_also("bluestore_prefer_deferred_size"),

    Option("bluestore_compression_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("none")
    .set_enum_allowed({"none", "passive", "aggressive", "force"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default policy for using compression when pool does not specify")
    .set_long_description("'none' means never use compression.  'passive' means use compression when clients hint that data is compressible.  'aggressive' means use compression unless clients hint that data is not compressible.  This option is used when the per-pool property for the compression mode is not present."),

    Option("bluestore_compression_algorithm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("snappy")
    .set_enum_allowed({"", "snappy", "zlib", "zstd", "lz4"})
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default compression algorithm to use when writing object data")
    .set_long_description("This controls the default compressor to use (if any) if the per-pool property is not set.  Note that zstd is *not* recommended for bluestore due to high CPU overhead when compressing small amounts of data."),

    Option("bluestore_compression_min_blob_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum chunk size to apply compression to when random access is expected for an object.")
    .set_long_description("Chunks larger than this are broken into smaller chunks before being compressed"),

    Option("bluestore_compression_min_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default value of bluestore_compression_min_blob_size for rotational media")
    .add_see_also("bluestore_compression_min_blob_size"),

    Option("bluestore_compression_min_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default value of bluestore_compression_min_blob_size for non-rotational (solid state) media")
    .add_see_also("bluestore_compression_min_blob_size"),

    Option("bluestore_compression_max_blob_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum chunk size to apply compression to when non-random access is expected for an object.")
    .set_long_description("Chunks larger than this are broken into smaller chunks before being compressed"),

    Option("bluestore_compression_max_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default value of bluestore_compression_max_blob_size for rotational media")
    .add_see_also("bluestore_compression_max_blob_size"),

    Option("bluestore_compression_max_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default value of bluestore_compression_max_blob_size for non-rotational (solid state) media")
    .add_see_also("bluestore_compression_max_blob_size"),

    Option("bluestore_gc_enable_blob_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description(""),

    Option("bluestore_gc_enable_total_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description(""),

    Option("bluestore_max_blob_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("")
    .set_long_description("Bluestore blobs are collections of extents (ie on-disk data) originating from one or more objects.  Blobs can be compressed, typically have checksum data, may be overwritten, may be shared (with an extent ref map), or split.  This setting controls the maximum size a blob is allowed to be."),

    Option("bluestore_max_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("")
    .add_see_also("bluestore_max_blob_size"),

    Option("bluestore_max_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("")
    .add_see_also("bluestore_max_blob_size"),

    Option("bluestore_compression_required_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.875)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Compression ratio required to store compressed data")
    .set_long_description("If we compress data and get less than this we discard the result and store the original uncompressed data."),

    Option("bluestore_extent_map_shard_max_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1200)
    .set_description("Max size (bytes) for a single extent map shard before splitting"),

    Option("bluestore_extent_map_shard_target_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(500)
    .set_description("Target size (bytes) for a single extent map shard"),

    Option("bluestore_extent_map_shard_min_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(150)
    .set_description("Min size (bytes) for a single extent map shard before merging"),

    Option("bluestore_extent_map_shard_target_size_slop", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.2)
    .set_description("Ratio above/below target for a shard when trying to align to an existing extent or blob boundary"),

    Option("bluestore_extent_map_inline_shard_prealloc_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(256)
    .set_description("Preallocated buffer for inline shards"),

    Option("bluestore_cache_trim_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
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

    Option("bluestore_cache_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Cache size (in bytes) for BlueStore")
    .set_long_description("This includes data and metadata cached by BlueStore as well as memory devoted to rocksdb's cache(s)."),

    Option("bluestore_cache_size_hdd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_G)
    .set_description("Default bluestore_cache_size for rotational media")
    .add_see_also("bluestore_cache_size"),

    Option("bluestore_cache_size_ssd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(3_G)
    .set_description("Default bluestore_cache_size for non-rotational (solid state) media")
    .add_see_also("bluestore_cache_size"),

    Option("bluestore_cache_meta_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.4)
    .add_see_also("bluestore_cache_size")
    .set_description("Ratio of bluestore cache to devote to metadata"),

    Option("bluestore_cache_kv_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.4)
    .add_see_also("bluestore_cache_size")
    .set_description("Ratio of bluestore cache to devote to kv database (rocksdb)"),

    Option("bluestore_cache_autotune", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_see_also("bluestore_cache_size")
    .add_see_also("bluestore_cache_meta_ratio")
    .set_description("Automatically tune the ratio of caches while respecting min values."),

    Option("bluestore_cache_autotune_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5)
    .add_see_also("bluestore_cache_autotune")
    .set_description("The number of seconds to wait between rebalances when cache autotune is enabled."),

    Option("bluestore_alloc_stats_dump_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
      .set_default(3600 * 24)
      .set_description("The period (in second) for logging allocation statistics."),

    Option("bluestore_kvbackend", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("rocksdb")
    .set_flag(Option::FLAG_CREATE)
    .set_description("Key value database to use for bluestore"),

    Option("bluestore_allocator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("hybrid")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid", "zoned"})
    .set_description("Allocator policy")
    .set_long_description("Allocator to use for bluestore.  Stupid should only be used for testing."),

    Option("bluestore_freelist_blocks_per_key", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128)
    .set_description("Block (and bits) per database key"),

    Option("bluestore_bitmapallocator_blocks_per_zone", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description(""),

    Option("bluestore_bitmapallocator_span_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description(""),

    Option("bluestore_max_deferred_txc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Max transactions with deferred writes that can accumulate before we force flush deferred writes"),

    Option("bluestore_max_defer_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("max duration to force deferred submit"),

    Option("bluestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("compression=kNoCompression,max_write_buffer_number=4,min_write_buffer_number_to_merge=1,recycle_log_file_num=4,write_buffer_size=268435456,writable_file_max_buffer_size=0,compaction_readahead_size=2097152,max_background_compactions=2,max_total_wal_size=1073741824")
    .set_description("Rocksdb options"),

    Option("bluestore_rocksdb_cf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable use of rocksdb column families for bluestore metadata"),

    Option("bluestore_rocksdb_cfs", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("m(3) O(3,0-13) L")
    .set_description("Definition of column families and their sharding")
    .set_long_description("Space separated list of elements: column_def [ '=' rocksdb_options ]. "
			  "column_def := column_name [ '(' shard_count [ ',' hash_begin '-' [ hash_end ] ] ')' ]. "
			  "Example: 'I=write_buffer_size=1048576 O(6) m(7,10-)'. "
			  "Interval [hash_begin..hash_end) defines characters to use for hash calculation. "
			  "Recommended hash ranges: O(0-13) P(0-8) m(0-16). "
			  "Sharding of S,T,C,M,B prefixes is inadvised"),

    Option("bluestore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run fsck at mount"),

    Option("bluestore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run deep fsck at mount when bluestore_fsck_on_mount is set to true"),

    Option("bluestore_fsck_quick_fix_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
      .set_default(true)
      .set_description("Do quick-fix for the store at mount"),

    Option("bluestore_fsck_on_umount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run fsck at umount"),

    Option("bluestore_fsck_on_umount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run deep fsck at umount when bluestore_fsck_on_umount is set to true"),

    Option("bluestore_fsck_on_mkfs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description("Run fsck after mkfs"),

    Option("bluestore_fsck_on_mkfs_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Run deep fsck after mkfs"),

    Option("bluestore_sync_submit_transaction", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Try to submit metadata transaction to rocksdb in queuing thread context"),

    Option("bluestore_fsck_read_bytes_cap", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum bytes read at once by deep fsck"),

    Option("bluestore_fsck_quick_fix_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
      .set_default(2)
      .set_description("Number of additional threads to perform quick-fix (shallow fsck) command"),

    Option("bluestore_throttle_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum bytes in flight before we throttle IO submission"),

    Option("bluestore_throttle_deferred_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128_M)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Maximum bytes for deferred writes before we throttle IO submission"),

    Option("bluestore_throttle_cost_per_io", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Overhead added to transaction cost (in bytes) for each IO"),

  Option("bluestore_throttle_cost_per_io_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(670000)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_throttle_cost_per_io for rotational media")
    .add_see_also("bluestore_throttle_cost_per_io"),

    Option("bluestore_throttle_cost_per_io_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4000)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_throttle_cost_per_io for non-rotation (solid state) media")
    .add_see_also("bluestore_throttle_cost_per_io"),

    Option("bluestore_deferred_batch_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Max number of deferred writes before we flush the deferred write queue"),

    Option("bluestore_deferred_batch_ops_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_deferred_batch_ops for rotational media")
    .add_see_also("bluestore_deferred_batch_ops"),

    Option("bluestore_deferred_batch_ops_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Default bluestore_deferred_batch_ops for non-rotational (solid state) media")
    .add_see_also("bluestore_deferred_batch_ops"),

    Option("bluestore_nid_prealloc", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description("Number of unique object ids to preallocate at a time"),

    Option("bluestore_blobid_prealloc", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10240)
    .set_description("Number of unique blob ids to preallocate at a time"),

    Option("bluestore_clone_cow", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Use copy-on-write when cloning objects (versus reading and rewriting them at clone time)"),

    Option("bluestore_default_buffered_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("Cache read results by default (unless hinted NOCACHE or WONTNEED)"),

    Option("bluestore_default_buffered_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
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

    Option("bluestore_debug_too_many_blobs_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(24*1024)
    .set_description(""),

    Option("bluestore_debug_freelist", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_prefill", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("simulate fragmentation"),

    Option("bluestore_debug_prefragment_max", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_M)
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

    Option("bluestore_debug_random_read_err", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("bluestore_debug_inject_bug21040", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("bluestore_debug_inject_csum_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .set_description("inject crc verification errors into bluestore device reads"),

    Option("bluestore_fsck_error_on_no_per_pool_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Make fsck error (instead of warn) when bluestore lacks per-pool stats, e.g., after an upgrade"),

    Option("bluestore_warn_on_bluefs_spillover", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable health indication on bluefs slow device usage"),

    Option("bluestore_warn_on_legacy_statfs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable health indication on lack of per-pool statfs reporting from bluestore"),

    Option("bluestore_warn_on_spurious_read_errors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable health indication when spurious read errors are observed by OSD"),

    Option("bluestore_fsck_error_on_no_per_pool_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Make fsck error (instead of warn) when objects without per-pool omap are found"),

    Option("bluestore_warn_on_no_per_pool_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable health indication on lack of per-pool omap"),

    Option("bluestore_log_op_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("log operation if it's slower than this age (seconds)"),

    Option("bluestore_log_omap_iterator_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("log omap iteration operation if it's slower than this age (seconds)"),

    Option("bluestore_log_collection_list_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("log collection list operation if it's slower than this age (seconds)"),

    Option("bluestore_debug_enforce_settings", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("default")
    .set_enum_allowed({"default", "hdd", "ssd"})
    .set_description("Enforces specific hw profile settings")
    .set_long_description("'hdd' enforces settings intended for BlueStore above a rotational drive. 'ssd' enforces settings intended for BlueStore above a solid drive. 'default' - using settings for the actual hardware."),

    Option("bluestore_avl_alloc_bf_threshold", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(131072)
    .set_description(""),

    Option("bluestore_avl_alloc_bf_free_pct", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(4)
    .set_description(""),

    Option("bluestore_hybrid_alloc_mem_cap", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(64_M)
    .set_description("Maximum RAM hybrid allocator should use before enabling bitmap supplement"),

    Option("bluestore_volume_selection_policy", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("rocksdb_original")
    .set_enum_allowed({ "rocksdb_original", "use_some_extra" })
    .set_description("Determines bluefs volume selection policy")
    .set_long_description("Determines bluefs volume selection policy. 'use_some_extra' policy allows to override RocksDB level granularity and put high level's data to faster device even when the level doesn't completely fit there"),

    Option("bluestore_volume_selection_reserved_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
      .set_flag(Option::FLAG_STARTUP)
      .set_default(2.0)
      .set_description("DB level size multiplier. Determines amount of space at DB device to bar from the usage when 'use some extra' policy is in action. Reserved size is determined as sum(L_max_size[0], L_max_size[L-1]) + L_max_size[L] * this_factor"),

    Option("bluestore_volume_selection_reserved", Option::TYPE_INT, Option::LEVEL_ADVANCED)
      .set_flag(Option::FLAG_STARTUP)
      .set_default(0)
      .set_description("Space reserved at DB device and not allowed for 'use some extra' policy usage. Overrides 'bluestore_volume_selection_reserved_factor' setting and introduces straightforward limit."),

    Option("bluestore_ioring", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enables Linux io_uring API instead of libaio"),

    // -----------------------------------------
    // kstore

    Option("kstore_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512)
    .set_description(""),

    Option("kstore_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_description(""),

    Option("kstore_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb")
    .set_description(""),

    Option("kstore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("compression=kNoCompression")
    .set_description("Options to pass through when RocksDB is used as the KeyValueDB for kstore."),

    Option("kstore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Whether or not to run fsck on mount for kstore."),

    Option("kstore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Whether or not to run deep fsck on mount for kstore"),

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

    Option("kstore_default_stripe_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(65536)
    .set_description(""),

    // ---------------------
    // filestore

    Option("filestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("max_background_jobs=10,compaction_readahead_size=2097152,compression=kNoCompression")
    .set_description("Options to pass through when RocksDB is used as the KeyValueDB for filestore."),

    Option("filestore_omap_backend", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("rocksdb")
    .set_enum_allowed({"leveldb", "rocksdb"})
    .set_description("The KeyValueDB to use for filestore metadata (ie omap)."),

    Option("filestore_omap_backend_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("The path where the filestore KeyValueDB should store it's database(s)."),

    Option("filestore_wbthrottle_enable", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enabling throttling of operations to backing file system"),

    Option("filestore_wbthrottle_btrfs_bytes_start_flusher", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(41943040)
    .set_description("Start flushing (fsyncing) when this many bytes are written(btrfs)"),

    Option("filestore_wbthrottle_btrfs_bytes_hard_limit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(419430400)
    .set_description("Block writes when this many bytes haven't been flushed (fsynced) (btrfs)"),

    Option("filestore_wbthrottle_btrfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description("Start flushing (fsyncing) when this many IOs are written (brtrfs)"),

    Option("filestore_wbthrottle_btrfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description("Block writes when this many IOs haven't been flushed (fsynced) (btrfs)"),

    Option("filestore_wbthrottle_btrfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description("Start flushing (fsyncing) when this many distinct inodes have been modified (btrfs)"),

    Option("filestore_wbthrottle_xfs_bytes_start_flusher", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(41943040)
    .set_description("Start flushing (fsyncing) when this many bytes are written(xfs)"),

    Option("filestore_wbthrottle_xfs_bytes_hard_limit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(419430400)
    .set_description("Block writes when this many bytes haven't been flushed (fsynced) (xfs)"),

    Option("filestore_wbthrottle_xfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description("Start flushing (fsyncing) when this many IOs are written (xfs)"),

    Option("filestore_wbthrottle_xfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description("Block writes when this many IOs haven't been flushed (fsynced) (xfs)"),

    Option("filestore_wbthrottle_xfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .set_description("Start flushing (fsyncing) when this many distinct inodes have been modified (xfs)"),

    Option("filestore_wbthrottle_btrfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description("Block writing when this many inodes have outstanding writes (btrfs)"),

    Option("filestore_wbthrottle_xfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description("Block writing when this many inodes have outstanding writes (xfs)"),

    Option("filestore_odsync_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Write with O_DSYNC"),

    Option("filestore_index_retry_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
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

    Option("filestore_omap_header_cache_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1024)
    .set_description(""),

    Option("filestore_max_inline_xattr_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_xfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(65536)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_btrfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(2048)
    .set_description(""),

    Option("filestore_max_inline_xattr_size_other", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(512)
    .set_description(""),

    Option("filestore_max_inline_xattrs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_inline_xattrs_xfs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10)
    .set_description(""),

    Option("filestore_max_inline_xattrs_btrfs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10)
    .set_description(""),

    Option("filestore_max_inline_xattrs_other", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(2)
    .set_description(""),

    Option("filestore_max_xattr_value_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_max_xattr_value_size_xfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_description(""),

    Option("filestore_max_xattr_value_size_btrfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_description(""),

    Option("filestore_max_xattr_value_size_other", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_K)
    .set_description(""),

    Option("filestore_sloppy_crc", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_sloppy_crc_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(65536)
    .set_description(""),

    Option("filestore_max_alloc_hint_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1ULL << 20)
    .set_description(""),

    Option("filestore_max_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("Period between calls to syncfs(2) and journal trims (seconds)"),

    Option("filestore_min_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.01)
    .set_description("Minimum period between calls to syncfs(2)"),

    Option("filestore_btrfs_snap", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("filestore_btrfs_clone_range", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Use btrfs clone_range ioctl to efficiently duplicate objects"),

    Option("filestore_zfs_snap", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_fsync_flushes_journal_data", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_fiemap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use fiemap ioctl(2) to determine which parts of objects are sparse"),

    Option("filestore_punch_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use fallocate(2) FALLOC_FL_PUNCH_HOLE to efficiently zero ranges of objects"),

    Option("filestore_seek_data_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use lseek(2) SEEK_HOLE and SEEK_DATA to determine which parts of objects are sparse"),

    Option("filestore_splice", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use splice(2) to more efficiently copy data between files"),

    Option("filestore_fadvise", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Use posix_fadvise(2) to pass hints to file system"),

    Option("filestore_collect_device_partition_information", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Collect metadata about the backing file system on OSD startup"),

    Option("filestore_xfs_extsize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Use XFS extsize ioctl(2) to hint allocator about expected write sizes"),

    Option("filestore_journal_parallel", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_journal_writeahead", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_journal_trailing", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_queue_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description("Max IO operations in flight"),

    Option("filestore_queue_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description("Max (written) bytes in flight"),

    Option("filestore_caller_concurrency", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(10)
    .set_description(""),

    Option("filestore_expected_throughput_bytes", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(200_M)
    .set_description("Expected throughput of backend device (aids throttling calculations)"),

    Option("filestore_expected_throughput_ops", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description("Expected through of backend device in IOPS (aids throttling calculations)"),

    Option("filestore_queue_max_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_high_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_max_delay_multiple_bytes", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_high_delay_multiple_bytes", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_max_delay_multiple_ops", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_high_delay_multiple_ops", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_queue_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.3)
    .set_description(""),

    Option("filestore_queue_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.9)
    .set_description(""),

    Option("filestore_op_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("Threads used to apply changes to backing file system"),

    Option("filestore_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("Seconds before a worker thread is considered stalled"),

    Option("filestore_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(180)
    .set_description("Seconds before a worker thread is considered dead"),

    Option("filestore_commit_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description("Seconds before backing file system is considered hung"),

    Option("filestore_fiemap_threshold", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K)
    .set_description(""),

    Option("filestore_merge_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-10)
    .set_description(""),

    Option("filestore_split_multiple", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2)
    .set_description(""),

    Option("filestore_split_rand_factor", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(20)
    .set_description(""),

    Option("filestore_update_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1000)
    .set_description(""),

    Option("filestore_blackhole", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("filestore_fd_cache_size", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(128)
    .set_description(""),

    Option("filestore_fd_cache_shards", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(16)
    .set_description(""),

    Option("filestore_ondisk_finisher_threads", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("filestore_apply_finisher_threads", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("filestore_dump_file", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("filestore_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_inject_stall", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("filestore_fail_eio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("filestore_debug_verify_split", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("journal_dio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("journal_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("journal_force_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("journal_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K)
    .set_description(""),

    Option("journal_block_align", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .set_description(""),

    Option("journal_write_header_frequency", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("journal_max_write_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(10_M)
    .set_description("Max bytes in flight to journal"),

    Option("journal_max_write_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("Max IOs in flight to journal"),

    Option("journal_throttle_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.6)
    .set_description(""),

    Option("journal_throttle_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.9)
    .set_description(""),

    Option("journal_throttle_high_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("journal_throttle_max_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("journal_align_min_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_description(""),

    Option("journal_replay_from", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mgr_stats_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default((int64_t)PerfCountersBuilder::PRIO_USEFUL)
    .set_description("Lowest perfcounter priority collected by mgr")
    .set_long_description("Daemons only set perf counter data to the manager "
			  "daemon if the counter has a priority higher than this.")
    .set_min_max((int64_t)PerfCountersBuilder::PRIO_DEBUGONLY,
		 (int64_t)PerfCountersBuilder::PRIO_CRITICAL + 1),

    Option("journal_zero_on_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("journal_ignore_corruption", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("journal_discard", Option::TYPE_BOOL, Option::LEVEL_DEV)
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

    Option("cephadm_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/usr/sbin/cephadm")
    .add_service("mgr")
    .set_description("Path to cephadm utility"),

    Option("mgr_module_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(CEPH_DATADIR "/mgr")
    .add_service("mgr")
    .set_description("Filesystem path to manager modules."),

    Option("mgr_disabled_modules", Option::TYPE_STR, Option::LEVEL_ADVANCED)
#ifdef MGR_DISABLED_MODULES
    .set_default(MGR_DISABLED_MODULES)
#endif
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mgr")
    .set_description("List of manager modules never get loaded")
    .set_long_description("A comma delimited list of module names. This list "
        "is read by manager when it starts. By default, manager loads all "
        "modules found in specified 'mgr_module_path', and it starts the "
        "enabled ones as instructed. The modules in this list will not be "
        "loaded at all.")
    .add_see_also("mgr_module_path"),

    Option("mgr_initial_modules", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("restful iostat")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mon")
    .set_description("List of manager modules to enable when the cluster is "
                     "first started")
    .set_long_description("This list of module names is read by the monitor "
        "when the cluster is first started after installation, to populate "
        "the list of enabled manager modules.  Subsequent updates are done using "
        "the 'mgr module [enable|disable]' commands.  List may be comma "
        "or space separated."),

    Option("mgr_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/mgr/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mgr")
    .set_description("Filesystem path to the ceph-mgr data directory, used to "
                     "contain keyring."),

    Option("mgr_tick_period", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(2)
    .add_service("mgr")
    .set_description("Period in seconds of beacon messages to monitor"),

    Option("mgr_stats_period", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(5)
    .add_service("mgr")
    .set_description("Period in seconds of OSD/MDS stats reports to manager")
    .set_long_description("Use this setting to control the granularity of "
                          "time series data collection from daemons.  Adjust "
                          "upwards if the manager CPU load is too high, or "
                          "if you simply do not require the most up to date "
                          "performance counter data."),

    Option("mgr_client_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_client_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(512)
    .add_service("mgr"),

    Option("mgr_osd_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(512_M)
    .add_service("mgr"),

    Option("mgr_osd_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(8192)
    .add_service("mgr"),

    Option("mgr_mds_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_mds_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(128)
    .add_service("mgr"),

    Option("mgr_mon_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_mon_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(128)
    .add_service("mgr"),

    Option("mgr_connect_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .add_service("common"),

    Option("mgr_service_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .add_service("mgr")
    .set_description("Period in seconds from last beacon to manager dropping "
                     "state about a monitored service (RGW, rbd-mirror etc)"),

    Option("mgr_client_service_daemon_unregister_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .set_description("Time to wait during shutdown to deregister service with mgr"),

    Option("mgr_debug_aggressive_pg_num_changes", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Bypass most throttling and safety checks in pg[p]_num controller")
    .add_service("mgr"),

    Option("mon_mgr_digest_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .add_service("mon")
    .set_description("Period in seconds between monitor-to-manager "
                     "health/status updates"),

    Option("mon_mgr_beacon_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(30)
    .add_service("mon")
    .set_description("Period in seconds from last beacon to monitor marking "
                     "a manager daemon as failed"),

    Option("mon_mgr_inactive_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .add_service("mon")
    .set_description("Period in seconds after cluster creation during which "
                     "cluster may have no active manager")
    .set_long_description("This grace period enables the cluster to come "
                          "up cleanly without raising spurious health check "
                          "failures about managers that aren't online yet"),

    Option("mon_mgr_mkfs_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .add_service("mon")
    .set_description("Period in seconds that the cluster may have no active "
                     "manager before this is reported as an ERR rather than "
                     "a WARN"),

    Option("throttler_perf_counter", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description(""),

    Option("event_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    Option("bluestore_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable bluestore event tracing."),

    Option("bluestore_throttle_trace_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Rate at which to sample bluestore transactions (per second)"),

    Option("debug_deliberately_leak_memory", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("debug_asserts_on_shutdown", Option::TYPE_BOOL,Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Enable certain asserts to check for refcounting bugs on shutdown; see http://tracker.ceph.com/issues/21738"),

    Option("debug_asok_assert_abort", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("allow commands 'assert' and 'abort' via asok for testing crash dumps etc"),

    Option("target_max_misplaced_ratio", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_default(.05)
    .set_description("Max ratio of misplaced objects to target when throttling data rebalancing activity"),

    Option("device_failure_prediction_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("none")
    .set_flag(Option::FLAG_RUNTIME)
    .set_enum_allowed({"none", "local", "cloud"})
    .set_description("Method used to predict device failures")
    .set_long_description("To disable prediction, use 'none',  'local' uses a prediction model that runs inside the mgr daemon.  'cloud' will share metrics with a cloud service and query the service for devicelife expectancy."),

    /*  KRB Authentication. */
    Option("gss_ktab_client_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/$name/gss_client_$name.ktab")
    .set_description("GSS/KRB5 Keytab file for client authentication")
    .add_service({"mon", "osd"})
    .set_long_description("This sets the full path for the GSS/Kerberos client keytab file location."),

    Option("gss_target_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ceph")
    .set_description("")
    .add_service({"mon", "osd"})
    .set_long_description("This sets the gss target service name."),

    Option("debug_disable_randomized_ping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Disable heartbeat ping randomization for testing purposes"),

    Option("debug_heartbeat_testing_span", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Override 60 second periods for testing only"),

    Option("librados_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_min(1)
    .set_description("Size of thread pool for Objecter")
    .add_tag("client"),

    Option("osd_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_min(1)
    .set_description("Size of thread pool for ASIO completions")
    .add_tag("osd"),

    // ----------------------------
    // Crimson specific options

    Option("crimson_osd_obc_lru_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Number of obcs to cache"),

    Option("crimson_osd_scheduler_concurrency", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("The maximum number concurrent IO operations, 0 for unlimited")

  });
}

std::vector<Option> get_rgw_options() {
  return std::vector<Option>({
    Option("rgw_acl_grants_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("Max number of ACL grants in a single request"),

    Option("rgw_cors_rules_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("Max number of cors rules in a single request"),

    Option("rgw_delete_multi_obj_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Max number of objects in a single multi-object delete request"),

    Option("rgw_website_routing_rules_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description("Max number of website routing rules in a single request"),

    Option("rgw_rados_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("true if LTTng-UST tracepoints should be enabled"),

    Option("rgw_op_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("true if LTTng-UST tracepoints should be enabled"),

    Option("rgw_max_chunk_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_M)
    .set_description("Set RGW max chunk size")
    .set_long_description(
        "The chunk size is the size of RADOS I/O requests that RGW sends when accessing "
        "data objects. RGW read and write operation will never request more than this amount "
        "in a single request. This also defines the rgw object head size, as head operations "
        "need to be atomic, and anything larger than this would require more than a single "
        "operation."),

    Option("rgw_put_obj_min_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_M)
    .set_description("The minimum RADOS write window size (in bytes).")
    .set_long_description(
        "The window size determines the total concurrent RADOS writes of a single rgw object. "
        "When writing an object RGW will send multiple chunks to RADOS. The total size of the "
        "writes does not exceed the window size. The window size can be automatically "
        "in order to better utilize the pipe.")
    .add_see_also({"rgw_put_obj_max_window_size", "rgw_max_chunk_size"}),

    Option("rgw_put_obj_max_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M)
    .set_description("The maximum RADOS write window size (in bytes).")
    .set_long_description("The window size may be dynamically adjusted, but will not surpass this value.")
    .add_see_also({"rgw_put_obj_min_window_size", "rgw_max_chunk_size"}),

    Option("rgw_max_put_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5_G)
    .set_description("Max size (in bytes) of regular (non multi-part) object upload.")
    .set_long_description(
        "Plain object upload is capped at this amount of data. In order to upload larger "
        "objects, a special upload mechanism is required. The S3 API provides the "
        "multi-part upload, and Swift provides DLO and SLO."),

    Option("rgw_max_put_param_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description("The maximum size (in bytes) of data input of certain RESTful requests."),

    Option("rgw_max_attr_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("The maximum length of metadata value. 0 skips the check"),

    Option("rgw_max_attr_name_len", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("The maximum length of metadata name. 0 skips the check"),

    Option("rgw_max_attrs_num_in_req", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("The maximum number of metadata items that can be put via single request"),

    Option("rgw_override_bucket_index_max_shards", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("The default number of bucket index shards for newly-created "
        "buckets. This value overrides bucket_index_max_shards stored in the zone. "
        "Setting this value in the zone is preferred, because it applies globally "
        "to all radosgw daemons running in the zone."),

    Option("rgw_bucket_index_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description("Max number of concurrent RADOS requests when handling bucket shards."),

    Option("rgw_enable_quota_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enables the quota maintenance thread.")
    .set_long_description(
        "The quota maintenance thread is responsible for quota related maintenance work. "
        "The thread itself can be disabled, but in order for quota to work correctly, at "
        "least one RGW in each zone needs to have this thread running. Having the thread "
        "enabled on multiple RGW processes within the same zone can spread "
        "some of the maintenance work between them.")
    .add_see_also({"rgw_enable_gc_threads", "rgw_enable_lc_threads"}),

    Option("rgw_enable_gc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enables the garbage collection maintenance thread.")
    .set_long_description(
        "The garbage collection maintenance thread is responsible for garbage collector "
        "maintenance work. The thread itself can be disabled, but in order for garbage "
        "collection to work correctly, at least one RGW in each zone needs to have this "
        "thread running.  Having the thread enabled on multiple RGW processes within the "
        "same zone can spread some of the maintenance work between them.")
    .add_see_also({"rgw_enable_quota_threads", "rgw_enable_lc_threads"}),

    Option("rgw_enable_lc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enables the lifecycle maintenance thread. This is required on at least one rgw for each zone.")
    .set_long_description(
        "The lifecycle maintenance thread is responsible for lifecycle related maintenance "
        "work. The thread itself can be disabled, but in order for lifecycle to work "
        "correctly, at least one RGW in each zone needs to have this thread running. Having"
        "the thread enabled on multiple RGW processes within the same zone can spread "
        "some of the maintenance work between them.")
    .add_see_also({"rgw_enable_gc_threads", "rgw_enable_quota_threads"}),

    Option("rgw_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/radosgw/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description("Alternative location for RGW configuration.")
    .set_long_description(
        "If this is set, the different Ceph system configurables (such as the keyring file "
        "will be located in the path that is specified here. "),

    Option("rgw_enable_apis", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("s3, s3website, swift, swift_auth, admin, sts, iam, pubsub")
    .set_description("A list of set of RESTful APIs that rgw handles."),

    Option("rgw_cache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable RGW metadata cache.")
    .set_long_description(
        "The metadata cache holds metadata entries that RGW requires for processing "
        "requests. Metadata entries can be user info, bucket info, and bucket instance "
        "info. If not found in the cache, entries will be fetched from the backing "
        "RADOS store.")
    .add_see_also("rgw_cache_lru_size"),

    Option("rgw_cache_lru_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("Max number of items in RGW metadata cache.")
    .set_long_description(
        "When full, the RGW metadata cache evicts least recently used entries.")
    .add_see_also("rgw_cache_enabled"),

    Option("rgw_socket_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("RGW FastCGI socket path (for FastCGI over Unix domain sockets).")
    .add_see_also("rgw_fcgi_socket_backlog"),

    Option("rgw_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("RGW FastCGI host name (for FastCGI over TCP)")
    .add_see_also({"rgw_port", "rgw_fcgi_socket_backlog"}),

    Option("rgw_port", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_description("RGW FastCGI port number (for FastCGI over TCP)")
    .add_see_also({"rgw_host", "rgw_fcgi_socket_backlog"}),

    Option("rgw_dns_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("The host name that RGW uses.")
    .set_long_description(
        "This is Needed for virtual hosting of buckets to work properly, unless configured "
        "via zonegroup configuration."),

    Option("rgw_dns_s3website_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("The host name that RGW uses for static websites (S3)")
    .set_long_description(
        "This is needed for virtual hosting of buckets, unless configured via zonegroup "
        "configuration."),
    
    Option("rgw_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("set rgw's cpu affinity to a numa node (-1 for none)"),

    Option("rgw_service_provider_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Service provider name which is contained in http response headers")
    .set_long_description(
        "As S3 or other cloud storage providers do, http response headers should contain the name of the provider. "
        "This name will be placed in http header 'Server'."),

    Option("rgw_content_length_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Multiple content length headers compatibility")
    .set_long_description(
        "Try to handle requests with abiguous multiple content length headers "
        "(Content-Length, Http-Content-Length)."),

    Option("rgw_relaxed_region_enforcement", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Disable region constraint enforcement")
    .set_long_description(
        "Enable requests such as bucket creation to succeed irrespective of region restrictions (Jewel compat)."),

    Option("rgw_lifecycle_work_time", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("00:00-06:00")
    .set_description("Lifecycle allowed work time")
    .set_long_description("Local time window in which the lifecycle maintenance thread can work."),

    Option("rgw_lc_lock_max_time", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(90)
    .set_description(""),

    Option("rgw_lc_thread_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("Delay after processing of bucket listing chunks (i.e., per 1000 entries) in milliseconds"),

    Option("rgw_lc_max_worker", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("Number of LCWorker tasks that will be run in parallel")
    .set_long_description(
      "Number of LCWorker tasks that will run in parallel--used to permit >1 "
      "bucket/index shards to be processed simultaneously"),

    Option("rgw_lc_max_wp_worker", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("Number of workpool threads per LCWorker")
    .set_long_description(
      "Number of threads in per-LCWorker workpools--used to accelerate "
      "per-bucket processing"),

    Option("rgw_lc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Number of lifecycle data shards")
    .set_long_description(
          "Number of RADOS objects to use for storing lifecycle index. This "
	  "affects concurrency of lifecycle maintenance, as shards can be "
          "processed in parallel."),

    Option("rgw_lc_max_rules", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Max number of lifecycle rules set on one bucket")
    .set_long_description("Number of lifecycle rules set on one bucket should be limited."),

    Option("rgw_lc_debug_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .set_description(""),

    Option("rgw_mp_lock_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description("Multipart upload max completion time")
    .set_long_description(
        "Time length to allow completion of a multipart upload operation. This is done "
        "to prevent concurrent completions on the same object with the same upload id."),

    Option("rgw_script_uri", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("rgw_request_uri", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("rgw_ignore_get_invalid_range", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Treat invalid (e.g., negative) range request as full")
    .set_long_description("Treat invalid (e.g., negative) range request "
			  "as request for the full object (AWS compatibility)"),

    Option("rgw_swift_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Swift-auth storage URL")
    .set_long_description(
        "Used in conjunction with rgw internal swift authentication. This affects the "
        "X-Storage-Url response header value.")
    .add_see_also("rgw_swift_auth_entry"),

    Option("rgw_swift_url_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("swift")
    .set_description("Swift URL prefix")
    .set_long_description("The URL path prefix for swift requests."),

    Option("rgw_swift_auth_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Swift auth URL")
    .set_long_description(
        "Default url to which RGW connects and verifies tokens for v1 auth (if not using "
        "internal swift auth)."),

    Option("rgw_swift_auth_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("auth")
    .set_description("Swift auth URL prefix")
    .set_long_description("URL path prefix for internal swift auth requests.")
    .add_see_also("rgw_swift_url"),

    Option("rgw_swift_tenant_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Swift tenant name")
    .set_long_description("Tenant name that is used when constructing the swift path.")
    .add_see_also("rgw_swift_account_in_url"),

    Option("rgw_swift_account_in_url", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Swift account encoded in URL")
    .set_long_description("Whether the swift account is encoded in the uri path (AUTH_<account>).")
    .add_see_also("rgw_swift_tenant_name"),

    Option("rgw_swift_enforce_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Send content length when listing containers (Swift)")
    .set_long_description(
        "Whether content length header is needed when listing containers. When this is "
        "set to false, RGW will send extra info for each entry in the response."),

    Option("rgw_keystone_url", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_description("The URL to the Keystone server."),

    Option("rgw_keystone_admin_token", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("DEPRECATED: The admin token (shared secret) that is used for the Keystone requests."),

    Option("rgw_keystone_admin_token_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Path to a file containing the admin token (shared secret) that is used for the Keystone requests."),

    Option("rgw_keystone_admin_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone admin user."),

    Option("rgw_keystone_admin_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("DEPRECATED: Keystone admin password."),

    Option("rgw_keystone_admin_password_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Path to a file containing the Keystone admin password."),

    Option("rgw_keystone_admin_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone admin user tenant."),

    Option("rgw_keystone_admin_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone admin user project (for Keystone v3)."),

    Option("rgw_keystone_admin_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone admin user domain (for Keystone v3)."),

    Option("rgw_keystone_barbican_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone user to access barbican secrets."),

    Option("rgw_keystone_barbican_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone password for barbican user."),

    Option("rgw_keystone_barbican_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone barbican user tenant (Keystone v2.0)."),

    Option("rgw_keystone_barbican_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone barbican user project (Keystone v3)."),

    Option("rgw_keystone_barbican_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Keystone barbican user domain."),

    Option("rgw_keystone_api_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("Version of Keystone API to use (2 or 3)."),

    Option("rgw_keystone_accepted_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("Member, admin")
    .set_description("Only users with one of these roles will be served when doing Keystone authentication."),

    Option("rgw_keystone_accepted_admin_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("List of roles allowing user to gain admin privileges (Keystone)."),

    Option("rgw_keystone_token_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("Keystone token cache size")
    .set_long_description(
        "Max number of Keystone tokens that will be cached. Token that is not cached "
        "requires RGW to access the Keystone server when authenticating."),

    Option("rgw_keystone_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Should RGW verify the Keystone server SSL certificate."),

    Option("rgw_keystone_implicit_tenants", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("false")
    .set_enum_allowed( { "false", "true", "swift", "s3", "both", "0", "1", "none" } )
    .set_description("RGW Keystone implicit tenants creation")
    .set_long_description(
        "Implicitly create new users in their own tenant with the same name when "
        "authenticating via Keystone.  Can be limited to s3 or swift only."),

    Option("rgw_cross_domain_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("<allow-access-from domain=\"*\" secure=\"false\" />")
    .set_description("RGW handle cross domain policy")
    .set_long_description("Returned cross domain policy when accessing the crossdomain.xml "
                          "resource (Swift compatiility)."),

    Option("rgw_healthcheck_disabling_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("Swift health check api can be disabled if a file can be accessed in this path."),

    Option("rgw_s3_auth_use_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Should S3 authentication use credentials stored in RADOS backend."),

    Option("rgw_s3_auth_use_keystone", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should S3 authentication use Keystone."),

    Option("rgw_s3_auth_order", Option::TYPE_STR, Option::LEVEL_ADVANCED)
     .set_default("sts, external, local")
     .set_description("Authentication strategy order to use for s3 authentication")
     .set_long_description(
	  "Order of authentication strategies to try for s3 authentication, the allowed "
	   "options are a comma separated list of engines external, local. The "
	   "default order is to try all the externally configured engines before "
	   "attempting local rados based authentication"),

    Option("rgw_barbican_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("URL to barbican server."),

    Option("rgw_ldap_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ldaps://<ldap.your.domain>")
    .set_description("Space-separated list of LDAP servers in URI format."),

    Option("rgw_ldap_binddn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("uid=admin,cn=users,dc=example,dc=com")
    .set_description("LDAP entry RGW will bind with (user match)."),

    Option("rgw_ldap_searchdn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cn=users,cn=accounts,dc=example,dc=com")
    .set_description("LDAP search base (basedn)."),

    Option("rgw_ldap_dnattr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("uid")
    .set_description("LDAP attribute containing RGW user names (to form binddns)."),

    Option("rgw_ldap_secret", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/etc/openldap/secret")
    .set_description("Path to file containing credentials for rgw_ldap_binddn."),

    Option("rgw_s3_auth_use_ldap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should S3 authentication use LDAP."),

    Option("rgw_ldap_searchfilter", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("LDAP search filter."),

    Option("rgw_opa_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("URL to OPA server."),

    Option("rgw_opa_token", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("The Bearer token OPA uses to authenticate client requests."),

    Option("rgw_opa_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Should RGW verify the OPA server SSL certificate."),

    Option("rgw_use_opa_authz", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should OPA be used to authorize client requests."),

    Option("rgw_admin_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("admin")
    .set_description("Path prefix to be used for accessing RGW RESTful admin API."),

    Option("rgw_enforce_swift_acls", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("RGW enforce swift acls")
    .set_long_description(
        "Should RGW enforce special Swift-only ACLs. Swift has a special ACL that gives "
        "permission to access all objects in a container."),

    Option("rgw_swift_token_expiration", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description("Expiration time (in seconds) for token generated through RGW Swift auth."),

    Option("rgw_print_continue", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("RGW support of 100-continue")
    .set_long_description(
        "Should RGW explicitly send 100 (continue) responses. This is mainly relevant when "
        "using FastCGI, as some FastCGI modules do not fully support this feature."),

    Option("rgw_print_prohibited_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("RGW RFC-7230 compatibility")
    .set_long_description(
        "Specifies whether RGW violates RFC 7230 and sends Content-Length with 204 or 304 "
        "statuses."),

    Option("rgw_remote_addr_param", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("REMOTE_ADDR")
    .set_description("HTTP header that holds the remote address in incoming requests.")
    .set_long_description(
        "RGW will use this header to extract requests origin. When RGW runs behind "
        "a reverse proxy, the remote address header will point at the proxy's address "
        "and not at the originator's address. Therefore it is sometimes possible to "
        "have the proxy add the originator's address in a separate HTTP header, which "
        "will allow RGW to log it correctly."
        )
    .add_see_also("rgw_enable_ops_log"),

    Option("rgw_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(10*60)
    .set_description("Timeout for async rados coroutine operations."),

    Option("rgw_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("rgw_thread_pool_size", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(512)
    .set_description("RGW requests handling thread pool size.")
    .set_long_description(
        "This parameter determines the number of concurrent requests RGW can process "
        "when using either the civetweb, or the fastcgi frontends. The higher this "
        "number is, RGW will be able to deal with more concurrent requests at the "
        "cost of more resource utilization."),

    Option("rgw_num_control_oids", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_description("Number of control objects used for cross-RGW communication.")
    .set_long_description(
        "RGW uses certain control objects to send messages between different RGW "
        "processes running on the same zone. These messages include metadata cache "
        "invalidation info that is being sent when metadata is modified (such as "
        "user or bucket information). A higher number of control objects allows "
        "better concurrency of these messages, at the cost of more resource "
        "utilization."),

    Option("rgw_num_rados_handles", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("Number of librados handles that RGW uses.")
    .set_long_description(
        "This param affects the number of separate librados handles it uses to "
        "connect to the RADOS backend, which directly affects the number of connections "
        "RGW will have to each OSD. A higher number affects resource utilization."),

    Option("rgw_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Should RGW verify SSL when connecing to a remote HTTP server")
    .set_long_description(
        "RGW can send requests to other RGW servers (e.g., in multi-site sync work). "
        "This configurable selects whether RGW should verify the certificate for "
        "the remote peer and host.")
    .add_see_also("rgw_keystone_verify_ssl"),

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

    Option("rgw_nfs_s3_fast_attrs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("use fast S3 attrs from bucket index (immutable only)")
    .set_long_description("use fast S3 attrs from bucket index (assumes NFS "
			  "mounts are immutable)"),

    Option("rgw_nfs_run_gc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("run GC threads in librgw (default off)"),

    Option("rgw_nfs_run_lc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("run lifecycle threads in librgw (default off)"),

    Option("rgw_nfs_run_quota_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("run quota threads in librgw (default off)"),

    Option("rgw_nfs_run_sync_thread", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("run sync thread in librgw (default off)"),

    Option("rgw_rados_pool_autoscale_bias", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(4.0)
    .set_min_max(0.01, 100000.0)
    .set_description("pg_autoscale_bias value for RGW metadata (omap-heavy) pools"),

    Option("rgw_rados_pool_pg_num_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_min_max(1, 1024)
    .set_description("pg_num_min value for RGW metadata (omap-heavy) pools"),

    Option("rgw_rados_pool_recovery_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_min_max(-10, 10)
    .set_description("recovery_priority value for RGW metadata (omap-heavy) pools"),

    Option("rgw_zone", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Zone name")
    .add_see_also({"rgw_zonegroup", "rgw_realm"}),

    Option("rgw_zone_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description("Zone root pool name")
    .set_long_description(
        "The zone root pool, is the pool where the RGW zone configuration located."
    )
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_realm_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_zone_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.zone")
    .set_description("Default zone info object id")
    .set_long_description(
        "Name of the RADOS object that holds the default zone information."
    ),

    Option("rgw_region", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Region name")
    .set_long_description(
        "Obsolete config option. The rgw_zonegroup option should be used instead.")
    .add_see_also("rgw_zonegroup"),

    Option("rgw_region_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description("Region root pool")
    .set_long_description(
        "Obsolete config option. The rgw_zonegroup_root_pool should be used instead.")
    .add_see_also("rgw_zonegroup_root_pool"),

    Option("rgw_default_region_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.region")
    .set_description("Default region info object id")
    .set_long_description(
        "Obsolete config option. The rgw_default_zonegroup_info_oid should be used instead.")
    .add_see_also("rgw_default_zonegroup_info_oid"),

    Option("rgw_zonegroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Zonegroup name")
    .add_see_also({"rgw_zone", "rgw_realm"}),

    Option("rgw_zonegroup_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description("Zonegroup root pool")
    .set_long_description(
        "The zonegroup root pool, is the pool where the RGW zonegroup configuration located."
    )
    .add_see_also({"rgw_zone_root_pool", "rgw_realm_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_zonegroup_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.zonegroup")
    .set_description(""),

    Option("rgw_realm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(""),

    Option("rgw_realm_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description("Realm root pool")
    .set_long_description(
        "The realm root pool, is the pool where the RGW realm configuration located."
    )
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_zone_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_realm_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.realm")
    .set_description(""),

    Option("rgw_period_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".rgw.root")
    .set_description("Period root pool")
    .set_long_description(
        "The period root pool, is the pool where the RGW period configuration located."
    )
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_zone_root_pool", "rgw_realm_root_pool"}),

    Option("rgw_period_latest_epoch_info_oid", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default(".latest_epoch")
    .set_description(""),

    Option("rgw_log_nonexistent_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should RGW log operations on bucket that does not exist")
    .set_long_description(
        "This config option applies to the ops log. When this option is set, the ops log "
        "will log operations that are sent to non existing buckets. These operations "
        "inherently fail, and do not correspond to a specific user.")
    .add_see_also("rgw_enable_ops_log"),

    Option("rgw_log_object_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("%Y-%m-%d-%H-%i-%n")
    .set_description("Ops log object name format")
    .set_long_description(
        "Defines the format of the RADOS objects names that ops log uses to store ops "
        "log data")
    .add_see_also("rgw_enable_ops_log"),

    Option("rgw_log_object_name_utc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should ops log object name based on UTC")
    .set_long_description(
        "If set, the names of the RADOS objects that hold the ops log data will be based "
        "on UTC time zone. If not set, it will use the local time zone.")
    .add_see_also({"rgw_enable_ops_log", "rgw_log_object_name"}),

    Option("rgw_usage_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Number of shards for usage log.")
    .set_long_description(
        "The number of RADOS objects that RGW will use in order to store the usage log "
        "data.")
    .add_see_also("rgw_enable_usage_log"),

    Option("rgw_usage_max_user_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("Number of shards for single user in usage log")
    .set_long_description(
        "The number of shards that a single user will span over in the usage log.")
    .add_see_also("rgw_enable_usage_log"),

    Option("rgw_enable_ops_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable ops log")
    .add_see_also({"rgw_log_nonexistent_bucket", "rgw_log_object_name", "rgw_ops_log_rados",
               "rgw_ops_log_socket_path"}),

    Option("rgw_enable_usage_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable usage log")
    .add_see_also("rgw_usage_max_shards"),

    Option("rgw_ops_log_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Use RADOS for ops log")
    .set_long_description(
       "If set, RGW will store ops log information in RADOS.")
    .add_see_also({"rgw_enable_ops_log"}),

    Option("rgw_ops_log_socket_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Unix domain socket path for ops log.")
    .set_long_description(
        "Path to unix domain socket that RGW will listen for connection on. When connected, "
        "RGW will send ops log data through it.")
    .add_see_also({"rgw_enable_ops_log", "rgw_ops_log_data_backlog"}),

    Option("rgw_ops_log_data_backlog", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5 << 20)
    .set_description("Ops log socket backlog")
    .set_long_description(
        "Maximum amount of data backlog that RGW can keep when ops log is configured to "
        "send info through unix domain socket. When data backlog is higher than this, "
        "ops log entries will be lost. In order to avoid ops log information loss, the "
        "listener needs to clear data (by reading it) quickly enough.")
    .add_see_also({"rgw_enable_ops_log", "rgw_ops_log_socket_path"}),

    Option("rgw_fcgi_socket_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("FastCGI socket connection backlog")
    .set_long_description(
        "Size of FastCGI connection backlog. This reflects the maximum number of new "
        "connection requests that RGW can handle concurrently without dropping any. ")
    .add_see_also({"rgw_host", "rgw_socket_path"}),

    Option("rgw_usage_log_flush_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("Number of entries in usage log before flushing")
    .set_long_description(
        "This is the max number of entries that will be held in the usage log, before it "
        "will be flushed to the backend. Note that the usage log is periodically flushed, "
        "even if number of entries does not reach this threshold. A usage log entry "
        "corresponds to one or more operations on a single bucket.i")
    .add_see_also({"rgw_enable_usage_log", "rgw_usage_log_tick_interval"}),

    Option("rgw_usage_log_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("Number of seconds between usage log flush cycles")
    .set_long_description(
        "The number of seconds between consecutive usage log flushes. The usage log will "
        "also flush itself to the backend if the number of pending entries reaches a "
        "certain threshold.")
    .add_see_also({"rgw_enable_usage_log", "rgw_usage_log_flush_threshold"}),

    Option("rgw_init_timeout", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(300)
    .set_description("Initialization timeout")
    .set_long_description(
        "The time length (in seconds) that RGW will allow for its initialization. RGW "
        "process will give up and quit if initialization is not complete after this amount "
        "of time."),

    Option("rgw_mime_types_file", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("/etc/mime.types")
    .set_description("Path to local mime types file")
    .set_long_description(
        "The mime types file is needed in Swift when uploading an object. If object's "
        "content type is not specified, RGW will use data from this file to assign "
        "a content type to the object."),

    Option("rgw_gc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Number of shards for garbage collector data")
    .set_long_description(
        "The number of garbage collector data shards, is the number of RADOS objects that "
        "RGW will use to store the garbage collection information on.")
    .add_see_also({"rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_obj_min_wait", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2_hr)
    .set_description("Garbage collection object expiration time")
    .set_long_description(
       "The length of time (in seconds) that the RGW collector will wait before purging "
       "a deleted object's data. RGW will not remove object immediately, as object could "
       "still have readers. A mechanism exists to increase the object's expiration time "
       "when it's being read. The recommended value of its lower limit is 30 minutes")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_processor_max_time", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_processor_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description("Length of time GC processor can lease shard")
    .set_long_description(
        "Garbage collection thread in RGW process holds a lease on its data shards. These "
        "objects contain the information about the objects that need to be removed. RGW "
        "takes a lease in order to prevent multiple RGW processes from handling the same "
        "objects concurrently. This time signifies that maximum amount of time (in seconds) that RGW "
        "is allowed to hold that lease. In the case where RGW goes down uncleanly, this "
        "is the amount of time where processing of that data shard will be blocked.")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_processor_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_hr)
    .set_description("Garbage collector cycle run time")
    .set_long_description(
        "The amount of time between the start of consecutive runs of the garbage collector "
        "threads. If garbage collector runs takes more than this period, it will not wait "
        "before running again.")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_concurrent_io", "rgw_gc_max_trim_chunk"}),

    Option("rgw_gc_max_concurrent_io", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Max concurrent RADOS IO operations for garbage collection")
    .set_long_description(
        "The maximum number of concurrent IO operations that the RGW garbage collection "
        "thread will use when purging old data.")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_trim_chunk"}),

    Option("rgw_gc_max_trim_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description("Max number of keys to remove from garbage collector log in a single operation")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_max_deferred_entries_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3072)
    .set_description("maximum allowed size of deferred entries in queue head for gc"),

    Option("rgw_gc_max_queue_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(134213632)
    .set_description("Maximum allowed queue size for gc")
    .set_long_description(
        "The maximum allowed size of each gc queue, and its value should not "
        "be greater than (osd_max_object_size - rgw_gc_max_deferred_entries_size - 1K).")
    .add_see_also({"osd_max_object_size", "rgw_gc_max_deferred_entries_size"}),

    Option("rgw_gc_max_deferred", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description("Number of maximum deferred data entries to be stored in queue for gc"),

    Option("rgw_s3_success_create_obj_status", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("HTTP return code override for object creation")
    .set_long_description(
        "If not zero, this is the HTTP return code that will be returned on a successful S3 "
        "object creation."),

    Option("rgw_resolve_cname", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Support vanity domain names via CNAME")
    .set_long_description(
        "If true, RGW will query DNS when detecting that it's serving a request that was "
        "sent to a host in another domain. If a CNAME record is configured for that domain "
        "it will use it instead. This gives user to have the ability of creating a unique "
        "domain of their own to point at data in their bucket."),

    Option("rgw_obj_stripe_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_M)
    .set_description("RGW object stripe size")
    .set_long_description(
        "The size of an object stripe for RGW objects. This is the maximum size a backing "
        "RADOS object will have. RGW objects that are larger than this will span over "
        "multiple objects."),

    Option("rgw_extended_http_attrs", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("RGW support extended HTTP attrs")
    .set_long_description(
        "Add new set of attributes that could be set on an object. These extra attributes "
        "can be set through HTTP header fields when putting the objects. If set, these "
        "attributes will return as HTTP fields when doing GET/HEAD on the object."),

    Option("rgw_exit_timeout_secs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(120)
    .set_description("RGW shutdown timeout")
    .set_long_description("Number of seconds to wait for a process before exiting unconditionally."),

    Option("rgw_get_obj_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_M)
    .set_description("RGW object read window size")
    .set_long_description("The window size in bytes for a single object read request"),

    Option("rgw_get_obj_max_req_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_M)
    .set_description("RGW object read chunk size")
    .set_long_description(
        "The maximum request size of a single object read operation sent to RADOS"),

    Option("rgw_relaxed_s3_bucket_names", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("RGW enable relaxed S3 bucket names")
    .set_long_description("RGW enable relaxed S3 bucket name rules for US region buckets."),

    Option("rgw_defer_to_bucket_acls", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Bucket ACLs override object ACLs")
    .set_long_description(
        "If not empty, a string that selects that mode of operation. 'recurse' will use "
        "bucket's ACL for the authorizaton. 'full-control' will allow users that users "
        "that have full control permission on the bucket have access to the object."),

    Option("rgw_list_buckets_max_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Max number of buckets to retrieve in a single listing operation")
    .set_long_description(
        "When RGW fetches lists of user's buckets from the backend, this is the max number "
        "of entries it will try to retrieve in a single operation. Note that the backend "
        "may choose to return a smaller number of entries."),

    Option("rgw_md_log_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description("RGW number of metadata log shards")
    .set_long_description(
        "The number of shards the RGW metadata log entries will reside in. This affects "
        "the metadata sync parallelism as a shard can only be processed by a single "
        "RGW at a time"),

    Option("rgw_curl_wait_timeout_ms", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1000)
    .set_description(""),

    Option("rgw_curl_low_speed_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_long_description(
        "It contains the average transfer speed in bytes per second that the "
        "transfer should be below during rgw_curl_low_speed_time seconds for libcurl "
        "to consider it to be too slow and abort. Set it zero to disable this."),

    Option("rgw_curl_low_speed_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_long_description(
        "It contains the time in number seconds that the transfer speed should be below "
        "the rgw_curl_low_speed_limit for the library to consider it too slow and abort. "
        "Set it zero to disable this."),

    Option("rgw_copy_obj_progress", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Send progress report through copy operation")
    .set_long_description(
        "If true, RGW will send progress information when copy operation is executed. "),

    Option("rgw_copy_obj_progress_every_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description("Send copy-object progress info after these many bytes"),

    Option("rgw_sync_obj_etag_verify", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Verify if the object copied from remote is identical to its source")
    .set_long_description(
        "If true, this option computes the MD5 checksum of the data which is written at the "
	"destination and checks if it is identical to the ETAG stored in the source. "
        "It ensures integrity of the objects fetched from a remote server over HTTP including "
        "multisite sync."),

    Option("rgw_obj_tombstone_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Max number of entries to keep in tombstone cache")
    .set_long_description(
        "The tombstone cache is used when doing a multi-zone data sync. RGW keeps "
        "there information about removed objects which is needed in order to prevent "
        "re-syncing of objects that were already removed."),

    Option("rgw_data_log_window", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("Data log time window")
    .set_long_description(
        "The data log keeps information about buckets that have objectst that were "
        "modified within a specific timeframe. The sync process then knows which buckets "
        "are needed to be scanned for data sync."),

    Option("rgw_data_log_changes_size", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1000)
    .set_description("Max size of pending changes in data log")
    .set_long_description(
        "RGW will trigger update to the data log if the number of pending entries reached "
        "this number."),

    Option("rgw_data_log_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description("Number of data log shards")
    .set_long_description(
        "The number of shards the RGW data log entries will reside in. This affects the "
        "data sync parallelism as a shard can only be processed by a single RGW at a time."),

    Option("rgw_data_log_obj_prefix", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("data_log")
    .set_description(""),

    Option("rgw_bucket_quota_ttl", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description("Bucket quota stats cache TTL")
    .set_long_description(
        "Length of time for bucket stats to be cached within RGW instance."),

    Option("rgw_bucket_quota_soft_threshold", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_default(0.95)
    .set_description("RGW quota soft threshold")
    .set_long_description(
        "Threshold from which RGW doesn't rely on cached info for quota "
        "decisions. This is done for higher accuracy of the quota mechanism at "
        "cost of performance, when getting close to the quota limit. The value "
        "configured here is the ratio between the data usage to the max usage "
        "as specified by the quota."),

    Option("rgw_bucket_quota_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("RGW quota stats cache size")
    .set_long_description(
        "Maximum number of entries in the quota stats cache."),

    Option("rgw_bucket_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(-1)
    .set_description("Default quota for max objects in a bucket")
    .set_long_description(
        "The default quota configuration for max number of objects in a bucket. A "
        "negative number means 'unlimited'."),

    Option("rgw_bucket_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("Default quota for total size in a bucket")
    .set_long_description(
        "The default quota configuration for total size of objects in a bucket. A "
        "negative number means 'unlimited'."),

    Option("rgw_expose_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Send Bucket HTTP header with the response")
    .set_long_description(
        "If true, RGW will send a Bucket HTTP header with the responses. The header will "
        "contain the name of the bucket the operation happened on."),

    Option("rgw_frontends", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("beast port=7480")
    .set_description("RGW frontends configuration")
    .set_long_description(
        "A comma delimited list of frontends configuration. Each configuration contains "
        "the type of the frontend followed by an optional space delimited set of "
        "key=value config parameters."),

    Option("rgw_frontend_defaults", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("beast ssl_certificate=config://rgw/cert/$realm/$zone.crt ssl_private_key=config://rgw/cert/$realm/$zone.key")
    .set_description("RGW frontends default configuration")
    .set_long_description(
        "A comma delimited list of default frontends configuration."),

    Option("rgw_user_quota_bucket_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(180)
    .set_description("User quota bucket sync interval")
    .set_long_description(
        "Time period for accumulating modified buckets before syncing these stats."),

    Option("rgw_user_quota_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description("User quota sync interval")
    .set_long_description(
        "Time period for accumulating modified buckets before syncing entire user stats."),

    Option("rgw_user_quota_sync_idle_users", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should sync idle users quota")
    .set_long_description(
        "Whether stats for idle users be fully synced."),

    Option("rgw_user_quota_sync_wait_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_day)
    .set_description("User quota full-sync wait time")
    .set_long_description(
        "Minimum time between two full stats sync for non-idle users."),

    Option("rgw_user_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(-1)
    .set_description("User quota max objects")
    .set_long_description(
        "The default quota configuration for total number of objects for a single user. A "
        "negative number means 'unlimited'."),

    Option("rgw_user_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(-1)
    .set_description("User quota max size")
    .set_long_description(
        "The default quota configuration for total size of objects for a single user. A "
        "negative number means 'unlimited'."),

    Option("rgw_multipart_min_part_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5_M)
    .set_description("Minimum S3 multipart-upload part size")
    .set_long_description(
        "When doing a multipart upload, each part (other than the last part) should be "
        "at least this size."),

    Option("rgw_multipart_part_upload_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("Max number of parts in multipart upload"),

    Option("rgw_max_slo_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Max number of entries in Swift Static Large Object manifest"),

    Option("rgw_olh_pending_timeout_sec", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1_hr)
    .set_description("Max time for pending OLH change to complete")
    .set_long_description(
        "OLH is a versioned object's logical head. Operations on it are journaled and "
        "as pending before completion. If an operation doesn't complete with this amount "
        "of seconds, we remove the operation from the journal."),

    Option("rgw_user_max_buckets", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(1000)
    .set_description("Max number of buckets per user")
    .set_long_description(
      "A user can create at most this number of buckets. Zero means "
      "no limit; a negative value means users cannot create any new "
      "buckets, although users will retain buckets already created."),

    Option("rgw_objexp_gc_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10_min)
    .set_description("Swift objects expirer garbage collector interval"),

    Option("rgw_objexp_hints_num_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(127)
    .set_description("Number of object expirer data shards")
    .set_long_description(
        "The number of shards the (Swift) object expirer will store its data on."),

    Option("rgw_objexp_chunk_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100)
    .set_description(""),

    Option("rgw_enable_static_website", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("Enable static website APIs")
    .set_long_description(
        "This configurable controls whether RGW handles the website control APIs. RGW can "
        "server static websites if s3website hostnames are configured, and unrelated to "
        "this configurable."),

     Option("rgw_user_unique_email", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_description("Require local RGW users to have unique email addresses")
    .set_long_description(
        "Enforce builtin user accounts to have unique email addresses.  This "
	"setting is historical.  In future, non-enforcement of email address "
        "uniqueness is likely to become the default."),

    Option("rgw_log_http_headers", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_description("List of HTTP headers to log")
    .set_long_description(
        "A comma delimited list of HTTP headers to log when seen, ignores case (e.g., "
        "http_x_forwarded_for)."),

    Option("rgw_num_async_rados_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Number of concurrent RADOS operations in multisite sync")
    .set_long_description(
        "The number of concurrent RADOS IO operations that will be triggered for handling "
        "multisite sync operations. This includes control related work, and not the actual "
        "sync operations."),

    Option("rgw_md_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description("Length of time to aggregate metadata changes")
    .set_long_description(
        "Length of time (in milliseconds) in which the master zone aggregates all the "
        "metadata changes that occurred, before sending notifications to all the other "
        "zones."),

    Option("rgw_run_sync_thread", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Should run sync thread"),

    Option("rgw_sync_lease_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(120)
    .set_description(""),

    Option("rgw_sync_log_trim_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1200)
    .set_description("Sync log trim interval")
    .set_long_description(
        "Time in seconds between attempts to trim sync logs."),

    Option("rgw_sync_log_trim_max_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_description("Maximum number of buckets to trim per interval")
    .set_long_description("The maximum number of buckets to consider for bucket index log trimming each trim interval, regardless of the number of bucket index shards. Priority is given to buckets with the most sync activity over the last trim interval.")
    .add_see_also("rgw_sync_log_trim_interval")
    .add_see_also("rgw_sync_log_trim_min_cold_buckets")
    .add_see_also("rgw_sync_log_trim_concurrent_buckets"),

    Option("rgw_sync_log_trim_min_cold_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("Minimum number of cold buckets to trim per interval")
    .set_long_description("Of the `rgw_sync_log_trim_max_buckets` selected for bucket index log trimming each trim interval, at least this many of them must be 'cold' buckets. These buckets are selected in order from the list of all bucket instances, to guarantee that all buckets will be visited eventually.")
    .add_see_also("rgw_sync_log_trim_interval")
    .add_see_also("rgw_sync_log_trim_max_buckets")
    .add_see_also("rgw_sync_log_trim_concurrent_buckets"),

    Option("rgw_sync_log_trim_concurrent_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("Maximum number of buckets to trim in parallel")
    .add_see_also("rgw_sync_log_trim_interval")
    .add_see_also("rgw_sync_log_trim_max_buckets")
    .add_see_also("rgw_sync_log_trim_min_cold_buckets"),

    Option("rgw_sync_data_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("rgw_sync_meta_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("rgw_sync_trace_history_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4096)
    .set_description("Sync trace history size")
    .set_long_description(
      "Maximum number of complete sync trace entries to keep."),

    Option("rgw_sync_trace_per_node_log_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("Sync trace per-node log size")
    .set_long_description(
        "The number of log entries to keep per sync-trace node."),

    Option("rgw_sync_trace_servicemap_update_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Sync-trace service-map update interval")
    .set_long_description(
        "Number of seconds between service-map updates of sync-trace events."),

    Option("rgw_period_push_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("Period push interval")
    .set_long_description(
        "Number of seconds to wait before retrying 'period push' operation."),

    Option("rgw_period_push_interval_max", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("Period push maximum interval")
    .set_long_description(
        "The max number of seconds to wait before retrying 'period push' after exponential "
        "backoff."),

    Option("rgw_safe_max_objects_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100*1024)
    .set_description("Safe number of objects per shard")
    .set_long_description(
        "This is the max number of objects per bucket index shard that RGW considers "
        "safe. RGW will warn if it identifies a bucket where its per-shard count is "
        "higher than a percentage of this number.")
    .add_see_also("rgw_shard_warning_threshold"),

    Option("rgw_shard_warning_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(90)
    .set_description("Warn about max objects per shard")
    .set_long_description(
        "Warn if number of objects per shard in a specific bucket passed this percentage "
        "of the safe number.")
    .add_see_also("rgw_safe_max_objects_per_shard"),

    Option("rgw_swift_versioning_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable Swift versioning"),

    Option("rgw_swift_custom_header", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Enable swift custom header")
    .set_long_description(
        "If not empty, specifies a name of HTTP header that can include custom data. When "
        "uploading an object, if this header is passed RGW will store this header info "
        "and it will be available when listing the bucket."),

    Option("rgw_swift_need_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Enable stats on bucket listing in Swift"),

    Option("rgw_reshard_num_logs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_min(1)
    .set_description("")
    .add_service("rgw"),

    Option("rgw_reshard_bucket_lock_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(360)
    .set_min(30)
    .set_description("Number of seconds the timeout on the reshard locks (bucket reshard lock and reshard log lock) are set to. As a reshard proceeds these locks can be renewed/extended. If too short, reshards cannot complete and will fail, causing a future reshard attempt. If too long a hung or crashed reshard attempt will keep the bucket locked for an extended period, not allowing RGW to detect the failed reshard attempt and recover.")
    .add_tag("performance")
    .add_service("rgw"),

    Option("rgw_reshard_batch_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_min(8)
    .set_description("Number of reshard entries to batch together before sending the operations to the CLS back-end")
    .add_tag("performance")
    .add_service("rgw"),

    Option("rgw_reshard_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_min(16)
    .set_description("Maximum number of outstanding asynchronous I/O operations to allow at a time during resharding")
    .add_tag("performance")
    .add_service("rgw"),

    Option("rgw_trust_forwarded_https", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Trust Forwarded and X-Forwarded-Proto headers")
    .set_long_description(
        "When a proxy in front of radosgw is used for ssl termination, radosgw "
        "does not know whether incoming http connections are secure. Enable "
        "this option to trust the Forwarded and X-Forwarded-Proto headers sent "
        "by the proxy when determining whether the connection is secure. This "
        "is required for some features, such as server side encryption. "
        "(Never enable this setting if you do not have a trusted proxy in "
        "front of radosgw, or else malicious users will be able to set these "
        "headers in any request.)")
    .add_see_also("rgw_crypt_require_ssl"),

    Option("rgw_crypt_require_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Requests including encryption key headers must be sent over ssl"),

    Option("rgw_crypt_default_encryption_key", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("rgw_crypt_s3_kms_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("barbican")
    .set_enum_allowed({"barbican", "vault", "testing"})
    .set_description(
        "Where the SSE-KMS encryption keys are stored. Supported KMS "
        "systems are OpenStack Barbican ('barbican', the default) and HashiCorp "
        "Vault ('vault')."),

    Option("rgw_crypt_s3_kms_encryption_keys", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description(""),

    Option("rgw_crypt_vault_auth", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("token")
    .set_enum_allowed({"token", "agent"})
    .set_description(
        "Type of authentication method to be used with Vault. ")
    .add_see_also({
        "rgw_crypt_s3_kms_backend",
        "rgw_crypt_vault_addr",
        "rgw_crypt_vault_token_file"}),

    Option("rgw_crypt_vault_token_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description(
        "If authentication method is 'token', provide a path to the token file, "
        "which for security reasons should readable only by Rados Gateway.")
    .add_see_also({
      "rgw_crypt_s3_kms_backend",
      "rgw_crypt_vault_auth",
      "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_vault_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Vault server base address.")
    .add_see_also({
      "rgw_crypt_s3_kms_backend",
      "rgw_crypt_vault_auth",
      "rgw_crypt_vault_prefix"}),

    Option("rgw_crypt_vault_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Vault secret URL prefix, which can be used to restrict "
                     "access to a particular subset of the Vault secret space.")
    .add_see_also({
      "rgw_crypt_s3_kms_backend",
      "rgw_crypt_vault_addr",
      "rgw_crypt_vault_auth"}),


    Option("rgw_crypt_vault_secret_engine", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_enum_allowed({"kv", "transit"})
    .set_default("transit")
    .set_description(
        "Vault Secret Engine to be used to retrieve encryption keys.")
    .add_see_also({
      "rgw_crypt_s3_kms_backend",
      "rgw_crypt_vault_auth",
      "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_vault_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Vault Namespace to be used to select your tenant")
    .add_see_also({
      "rgw_crypt_s3_kms_backend",
      "rgw_crypt_vault_auth",
      "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_suppress_logs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Suppress logs that might print client key"),

    Option("rgw_list_bucket_min_readahead", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("Minimum number of entries to request from rados for bucket listing"),

    Option("rgw_rest_getusage_op_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("REST GetUsage request backward compatibility"),

    Option("rgw_torrent_flag", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("When true, uploaded objects will calculate and store "
                     "a SHA256 hash of object data so the object can be "
                     "retrieved as a torrent file"),

    Option("rgw_torrent_tracker", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Torrent field announce and announce list"),

    Option("rgw_torrent_createby", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("torrent field created by"),

    Option("rgw_torrent_comment", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Torrent field comment"),

    Option("rgw_torrent_encoding", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("torrent field encoding"),

    Option("rgw_data_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(200)
    .set_description("data changes notification interval to followers"),

    Option("rgw_torrent_origin", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Torrent origin"),

    Option("rgw_torrent_sha_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512*1024)
    .set_description(""),

    Option("rgw_dynamic_resharding", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(true)
    .set_description("Enable dynamic resharding")
    .set_long_description(
        "If true, RGW will dynamicall increase the number of shards in buckets that have "
        "a high number of objects per shard.")
    .add_see_also("rgw_max_objs_per_shard")
    .add_see_also("rgw_max_dynamic_shards"),

    Option("rgw_max_objs_per_shard", Option::TYPE_UINT, Option::LEVEL_BASIC)
    .set_default(100000)
    .set_description("Max objects per shard for dynamic resharding")
    .set_long_description(
        "This is the max number of objects per bucket index shard that RGW will "
        "allow with dynamic resharding. RGW will trigger an automatic reshard operation "
        "on the bucket if it exceeds this number.")
    .add_see_also("rgw_dynamic_resharding")
    .add_see_also("rgw_max_dynamic_shards"),

    Option("rgw_max_dynamic_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1999)
    .set_min(1)
    .set_description("Max shards that dynamic resharding can create")
    .set_long_description(
        "This is the maximum number of bucket index shards that dynamic "
	"sharding is able to create on its own. This does not limit user "
	"requested resharding. Ideally this value is a prime number.")
    .add_see_also("rgw_dynamic_resharding")
    .add_see_also("rgw_max_objs_per_shard"),

    Option("rgw_reshard_thread_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10_min)
    .set_min(10)
    .set_description("Number of seconds between processing of reshard log entries"),

    Option("rgw_cache_expiry_interval", Option::TYPE_UINT,
	   Option::LEVEL_ADVANCED)
    .set_default(15_min)
    .set_description("Number of seconds before entries in the cache are "
		     "assumed stale and re-fetched. Zero is never.")
    .add_tag("performance")
    .add_service("rgw")
    .set_long_description("The Rados Gateway stores metadata and objects in "
			  "an internal cache. This should be kept consistent "
			  "by the OSD's relaying notify events between "
			  "multiple watching RGW processes. In the event "
			  "that this notification protocol fails, bounding "
			  "the length of time that any data in the cache will "
			  "be assumed valid will ensure that any RGW instance "
			  "that falls out of sync will eventually recover. "
			  "This seems to be an issue mostly for large numbers "
			  "of RGW instances under heavy use. If you would like "
			  "to turn off cache expiry, set this value to zero."),

    Option("rgw_inject_notify_timeout_probability", Option::TYPE_FLOAT,
	   Option::LEVEL_DEV)
    .set_default(0)
    .add_tag("fault injection")
    .add_tag("testing")
    .add_service("rgw")
    .set_min_max(0.0, 1.0)
    .set_description("Likelihood of ignoring a notify")
    .set_long_description("This is the probability that the RGW cache will "
			  "ignore a cache notify message. It exists to help "
			  "with the development and testing of cache "
			  "consistency and recovery improvements. Please "
			  "do not set it in a production cluster, as it "
			  "actively causes failures. Set this to a floating "
			  "point value between 0 and 1."),
    Option("rgw_max_notify_retries", Option::TYPE_UINT,
	   Option::LEVEL_ADVANCED)
    .set_default(3)
    .add_tag("error recovery")
    .add_service("rgw")
    .set_description("Number of attempts to notify peers before giving up.")
    .set_long_description("The number of times we will attempt to update "
			  "a peer's cache in the event of error before giving "
			  "up. This is unlikely to be an issue unless your "
			  "cluster is very heavily loaded. Beware that "
			  "increasing this value may cause some operations to "
			  "take longer in exceptional cases and thus may, "
			  "rarely, cause clients to time out."),
    Option("rgw_sts_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("sts")
    .set_description("STS URL prefix")
    .set_long_description("URL path prefix for internal STS requests."),

    Option("rgw_sts_key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("sts")
    .set_description("STS Key")
    .set_long_description("Key used for encrypting/ decrypting session token."),

    Option("rgw_s3_auth_use_sts", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Should S3 authentication use STS."),

    Option("rgw_sts_max_session_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(43200)
    .set_description("Session token max duration")
    .set_long_description("Max duration in seconds for which the session token is valid."),

    Option("rgw_max_listing_results", Option::TYPE_UINT,
	   Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_min_max(1, 100000)
    .add_service("rgw")
    .set_description("Upper bound on results in listing operations, ListBucket max-keys")
    .set_long_description("This caps the maximum permitted value for listing-like operations in RGW S3. "
			  "Affects ListBucket(max-keys), "
			  "ListBucketVersions(max-keys), "
			  "ListBucketMultipartUploads(max-uploads), "
			  "ListMultipartUploadParts(max-parts)"),

    Option("rgw_sts_token_introspection_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("STS Web Token introspection URL")
    .set_long_description("URL for introspecting an STS Web Token."),

    Option("rgw_sts_client_id", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Client Id")
    .set_long_description("Client Id needed for introspecting a Web Token."),

    Option("rgw_sts_client_secret", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("Client Secret")
    .set_long_description("Client Secret needed for introspecting a Web Token."),

    Option("rgw_max_concurrent_requests", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_default(1024)
    .set_description("Maximum number of concurrent HTTP requests.")
    .set_long_description(
        "Maximum number of concurrent HTTP requests that the beast frontend "
        "will process. Tuning this can help to limit memory usage under heavy "
        "load.")
    .add_tag("performance")
    .add_see_also("rgw_frontends"),

    Option("rgw_scheduler_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("throttler")
    .set_description("Set the type of dmclock scheduler, defaults to throttler "
		     "Other valid values are dmclock which is experimental"),

    Option("rgw_dmclock_admin_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(100.0)
    .set_description("mclock reservation for admin requests")
    .add_see_also("rgw_dmclock_admin_wgt")
    .add_see_also("rgw_dmclock_admin_lim"),

    Option("rgw_dmclock_admin_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(100.0)
    .set_description("mclock weight for admin requests")
    .add_see_also("rgw_dmclock_admin_res")
    .add_see_also("rgw_dmclock_admin_lim"),

    Option("rgw_dmclock_admin_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit for admin requests")
    .add_see_also("rgw_dmclock_admin_res")
    .add_see_also("rgw_dmclock_admin_wgt"),

    Option("rgw_dmclock_auth_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(200.0)
    .set_description("mclock reservation for object data requests")
    .add_see_also("rgw_dmclock_auth_wgt")
    .add_see_also("rgw_dmclock_auth_lim"),

    Option("rgw_dmclock_auth_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(100.0)
    .set_description("mclock weight for object data requests")
    .add_see_also("rgw_dmclock_auth_res")
    .add_see_also("rgw_dmclock_auth_lim"),

    Option("rgw_dmclock_auth_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit for object data requests")
    .add_see_also("rgw_dmclock_auth_res")
    .add_see_also("rgw_dmclock_auth_wgt"),

    Option("rgw_dmclock_data_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock reservation for object data requests")
    .add_see_also("rgw_dmclock_data_wgt")
    .add_see_also("rgw_dmclock_data_lim"),

    Option("rgw_dmclock_data_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock weight for object data requests")
    .add_see_also("rgw_dmclock_data_res")
    .add_see_also("rgw_dmclock_data_lim"),

    Option("rgw_dmclock_data_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit for object data requests")
    .add_see_also("rgw_dmclock_data_res")
    .add_see_also("rgw_dmclock_data_wgt"),

    Option("rgw_dmclock_metadata_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock reservation for metadata requests")
    .add_see_also("rgw_dmclock_metadata_wgt")
    .add_see_also("rgw_dmclock_metadata_lim"),

    Option("rgw_dmclock_metadata_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(500.0)
    .set_description("mclock weight for metadata requests")
    .add_see_also("rgw_dmclock_metadata_res")
    .add_see_also("rgw_dmclock_metadata_lim"),

    Option("rgw_dmclock_metadata_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0)
    .set_description("mclock limit for metadata requests")
    .add_see_also("rgw_dmclock_metadata_res")
    .add_see_also("rgw_dmclock_metadata_wgt"),
  });
}

static std::vector<Option> get_rbd_options() {
  return std::vector<Option>({
    Option("rbd_default_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rbd")
    .set_description("default pool for storing new images")
    .set_validator([](std::string *value, std::string *error_message){
      std::regex pattern("^[^@/]+$");
      if (!std::regex_match (*value, pattern)) {
        *value = "rbd";
        *error_message = "invalid RBD default pool, resetting to 'rbd'";
      }
      return 0;
    }),

    Option("rbd_default_data_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("default pool for storing data blocks for new images")
    .set_validator([](std::string *value, std::string *error_message){
      std::regex pattern("^[^@/]*$");
      if (!std::regex_match (*value, pattern)) {
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
    .set_flag(Option::FLAG_RUNTIME)
    .set_validator([](std::string *value, std::string *error_message) {
	ostringstream ss;
	uint64_t features = librbd::rbd_features_from_string(*value, &ss);
	// Leave this in integer form to avoid breaking Cinder.  Someday
	// we would like to present this in string form instead...
	*value = stringify(features);
	if (ss.str().size()) {
	  return -EINVAL;
	}
	return 0;
      }),

    Option("rbd_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("number of threads to utilize for internal processing"),

    Option("rbd_op_thread_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("time in seconds for detecting a hung thread"),

    Option("rbd_disable_zero_copy_writes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Disable the use of zero-copy writes to ensure unstable "
                     "writes from clients cannot cause a CRC mismatch"),

    Option("rbd_non_blocking_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("process AIO ops from a dispatch thread to prevent blocking"),

    Option("rbd_cache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to enable caching (writeback unless rbd_cache_max_dirty is 0)"),

    Option("rbd_cache_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_enum_allowed({"writethrough", "writeback", "writearound"})
    .set_default("writearound")
    .set_description("cache policy for handling writes."),

    Option("rbd_cache_writethrough_until_flush", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to make writeback caching writethrough until "
                     "flush is called, to be sure the user of librbd will send "
                     "flushes so that writeback is safe"),

    Option("rbd_cache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(32_M)
    .set_description("cache size in bytes"),

    Option("rbd_cache_max_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(24_M)
    .set_description("dirty limit in bytes - set to 0 for write-through caching"),

    Option("rbd_cache_target_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_M)
    .set_description("target dirty limit in bytes"),

    Option("rbd_cache_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("seconds in cache before writeback starts"),

    Option("rbd_cache_max_dirty_object", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("dirty limit for objects - set to 0 for auto calculate from rbd_cache_size"),

    Option("rbd_cache_block_writes_upfront", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("whether to block writes to the cache before the aio_write call completes"),

    Option("rbd_parent_cache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("whether to enable rbd shared ro cache"),

    Option("rbd_concurrent_management_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
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

    Option("rbd_sparse_read_threshold_bytes", Option::TYPE_SIZE,
           Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description("threshold for issuing a sparse-read")
    .set_long_description("minimum number of sequential bytes to read against "
                          "an object before issuing a sparse-read request to "
                          "the cluster. 0 implies it must be a full object read "
                          "to issue a sparse-read, 1 implies always use "
                          "sparse-read, and any value larger than the maximum "
                          "object size will disable sparse-read for all "
                          "requests"),

    Option("rbd_readahead_trigger_requests", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("number of sequential requests necessary to trigger readahead"),

    Option("rbd_readahead_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_description("set to 0 to disable readahead"),

    Option("rbd_readahead_disable_after_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(50_M)
    .set_description("how many bytes are read in total before readahead is disabled"),

    Option("rbd_clone_copy_on_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("copy-up parent image blocks to clone upon read request"),

    Option("rbd_blacklist_on_break_lock", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("whether to blacklist clients whose lock was broken"),

    Option("rbd_blacklist_expire_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("number of seconds to blacklist - set to 0 for OSD default"),

    Option("rbd_request_timed_out_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("number of seconds before maintenance request times out"),

    Option("rbd_skip_partial_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("skip discard (zero) of unaligned extents within an object"),

    Option("rbd_discard_granularity_bytes", Option::TYPE_UINT,
           Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_min_max(4_K, 32_M)
    .set_validator([](std::string *value, std::string *error_message){
        uint64_t f = strict_si_cast<uint64_t>(value->c_str(), error_message);
        if (!error_message->empty()) {
          return -EINVAL;
        } else if (!isp2(f)) {
          *error_message = "value must be a power of two";
          return -EINVAL;
        }
        return 0;
      })
    .set_description("minimum aligned size of discard operations"),

    Option("rbd_enable_alloc_hint", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("when writing a object, it will issue a hint to osd backend to indicate the expected size object need"),

    Option("rbd_compression_hint", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_enum_allowed({"none", "compressible", "incompressible"})
    .set_default("none")
    .set_description("Compression hint to send to the OSDs during writes")
    .set_flag(Option::FLAG_RUNTIME),

    Option("rbd_read_from_replica_policy", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_enum_allowed({"default", "balance", "localize"})
    .set_default("default")
    .set_description("Read replica policy send to the OSDS during reads")
    .set_flag(Option::FLAG_RUNTIME),

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

    Option("rbd_move_to_trash_on_remove", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("automatically move images to the trash when deleted"),

    Option("rbd_move_to_trash_on_remove_expire_seconds", Option::TYPE_UINT, Option::LEVEL_BASIC)
    .set_default(0)
    .set_description("default number of seconds to protect deleted images in the trash"),

    Option("rbd_move_parent_to_trash_on_remove", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_default(false)
    .set_description("move parent with clone format v2 children to the trash when deleted"),

    Option("rbd_mirroring_resync_after_disconnect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("automatically start image resync after mirroring is disconnected due to being laggy"),

    Option("rbd_mirroring_delete_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("time-delay in seconds for rbd-mirror delete propagation"),

    Option("rbd_mirroring_replay_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("time-delay in seconds for rbd-mirror asynchronous replication"),

    Option("rbd_mirroring_max_mirroring_snapshots", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min(3)
    .set_description("mirroring snapshots limit"),

    Option("rbd_default_format", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("default image format for new images"),

    Option("rbd_default_order", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(22)
    .set_description("default order (data block object size) for new images"),

    Option("rbd_default_stripe_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default stripe count for new images"),

    Option("rbd_default_stripe_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default stripe width for new images"),

    Option("rbd_default_map_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("default krbd map options"),

    Option("rbd_default_clone_format", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_enum_allowed({"1", "2", "auto"})
    .set_default("auto")
    .set_description("default internal format for handling clones")
    .set_long_description("This sets the internal format for tracking cloned "
                          "images. The setting of '1' requires attaching to "
                          "protected snapshots that cannot be removed until "
                          "the clone is removed/flattened. The setting of '2' "
                          "will allow clones to be attached to any snapshot "
                          "and permits removing in-use parent snapshots but "
                          "requires Mimic or later clients. The default "
                          "setting of 'auto' will use the v2 format if the "
                          "cluster is configured to require mimic or later "
                          "clients.")
    .set_flag(Option::FLAG_RUNTIME),

    Option("rbd_journal_order", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_min_max(12, 26)
    .set_default(24)
    .set_description("default order (object size) for journal data objects"),

    Option("rbd_journal_splay_width", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("number of active journal objects"),

    Option("rbd_journal_commit_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("commit time interval, seconds"),

    Option("rbd_journal_object_writethrough_until_flush", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("when enabled, the rbd_journal_object_flush* configuration "
                     "options are ignored until the first flush so that batched "
                     "journal IO is known to be safe for consistency"),

    Option("rbd_journal_object_flush_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of pending commits per journal object"),

    Option("rbd_journal_object_flush_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description("maximum number of pending bytes per journal object"),

    Option("rbd_journal_object_flush_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum age (in seconds) for pending commits"),

    Option("rbd_journal_object_max_in_flight_appends", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of in-flight appends per journal object"),

    Option("rbd_journal_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("pool for journal objects"),

    Option("rbd_journal_max_payload_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16384)
    .set_description("maximum journal payload size before splitting"),

    Option("rbd_journal_max_concurrent_object_sets", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum number of object sets a journal client can be behind before it is automatically unregistered"),

    Option("rbd_qos_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of IO operations per second"),

    Option("rbd_qos_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of IO bytes per second"),

    Option("rbd_qos_read_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of read operations per second"),

    Option("rbd_qos_write_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of write operations per second"),

    Option("rbd_qos_read_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of read bytes per second"),

    Option("rbd_qos_write_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired limit of write bytes per second"),

    Option("rbd_qos_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of IO operations"),

    Option("rbd_qos_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of IO bytes"),

    Option("rbd_qos_read_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of read operations"),

    Option("rbd_qos_write_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of write operations"),

    Option("rbd_qos_read_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of read bytes"),

    Option("rbd_qos_write_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("the desired burst limit of write bytes"),

    Option("rbd_qos_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of IO operations"),

    Option("rbd_qos_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of IO bytes"),

    Option("rbd_qos_read_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of read operations"),

    Option("rbd_qos_write_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of write operations"),

    Option("rbd_qos_read_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of read bytes"),

    Option("rbd_qos_write_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("the desired burst duration in seconds of write bytes"),

    Option("rbd_qos_schedule_tick_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_min(1)
    .set_description("minimum schedule tick (in milliseconds) for QoS"),

    Option("rbd_discard_on_zeroed_write_same", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("discard data on zeroed write same instead of writing zero"),

    Option("rbd_mtime_update_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_min(0)
    .set_description("RBD Image modify timestamp refresh interval. Set to 0 to disable modify timestamp update."),

    Option("rbd_atime_update_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_min(0)
    .set_description("RBD Image access timestamp refresh interval. Set to 0 to disable access timestamp update."),

    Option("rbd_io_scheduler", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("simple")
    .set_enum_allowed({"none", "simple"})
    .set_description("RBD IO scheduler"),

    Option("rbd_io_scheduler_simple_max_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_min(0)
    .set_description("maximum io delay (in milliseconds) for simple io scheduler (if set to 0 dalay is calculated based on latency stats)"),

    Option("rbd_rwl_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("enable persistent write back cache for this volume"),

    Option("rbd_rwl_log_periodic_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("emit periodic perf stats to debug log"),

    Option("rbd_rwl_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1073741824)
    .set_min(1073741824)
    .set_description("size of the persistent write back cache for this volume"),

    Option("rbd_rwl_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/tmp")
    .set_description("location of the persistent write back cache in a DAX-enabled filesystem on persistent memory"),

    Option("rbd_quiesce_notification_attempts", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10)
    .set_min(1)
    .set_description("the number of quiesce notification attempts"),

    Option("rbd_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("comma-delimited list of librbd plugins to enable"),

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

    Option("rbd_mirror_sync_point_update_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("number of seconds between each update of the image sync point object number"),

    Option("rbd_mirror_concurrent_image_syncs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("maximum number of image syncs in parallel"),

    Option("rbd_mirror_pool_replayers_refresh_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("interval to refresh peers in rbd-mirror daemon"),

    Option("rbd_mirror_concurrent_image_deletions", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("maximum number of image deletions in parallel"),

    Option("rbd_mirror_delete_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("interval to check and retry the failed deletion requests"),

    Option("rbd_mirror_image_state_check_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_min(1)
    .set_description("interval to get images from pool watcher and set sources in replayer"),

    Option("rbd_mirror_leader_heartbeat_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_min(1)
    .set_description("interval (in seconds) between mirror leader heartbeats"),

    Option("rbd_mirror_leader_max_missed_heartbeats", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("number of missed heartbeats for non-lock owner to attempt to acquire lock"),

    Option("rbd_mirror_leader_max_acquire_attempts_before_break", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_description("number of failed attempts to acquire lock after missing heartbeats before breaking lock"),

    Option("rbd_mirror_image_policy_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("simple")
    .set_enum_allowed({"none", "simple"})
    .set_description("active/active policy type for mapping images to instances"),

    Option("rbd_mirror_image_policy_migration_throttle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(300)
    .set_description("number of seconds after which an image can be reshuffled (migrated) again"),

    Option("rbd_mirror_image_policy_update_throttle_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_min(1)
    .set_description("interval (in seconds) to throttle images for mirror daemon peer updates"),

    Option("rbd_mirror_image_policy_rebalance_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("number of seconds policy should be idle before trigerring reshuffle (rebalance) of images"),

    Option("rbd_mirror_perf_stats_prio", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default((int64_t)PerfCountersBuilder::PRIO_USEFUL)
    .set_description("Priority level for mirror daemon replication perf counters")
    .set_long_description("The daemon will send perf counter data to the "
                          "manager daemon if the priority is not lower than "
                          "mgr_stats_threshold.")
    .set_min_max((int64_t)PerfCountersBuilder::PRIO_DEBUGONLY,
                 (int64_t)PerfCountersBuilder::PRIO_CRITICAL + 1),

    Option("rbd_mirror_image_perf_stats_prio", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default((int64_t)PerfCountersBuilder::PRIO_USEFUL)
    .set_description("Priority level for mirror daemon per-image replication perf counters")
    .set_long_description("The daemon will send per-image perf counter data to the "
                          "manager daemon if the priority is not lower than "
                          "mgr_stats_threshold.")
    .set_min_max((int64_t)PerfCountersBuilder::PRIO_DEBUGONLY,
                 (int64_t)PerfCountersBuilder::PRIO_CRITICAL + 1),

    Option("rbd_mirror_memory_autotune", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_see_also("rbd_mirror_memory_target")
    .set_description("Automatically tune the ratio of caches while respecting min values."),

    Option("rbd_mirror_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_default(4_G)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, try to keep this many bytes mapped in memory."),

    Option("rbd_mirror_memory_base", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(768_M)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the minimum amount of memory in bytes the rbd-mirror daemon will need."),

    Option("rbd_mirror_memory_expected_fragmentation", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.15)
    .set_min_max(0.0, 1.0)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the percent of memory fragmentation."),

    Option("rbd_mirror_memory_cache_min", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, set the minimum amount of memory used for cache."),

    Option("rbd_mirror_memory_cache_resize_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("When tcmalloc and cache autotuning is enabled, wait this many seconds between resizing caches."),

    Option("rbd_mirror_memory_cache_autotune_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(30)
    .add_see_also("rbd_mirror_memory_autotune")
    .set_description("The number of seconds to wait between rebalances when cache autotune is enabled."),
  });
}

static std::vector<Option> get_immutable_object_cache_options() {
  return std::vector<Option>({
    Option("immutable_object_cache_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/tmp")
    .set_description("immutable object cache data dir"),

    Option("immutable_object_cache_sock", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/run/ceph/immutable_object_cache_sock")
    .set_description("immutable object cache domain socket"),

    Option("immutable_object_cache_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G)
    .set_description("max immutable object cache data size"),

    Option("immutable_object_cache_max_inflight_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description("max inflight promoting requests for immutable object cache daemon"),

    Option("immutable_object_cache_client_dedicated_thread_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_description("immutable object cache client dedicated thread number"),

    Option("immutable_object_cache_watermark", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.1)
    .set_description("immutable object cache water mark"),
  });
}

std::vector<Option> get_mds_options() {
  return std::vector<Option>({
    Option("mds_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .set_description("set mds's cpu affinity to a numa node (-1 for none)"),

    Option("mds_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/var/lib/ceph/mds/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description("path to MDS data and keyring"),

    Option("mds_join_fs", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_default("")
    .set_description("file system MDS prefers to join")
    .set_long_description("This setting indicates which file system name the MDS should prefer to join (affinity). The monitors will try to have the MDS cluster safely reach a state where all MDS have strong affinity, even via failovers to a standby.")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_max_xattr_pairs_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description("maximum aggregate size of extended attributes on a file"),

    Option("mds_cache_trim_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("interval in seconds between cache trimming")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_cache_release_free_interval", Option::TYPE_SECS, Option::LEVEL_DEV)
    .set_default(10)
    .set_description("interval in seconds between heap releases")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_cache_memory_limit", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_default(4_G)
    .set_description("target maximum memory usage of MDS cache")
    .set_flag(Option::FLAG_RUNTIME)
    .set_long_description("This sets a target maximum memory usage of the MDS cache and is the primary tunable to limit the MDS memory usage. The MDS will try to stay under a reservation of this limit (by default 95%; 1 - mds_cache_reservation) by trimming unused metadata in its cache and recalling cached items in the client caches. It is possible for the MDS to exceed this limit due to slow recall from clients. The mds_health_cache_threshold (150%) sets a cache full threshold for when the MDS signals a cluster health warning."),

    Option("mds_cache_reservation", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.05)
    .set_description("amount of memory to reserve for future cached objects")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_health_cache_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5)
    .set_description("threshold for cache size to generate health warning"),

    Option("mds_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.7)
    .set_description("midpoint for MDS cache LRU"),

    Option("mds_cache_trim_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("decay rate for trimming MDS cache throttle")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_cache_trim_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description("threshold for number of dentries that can be trimmed")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_max_file_recover", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32)
    .set_description("maximum number of files to recover file sizes in parallel"),

    Option("mds_dir_max_commit_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("maximum size in megabytes for a RADOS write to a directory"),

    Option("mds_dir_keys_per_op", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16384)
    .set_description("number of directory entries to read in one RADOS operation"),

    Option("mds_decay_halflife", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("rate of decay for temperature counters on each directory for balancing"),

    Option("mds_beacon_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("interval in seconds between MDS beacons to monitors"),

    Option("mds_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description("tolerance in seconds for missed MDS beacons to monitors"),

    Option("mds_heartbeat_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(15)
    .set_description("tolerance in seconds for MDS internal heartbeat"),

    Option("mds_enforce_unique_name", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("require MDS name is unique in the cluster"),

    Option("mds_session_blacklist_on_timeout", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("blacklist clients whose sessions have become stale"),

    Option("mds_session_blacklist_on_evict", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("blacklist clients that have been evicted"),

    Option("mds_sessionmap_keys_per_op", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("number of omap keys to read from the SessionMap in one operation"),

    Option("mds_recall_max_caps", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5000)
    .set_description("maximum number of caps to recall from client session in single recall")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_recall_max_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(2.5)
    .set_description("decay rate for throttle on recalled caps on a session")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_recall_max_decay_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_K)
    .set_description("decay threshold for throttle on recalled caps on a session")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_recall_global_max_decay_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K)
    .set_description("decay threshold for throttle on recalled caps globally")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_recall_warning_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(32_K)
    .set_description("decay threshold for warning on slow session cap recall")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_recall_warning_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60.0)
    .set_description("decay rate for warning on slow session cap recall")
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_session_cache_liveness_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .add_see_also("mds_session_cache_liveness_magnitude")
    .set_default(5_min)
    .set_description("decay rate for session liveness leading to preemptive cap recall")
    .set_flag(Option::FLAG_RUNTIME)
    .set_long_description("This determines how long a session needs to be quiescent before the MDS begins preemptively recalling capabilities. The default of 5 minutes will cause 10 halvings of the decay counter after 1 hour, or 1/1024. The default magnitude of 10 (1^10 or 1024) is chosen so that the MDS considers a previously chatty session (approximately) to be quiescent after 1 hour."),

    Option("mds_session_cache_liveness_magnitude", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .add_see_also("mds_session_cache_liveness_decay_rate")
    .set_default(10)
    .set_description("decay magnitude for preemptively recalling caps on quiet client")
    .set_flag(Option::FLAG_RUNTIME)
    .set_long_description("This is the order of magnitude difference (in base 2) of the internal liveness decay counter and the number of capabilities the session holds. When this difference occurs, the MDS treats the session as quiescent and begins recalling capabilities."),

    Option("mds_freeze_tree_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(30)
    .set_description(""),

    Option("mds_health_summarize_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("threshold of number of clients to summarize late client recall"),

    Option("mds_reconnect_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(45)
    .set_description("timeout in seconds to wait for clients to reconnect during MDS reconnect recovery state"),

    Option("mds_deny_all_reconnect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("flag to deny all client reconnects during failover"),

    Option("mds_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("time in seconds between upkeep tasks"),

    Option("mds_dirstat_min_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("mds_scatter_nudge_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("minimum interval between scatter lock updates"),

    Option("mds_client_prealloc_inos", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("number of unused inodes to pre-allocate to clients for file creation"),

    Option("mds_client_delegate_inos_pct", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("percentage of preallocated inos to delegate to client"),

    Option("mds_early_reply", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("additional reply to clients that metadata requests are complete but not yet durable"),

    Option("mds_replay_unsafe_with_closed_session", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("complete all the replay request when mds is restarted, no matter the session is closed or not"),

    Option("mds_default_dir_hash", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(CEPH_STR_HASH_RJENKINS)
    .set_description("hash function to select directory fragment for dentry name"),

    Option("mds_log_pause", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_log_skip_corrupt_events", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_log_max_events", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("maximum number of events in the MDS journal (-1 is unlimited)"),

    Option("mds_log_events_per_segment", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("maximum number of events in an MDS journal segment"),

    Option("mds_log_segment_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("size in bytes of each MDS log segment"),

    Option("mds_log_max_segments", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(128)
    .set_description("maximum number of segments which may be untrimmed"),

    Option("mds_bal_export_pin", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("allow setting directory export pins to particular ranks"),

    Option("mds_bal_sample_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3.0)
    .set_description("interval in seconds between balancer ticks"),

    Option("mds_bal_replicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(8000)
    .set_description("hot popularity threshold to replicate a subtree"),

    Option("mds_bal_unreplicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("cold popularity threshold to merge subtrees"),

    Option("mds_bal_split_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("minimum size of directory fragment before splitting"),

    Option("mds_bal_split_rd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(25000)
    .set_description("hot read popularity threshold for splitting a directory fragment"),

    Option("mds_bal_split_wr", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("hot write popularity threshold for splitting a directory fragment"),

    Option("mds_bal_split_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .set_min_max(1, 24)
    .set_description("power of two child fragments for a fragment on split"),

    Option("mds_bal_merge_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50)
    .set_description("size of fragments where merging should occur"),

    Option("mds_bal_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("interval between MDS balancer cycles"),

    Option("mds_bal_fragment_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("delay in seconds before interrupting client IO to perform splits"),

    Option("mds_bal_fragment_size_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000*10)
    .set_description("maximum size of a directory fragment before new creat/links fail"),

    Option("mds_bal_fragment_fast_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5)
    .set_description("ratio of mds_bal_split_size at which fast fragment splitting occurs"),

    Option("mds_bal_fragment_dirs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("enable directory fragmentation")
    .set_long_description("Directory fragmentation is a standard feature of CephFS that allows sharding directories across multiple objects for performance and stability. Additionally, this allows fragments to be distributed across multiple active MDSs to increase throughput. Disabling (new) fragmentation should only be done in exceptional circumstances and may lead to performance issues."),

    Option("mds_bal_idle_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("idle metadata popularity threshold before rebalancing"),

    Option("mds_bal_max", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .set_description(""),

    Option("mds_bal_max_until", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .set_description(""),

    Option("mds_bal_mode", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_bal_min_rebalance", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.1)
    .set_description("amount overloaded over internal target before balancer begins offloading"),

    Option("mds_bal_min_start", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.2)
    .set_description(""),

    Option("mds_bal_need_min", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.8)
    .set_description(""),

    Option("mds_bal_need_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.2)
    .set_description(""),

    Option("mds_bal_midchunk", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.3)
    .set_description(""),

    Option("mds_bal_minchunk", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(.001)
    .set_description(""),

    Option("mds_bal_target_decay", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0)
    .set_description("rate of decay for export targets communicated to clients"),

    Option("mds_replay_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .set_description("time in seconds between replay of updates to journal by standby replay MDS"),

    Option("mds_shutdown_check", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_thrash_exports", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_thrash_fragments", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_dump_cache_on_map", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_dump_cache_after_rejoin", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("mds_verify_scatter", Option::TYPE_BOOL, Option::LEVEL_DEV)
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

    Option("mds_max_export_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(20_M)
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

    Option("mds_journal_format", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("mds_kill_create_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_inject_traceless_reply_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_wipe_sessions", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_wipe_ino_prealloc", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_skip_ino", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("mds_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("track remote operation progression and statistics"),

    Option("mds_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20)
    .set_description("maximum size for list of historical operations"),

    Option("mds_op_history_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(600)
    .set_description("expiration time in seconds of historical operations"),

    Option("mds_op_complaint_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30)
    .set_description("time in seconds to consider an operation blocked after no updates"),

    Option("mds_op_log_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .set_description(""),

    Option("mds_snap_min_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("minimum uid of client to perform snapshots"),

    Option("mds_snap_max_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4294967294)
    .set_description("maximum uid of client to perform snapshots"),

    Option("mds_snap_rstat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("enabled nested rstat for snapshots"),

    Option("mds_verify_backtrace", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(1)
    .set_description(""),

    Option("mds_max_completed_flushes", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100000)
    .set_description(""),

    Option("mds_max_completed_requests", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100000)
    .set_description(""),

    Option("mds_action_on_write_error", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_description("action to take when MDS cannot write to RADOS (0:ignore, 1:read-only, 2:suicide)"),

    Option("mds_mon_shutdown_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("time to wait for mon to receive damaged MDS rank notification"),

    Option("mds_max_purge_files", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64)
    .set_description("maximum number of deleted files to purge in parallel"),

    Option("mds_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8192)
    .set_description("maximum number of purge operations performed in parallel"),

    Option("mds_max_purge_ops_per_pg", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.5)
    .set_description("number of parallel purge operations performed per PG"),

    Option("mds_purge_queue_busy_flush_period", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .set_description(""),

    Option("mds_root_ino_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default uid for new root directory"),

    Option("mds_root_ino_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("default gid for new root directory"),

    Option("mds_max_scrub_ops_in_progress", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_description("maximum number of scrub operations performed in parallel"),

    Option("mds_forward_all_requests_to_auth", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .set_description("always process op on auth mds"),
    
    Option("mds_damage_table_max_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10000)
    .set_description("maximum number of damage table entries"),

    Option("mds_client_writeable_range_max_inc_objs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1024)
    .set_description("maximum number of objects in writeable range of a file for a client"),

    Option("mds_min_caps_per_client", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100)
    .set_description("minimum number of capabilities a client may hold"),

    Option("mds_max_caps_per_client", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_M)
    .set_description("maximum number of capabilities a client may hold"),

    Option("mds_hack_allow_loading_invalid_metadata", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
     .set_default(0)
     .set_description("INTENTIONALLY CAUSE DATA LOSS by bypasing checks for invalid metadata on disk. Allows testing repair tools."),

    Option("mds_defer_session_stale", Option::TYPE_BOOL, Option::LEVEL_DEV)
     .set_default(true),

    Option("mds_inject_migrator_session_race", Option::TYPE_BOOL, Option::LEVEL_DEV)
     .set_default(false),

    Option("mds_request_load_average_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(60)
    .set_description("rate of decay in seconds for calculating request load average"),

    Option("mds_cap_revoke_eviction_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
     .set_default(0)
     .set_description("number of seconds after which clients which have not responded to cap revoke messages by the MDS are evicted."),

    Option("mds_max_retries_on_remount_failure", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
     .set_default(5)
     .set_description("number of consecutive failed remount attempts for invalidating kernel dcache after which client would abort."),

    Option("mds_dump_cache_threshold_formatter", Option::TYPE_SIZE, Option::LEVEL_DEV)
     .set_default(1_G)
     .set_description("threshold for cache usage to disallow \"dump cache\" operation to formatter")
     .set_long_description("Disallow MDS from dumping caches to formatter via \"dump cache\" command if cache usage exceeds this threshold."),

    Option("mds_dump_cache_threshold_file", Option::TYPE_SIZE, Option::LEVEL_DEV)
     .set_default(0)
     .set_description("threshold for cache usage to disallow \"dump cache\" operation to file")
     .set_long_description("Disallow MDS from dumping caches to file via \"dump cache\" command if cache usage exceeds this threshold."),

    Option("mds_task_status_update_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
     .set_default(2.0)
     .set_description("task status update interval to manager")
     .set_long_description("interval (in seconds) for sending mds task status to ceph manager"),

    Option("mds_max_snaps_per_dir", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
     .set_default(100)
     .set_min_max(0, 4096)
     .set_flag(Option::FLAG_RUNTIME)
     .set_description("max snapshots per directory")
     .set_long_description("maximum number of snapshots that can be created per directory"),

    Option("mds_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_min(1)
    .set_description("Size of thread pool for ASIO completions")
    .add_tag("mds"),

    Option("mds_ping_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
     .set_default(15)
     .set_flag(Option::FLAG_RUNTIME)
     .set_description("timeout after which an MDS is considered laggy by rank 0 MDS.")
     .set_long_description("timeout for replying to a ping message sent by rank 0 after which an active MDS considered laggy (delayed metrics) by rank 0."),

    Option("mds_ping_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
     .set_default(5)
     .set_flag(Option::FLAG_RUNTIME)
     .set_description("interval in seconds for sending ping messages to active MDSs.")
     .set_long_description("interval in seconds for rank 0 to send ping messages to all active MDSs."),

    Option("mds_metrics_update_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
     .set_default(2)
     .set_flag(Option::FLAG_RUNTIME)
     .set_description("interval in seconds for metrics data update.")
     .set_long_description("interval in seconds after which active MDSs send client metrics data to rank 0.")
  });
}

std::vector<Option> get_mds_client_options() {
  return std::vector<Option>({
    Option("client_cache_size", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_default(16384)
    .set_description("soft maximum number of directory entries in client cache"),

    Option("client_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(.75)
    .set_description("mid-point of client cache LRU"),

    Option("client_use_random_mds", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("issue new requests to a random active MDS"),

    Option("client_mount_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(300.0)
    .set_description("timeout for mounting CephFS (seconds)"),

    Option("client_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .set_description("seconds between client upkeep ticks"),

    Option("client_trace", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("")
    .set_description("file containing trace of client operations"),

    Option("client_readahead_min", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128*1024)
    .set_description("minimum bytes to readahead in a file"),

    Option("client_readahead_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("maximum bytes to readahead in a file (zero is unlimited)"),

    Option("client_readahead_max_periods", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .set_description("maximum stripe periods to readahead in a file"),

    Option("client_reconnect_stale", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("reconnect when the session becomes stale"),

    Option("client_snapdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".snap")
    .set_description("pseudo directory for snapshot access to a directory"),

    Option("client_mountpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/")
    .set_description("default mount-point"),

    Option("client_mount_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("uid to mount as"),

    Option("client_mount_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_description("gid to mount as"),

    /* RADOS client option */
    Option("client_notify_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(10)
    .set_description(""),

    /* RADOS client option */
    Option("osd_client_watch_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(30)
    .set_description(""),

    Option("client_caps_release_delay", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .set_description(""),

    Option("client_quota_df", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("show quota usage for statfs (df)"),

    Option("client_oc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("enable object caching"),

    Option("client_oc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(200_M)
    .set_description("maximum size of object cache"),

    Option("client_oc_max_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .set_description("maximum size of dirty pages in object cache"),

    Option("client_oc_target_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M)
    .set_description("target size of dirty pages object cache"),

    Option("client_oc_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0)
    .set_description("maximum age of dirty pages in object cache (seconds)"),

    Option("client_oc_max_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000)
    .set_description("maximum number of objects in cache"),

    Option("client_debug_getattr_caps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_debug_force_sync_read", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_debug_inject_tick_delay", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description(""),

    Option("client_max_inline_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K)
    .set_description(""),

    Option("client_inject_release_failure", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_inject_fixed_oldest_tid", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_metadata", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("metadata key=value comma-delimited pairs appended to session metadata"),

    Option("client_acl_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("")
    .set_description("ACL type to enforce (none or \"posix_acl\")"),

    Option("client_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("client-enforced permission checking"),

    Option("client_dirsize_rbytes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("set the directory size as the number of file bytes recursively used")
    .set_long_description("This option enables a CephFS feature that stores the recursive directory size (the bytes used by files in the directory and its descendents) in the st_size field of the stat structure."),

    Option("client_force_lazyio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description(""),

    // note: the max amount of "in flight" dirty data is roughly (max - target)
    Option("fuse_use_invalidate_cb", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("use fuse 2.8+ invalidate callback to keep page cache consistent"),

    Option("fuse_disable_pagecache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("disable page caching in the kernel for this FUSE mount"),

    Option("fuse_allow_other", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("pass allow_other to FUSE on mount"),

    Option("fuse_default_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("pass default_permisions to FUSE on mount")
    .set_flag(Option::FLAG_STARTUP),

    Option("fuse_big_writes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("big_writes is deprecated in libfuse 3.0.0"),

    Option("fuse_max_write", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_description("set the maximum number of bytes in a single write operation")
    .set_long_description("Set the maximum number of bytes in a single write operation that may pass atomically through FUSE. The FUSE default is 128kB and may be indicated by setting this option to 0."),

    Option("fuse_atomic_o_trunc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("pass atomic_o_trunc flag to FUSE on mount"),

    Option("fuse_debug", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_description("enable debugging for the libfuse"),

    Option("fuse_multithreaded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("allow parallel processing through FUSE library"),

    Option("fuse_require_active_mds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("require active MDSs in the file system when mounting"),

    Option("fuse_syncfs_on_mksnap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("synchronize all local metadata/file changes after snapshot"),

    Option("fuse_set_user_groups", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("check for ceph-fuse to consider supplementary groups for permissions"),

    Option("client_try_dentry_invalidate", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_die_on_failed_remount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_die_on_failed_dentry_invalidate", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("kill the client when no dentry invalidation options are available")
    .set_long_description("The CephFS client requires a mechanism to invalidate dentries in the caller (e.g. the kernel for ceph-fuse) when capabilities must be recalled. If the client cannot do this then the MDS cache cannot shrink which can cause the MDS to fail."),

    Option("client_check_pool_perm", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("confirm access to inode's data pool/namespace described in file layout"),

    Option("client_use_faked_inos", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description(""),

    Option("client_fs", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .set_default("")
    .set_description("CephFS file system name to mount")
    .set_long_description("Use this with ceph-fuse, or with any process "
        "that uses libcephfs.  Programs using libcephfs may also pass "
        "the filesystem name into mount(), which will override this setting. "
        "If no filesystem name is given in mount() or this setting, the default "
        "filesystem will be mounted (usually the first created)."),

    /* Alias for client_fs. Deprecated */
    Option("client_mds_namespace", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_flag(Option::FLAG_STARTUP)
    .set_default(""),

    Option("fake_statfs_for_testing", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_description("Set a value for kb and compute kb_used from total of num_bytes"),

    Option("debug_allow_any_pool_priority", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_description("Allow any pool priority to be set to test conversion to new range"),

    Option("client_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_min(1)
    .set_description("Size of thread pool for ASIO completions")
    .add_tag("client")
  });
}


static std::vector<Option> build_options()
{
  std::vector<Option> result = get_global_options();

  auto ingest = [&result](std::vector<Option>&& options, const char* svc) {
    for (auto &o : options) {
      o.add_service(svc);
      result.push_back(std::move(o));
    }
  };

  ingest(get_rgw_options(), "rgw");
  ingest(get_rbd_options(), "rbd");
  ingest(get_rbd_mirror_options(), "rbd-mirror");
  ingest(get_immutable_object_cache_options(), "immutable-objet-cache");
  ingest(get_mds_options(), "mds");
  ingest(get_mds_client_options(), "mds_client");

  return result;
}

const std::vector<Option> ceph_options = build_options();
