// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "options.h"
#include "common/Formatter.h"
#include "common/options/build_options.h"

// Helpers for validators
#include "include/stringify.h"
#include "include/common_fwd.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <regex>

// Definitions for enums
#include "common/perf_counters.h"

// rbd feature and io operation validation
#include "librbd/Features.h"
#include "librbd/io/IoOperations.h"

using std::ostream;
using std::ostringstream;

using ceph::Formatter;
using ceph::parse_timespan;

namespace {
class printer {
  ostream& out;
public:
  explicit printer(ostream& os)
    : out(os) {}
  template<typename T>
  void operator()(const T& v) const {
    out << v;
  }
  void operator()(std::monostate) const {
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
  void operator()(const std::chrono::milliseconds v) const {
    out << v.count();
  }
};
}

ostream& operator<<(ostream& os, const Option::value_t& v) {
  printer p{os};
  std::visit(p, v);
  return os;
}

void Option::dump_value(const char *field_name,
    const Option::value_t &v, Formatter *f) const
{
  if (v == value_t{}) {
    // This should be nil but Formatter doesn't allow it.
    f->dump_string(field_name, "");
    return;
  }
  switch (type) {
  case TYPE_INT:
    f->dump_int(field_name, std::get<int64_t>(v)); break;
  case TYPE_UINT:
    f->dump_unsigned(field_name, std::get<uint64_t>(v)); break;
  case TYPE_STR:
    f->dump_string(field_name, std::get<std::string>(v)); break;
  case TYPE_FLOAT:
    f->dump_float(field_name, std::get<double>(v)); break;
  case TYPE_BOOL:
    f->dump_bool(field_name, std::get<bool>(v)); break;
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
  if (min != value_t{}) {
    if (new_value < min) {
      std::ostringstream oss;
      oss << "Value '" << new_value << "' is below minimum " << min;
      *err = oss.str();
      return -EINVAL;
    }
  }

  // Generic validation: max
  if (max != value_t{}) {
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
                           std::get<std::string>(new_value));
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
    int64_t f = strict_si_cast<int64_t>(val, error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = f;
  } else if (type == Option::TYPE_UINT) {
    uint64_t f = strict_si_cast<uint64_t>(val, error_message);
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
    bool b = strict_strtob(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    } else {
      *out = b;
    }
  } else if (type == Option::TYPE_ADDR) {
    entity_addr_t addr;
    if (!addr.parse(val)){
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
    Option::size_t sz{strict_iecstrtoll(val, error_message)};
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
  } else if (type == Option::TYPE_MILLISECS) {
    try {
      *out = std::chrono::milliseconds(std::stoull(val));
    } catch (const std::logic_error& e) {
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
  if (daemon_value != value_t{}) {
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
  if (min != value_t{}) {
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

    Option("qat_compressor_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable Intel QAT acceleration support for compression if available"),

    Option("plugin_crypto_accelerator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("crypto_isal")
    .set_description("Crypto accelerator library to use"),

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
    .set_default("cephfs hello journal lock log numops " "otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap")
    .set_description(""),

    Option("osd_class_default_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops " "otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap")
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
    .set_default("bitmap")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid"})
    .set_description(""),

    Option("bluefs_preextend_wal_files", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_description("Preextent rocksdb wal files on mkfs to avoid performance penalty"),

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
    .set_default(64_K)
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
    .set_default(65536)
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
    .set_default(128_K)
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
    .set_default(512_K)
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
    .set_default(512_K)
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
    .set_default("bitmap")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid"})
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
    .set_default("compression=kNoCompression,max_write_buffer_number=4,min_write_buffer_number_to_merge=1,recycle_log_file_num=4,write_buffer_size=268435456,writable_file_max_buffer_size=0,compaction_readahead_size=2097152,max_background_compactions=2")
    .set_description("Rocksdb options"),

    Option("bluestore_rocksdb_cf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .set_description("Enable use of rocksdb column families for bluestore metadata"),

    Option("bluestore_rocksdb_cfs", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("M= P= L=")
    .set_description("List of whitespace-separate key/value pairs where key is CF name and value is CF options"),

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

    // ----------------------------
    // Crimson specific options

    Option("crimson_osd_obc_lru_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_description("Number of obcs to cache")

  });
}

std::vector<Option> get_rgw_options() {
  return std::vector<Option>({
     Option("rgw_directory_address", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description("rgw directory address"),

    Option("rgw_directory_address2", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description("rgw directory address"),

    Option("rgw_directory_address3", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description("rgw directory address"),

    Option("rgw_directory_address4", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("127.0.0.1")
    .set_description("rgw directory address"),

    Option("rgw_directory_port", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7000)
    .set_description("rgw directory port"),

    Option("rgw_directory_port2", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7000)
    .set_description("rgw directory port"),

    Option("rgw_directory_port3", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7000)
    .set_description("rgw directory port"),

    Option("rgw_directory_port4", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(7000)
    .set_description("rgw directory port"),
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
  ingest(get_cephfs_shell_options(), "cephfs-shell");

  return result;
}

const std::vector<Option> ceph_options = build_options();
