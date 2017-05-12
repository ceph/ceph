// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "options.h"

Option _ceph_options[] = {

  // ** global basics **

  Option("host", Option::TYPE_STR, Option::LEVEL_BASIC)
  .set_description("local hostname")
  .set_long_description("if blank, ceph assumes the short hostname (hostname -s)")
  .add_tag("common"),

  Option("fsid", Option::TYPE_UUID, Option::LEVEL_BASIC)
  .set_description("cluster fsid (uuid)")
  .add_tag("common"),

  Option("public_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
  .set_description("public-facing address to bind to")
  .add_tag("mon mds osd mgr"),

  Option("cluster_addr", Option::TYPE_STR, Option::LEVEL_BASIC)
  .set_description("cluster-facing address to bind to")
  .add_tag("osd"),

  Option("monmap", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("path to MonMap file")
  .set_long_description("This option is normally used during mkfs, but can also "
			"be used to identify which monitors to connect to.")
  .add_tag("mon").add_tag("mkfs"),

  Option("mon_host", Option::TYPE_STR, Option::LEVEL_BASIC)
  .set_description("list of hosts or addresses to search for a monitor")
  .set_long_description("This is a comma, whitespace, or semicolon separated "
			"list of IP addresses or hostnames. Hostnames are "
			"resolved via DNS and all A or AAAA records are "
			"included in the search list.")
  .add_tag("common"),

  Option("mon_dns_srv_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("name of DNS SRV record to check for monitor addresses")
  .add_tag("common")
  .add_see_also("mon_host"),

  // lockdep
  Option("lockdep", Option::TYPE_BOOL, Option::LEVEL_DEV)
  .set_description("enable lockdep lock dependency analyzer")
  .add_tag("common"),

  Option("lockdep_force_backtrace", Option::TYPE_BOOL, Option::LEVEL_DEV)
  .set_description("always gather current backtrace at every lock")
  .add_tag("common")
  .add_see_also("lockdep"),

  Option("run_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_daemon_default("/var/run/ceph")
  .set_description("path for the 'run' directory for storing pid and socket files")
  .add_tag("common")
  .add_see_also("admin_socket"),

  Option("admin_socket", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_daemon_default("$run_dir/$cluster-$name.asok")
  .set_description("path for the runtime control socket file, used by the 'ceph daemon' command")
  .add_tag("common"),

  Option("admin_socket_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("file mode to set for the admin socket file, e.g, '0755'")
  .add_tag("common")
  .add_see_also("admin_socket"),

  Option("crushtool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("name of the 'crushtool' utility")
  .add_tag("mon"),

  // daemon
  Option("daemonize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
  .set_default(false)
  .set_daemon_default(true)
  .set_description("whether to daemonize (background) after startup")
  .add_tag("daemon")
  .add_see_also("pid_file").add_see_also("chdir"),

  Option("setuser", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("uid or user name to switch to on startup")
  .set_long_description("This is normally specified by the systemd unit file.")
  .add_tag("daemon")
  .add_see_also("setgroup"),

  Option("setgroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("gid or group name to switch to on startup")
  .set_long_description("This is normally specified by the systemd unit file.")
  .add_tag("daemon")
  .add_see_also("setuser"),

  Option("setuser_match_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("if set, setuser/setgroup is condition on this path matching ownership")
  .set_long_description("If setuser or setgroup are specified, and this option is non-empty, then the uid/gid of the daemon will only be changed if the file or directory specified by this option has a matching uid and/or gid.  This exists primarily to allow switching to user ceph for OSDs to be conditional on whether the osd data contents have also been chowned after an upgrade.  This is normally specified by the systemd unit file.")
  .add_tag("daemon").add_tag("osd")
  .add_see_also("setuser").add_see_also("setgroup"),

  Option("pid_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("path to write a pid file (if any)")
  .add_tag("daemon"),

  Option("chdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("path to chdir(2) to after daemonizing")
  .add_tag("daemon")
  .add_see_also("daemonize"),

  Option("fatal_signal_handlers", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
  .set_default(true)
  .set_description("whether to register signal handlers for SIGABRT etc that dump a stack trace")
  .set_long_description("This is normally true for daemons and values for libraries.")
  .add_tag("daemon"),

  // restapi
  Option("restapi_log_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("default set by python code"),

  Option("restapi_base_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_description("default set by python code"),



  Option("erasure_code_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
  .set_default(CEPH_PKGLIBDIR"/erasure-code")
  .set_description("directory where erasure-code plugins can be found")
  .add_tag("mon").add_tag("osd"),


  // logging
  Option("log_file", Option::TYPE_STR, Option::LEVEL_BASIC)
  .set_daemon_default("/var/log/ceph/$cluster-$name.log")
  .set_description("path to log file")
  .add_see_also("log_to_stderr")
  .add_see_also("err_to_stderr")
  .add_see_also("log_to_syslog")
  .add_see_also("err_to_syslog"),

  Option("log_max_new", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  .set_default(1000)
  .set_description("max unwritten log entries to allow before waiting to flush to the log")
  .add_see_also("log_max_recent"),

  Option("log_max_recent", Option::TYPE_INT, Option::LEVEL_ADVANCED)
  .set_daemon_default(10000)
  .set_default(500)
  .set_description("recent log entries to keep in memory to dump in the event of a crash")
  .set_long_description("The purpose of this option is to log at a higher debug level only to the in-memory buffer, and write out the detailed log messages only if there is a crash.  Only log entries below the lower log level will be written unconditionally to the log.  For example, debug_osd=1/5 will write everything <= 1 to the log unconditionally but keep entries at levels 2-5 in memory.  If there is a seg fault or assertion failure, all entries will be dumped to the log."),

  Option("log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
  .set_daemon_default(false)
  .set_default(true)
  .set_description("send log lines to stderr"),

  Option("err_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
  .set_daemon_default(true)
  .set_default(false)
  .set_description("send critical error log lines to stderr"),

  Option("log_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
  .set_default(false)
  .set_description("send log lines to syslog facility"),

  Option("err_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
  .set_default(false)
  .set_description("send critical error log lines to syslog facility"),

  Option("log_flush_on_exit", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
  .set_daemon_default(true)
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
  .add_see_also("err_to_graylog")
  .add_see_also("log_graylog_host")
  .add_see_also("log_graylog_port"),

  Option("err_to_graylog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
  .set_default(false)
  .set_description("send critical error log lines to remote graylog server")
  .add_see_also("log_to_graylog")
  .add_see_also("log_graylog_host")
  .add_see_also("log_graylog_port"),

  Option("log_graylog_host", Option::TYPE_STR, Option::LEVEL_BASIC)
  .set_default("127.0.0.1")
  .set_description("address or hostname of graylog server to log to")
  .add_see_also("log_to_graylog")
  .add_see_also("err_to_graylog")
  .add_see_also("log_graylog_port"),

  Option("log_graylog_port", Option::TYPE_INT, Option::LEVEL_BASIC)
  .set_default(12201)
  .set_description("port number for the remote graylog server")
  .add_see_also("log_graylog_host"),



  
  // ** end **
  Option("", Option::TYPE_INT, Option::LEVEL_BASIC)
};


Option *ceph_options = _ceph_options;
