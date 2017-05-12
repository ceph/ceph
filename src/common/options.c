// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/acconfig.h"
#include "options.h"

struct ceph_option _ceph_options[] = {
  {
    .name = "host",
    .type = OPT_STR,
    .description = "local hostname; if blank, we will use the short hostname (hostname -s)",
    .tags = "common",
  },
  {
    .name = "fsid",
    .type = OPT_UUID,
    .description = "cluster fsid (uuid)",
    .tags = "common",
  },
  {
    .name = "public_addr",
    .type = OPT_STR,
    .description = "public-facing address to bind to",
    .tags = "mon mds osd mgr",
  },
  {
    .name = "cluster_addr",
    .type = OPT_STR,
    .description = "cluster-facing address to bind to",
    .tags = "osd",
  },
  {
    .name = "monmap",
    .type = OPT_STR,
    .description = "path to MonMap file",
    .long_description = "This option is normally used during mkfs.",
    .tags = "mon mkfs",
  },
  {
    .name = "mon_host",
    .type = OPT_STR,
    .description = "list of hosts or addresses to search for a monitor",
    .tags = "common",
  },
  {
    .name = "mon_dns_srv_name",
    .type = OPT_STR,
    .description = "name of DNS SRV record to check for monitor addresses",
    .tags = "common",
    .see_also = "mon_host",
  },

  {
    .name = "lockdep",
    .type = OPT_BOOL,
    .value = "false",
    .level = OPT_DEV,
    .description = "enable lockdep lock dependency analyzer",
    .tags = "common",
  },
  {
    .name = "lockdep_force_backtrace",
    .type = OPT_BOOL,
    .level = OPT_DEV,
    .description = "always gather current backtrace at every lock",
    .tags = "common",
    .see_also = "lockdep",
  },

  {
    .name = "run_dir",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .daemon_value = "/var/run/ceph",
    .description = "path for the 'run' directory for storing pid and socket files",
    .tags = "common",
    .see_also = "admin_socket",
  },
  {
    .name = "admin_socket",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .daemon_value = "$run_dir/$cluster-$name.asok",
    .description = "path for the runtime control socket file, used by the 'ceph daemon' command",
    .tags = "common",
  },
  {
    .name = "admin_socket_mode",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "file mode to set for the admin socket file, e.g, '0755'",
    .tags = "common",
    .see_also = "admin_socket",
  },
  {
    .name = "crushtool",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "name of crushtool utility",
    .tags = "mon",
  },

  {
    .name = "daemonize",
    .type = OPT_BOOL,
    .level = OPT_ADVANCED,
    .description = "whether to daemonize on startup",
    .value = "false",
    .daemon_value = "true",
    .tags = "daemon",
    .see_also = "pid_file chdir",
  },
  {
    .name = "setuser",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "uid or user name to switch to on startup",
    .long_description = "This is normally specified by the systemd unit file.",
    .tags = "daemon",
    .see_also = "setgroup",
  },
  {
    .name = "setgroup",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "gid or group name to switch to on startup",
    .long_description = "This is normally specified by the systemd unit file.",
    .tags = "daemon",
    .see_also = "setuser",
  },
  {
    .name = "setuser_match_path",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "if set, setuser/setgroup is condition on this path matching ownership",
    .long_description = "If setuser or setgroup are specified, and this option is non-empty, then the uid/gid of the daemon will only be changed if the file or directory specified by this option has a matching uid and/or gid.  This exists primarily to allow switching to user ceph for OSDs to be conditional on whether the osd data contents have also been chowned after an upgrade.  This is normally specified by the systemd unit file.",
    .tags = "daemon osd",
    .see_also = "setuser setgroup",
  },
  {
    .name = "pid_file",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "path to write a pid file (if any)",
    .tags = "daemon",
  },
  {
    .name = "chdir",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "path to chdir(2) to after daemonizing",
    .tags = "daemon",
    .see_also = "daemonize",
  },

  {
    .name = "restapi_log_level",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "default set by python code",
  },
  {
    .name = "restapi_base_url",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .description = "default set by python code",
  },

  {
    .name = "fatal_signal_handlers",
    .type = OPT_BOOL,
    .level = OPT_ADVANCED,
    .value = "true",
    .description = "whether to register signal handlers for SIGABRT etc that dump a stack trace",
    .long_description = "This is normally true for daemons and values for libraries.",
    .tags = "daemon",
  },

  {
    .name = "erasure_code_dir",
    .type = OPT_STR,
    .level = OPT_ADVANCED,
    .value = CEPH_PKGLIBDIR"/erasure-code",
    .description = "directory where erasure-code plugins can be found",
    .tags = "mon osd",
  },

  // -------------
  // internal logging facility
  {
    .name = "log_file",
    .type = OPT_STR,
    .level = OPT_BASIC,
    .daemon_value = "/var/log/ceph/$cluster-$name.log",
    .description = "path to log file",
    .see_also = "log_to_stderr err_to_stderr log_to_syslog err_to_syslog",
  },
  {
    .name = "log_max_new",
    .type = OPT_INT,
    .level = OPT_ADVANCED,
    .value = "1000",
    .description = "max unwritten log entries to allow before waiting to flush to the log",
    .see_also = "log_max_recent",
  },
  {
    .name = "log_max_recent",
    .type = OPT_INT,
    .level = OPT_BASIC,
    .daemon_value = "10000",
    .nondaemon_value = "500",
    .description = "recent log entries to keep in memory to dump in the event of a crash",
    .long_description = "The purpose of this option is to log at a higher debug level only to the in-memory buffer, and write out the detailed log messages only if there is a crash.  Only log entries below the lower log level will be written unconditionally to the log.  For example, debug_osd=1/5 will write everything <= 1 to the log unconditionally but keep entries at levels 2-5 in memory.  If there is a seg fault or assertion failure, all entries will be dumped to the log.",
  },
  {
    .name = "log_to_stderr",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .description = "send log lines to stderr",
    .daemon_value = "false",
    .nondaemon_value = "true",
  },
  {
    .name = "err_to_stderr",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .description = "send critical error log lines to stderr",
    .daemon_value = "true",
    .nondaemon_value = "false",
  },
  {
    .name = "log_to_syslog",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .value = "false",
    .description = "send log lines to syslog facility",
  },
  {
    .name = "err_to_syslog",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .value = "false",
    .description = "send critical error log lines to syslog facility",
  },
  {
    .name = "log_flush_on_exit",
    .type = OPT_BOOL,
    .level = OPT_ADVANCED,
    .description = "set a process exit handler to ensure the log is flushed on exit",
    .nondaemon_value = "false",
    .daemon_value = "true",
  },
  {
    .name = "log_stop_at_utilization",
    .type = OPT_FLOAT,
    .level = OPT_BASIC,
    .description = "stop writing to the log file when device utilization reaches this ratio",
    .min_value = "0.0",
    .max_value = "1.0",
    .value = ".97",
    .see_also = "log_file",
  },
  {
    .name = "log_to_graylog",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .value = "false",
    .description = "send log lines to remote graylog server",
    .see_also = "err_to_graylog log_graylog_host log_graylog_port",
  },
  {
    .name = "err_to_graylog",
    .type = OPT_BOOL,
    .level = OPT_BASIC,
    .value = "false",
    .description = "send critical error log lines to remote graylog server",
    .see_also = "log_to_graylog log_graylog_host log_graylog_port",
  },
  {
    .name = "log_graylog_host",
    .type = OPT_STR,
    .level = OPT_BASIC,
    .value = "127.0.0.1",
    .description = "address or hostname of graylog server to log to",
    .see_also = "log_to_graylog err_to_graylog log_graylog_port",
  },
  {
    .name = "log_graylog_port",
    .type = OPT_INT,
    .level = OPT_BASIC,
    .value = "12201",
    .description = "port number for the remote graylog server",
    .see_also = "log_graylog_host",
  },

  // cluster log facility





  {
    // *** the last entry (name) is empty to mark the end of the list ***
  }
};

struct ceph_option *ceph_options = _ceph_options;
