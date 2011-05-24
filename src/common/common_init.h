#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <deque>
#include <stdint.h>
#include <string>
#include <vector>

#include "common/code_environment.h"

class CephContext;
class CephInitParameters;

enum common_init_flags_t {
  // Set up defaults that make sense for an unprivileged deamon
  CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS = 0x1,

  // By default, don't read a configuration file
  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE = 0x2,

  // Don't close stderr
  CINIT_FLAG_NO_CLOSE_STDERR = 0x4,
};

CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags);
void complain_about_parse_errors(std::deque<std::string> *parse_errors);
void common_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env, int flags);
void output_ceph_version();
int common_init_shutdown_stderr();
void common_init_daemonize(const CephContext *cct, int flags);
int common_init_shutdown_stderr(void);
void common_init_finish(CephContext *cct);

#endif
