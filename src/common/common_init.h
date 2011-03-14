#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <stdint.h>
#include <string>
#include <vector>

#include "common/code_environment.h"

class md_config_t;
class CephInitParameters;

enum common_init_flags_t {
  // Set up defaults that make sense for an unprivileged deamon
  CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS = 0x1,
};

int keyring_init(md_config_t *conf);
md_config_t *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags);
void common_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env, int flags);

#endif
