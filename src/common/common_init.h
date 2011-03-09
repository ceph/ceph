#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <stdint.h>
#include <string>
#include <vector>

#include "common/code_environment.h"

class md_config_t;
class CephInitParameters;

int keyring_init(md_config_t *conf);
md_config_t *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env);
void common_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env);

#endif
