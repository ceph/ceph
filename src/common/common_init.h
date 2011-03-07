#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <stdint.h>
#include <string>
#include <vector>

enum {
  STARTUP_FLAG_INIT_KEYS =                      0x01,
  STARTUP_FLAG_FORCE_FG_LOGGING =               0x02,
  STARTUP_FLAG_DAEMON =                         0x04,
  STARTUP_FLAG_LIBRARY =                        0x08,
};

void common_init(std::vector<const char*>& args,
	  uint32_t module_type, int flags, std::string id = "admin");

#endif
