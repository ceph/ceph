#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <vector>

enum {
  STARTUP_FLAG_INIT_KEYS =                      0x01,
  STARTUP_FLAG_FORCE_FG_LOGGING =               0x02,
  STARTUP_FLAG_DAEMON =                         0x04,
};

void common_init(std::vector<const char*>& args,
		 const char *module_type,
                 int flags);
void set_foreground_logging();

#endif
