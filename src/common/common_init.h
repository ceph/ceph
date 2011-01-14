#ifndef CEPH_COMMON_INIT_H
#define CEPH_COMMON_INIT_H

#include <vector>

void common_set_defaults(bool daemon);
void common_init(std::vector<const char*>& args,
		 const char *module_type,
                 bool init_keys);
void set_no_logging();
void set_foreground_logging();

#endif
