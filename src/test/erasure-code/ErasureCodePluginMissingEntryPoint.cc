#include "ceph_ver.h"

// missing int __erasure_code_init(char *plugin_name, char *directory) {}

extern "C" const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

