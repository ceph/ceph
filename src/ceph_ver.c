
#include "acconfig.h"
#include "ceph_ver.h"

#define CONCAT_VER_SYMBOL(x) ceph_ver__##x

#define DEFINE_VER_SYMBOL(x) int CONCAT_VER_SYMBOL(x)

DEFINE_VER_SYMBOL(CEPH_GIT_VER);



