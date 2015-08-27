#ifndef CEPH_SOLARIS_ERRNO_H
#define CEPH_SOLARIS_ERRNO_H

#include "include/types.h"

__s32 translate_errno(CephContext *cct,__s32 e);

#endif
