#ifndef CEPH_SOCK_COMPAT_H
#define CEPH_SOCK_COMPAT_H

#include "include/compat.h"

/*
 * This optimization may not be available on all platforms (e.g. OSX).
 * Apparently a similar approach based on TCP_CORK can be used.
 */
#ifndef MSG_MORE
# define MSG_MORE 0
#endif

#endif
