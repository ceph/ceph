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

/*
 * On BSD SO_NOSIGPIPE can be set via setsockopt to block SIGPIPE.
 */
#ifndef MSG_NOSIGNAL
# define MSG_NOSIGNAL 0
# ifdef SO_NOSIGPIPE
#  define CEPH_USE_SO_NOSIGPIPE
# else
#  define CEPH_USE_SIGPIPE_BLOCKER
#  warning "Using SIGPIPE blocking instead of suppression; this is not well-tested upstream!"
# endif
#endif

#endif
