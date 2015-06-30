
#ifndef PORTING_H
#define PORTING_H 
#include "acconfig.h"


#if defined(DARWIN) 
#include <sys/socket.h>

/* O_LARGEFILE is not defined/required on OS X */
#define O_LARGEFILE 0

/* Wonder why this is missing */
#define PATH_MAX 1024

/* Could be relevant for other platforms */
#ifndef ERESTART
#define ERESTART EINTR
#endif

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
#  error "Cannot block SIGPIPE!"
# endif
#endif

#endif /* DARWIN */
#endif /* PORTING_H */
