/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_SOCK_COMPAT_H
#define CEPH_SOCK_COMPAT_H

#include "include/compat.h"
#include <sys/socket.h>

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

int socket_cloexec(int domain, int type, int protocol);
int socketpair_cloexec(int domain, int type, int protocol, int sv[2]);
int accept_cloexec(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

#endif
