// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2002-2016 Free Software Foundation, Inc.
 * Copyright (C) 2019 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef IFADDRS_H
#define IFADDRS_H

#include "winsock_compat.h"
#include <ifdef.h>

struct ifaddrs {
  struct ifaddrs  *ifa_next;    /* Next item in list */
  char            *ifa_name;    /* Name of interface */
  unsigned int     ifa_flags;   /* Flags from SIOCGIFFLAGS */
  struct sockaddr *ifa_addr;    /* Address of interface */
  struct sockaddr *ifa_netmask; /* Netmask of interface */

  struct sockaddr_storage in_addrs;
  struct sockaddr_storage in_netmasks;

  char             ad_name[IF_MAX_STRING_SIZE];
  size_t           speed;
};

int getifaddrs(struct ifaddrs **ifap);
void freeifaddrs(struct ifaddrs *ifa);

#endif
