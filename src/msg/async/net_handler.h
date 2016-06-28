// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_NET_UTILS_H
#define CEPH_COMMON_NET_UTILS_H
#include "common/config.h"

namespace ceph {
  class NetHandler {
   private:
    int create_socket(int domain, bool reuse_addr=false);
    int generic_connect(const entity_addr_t& addr, bool nonblock);

    CephContext *cct;
   public:
    explicit NetHandler(CephContext *c): cct(c) {}
    int set_nonblock(int sd);
    void set_socket_options(int sd);
    int connect(const entity_addr_t &addr);
    
    /**
     * Try to reconnect the socket.
     *
     * @return    0         success
     *            > 0       just break, and wait for event
     *            < 0       need to goto fail
     */
    int reconnect(const entity_addr_t &addr, int sd);
    int nonblock_connect(const entity_addr_t &addr);
    void set_priority(int sd, int priority);
  };
}

#endif
