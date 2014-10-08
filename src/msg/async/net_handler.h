// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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
    NetHandler(CephContext *c): cct(c) {}
    int set_nonblock(int sd);
    void set_socket_options(int sd);
    int connect(const entity_addr_t &addr);
    int nonblock_connect(const entity_addr_t &addr);
  };
}

#endif
