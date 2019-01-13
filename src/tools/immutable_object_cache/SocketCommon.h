// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SOCKET_COMMON_H
#define CEPH_CACHE_SOCKET_COMMON_H

#include "include/types.h"
#include "include/int_types.h"

namespace ceph {
namespace immutable_obj_cache {

static const int RBDSC_REGISTER        =  0X11;
static const int RBDSC_READ            =  0X12;
static const int RBDSC_LOOKUP          =  0X13;
static const int RBDSC_REGISTER_REPLY  =  0X14;
static const int RBDSC_READ_REPLY      =  0X15;
static const int RBDSC_LOOKUP_REPLY    =  0X16;
static const int RBDSC_READ_RADOS      =  0X17;



typedef std::function<void(uint64_t, std::string)> ProcessMsg;
typedef std::function<void(std::string)> ClientProcessMsg;
typedef uint8_t rbdsc_req_type;

//TODO(): switch to bufferlist
struct rbdsc_req_type_t {
  rbdsc_req_type type;
  uint64_t vol_size;
  uint64_t offset;
  uint64_t length;
  char pool_name[256];
  char vol_name[256];
  char oid[256];

  uint64_t size() {
    return sizeof(rbdsc_req_type_t);
  }

  std::string to_buffer() {
    std::stringstream ss;
    ss << type;
    ss << vol_size;
    ss << offset;
    ss << length;
    ss << pool_name;
    ss << vol_name;

    return ss.str();
  }
};

static const int RBDSC_MSG_LEN = sizeof(rbdsc_req_type_t);

} // namespace immutable_obj_cache
} // namespace ceph
#endif
