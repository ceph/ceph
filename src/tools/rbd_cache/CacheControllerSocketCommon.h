// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CACHE_CONTROLLER_SOCKET_COMMON_H
#define CACHE_CONTROLLER_SOCKET_COMMON_H

#define RBDSC_REGISTER         0X11
#define RBDSC_READ             0X12
#define RBDSC_LOOKUP           0X13
#define RBDSC_REGISTER_REPLY   0X14
#define RBDSC_READ_REPLY       0X15
#define RBDSC_LOOKUP_REPLY     0X16
#define RBDSC_READ_RADOS       0X16

namespace rbd {
namespace cache {

typedef std::function<void(uint64_t, std::string)> ProcessMsg;
typedef std::function<void(std::string)> ClientProcessMsg;
typedef uint8_t rbdsc_req_type;
struct rbdsc_req_type_t {
  rbdsc_req_type type;
  uint64_t vol_size;
  uint64_t offset;
  uint64_t length;
  char pool_name[256];
  char vol_name[256];

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

} // namespace cache
} // namespace rbd
#endif
