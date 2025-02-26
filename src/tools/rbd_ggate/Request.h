// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_GGATE_REQUEST_H
#define CEPH_RBD_GGATE_REQUEST_H

#include "ggate_drv.h"

namespace rbd {
namespace ggate {

struct Request {
  enum Command {
    Unknown = 0,
    Write = 1,
    Read = 2,
    Flush = 3,
    Discard = 4,
  };

  ggate_drv_req_t req;
  bufferlist bl;

  Request(ggate_drv_req_t req) : req(req) {
  }

  uint64_t get_id() {
    return ggate_drv_req_id(req);
  }

  Command get_cmd() {
    return static_cast<Command>(ggate_drv_req_cmd(req));
  }

  size_t get_length() {
    return ggate_drv_req_length(req);
  }

  uint64_t get_offset() {
    return ggate_drv_req_offset(req);
  }

  uint64_t get_error() {
    return ggate_drv_req_error(req);
  }

  void set_error(int error) {
    ggate_drv_req_set_error(req, error);
  }
};

} // namespace ggate
} // namespace rbd

#endif // CEPH_RBD_GGATE_REQUEST_H
