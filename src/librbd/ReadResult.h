// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_READ_RESULT_H
#define CEPH_LIBRBD_READ_RESULT_H

#include "include/int_types.h"
#include "include/buffer.h"
#include <sys/uio.h>
#include <boost/variant/variant.hpp>

namespace librbd {
namespace read_result {

struct Linear {
  char *buf;
  size_t buf_len;

  Linear() : buf(NULL), buf_len(0) {}
  Linear(char *_buf, size_t _buf_len) : buf(_buf), buf_len(_buf_len) {}
};

struct Vector {
  const struct iovec *iov;
  int iov_count;

  Vector() : iov(NULL), iov_count(0) {}
  Vector(const struct iovec *_iov, int _iov_count)
    : iov(_iov), iov_count(_iov_count) {}
};

struct Bufferlist {
  bufferlist *bl;

  Bufferlist() : bl(NULL) {}
  Bufferlist(bufferlist *_bl) : bl(_bl) {}
};

} // namespace read_result;

typedef boost::variant<read_result::Linear,
                       read_result::Vector,
                       read_result::Bufferlist> ReadResult;

} // namespace librbd

#endif // CEPH_LIBRBD_READ_RESULT_H
