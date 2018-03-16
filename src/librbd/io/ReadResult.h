// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_READ_RESULT_H
#define CEPH_LIBRBD_IO_READ_RESULT_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "librbd/io/Types.h"
#include "osdc/Striper.h"
#include <sys/uio.h>
#include <boost/variant/variant.hpp>

struct CephContext;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;
template <typename> struct ObjectReadRequest;

class ReadResult {
public:
  struct C_ImageReadRequest : public Context {
    AioCompletion *aio_completion;
    Extents image_extents;
    bufferlist bl;

    C_ImageReadRequest(AioCompletion *aio_completion,
                       const Extents image_extents);

    void finish(int r) override;
  };

  struct C_ObjectReadRequest : public Context {
    AioCompletion *aio_completion;
    uint64_t object_off;
    uint64_t object_len;
    Extents buffer_extents;
    bool ignore_enoent;

    bufferlist bl;
    ExtentMap extent_map;

    C_ObjectReadRequest(AioCompletion *aio_completion, uint64_t object_off,
                        uint64_t object_len, Extents&& buffer_extents,
                        bool ignore_enoent);

    void finish(int r) override;
  };

  ReadResult();
  ReadResult(char *buf, size_t buf_len);
  ReadResult(const struct iovec *iov, int iov_count);
  ReadResult(ceph::bufferlist *bl);

  void set_clip_length(size_t length);
  void assemble_result(CephContext *cct);

private:
  struct Empty {
  };

  struct Linear {
    char *buf;
    size_t buf_len;

    Linear(char *buf, size_t buf_len) : buf(buf), buf_len(buf_len) {
    }
  };

  struct Vector {
    const struct iovec *iov;
    int iov_count;

    Vector(const struct iovec *iov, int iov_count)
      : iov(iov), iov_count(iov_count) {
    }
  };

  struct Bufferlist {
    ceph::bufferlist *bl;

    Bufferlist(ceph::bufferlist *bl) : bl(bl) {
    }
  };

  typedef boost::variant<Empty,
                         Linear,
                         Vector,
                         Bufferlist> Buffer;
  struct SetClipLengthVisitor;
  struct AssembleResultVisitor;

  Buffer m_buffer;
  Striper::StripedReadResult m_destriper;

};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_READ_RESULT_H

