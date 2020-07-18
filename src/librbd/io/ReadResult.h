// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_READ_RESULT_H
#define CEPH_LIBRBD_IO_READ_RESULT_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "librbd/io/Types.h"
#include "osdc/Striper.h"
#include <sys/uio.h>
#include <boost/variant/variant.hpp>


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
    LightweightBufferExtents buffer_extents;

    bufferlist bl;
    Extents extent_map;

    C_ObjectReadRequest(AioCompletion *aio_completion, uint64_t object_off,
                        uint64_t object_len,
                        LightweightBufferExtents&& buffer_extents);

    void finish(int r) override;
  };

  ReadResult();
  ReadResult(char *buf, size_t buf_len);
  ReadResult(const struct iovec *iov, int iov_count);
  ReadResult(ceph::bufferlist *bl);
  ReadResult(std::map<uint64_t, uint64_t> *extent_map, ceph::bufferlist *bl);

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

  struct SparseBufferlist {
    std::map<uint64_t, uint64_t> *extent_map;
    ceph::bufferlist *bl;

    SparseBufferlist(std::map<uint64_t, uint64_t> *extent_map,
                     ceph::bufferlist *bl)
      : extent_map(extent_map), bl(bl) {
    }
  };

  typedef boost::variant<Empty,
                         Linear,
                         Vector,
                         Bufferlist,
                         SparseBufferlist> Buffer;
  struct SetClipLengthVisitor;
  struct AssembleResultVisitor;

  Buffer m_buffer;
  Striper::StripedReadResult m_destriper;

};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_READ_RESULT_H

