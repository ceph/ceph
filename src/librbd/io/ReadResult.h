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
namespace io {

struct AioCompletion;
template <typename> struct ObjectReadRequest;

class ReadResult {
private:
  struct C_ReadRequest : public Context {
    AioCompletion *aio_completion;
    bufferlist bl;

    C_ReadRequest(AioCompletion *aio_completion);

    void finish(int r) override;
  };

public:

  struct C_ImageReadRequest : public C_ReadRequest {
    Extents image_extents;

    C_ImageReadRequest(AioCompletion *aio_completion,
                       const Extents image_extents)
      : C_ReadRequest(aio_completion), image_extents(image_extents) {
    }

    void finish(int r) override;
  };

  struct C_SparseReadRequestBase : public C_ReadRequest {
    C_SparseReadRequestBase(AioCompletion *aio_completion)
      : C_ReadRequest(aio_completion) {
    }

    using C_ReadRequest::finish;
    void finish(ExtentMap &extent_map, const Extents &buffer_extents,
                uint64_t offset, size_t length, bufferlist &bl, int r);
  };

  template <typename ImageCtxT>
  struct C_SparseReadRequest : public C_SparseReadRequestBase {
    ObjectReadRequest<ImageCtxT> *request;

    C_SparseReadRequest(AioCompletion *aio_completion)
      : C_SparseReadRequestBase(aio_completion) {
    }

    void finish(int r) override {
      C_SparseReadRequestBase::finish(request->get_extent_map(),
                                      request->get_buffer_extents(),
                                      request->get_offset(),
                                      request->get_length(), request->data(),
                                      r);
    }
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

