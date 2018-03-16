// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ReadResult.h"
#include "include/buffer.h"
#include "common/dout.h"
#include "librbd/io/AioCompletion.h"
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ReadResult: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

struct ReadResult::SetClipLengthVisitor : public boost::static_visitor<void> {
  size_t length;

  SetClipLengthVisitor(size_t length) : length(length) {
  }

  void operator()(Linear &linear) const {
    assert(length <= linear.buf_len);
    linear.buf_len = length;
  }

  template <typename T>
  void operator()(T &t) const {
  }
};

struct ReadResult::AssembleResultVisitor : public boost::static_visitor<void> {
  CephContext *cct;
  Striper::StripedReadResult &destriper;

  AssembleResultVisitor(CephContext *cct, Striper::StripedReadResult &destriper)
    : cct(cct), destriper(destriper) {
  }

  void operator()(Empty &empty) const {
    ldout(cct, 20) << "dropping read result" << dendl;
  }

  void operator()(Linear &linear) const {
    ldout(cct, 20) << "copying resulting bytes to "
                   << reinterpret_cast<void*>(linear.buf) << dendl;
    destriper.assemble_result(cct, linear.buf, linear.buf_len);
  }

  void operator()(Vector &vector) const {
    bufferlist bl;
    destriper.assemble_result(cct, bl, true);

    ldout(cct, 20) << "copying resulting " << bl.length() << " bytes to iovec "
                   << reinterpret_cast<const void*>(vector.iov) << dendl;

    bufferlist::iterator it = bl.begin();
    size_t length = bl.length();
    size_t offset = 0;
    int idx = 0;
    for (; offset < length && idx < vector.iov_count; idx++) {
      size_t len = std::min(vector.iov[idx].iov_len, length - offset);
      it.copy(len, static_cast<char *>(vector.iov[idx].iov_base));
      offset += len;
    }
    assert(offset == bl.length());
  }

  void operator()(Bufferlist &bufferlist) const {
    bufferlist.bl->clear();
    destriper.assemble_result(cct, *bufferlist.bl, true);

    ldout(cct, 20) << "moved resulting " << bufferlist.bl->length() << " "
                   << "bytes to bl " << reinterpret_cast<void*>(bufferlist.bl)
                   << dendl;
  }
};

ReadResult::C_ImageReadRequest::C_ImageReadRequest(
    AioCompletion *aio_completion, const Extents image_extents)
  : aio_completion(aio_completion), image_extents(image_extents) {
  aio_completion->add_request();
}

void ReadResult::C_ImageReadRequest::finish(int r) {
  CephContext *cct = aio_completion->ictx->cct;
  ldout(cct, 10) << "C_ImageReadRequest: r=" << r
                 << dendl;
  if (r >= 0) {
    size_t length = 0;
    for (auto &image_extent : image_extents) {
      length += image_extent.second;
    }
    assert(length == bl.length());

    aio_completion->lock.Lock();
    aio_completion->read_result.m_destriper.add_partial_result(
      cct, bl, image_extents);
    aio_completion->lock.Unlock();
    r = length;
  }

  aio_completion->complete_request(r);
}

ReadResult::C_ObjectReadRequest::C_ObjectReadRequest(
    AioCompletion *aio_completion, uint64_t object_off, uint64_t object_len,
    Extents&& buffer_extents, bool ignore_enoent)
  : aio_completion(aio_completion), object_off(object_off),
    object_len(object_len), buffer_extents(std::move(buffer_extents)),
    ignore_enoent(ignore_enoent) {
  aio_completion->add_request();
}

void ReadResult::C_ObjectReadRequest::finish(int r) {
  CephContext *cct = aio_completion->ictx->cct;
  ldout(cct, 10) << "C_ObjectReadRequest: r=" << r
                 << dendl;

  if (ignore_enoent && r == -ENOENT) {
    r = 0;
  }
  if (r >= 0) {
    ldout(cct, 10) << " got " << extent_map
                   << " for " << buffer_extents
                   << " bl " << bl.length() << dendl;
    // handle the case where a sparse-read wasn't issued
    if (extent_map.empty()) {
      extent_map[object_off] = bl.length();
    }

    aio_completion->lock.Lock();
    aio_completion->read_result.m_destriper.add_partial_sparse_result(
      cct, bl, extent_map, object_off, buffer_extents);
    aio_completion->lock.Unlock();

    r = object_len;
  }

  aio_completion->complete_request(r);
}

ReadResult::ReadResult() : m_buffer(Empty()) {
}

ReadResult::ReadResult(char *buf, size_t buf_len)
  : m_buffer(Linear(buf, buf_len)) {
}

ReadResult::ReadResult(const struct iovec *iov, int iov_count)
  : m_buffer(Vector(iov, iov_count)) {
}

ReadResult::ReadResult(ceph::bufferlist *bl)
  : m_buffer(Bufferlist(bl)) {
}

void ReadResult::set_clip_length(size_t length) {
  boost::apply_visitor(SetClipLengthVisitor(length), m_buffer);
}

void ReadResult::assemble_result(CephContext *cct) {
  boost::apply_visitor(AssembleResultVisitor(cct, m_destriper), m_buffer);
}

} // namespace io
} // namespace librbd

