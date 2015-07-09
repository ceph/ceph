// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/snap_types.h"
#include "osd/osd_types.h"
#include <utility>
#include <vector>

namespace librbd {

class AioCompletion;
class ImageCtx;

class AioImageRequest {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  virtual ~AioImageRequest() {}

  static void aio_read(ImageCtx *ictx, AioCompletion *c,
                       const std::vector<std::pair<uint64_t,uint64_t> > &extents,
                       char *buf, bufferlist *pbl, int op_flags);
  static void aio_read(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                       size_t len, char *buf, bufferlist *pbl, int op_flags);
  static void aio_write(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                        size_t len, const char *buf, int op_flags);
  static void aio_discard(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                          uint64_t len);
  static void aio_flush(ImageCtx *ictx, AioCompletion *c);

  virtual bool is_write_op() const {
    return false;
  }

  void send();

protected:
  ImageCtx &m_image_ctx;
  AioCompletion *m_aio_comp;

  AioImageRequest(ImageCtx &image_ctx, AioCompletion *aio_comp)
    : m_image_ctx(image_ctx), m_aio_comp(aio_comp) {}

  virtual void send_request() = 0;
  virtual const char *get_request_type() const = 0;
};

class AioImageRead : public AioImageRequest {
public:
  AioImageRead(ImageCtx &image_ctx, AioCompletion *aio_comp, uint64_t off,
               size_t len, char *buf, bufferlist *pbl, int op_flags)
    : AioImageRequest(image_ctx, aio_comp), m_buf(buf), m_pbl(pbl),
      m_op_flags(op_flags) {
    m_image_extents.push_back(std::make_pair(off, len));
  }

  AioImageRead(ImageCtx &image_ctx, AioCompletion *aio_comp,
               const Extents &image_extents, char *buf, bufferlist *pbl,
               int op_flags)
    : AioImageRequest(image_ctx, aio_comp), m_image_extents(image_extents),
      m_buf(buf), m_pbl(pbl), m_op_flags(op_flags) {
  }

protected:
  virtual void send_request();
  virtual const char *get_request_type() const {
    return "aio_read";
  }
private:
  Extents m_image_extents;
  char *m_buf;
  bufferlist *m_pbl;
  int m_op_flags;
};

class AbstractAioImageWrite : public AioImageRequest {
public:
  virtual bool is_write_op() const {
    return true;
  }

protected:
  typedef std::vector<ObjectExtent> ObjectExtents;

  AbstractAioImageWrite(ImageCtx &image_ctx, AioCompletion *aio_comp,
                        uint64_t off, size_t len)
    : AioImageRequest(image_ctx, aio_comp), m_off(off), m_len(len) {
  }

  virtual void send_request();

  virtual void send_object_requests(const ObjectExtents &object_extents,
                                    const ::SnapContext &snapc);
  virtual void send_object_request(const ObjectExtent &object_extent,
                                   const ::SnapContext &snapc) = 0;
  virtual void update_stats(size_t length) = 0;

private:
  uint64_t m_off;
  size_t m_len;
};

class AioImageWrite : public AbstractAioImageWrite {
public:
  AioImageWrite(ImageCtx &image_ctx, AioCompletion *aio_comp, uint64_t off,
                size_t len, const char *buf, int op_flags)
    : AbstractAioImageWrite(image_ctx, aio_comp, off, len), m_buf(buf),
      m_op_flags(op_flags) {
  }

protected:
  virtual const char *get_request_type() const {
    return "aio_write";
  }

  virtual void send_object_request(const ObjectExtent &object_extent,
                                   const ::SnapContext &snapc);
  virtual void update_stats(size_t length);
private:
  const char *m_buf;
  int m_op_flags;
};

class AioImageDiscard : public AbstractAioImageWrite {
public:
  AioImageDiscard(ImageCtx &image_ctx, AioCompletion *aio_comp, uint64_t off,
                  uint64_t len)
    : AbstractAioImageWrite(image_ctx, aio_comp, off, len) {
  }

protected:
  virtual const char *get_request_type() const {
    return "aio_discard";
  }

  virtual void send_object_requests(const ObjectExtents &object_extents,
                                    const ::SnapContext &snapc);
  virtual void send_object_request(const ObjectExtent &object_extent,
                                   const ::SnapContext &snapc);
  virtual void update_stats(size_t length);
};

class AioImageFlush : public AioImageRequest {
public:
  AioImageFlush(ImageCtx &image_ctx, AioCompletion *aio_comp)
    : AioImageRequest(image_ctx, aio_comp) {
  }

protected:
  virtual void send_request();
  virtual const char *get_request_type() const {
    return "aio_flush";
  }
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
