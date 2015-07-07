// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include <utility>
#include <vector>

namespace librbd {

class AioCompletion;
class ImageCtx;

class AioImageRequest {
public:
  AioImageRequest(ImageCtx &image_ctx, AioCompletion *aio_comp)
    : m_image_ctx(image_ctx), m_aio_comp(aio_comp) {}
  virtual ~AioImageRequest() {}

  static void read(ImageCtx *ictx, AioCompletion *c,
                   const std::vector<std::pair<uint64_t,uint64_t> > &extents,
                   char *buf, bufferlist *pbl, int op_flags);
  static void read(ImageCtx *ictx, AioCompletion *c, uint64_t off, size_t len,
                   char *buf, bufferlist *pbl, int op_flags);
  static void write(ImageCtx *ictx, AioCompletion *c, uint64_t off, size_t len,
                    const char *buf, int op_flags);
  static void discard(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                      uint64_t len);
  static void flush(ImageCtx *ictx, AioCompletion *c);

  virtual bool is_write_op() const = 0;

  void send();
protected:
  ImageCtx &m_image_ctx;
  AioCompletion *m_aio_comp;

  virtual void execute_request() = 0;
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
               const std::vector<std::pair<uint64_t,uint64_t> > &image_extents,
               char *buf, bufferlist *pbl, int op_flags)
    : AioImageRequest(image_ctx, aio_comp), m_image_extents(image_extents),
      m_buf(buf), m_pbl(pbl), m_op_flags(op_flags) {
  }

  virtual bool is_write_op() const {
    return false;
  }
protected:
  virtual void execute_request();
  virtual const char *get_request_type() const {
    return "aio_read";
  }
private:
  std::vector<std::pair<uint64_t,uint64_t> > m_image_extents;
  char *m_buf;
  bufferlist *m_pbl;
  int m_op_flags;
};

class AioImageWrite : public AioImageRequest {
public:
  AioImageWrite(ImageCtx &image_ctx, AioCompletion *aio_comp, uint64_t off,
                size_t len, const char *buf, int op_flags)
    : AioImageRequest(image_ctx, aio_comp), m_off(off), m_len(len), m_buf(buf),
      m_op_flags(op_flags) {
  }

  virtual bool is_write_op() const {
    return true;
  }
protected:
  virtual void execute_request();
  virtual const char *get_request_type() const {
    return "aio_write";
  }
private:
  uint64_t m_off;
  uint64_t m_len;
  const char *m_buf;
  int m_op_flags;
};

class AioImageDiscard : public AioImageRequest {
public:
  AioImageDiscard(ImageCtx &image_ctx, AioCompletion *aio_comp, uint64_t off,
                  uint64_t len)
    : AioImageRequest(image_ctx, aio_comp), m_off(off), m_len(len) {
  }

  virtual bool is_write_op() const {
    return true;
  }
protected:
  virtual void execute_request();
  virtual const char *get_request_type() const {
    return "aio_discard";
  }
private:
  uint64_t m_off;
  uint64_t m_len;
};

class AioImageFlush : public AioImageRequest {
public:
  AioImageFlush(ImageCtx &image_ctx, AioCompletion *aio_comp)
    : AioImageRequest(image_ctx, aio_comp) {
  }

  virtual bool is_write_op() const {
    return false;
  }
protected:
  virtual void execute_request();
  virtual const char *get_request_type() const {
    return "aio_flush";
  }
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
