// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "common/snap_types.h"
#include "osd/osd_types.h"
#include "librbd/AioCompletion.h"
#include <list>
#include <utility>
#include <vector>

namespace librbd {

class AioCompletion;
class AioObjectRequestHandle;
class ImageCtx;

template <typename ImageCtxT = ImageCtx>
class AioImageRequest {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  virtual ~AioImageRequest() {}

  static void aio_read(ImageCtxT *ictx, AioCompletion *c,
                       Extents &&image_extents, char *buf, bufferlist *pbl,
                       int op_flags);
  static void aio_write(ImageCtxT *ictx, AioCompletion *c, uint64_t off,
                        size_t len, const char *buf, int op_flags);
  static void aio_write(ImageCtxT *ictx, AioCompletion *c,
                        Extents &&image_extents, bufferlist &&bl, int op_flags);
  static void aio_discard(ImageCtxT *ictx, AioCompletion *c, uint64_t off,
                          uint64_t len);
  static void aio_flush(ImageCtxT *ictx, AioCompletion *c);

  virtual bool is_write_op() const {
    return false;
  }

  void start_op() {
    m_aio_comp->start_op();
  }

  void send();
  void fail(int r);

  void set_bypass_image_cache() {
    m_bypass_image_cache = true;
  }

protected:
  typedef std::list<AioObjectRequestHandle *> AioObjectRequests;

  ImageCtxT &m_image_ctx;
  AioCompletion *m_aio_comp;
  Extents m_image_extents;
  bool m_bypass_image_cache = false;

  AioImageRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                  Extents &&image_extents)
    : m_image_ctx(image_ctx), m_aio_comp(aio_comp),
      m_image_extents(image_extents) {
  }

  virtual int clip_request();
  virtual void send_request() = 0;
  virtual void send_image_cache_request() = 0;

  virtual aio_type_t get_aio_type() const = 0;
  virtual const char *get_request_type() const = 0;
};

template <typename ImageCtxT = ImageCtx>
class AioImageRead : public AioImageRequest<ImageCtxT> {
public:
  using typename AioImageRequest<ImageCtxT>::Extents;

  AioImageRead(ImageCtxT &image_ctx, AioCompletion *aio_comp,
               Extents &&image_extents, char *buf, bufferlist *pbl,
               int op_flags)
    : AioImageRequest<ImageCtxT>(image_ctx, aio_comp, std::move(image_extents)),
      m_buf(buf), m_pbl(pbl), m_op_flags(op_flags) {
  }

protected:
  virtual void send_request() override;
  virtual void send_image_cache_request() override;

  virtual aio_type_t get_aio_type() const {
    return AIO_TYPE_READ;
  }
  virtual const char *get_request_type() const {
    return "aio_read";
  }
private:
  char *m_buf;
  bufferlist *m_pbl;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class AbstractAioImageWrite : public AioImageRequest<ImageCtxT> {
public:
  virtual bool is_write_op() const {
    return true;
  }

  inline void flag_synchronous() {
    m_synchronous = true;
  }

protected:
  using typename AioImageRequest<ImageCtxT>::AioObjectRequests;
  using typename AioImageRequest<ImageCtxT>::Extents;

  typedef std::vector<ObjectExtent> ObjectExtents;

  AbstractAioImageWrite(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                        Extents &&image_extents)
    : AioImageRequest<ImageCtxT>(image_ctx, aio_comp, std::move(image_extents)),
      m_synchronous(false) {
  }

  virtual void send_request();

  virtual void prune_object_extents(ObjectExtents &object_extents) {
  }
  virtual uint32_t get_object_cache_request_count(bool journaling) const {
    return 0;
  }
  virtual void send_object_cache_requests(const ObjectExtents &object_extents,
                                          uint64_t journal_tid) = 0;

  virtual void send_object_requests(const ObjectExtents &object_extents,
                                    const ::SnapContext &snapc,
                                    AioObjectRequests *aio_object_requests);
  virtual AioObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish) = 0;

  virtual uint64_t append_journal_event(const AioObjectRequests &requests,
                                        bool synchronous) = 0;
  virtual void update_stats(size_t length) = 0;

private:
  bool m_synchronous;
};

template <typename ImageCtxT = ImageCtx>
class AioImageWrite : public AbstractAioImageWrite<ImageCtxT> {
public:
  using typename AioImageRequest<ImageCtxT>::Extents;

  AioImageWrite(ImageCtxT &image_ctx, AioCompletion *aio_comp, uint64_t off,
                size_t len, const char *buf, int op_flags)
    : AbstractAioImageWrite<ImageCtxT>(image_ctx, aio_comp, {{off, len}}),
      m_op_flags(op_flags) {
    m_bl.append(buf, len);
  }
  AioImageWrite(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                Extents &&image_extents, bufferlist &&bl, int op_flags)
    : AbstractAioImageWrite<ImageCtxT>(image_ctx, aio_comp,
                                       std::move(image_extents)),
      m_bl(std::move(bl)), m_op_flags(op_flags) {
  }

protected:
  using typename AioImageRequest<ImageCtxT>::AioObjectRequests;
  using typename AbstractAioImageWrite<ImageCtxT>::ObjectExtents;

  virtual aio_type_t get_aio_type() const {
    return AIO_TYPE_WRITE;
  }
  virtual const char *get_request_type() const {
    return "aio_write";
  }

  void assemble_extent(const ObjectExtent &object_extent, bufferlist *bl);

  virtual void send_image_cache_request() override;

  virtual void send_object_cache_requests(const ObjectExtents &object_extents,
                                          uint64_t journal_tid);

  virtual void send_object_requests(const ObjectExtents &object_extents,
                                    const ::SnapContext &snapc,
                                    AioObjectRequests *aio_object_requests);
  virtual AioObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish);

  virtual uint64_t append_journal_event(const AioObjectRequests &requests,
                                        bool synchronous);
  virtual void update_stats(size_t length);
private:
  bufferlist m_bl;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class AioImageDiscard : public AbstractAioImageWrite<ImageCtxT> {
public:
  AioImageDiscard(ImageCtxT &image_ctx, AioCompletion *aio_comp, uint64_t off,
                  uint64_t len)
    : AbstractAioImageWrite<ImageCtxT>(image_ctx, aio_comp, {{off, len}}) {
  }

protected:
  using typename AioImageRequest<ImageCtxT>::AioObjectRequests;
  using typename AbstractAioImageWrite<ImageCtxT>::ObjectExtents;

  virtual aio_type_t get_aio_type() const {
    return AIO_TYPE_DISCARD;
  }
  virtual const char *get_request_type() const {
    return "aio_discard";
  }

  virtual void prune_object_extents(ObjectExtents &object_extents) override;

  virtual void send_image_cache_request() override;

  virtual uint32_t get_object_cache_request_count(bool journaling) const override;
  virtual void send_object_cache_requests(const ObjectExtents &object_extents,
                                          uint64_t journal_tid);

  virtual AioObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish);

  virtual uint64_t append_journal_event(const AioObjectRequests &requests,
                                        bool synchronous);
  virtual void update_stats(size_t length);
};

template <typename ImageCtxT = ImageCtx>
class AioImageFlush : public AioImageRequest<ImageCtxT> {
public:
  AioImageFlush(ImageCtxT &image_ctx, AioCompletion *aio_comp)
    : AioImageRequest<ImageCtxT>(image_ctx, aio_comp, {}) {
  }

  virtual bool is_write_op() const {
    return true;
  }

protected:
  using typename AioImageRequest<ImageCtxT>::AioObjectRequests;

  virtual int clip_request() {
    return 0;
  }
  virtual void send_request();
  virtual void send_image_cache_request() override;

  virtual aio_type_t get_aio_type() const {
    return AIO_TYPE_FLUSH;
  }
  virtual const char *get_request_type() const {
    return "aio_flush";
  }
};

} // namespace librbd

extern template class librbd::AioImageRequest<librbd::ImageCtx>;
extern template class librbd::AbstractAioImageWrite<librbd::ImageCtx>;
extern template class librbd::AioImageWrite<librbd::ImageCtx>;
extern template class librbd::AioImageDiscard<librbd::ImageCtx>;
extern template class librbd::AioImageFlush<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_H
