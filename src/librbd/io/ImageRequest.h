// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_REQUEST_H
#define CEPH_LIBRBD_IO_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "common/snap_types.h"
#include "common/zipkin_trace.h"
#include "osd/osd_types.h"
#include "librbd/Utils.h"
#include "librbd/io/Types.h"
#include <list>
#include <utility>
#include <vector>

namespace librbd {
class ImageCtx;

namespace io {

class AioCompletion;
class ObjectRequestHandle;
class ReadResult;

template <typename ImageCtxT = ImageCtx>
class ImageRequest {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  virtual ~ImageRequest() {
    m_trace.event("finish");
  }

  static ImageRequest* create_read_request(ImageCtxT &image_ctx,
                                           AioCompletion *aio_comp,
                                           Extents &&image_extents,
                                           ReadResult &&read_result,
                                           int op_flags,
                                           const ZTracer::Trace &parent_trace);
  static ImageRequest* create_write_request(ImageCtxT &image_ctx,
                                            AioCompletion *aio_comp,
                                            Extents &&image_extents,
                                            bufferlist &&bl, int op_flags,
                                            const ZTracer::Trace &parent_trace);
  static ImageRequest* create_discard_request(ImageCtxT &image_ctx,
                                              AioCompletion *aio_comp,
                                              uint64_t off, uint64_t len,
                                              bool skip_partial_discard,
                                              const ZTracer::Trace &parent_trace);
  static ImageRequest* create_flush_request(ImageCtxT &image_ctx,
                                            AioCompletion *aio_comp,
                                            const ZTracer::Trace &parent_trace);
  static ImageRequest* create_writesame_request(ImageCtxT &image_ctx,
                                                AioCompletion *aio_comp,
                                                uint64_t off, uint64_t len,
                                                bufferlist &&bl, int op_flags,
                                                const ZTracer::Trace &parent_trace);

  static void aio_read(ImageCtxT *ictx, AioCompletion *c,
                       Extents &&image_extents, ReadResult &&read_result,
                       int op_flags, const ZTracer::Trace &parent_trace);
  static void aio_write(ImageCtxT *ictx, AioCompletion *c,
                        Extents &&image_extents, bufferlist &&bl, int op_flags,
			const ZTracer::Trace &parent_trace);
  static void aio_discard(ImageCtxT *ictx, AioCompletion *c, uint64_t off,
                          uint64_t len, bool skip_partial_discard,
			  const ZTracer::Trace &parent_trace);
  static void aio_flush(ImageCtxT *ictx, AioCompletion *c,
			const ZTracer::Trace &parent_trace);
  static void aio_writesame(ImageCtxT *ictx, AioCompletion *c, uint64_t off,
                            uint64_t len, bufferlist &&bl, int op_flags,
			    const ZTracer::Trace &parent_trace);

  virtual bool is_write_op() const {
    return false;
  }

  void start_op();

  void send();
  void fail(int r);

  void set_bypass_image_cache() {
    m_bypass_image_cache = true;
  }

  inline const ZTracer::Trace &get_trace() const {
    return m_trace;
  }

protected:
  typedef std::list<ObjectRequestHandle *> ObjectRequests;

  ImageCtxT &m_image_ctx;
  AioCompletion *m_aio_comp;
  Extents m_image_extents;
  ZTracer::Trace m_trace;
  bool m_bypass_image_cache = false;

  ImageRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
               Extents &&image_extents, const char *trace_name,
	       const ZTracer::Trace &parent_trace)
    : m_image_ctx(image_ctx), m_aio_comp(aio_comp),
      m_image_extents(std::move(image_extents)),
      m_trace(util::create_trace(image_ctx, trace_name, parent_trace)) {
    m_trace.event("start");
  }
  

  virtual int clip_request();
  virtual void send_request() = 0;
  virtual void send_image_cache_request() = 0;

  virtual aio_type_t get_aio_type() const = 0;
  virtual const char *get_request_type() const = 0;
};

template <typename ImageCtxT = ImageCtx>
class ImageReadRequest : public ImageRequest<ImageCtxT> {
public:
  using typename ImageRequest<ImageCtxT>::Extents;

  ImageReadRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                   Extents &&image_extents, ReadResult &&read_result,
                   int op_flags, const ZTracer::Trace &parent_trace);

protected:
  int clip_request() override;

  void send_request() override;
  void send_image_cache_request() override;

  aio_type_t get_aio_type() const override {
    return AIO_TYPE_READ;
  }
  const char *get_request_type() const override {
    return "aio_read";
  }
private:
  char *m_buf;
  bufferlist *m_pbl;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class AbstractImageWriteRequest : public ImageRequest<ImageCtxT> {
public:
  bool is_write_op() const override {
    return true;
  }

  inline void flag_synchronous() {
    m_synchronous = true;
  }

protected:
  using typename ImageRequest<ImageCtxT>::ObjectRequests;
  using typename ImageRequest<ImageCtxT>::Extents;

  typedef std::vector<ObjectExtent> ObjectExtents;

  AbstractImageWriteRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                            Extents &&image_extents, const char *trace_name,
			    const ZTracer::Trace &parent_trace)
    : ImageRequest<ImageCtxT>(image_ctx, aio_comp, std::move(image_extents),
			      trace_name, parent_trace),
      m_synchronous(false) {
  }

  void send_request() override;

  virtual void prune_object_extents(ObjectExtents &object_extents) {
  }
  virtual uint32_t get_object_cache_request_count(bool journaling) const {
    return 0;
  }
  virtual void send_object_cache_requests(const ObjectExtents &object_extents,
                                          uint64_t journal_tid) = 0;

  virtual void send_object_requests(const ObjectExtents &object_extents,
                                    const ::SnapContext &snapc,
                                    ObjectRequests *object_requests);
  virtual ObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish) = 0;

  virtual uint64_t append_journal_event(const ObjectRequests &requests,
                                        bool synchronous) = 0;
  virtual void update_stats(size_t length) = 0;

private:
  bool m_synchronous;
};

template <typename ImageCtxT = ImageCtx>
class ImageWriteRequest : public AbstractImageWriteRequest<ImageCtxT> {
public:
  using typename ImageRequest<ImageCtxT>::Extents;

  ImageWriteRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                    Extents &&image_extents, bufferlist &&bl, int op_flags,
		    const ZTracer::Trace &parent_trace)
    : AbstractImageWriteRequest<ImageCtxT>(
	image_ctx, aio_comp, std::move(image_extents), "write", parent_trace),
      m_bl(std::move(bl)), m_op_flags(op_flags) {
  }

protected:
  using typename ImageRequest<ImageCtxT>::ObjectRequests;
  using typename AbstractImageWriteRequest<ImageCtxT>::ObjectExtents;

  aio_type_t get_aio_type() const override {
    return AIO_TYPE_WRITE;
  }
  const char *get_request_type() const override {
    return "aio_write";
  }

  void assemble_extent(const ObjectExtent &object_extent, bufferlist *bl);

  void send_image_cache_request() override;

  void send_object_cache_requests(const ObjectExtents &object_extents,
                                  uint64_t journal_tid) override;

  void send_object_requests(const ObjectExtents &object_extents,
                            const ::SnapContext &snapc,
                            ObjectRequests *aio_object_requests) override;

  ObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish) override;

  uint64_t append_journal_event(const ObjectRequests &requests,
                                bool synchronous) override;
  void update_stats(size_t length) override;

private:
  bufferlist m_bl;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class ImageDiscardRequest : public AbstractImageWriteRequest<ImageCtxT> {
public:
  ImageDiscardRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                      uint64_t off, uint64_t len, bool skip_partial_discard,
		      const ZTracer::Trace &parent_trace)
    : AbstractImageWriteRequest<ImageCtxT>(
	image_ctx, aio_comp, {{off, len}}, "discard", parent_trace),
      m_skip_partial_discard(skip_partial_discard) {
  }

protected:
  using typename ImageRequest<ImageCtxT>::ObjectRequests;
  using typename AbstractImageWriteRequest<ImageCtxT>::ObjectExtents;

  aio_type_t get_aio_type() const override {
    return AIO_TYPE_DISCARD;
  }
  const char *get_request_type() const override {
    return "aio_discard";
  }

  void prune_object_extents(ObjectExtents &object_extents) override;

  void send_image_cache_request() override;

  uint32_t get_object_cache_request_count(bool journaling) const override;
  void send_object_cache_requests(const ObjectExtents &object_extents,
                                  uint64_t journal_tid) override;

  ObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish) override;

  uint64_t append_journal_event(const ObjectRequests &requests,
                                bool synchronous) override;
  void update_stats(size_t length) override;
private:
  bool m_skip_partial_discard;
};

template <typename ImageCtxT = ImageCtx>
class ImageFlushRequest : public ImageRequest<ImageCtxT> {
public:
  ImageFlushRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
		    const ZTracer::Trace &parent_trace)
    : ImageRequest<ImageCtxT>(image_ctx, aio_comp, {}, "flush", parent_trace) {
  }

  bool is_write_op() const override {
    return true;
  }

protected:
  using typename ImageRequest<ImageCtxT>::ObjectRequests;

  int clip_request() override {
    return 0;
  }
  void send_request() override;
  void send_image_cache_request() override;

  aio_type_t get_aio_type() const override {
    return AIO_TYPE_FLUSH;
  }
  const char *get_request_type() const override {
    return "aio_flush";
  }
};

template <typename ImageCtxT = ImageCtx>
class ImageWriteSameRequest : public AbstractImageWriteRequest<ImageCtxT> {
public:
  ImageWriteSameRequest(ImageCtxT &image_ctx, AioCompletion *aio_comp,
                        uint64_t off, uint64_t len, bufferlist &&bl,
                        int op_flags, const ZTracer::Trace &parent_trace)
    : AbstractImageWriteRequest<ImageCtxT>(
	image_ctx, aio_comp, {{off, len}}, "writesame", parent_trace),
      m_data_bl(std::move(bl)), m_op_flags(op_flags) {
  }

protected:
  using typename ImageRequest<ImageCtxT>::ObjectRequests;
  using typename AbstractImageWriteRequest<ImageCtxT>::ObjectExtents;

  aio_type_t get_aio_type() const override {
    return AIO_TYPE_WRITESAME;
  }
  const char *get_request_type() const override {
    return "aio_writesame";
  }

  bool assemble_writesame_extent(const ObjectExtent &object_extent,
                                 bufferlist *bl, bool force_write);

  void send_image_cache_request() override;

  void send_object_cache_requests(const ObjectExtents &object_extents,
                                  uint64_t journal_tid) override;

  void send_object_requests(const ObjectExtents &object_extents,
                            const ::SnapContext &snapc,
                            ObjectRequests *object_requests) override;
  ObjectRequestHandle *create_object_request(
      const ObjectExtent &object_extent, const ::SnapContext &snapc,
      Context *on_finish) override;

  uint64_t append_journal_event(const ObjectRequests &requests,
                                bool synchronous) override;
  void update_stats(size_t length) override;
private:
  bufferlist m_data_bl;
  int m_op_flags;
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageRequest<librbd::ImageCtx>;
extern template class librbd::io::ImageReadRequest<librbd::ImageCtx>;
extern template class librbd::io::AbstractImageWriteRequest<librbd::ImageCtx>;
extern template class librbd::io::ImageWriteRequest<librbd::ImageCtx>;
extern template class librbd::io::ImageDiscardRequest<librbd::ImageCtx>;
extern template class librbd::io::ImageFlushRequest<librbd::ImageCtx>;
extern template class librbd::io::ImageWriteSameRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_REQUEST_H
