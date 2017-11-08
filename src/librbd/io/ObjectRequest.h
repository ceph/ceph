// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_REQUEST_H
#define CEPH_LIBRBD_IO_OBJECT_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "common/zipkin_trace.h"
#include "librbd/ObjectMap.h"
#include "librbd/io/Types.h"
#include <map>

class Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;
template <typename> class CopyupRequest;
template <typename> class ObjectRemoveRequest;
template <typename> class ObjectTruncateRequest;
template <typename> class ObjectWriteRequest;
template <typename> class ObjectZeroRequest;

struct ObjectRequestHandle {
  virtual ~ObjectRequestHandle() {
  }

  virtual void complete(int r) = 0;
  virtual void send() = 0;
};

/**
 * This class represents an I/O operation to a single RBD data object.
 * Its subclasses encapsulate logic for dealing with special cases
 * for I/O due to layering.
 */
template <typename ImageCtxT = ImageCtx>
class ObjectRequest : public ObjectRequestHandle {
public:
  static ObjectRequest* create_remove(ImageCtxT *ictx,
                                      const std::string &oid,
                                      uint64_t object_no,
                                      const ::SnapContext &snapc,
				      const ZTracer::Trace &parent_trace,
                                      Context *completion);
  static ObjectRequest* create_truncate(ImageCtxT *ictx,
                                        const std::string &oid,
                                        uint64_t object_no,
                                        uint64_t object_off,
                                        const ::SnapContext &snapc,
					const ZTracer::Trace &parent_trace,
                                        Context *completion);
  static ObjectRequest* create_trim(ImageCtxT *ictx, const std::string &oid,
                                    uint64_t object_no,
                                    const ::SnapContext &snapc,
                                    bool post_object_map_update,
                                    Context *completion);
  static ObjectRequest* create_write(ImageCtxT *ictx, const std::string &oid,
                                     uint64_t object_no,
                                     uint64_t object_off,
                                     const ceph::bufferlist &data,
                                     const ::SnapContext &snapc, int op_flags,
				     const ZTracer::Trace &parent_trace,
                                     Context *completion);
  static ObjectRequest* create_zero(ImageCtxT *ictx, const std::string &oid,
                                    uint64_t object_no, uint64_t object_off,
                                    uint64_t object_len,
                                    const ::SnapContext &snapc,
				    const ZTracer::Trace &parent_trace,
                                    Context *completion);
  static ObjectRequest* create_writesame(ImageCtxT *ictx,
                                         const std::string &oid,
                                         uint64_t object_no,
                                         uint64_t object_off,
                                         uint64_t object_len,
                                         const ceph::bufferlist &data,
                                         const ::SnapContext &snapc,
					 int op_flags,
					 const ZTracer::Trace &parent_trace,
                                         Context *completion);
  static ObjectRequest* create_compare_and_write(ImageCtxT *ictx,
                                                 const std::string &oid,
                                                 uint64_t object_no,
                                                 uint64_t object_off,
                                                 const ceph::bufferlist &cmp_data,
                                                 const ceph::bufferlist &write_data,
                                                 const ::SnapContext &snapc,
                                                 uint64_t *mismatch_offset, int op_flags,
                                                 const ZTracer::Trace &parent_trace,
                                                 Context *completion);

  ObjectRequest(ImageCtxT *ictx, const std::string &oid,
                uint64_t objectno, uint64_t off, uint64_t len,
                librados::snap_t snap_id, bool hide_enoent,
		const char *trace_name, const ZTracer::Trace &parent_trace,
		Context *completion);
  ~ObjectRequest() override {
    m_trace.event("finish");
  }

  static void add_write_hint(ImageCtxT& image_ctx,
                             librados::ObjectWriteOperation *wr);

  virtual void complete(int r);

  virtual bool should_complete(int r) = 0;
  void send() override = 0;

  bool has_parent() const {
    return m_has_parent;
  }

  virtual const char *get_op_type() const = 0;

protected:
  bool compute_parent_extents(Extents *parent_extents);

  ImageCtxT *m_ictx;
  std::string m_oid;
  uint64_t m_object_no, m_object_off, m_object_len;
  librados::snap_t m_snap_id;
  Context *m_completion;
  Extents m_parent_extents;
  bool m_hide_enoent;
  ZTracer::Trace m_trace;

  void async_finish(int r);
  void finish(int r);

private:
  bool m_has_parent = false;
};

template <typename ImageCtxT = ImageCtx>
class ObjectReadRequest : public ObjectRequest<ImageCtxT> {
public:
  typedef std::map<uint64_t, uint64_t> ExtentMap;

  static ObjectReadRequest* create(ImageCtxT *ictx, const std::string &oid,
                                   uint64_t objectno, uint64_t offset,
                                   uint64_t len, librados::snap_t snap_id,
                                   int op_flags, bool cache_initiated,
                                   const ZTracer::Trace &parent_trace,
                                   Context *completion) {
    return new ObjectReadRequest(ictx, oid, objectno, offset, len,
                                 snap_id, op_flags, cache_initiated,
                                 parent_trace, completion);
  }

  ObjectReadRequest(ImageCtxT *ictx, const std::string &oid,
                    uint64_t objectno, uint64_t offset, uint64_t len,
                    librados::snap_t snap_id, int op_flags,
                    bool cache_initiated, const ZTracer::Trace &parent_trace,
                    Context *completion);

  bool should_complete(int r) override;
  void send() override;

  inline uint64_t get_offset() const {
    return this->m_object_off;
  }
  inline uint64_t get_length() const {
    return this->m_object_len;
  }
  ceph::bufferlist &data() {
    return m_read_data;
  }
  ExtentMap &get_extent_map() {
    return m_ext_map;
  }

  const char *get_op_type() const override {
    return "read";
  }

private:
  /**
   * @verbatim
   *
   *           <start>
   *              |
   *              |
   *    /--------/ \--------\
   *    |                   |
   *    | (cache            | (cache
   *    v  disabled)        v  enabled)
   * READ_OBJECT      READ_CACHE
   *    |                   |
   *    |/------------------/
   *    |
   *    v (skip if not needed)
   * READ_PARENT
   *    |
   *    v (skip if not needed)
   * COPYUP
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  int m_op_flags;
  bool m_cache_initiated;

  ceph::bufferlist m_read_data;
  ExtentMap m_ext_map;

  void read_cache();
  void handle_read_cache(int r);

  void read_object();
  void handle_read_object(int r);

  void read_parent();
  void handle_read_parent(int r);

  void copyup();
};

template <typename ImageCtxT = ImageCtx>
class AbstractObjectWriteRequest : public ObjectRequest<ImageCtxT> {
public:
  AbstractObjectWriteRequest(ImageCtxT *ictx, const std::string &oid,
                             uint64_t object_no, uint64_t object_off,
                             uint64_t len, const ::SnapContext &snapc,
			     bool hide_enoent, const char *trace_name,
			     const ZTracer::Trace &parent_trace,
                             Context *completion);

  virtual bool is_empty_write_op() const {
    return false;
  }

  virtual uint8_t get_pre_write_object_map_state() const {
    return OBJECT_EXISTS;
  }

  virtual void add_copyup_ops(librados::ObjectWriteOperation *wr) {
    add_write_ops(wr);
  }

  void handle_copyup(int r);

  bool should_complete(int r) override;
  void send() override;

  /**
   * Writes go through the following state machine to deal with
   * layering and the object map:
   *
   *   <start>
   *      |
   *      |\
   *      | \       -or-
   *      |  ---------------------------------> LIBRBD_AIO_WRITE_PRE
   *      |                          .                            |
   *      |                          .                            |
   *      |                          .                            v
   *      |                          . . .  . > LIBRBD_AIO_WRITE_FLAT. . .
   *      |                                                       |      .
   *      |                                                       |      .
   *      |                                                       |      .
   *      v                need copyup   (copyup performs pre)    |      .
   * LIBRBD_AIO_WRITE_GUARD -----------> LIBRBD_AIO_WRITE_COPYUP  |      .
   *  .       |                               |        .          |      .
   *  .       |                               |        .          |      .
   *  .       |                         /-----/        .          |      .
   *  .       |                         |              .          |      .
   *  .       \-------------------\     |     /-------------------/      .
   *  .                           |     |     |        .                 .
   *  .                           v     v     v        .                 .
   *  .                       LIBRBD_AIO_WRITE_POST    .                 .
   *  .                               |                .                 .
   *  .                               |  . . . . . . . .                 .
   *  .                               |  .                               .
   *  .                               v  v                               .
   *  . . . . . . . . . . . . . . > <finish> < . . . . . . . . . . . . . .
   *
   * The _PRE/_POST states are skipped if the object map is disabled.
   * The write starts in _WRITE_GUARD or _FLAT depending on whether or not
   * there is a parent overlap.
   */
protected:
  enum write_state_d {
    LIBRBD_AIO_WRITE_GUARD,
    LIBRBD_AIO_WRITE_COPYUP,
    LIBRBD_AIO_WRITE_FLAT,
    LIBRBD_AIO_WRITE_PRE,
    LIBRBD_AIO_WRITE_POST,
    LIBRBD_AIO_WRITE_ERROR
  };

  write_state_d m_state;
  librados::ObjectWriteOperation m_write;
  uint64_t m_snap_seq;
  std::vector<librados::snap_t> m_snaps;
  bool m_object_may_exist = false;
  bool m_guard = true;

  virtual void add_write_hint(librados::ObjectWriteOperation *wr);
  virtual void add_write_ops(librados::ObjectWriteOperation *wr) = 0;

  virtual void guard_write();
  virtual bool post_object_map_update() {
    return false;
  }
  virtual void send_write();
  virtual void send_write_op();
  virtual void handle_write_guard();

  void send_pre_object_map_update();

private:
  bool send_post_object_map_update();
  void send_copyup();
};

template <typename ImageCtxT = ImageCtx>
class ObjectWriteRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectWriteRequest(ImageCtxT *ictx, const std::string &oid,
                     uint64_t object_no, uint64_t object_off,
                     const ceph::bufferlist &data, const ::SnapContext &snapc,
                     int op_flags, const ZTracer::Trace &parent_trace,
                     Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, object_off,
                                            data.length(), snapc, false,
                                            "write", parent_trace, completion),
      m_write_data(data), m_op_flags(op_flags) {
  }

  bool is_empty_write_op() const override {
    return (m_write_data.length() == 0);
  }

  const char *get_op_type() const override {
    return "write";
  }

protected:
  void add_write_ops(librados::ObjectWriteOperation *wr) override;

  void send_write() override;

private:
  ceph::bufferlist m_write_data;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class ObjectRemoveRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectRemoveRequest(ImageCtxT *ictx, const std::string &oid,
                      uint64_t object_no, const ::SnapContext &snapc,
		      const ZTracer::Trace &parent_trace, Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, 0, 0, snapc,
                                            true, "remove", parent_trace,
                                            completion) {
    if (this->has_parent()) {
      m_object_state = OBJECT_EXISTS;
    } else {
      m_object_state = OBJECT_PENDING;
    }
  }

  const char* get_op_type() const override {
    if (this->has_parent()) {
      return "remove (trunc)";
    }
    return "remove";
  }

  uint8_t get_pre_write_object_map_state() const override {
    return m_object_state;
  }

protected:
  void add_write_hint(librados::ObjectWriteOperation *wr) override {
    // no hint for remove
  }

  void add_write_ops(librados::ObjectWriteOperation *wr) override {
    if (this->has_parent()) {
      wr->truncate(0);
    } else {
      wr->remove();
    }
  }

  bool post_object_map_update() override {
    if (m_object_state == OBJECT_EXISTS) {
      return false;
    }
    return true;
  }

  void guard_write() override;
  void send_write() override;

private:
  uint8_t m_object_state;
};

template <typename ImageCtxT = ImageCtx>
class ObjectTrimRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  // we'd need to only conditionally specify if a post object map
  // update is needed. pre update is decided as usual (by checking
  // the state of the object in the map).
  ObjectTrimRequest(ImageCtxT *ictx, const std::string &oid, uint64_t object_no,
                    const ::SnapContext &snapc, bool post_object_map_update,
		    Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, 0, 0, snapc,
                                            true, "trim", {}, completion),
      m_post_object_map_update(post_object_map_update) {
  }

  const char* get_op_type() const override {
    return "remove (trim)";
  }

  uint8_t get_pre_write_object_map_state() const override {
    return OBJECT_PENDING;
  }

protected:
  void add_write_hint(librados::ObjectWriteOperation *wr) override {
    // no hint for remove
  }

  void add_write_ops(librados::ObjectWriteOperation *wr) override {
    wr->remove();
  }

  bool post_object_map_update() override {
    return m_post_object_map_update;
  }

private:
  bool m_post_object_map_update;
};

template <typename ImageCtxT = ImageCtx>
class ObjectTruncateRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectTruncateRequest(ImageCtxT *ictx, const std::string &oid,
                        uint64_t object_no, uint64_t object_off,
                        const ::SnapContext &snapc,
			const ZTracer::Trace &parent_trace, Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, object_off, 0,
                                            snapc, true, "truncate",
                                            parent_trace, completion) {
  }

  const char* get_op_type() const override {
    return "truncate";
  }

  uint8_t get_pre_write_object_map_state() const override {
    if (!this->m_object_may_exist && !this->has_parent())
      return OBJECT_NONEXISTENT;
    return OBJECT_EXISTS;
  }

protected:
  void add_write_hint(librados::ObjectWriteOperation *wr) override {
    // no hint for truncate
  }

  void add_write_ops(librados::ObjectWriteOperation *wr) override {
    wr->truncate(this->m_object_off);
  }

  void send_write() override;
};

template <typename ImageCtxT = ImageCtx>
class ObjectZeroRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectZeroRequest(ImageCtxT *ictx, const std::string &oid, uint64_t object_no,
                    uint64_t object_off, uint64_t object_len,
                    const ::SnapContext &snapc,
		    const ZTracer::Trace &parent_trace, Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, object_off,
                                            object_len, snapc, true, "zero",
                                            parent_trace, completion) {
  }

  const char* get_op_type() const override {
    return "zero";
  }

protected:
  void add_write_ops(librados::ObjectWriteOperation *wr) override {
    wr->zero(this->m_object_off, this->m_object_len);
  }

  void send_write() override;
};

template <typename ImageCtxT = ImageCtx>
class ObjectWriteSameRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectWriteSameRequest(ImageCtxT *ictx, const std::string &oid,
			 uint64_t object_no, uint64_t object_off,
			 uint64_t object_len, const ceph::bufferlist &data,
                         const ::SnapContext &snapc, int op_flags,
			 const ZTracer::Trace &parent_trace,
			 Context *completion)
    : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, object_off,
                                            object_len, snapc, false,
                                            "writesame", parent_trace,
                                            completion),
      m_write_data(data), m_op_flags(op_flags) {
  }

  const char *get_op_type() const override {
    return "writesame";
  }

protected:
  void add_write_ops(librados::ObjectWriteOperation *wr) override;

  void send_write() override;

private:
  ceph::bufferlist m_write_data;
  int m_op_flags;
};

template <typename ImageCtxT = ImageCtx>
class ObjectCompareAndWriteRequest : public AbstractObjectWriteRequest<ImageCtxT> {
public:
  ObjectCompareAndWriteRequest(ImageCtxT *ictx, const std::string &oid,
                               uint64_t object_no, uint64_t object_off,
                               const ceph::bufferlist &cmp_bl,
                               const ceph::bufferlist &write_bl,
                               const ::SnapContext &snapc,
                               uint64_t *mismatch_offset, int op_flags,
                               const ZTracer::Trace &parent_trace,
                               Context *completion)
   : AbstractObjectWriteRequest<ImageCtxT>(ictx, oid, object_no, object_off,
                                           cmp_bl.length(), snapc, false,
                                           "compare_and_write", parent_trace,
                                           completion),
    m_cmp_bl(cmp_bl), m_write_bl(write_bl),
    m_mismatch_offset(mismatch_offset), m_op_flags(op_flags) {
  }

  const char *get_op_type() const override {
    return "compare_and_write";
  }

  void complete(int r) override;

protected:
  void add_write_ops(librados::ObjectWriteOperation *wr) override;

  void send_write() override;

private:
  ceph::bufferlist m_cmp_bl;
  ceph::bufferlist m_write_bl;
  uint64_t *m_mismatch_offset;
  int m_op_flags;
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ObjectRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectReadRequest<librbd::ImageCtx>;
extern template class librbd::io::AbstractObjectWriteRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectWriteRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectRemoveRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectTrimRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectTruncateRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectZeroRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectWriteSameRequest<librbd::ImageCtx>;
extern template class librbd::io::ObjectCompareAndWriteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_OBJECT_REQUEST_H
