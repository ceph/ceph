// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_COPYUP_REQUEST_H
#define CEPH_LIBRBD_IO_COPYUP_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/buffer.h"
#include "common/Mutex.h"
#include "common/zipkin_trace.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/Types.h"

#include <string>
#include <vector>
#include <atomic>

namespace ZTracer { struct Trace; }

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;
template <typename I> class AbstractObjectWriteRequest;

template <typename ImageCtxT = librbd::ImageCtx>
class CopyupRequest {
public:
  static CopyupRequest* create(ImageCtxT *ictx, const std::string &oid,
                               uint64_t objectno, Extents &&image_extents,
                               const ZTracer::Trace &parent_trace) {
    return new CopyupRequest(ictx, oid, objectno, std::move(image_extents),
                             parent_trace);
  }

  CopyupRequest(ImageCtxT *ictx, const std::string &oid, uint64_t objectno,
                Extents &&image_extents, const ZTracer::Trace &parent_trace);
  ~CopyupRequest();

  void append_request(AbstractObjectWriteRequest<ImageCtxT> *req);

  void send();

  void complete(int r);

private:
  /**
   * Copyup requests go through the following state machine to read from the
   * parent image, update the object map, and copyup the object:
   *
   *
   * @verbatim
   *
   *              <start>
   *                 |
   *                 v
   *    . . .STATE_READ_FROM_PARENT. . .
   *    . .          |                 .
   *    . .          v                 .
   *    . .  STATE_OBJECT_MAP_HEAD     v (copy on read /
   *    . .          |                 .  no HEAD rev. update)
   *    v v          v                 .
   *    . .    STATE_OBJECT_MAP. . . . .
   *    . .          |
   *    . .          v
   *    . . . . > STATE_COPYUP
   *    .            |
   *    .            v
   *    . . . . > <finish>
   *
   * @endverbatim
   *
   * The _OBJECT_MAP state is skipped if the object map isn't enabled or if
   * an object map update isn't required. The _COPYUP state is skipped if
   * no data was read from the parent *and* there are no additional ops.
   */
  enum State {
    STATE_READ_FROM_PARENT,
    STATE_OBJECT_MAP_HEAD, // only update the HEAD revision
    STATE_OBJECT_MAP,      // update HEAD+snaps (if any)
    STATE_COPYUP
  };

  ImageCtx *m_ictx;
  std::string m_oid;
  uint64_t m_object_no;
  Extents m_image_extents;
  ZTracer::Trace m_trace;

  State m_state;
  bool m_flatten;
  ceph::bufferlist m_copyup_data;
  std::vector<AbstractObjectWriteRequest<ImageCtxT> *> m_pending_requests;
  unsigned m_pending_copyups = 0;

  AsyncOperation m_async_op;

  std::vector<uint64_t> m_snap_ids;

  Mutex m_lock;

  void complete_requests(int r);

  bool should_complete(int *r);

  void remove_from_list();
  void remove_from_list(Mutex &lock);

  bool send_object_map_head();
  bool send_object_map();
  bool send_copyup();
  bool is_copyup_required();
  bool is_update_object_map_required(int r);
  bool is_deep_copy() const;
};

} // namespace io
} // namespace librbd

extern template class librbd::io::CopyupRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_COPYUP_REQUEST_H
