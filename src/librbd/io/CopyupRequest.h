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

namespace ZTracer { struct Trace; }

namespace librbd {

struct ImageCtx;

namespace io {

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

  typedef std::vector<AbstractObjectWriteRequest<ImageCtxT> *> PendingRequests;

  ImageCtx *m_ictx;
  std::string m_oid;
  uint64_t m_object_no;
  Extents m_image_extents;
  ZTracer::Trace m_trace;

  bool m_deep_copy = false;
  bool m_flatten = false;
  bool m_copyup_required = true;

  ceph::bufferlist m_copyup_data;
  PendingRequests m_pending_requests;
  unsigned m_pending_copyups = 0;

  AsyncOperation m_async_op;

  std::vector<uint64_t> m_snap_ids;

  Mutex m_lock;

  void read_from_parent();
  void handle_read_from_parent(int r);

  void deep_copy();
  void handle_deep_copy(int r);

  void update_object_map_head();
  void handle_update_object_map_head(int r);

  void update_object_maps();
  void handle_update_object_maps(int r);

  void copyup();
  void handle_copyup(int r);

  void finish(int r);
  void complete_requests(int r);

  void remove_from_list();

  bool is_copyup_required();
  bool is_update_object_map_required(int r);
  bool is_deep_copy() const;

};

} // namespace io
} // namespace librbd

extern template class librbd::io::CopyupRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_COPYUP_REQUEST_H
