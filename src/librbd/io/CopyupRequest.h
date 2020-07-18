// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_COPYUP_REQUEST_H
#define CEPH_LIBRBD_IO_COPYUP_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/ceph_mutex.h"
#include "common/zipkin_trace.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/Types.h"

#include <map>
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
  static CopyupRequest* create(ImageCtxT *ictx, uint64_t objectno,
                               Extents &&image_extents,
                               const ZTracer::Trace &parent_trace) {
    return new CopyupRequest(ictx, objectno, std::move(image_extents),
                             parent_trace);
  }

  CopyupRequest(ImageCtxT *ictx, uint64_t objectno, Extents &&image_extents,
                const ZTracer::Trace &parent_trace);
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
   *      /---------/ \---------\
   *      |                     |
   *      v                     v
   *  READ_FROM_PARENT      DEEP_COPY
   *      |                     |
   *      \---------\ /---------/
   *                 |
   *                 v (skip if not needed)
   *         UPDATE_OBJECT_MAPS
   *                 |
   *                 v (skip if not needed)
   *              COPYUP
   *                 |
   *                 v
   *              <finish>
   *
   * @endverbatim
   *
   * The OBJECT_MAP state is skipped if the object map isn't enabled or if
   * an object map update isn't required. The COPYUP state is skipped if
   * no data was read from the parent *and* there are no additional ops.
   */

  typedef std::vector<AbstractObjectWriteRequest<ImageCtxT> *> WriteRequests;

  ImageCtxT *m_image_ctx;
  uint64_t m_object_no;
  Extents m_image_extents;
  ZTracer::Trace m_trace;

  bool m_flatten = false;
  bool m_copyup_required = true;
  bool m_copyup_is_zero = true;

  std::map<uint64_t, uint64_t> m_copyup_extent_map;
  ceph::bufferlist m_copyup_data;

  AsyncOperation m_async_op;

  std::vector<uint64_t> m_snap_ids;
  bool m_first_snap_is_clean = false;

  ceph::mutex m_lock = ceph::make_mutex("CopyupRequest", false);
  WriteRequests m_pending_requests;
  unsigned m_pending_copyups = 0;
  int m_copyup_ret_val = 0;

  WriteRequests m_restart_requests;
  bool m_append_request_permitted = true;

  void read_from_parent();
  void handle_read_from_parent(int r);

  void deep_copy();
  void handle_deep_copy(int r);

  void update_object_maps();
  void handle_update_object_maps(int r);

  void copyup();
  void handle_copyup(int r);

  void finish(int r);
  void complete_requests(bool override_restart_retval, int r);

  void disable_append_requests();
  void remove_from_list();

  bool is_copyup_required();
  bool is_update_object_map_required(int r);
  bool is_deep_copy() const;

  void compute_deep_copy_snap_ids();
};

} // namespace io
} // namespace librbd

extern template class librbd::io::CopyupRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_COPYUP_REQUEST_H
