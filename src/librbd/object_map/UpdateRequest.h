// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_UPDATE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_UPDATE_REQUEST_H

#include "include/int_types.h"
#include "librbd/object_map/Request.h"
#include "common/bit_vector.hpp"
#include "common/zipkin_trace.h"
#include "librbd/Utils.h"
#include <boost/optional.hpp>

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = librbd::ImageCtx>
class UpdateRequest : public Request {
public:
  static UpdateRequest *create(ImageCtx &image_ctx,
                               ceph::BitVector<2> *object_map,
                               uint64_t snap_id, uint64_t start_object_no,
                               uint64_t end_object_no, uint8_t new_state,
                               const boost::optional<uint8_t> &current_state,
                               const ZTracer::Trace &parent_trace,
                               Context *on_finish) {
    return new UpdateRequest(image_ctx, object_map, snap_id, start_object_no,
                             end_object_no, new_state, current_state,
                             parent_trace, on_finish);
  }

  UpdateRequest(ImageCtx &image_ctx, ceph::BitVector<2> *object_map,
                uint64_t snap_id, uint64_t start_object_no,
                uint64_t end_object_no, uint8_t new_state,
                const boost::optional<uint8_t> &current_state,
      	        const ZTracer::Trace &parent_trace, Context *on_finish)
    : Request(image_ctx, snap_id, on_finish), m_object_map(*object_map),
      m_start_object_no(start_object_no), m_end_object_no(end_object_no),
      m_update_start_object_no(start_object_no), m_new_state(new_state),
      m_current_state(current_state),
      m_trace(util::create_trace(image_ctx, "update object map", parent_trace))
  {
    m_trace.event("start");
  }
  virtual ~UpdateRequest() {
    m_trace.event("finish");
  }

  void send() override;

protected:
  void finish_request() override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |/------------------\
   *    v                   | (repeat in batches)
   * UPDATE_OBJECT_MAP -----/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ceph::BitVector<2> &m_object_map;
  uint64_t m_start_object_no;
  uint64_t m_end_object_no;
  uint64_t m_update_start_object_no;
  uint64_t m_update_end_object_no = 0;
  uint8_t m_new_state;
  boost::optional<uint8_t> m_current_state;
  ZTracer::Trace m_trace;

  void update_object_map();
  void handle_update_object_map(int r);

  void update_in_memory_object_map();

};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::UpdateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_UPDATE_REQUEST_H
