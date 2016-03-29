// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_RESIZE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_RESIZE_REQUEST_H

#include "include/int_types.h"
#include "librbd/object_map/Request.h"
#include "common/bit_vector.hpp"

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

class ResizeRequest : public Request {
public:
  ResizeRequest(ImageCtx &image_ctx, ceph::BitVector<2> *object_map,
                uint64_t snap_id, uint64_t new_size,
      	  uint8_t default_object_state, Context *on_finish)
    : Request(image_ctx, snap_id, on_finish), m_object_map(object_map),
      m_num_objs(0), m_new_size(new_size),
      m_default_object_state(default_object_state)
  {
  }

  static void resize(ceph::BitVector<2> *object_map, uint64_t num_objs,
                     uint8_t default_state);

  virtual void send();

protected:
  virtual void finish_request() override;

private:
  ceph::BitVector<2> *m_object_map;
  uint64_t m_num_objs;
  uint64_t m_new_size;
  uint8_t m_default_object_state;
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_RESIZE_REQUEST_H
