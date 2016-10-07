// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_BATCH_UPDATE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_BATCH_UPDATE_REQUEST_H

#include "include/int_types.h"
#include "librbd/object_map/Request.h"
#include "common/bit_vector.hpp"
#include <boost/optional.hpp>
#include "librbd/ObjectMap.h"

class Context;

namespace librbd {

class ImageCtx;
class ObjectMapView;

namespace object_map {

template<typename ImageCtxT = ImageCtx>
class BatchUpdateRequest {
public:
  BatchUpdateRequest(ImageCtxT &image_ctx, ObjectMapView *object_map,
                     uint64_t snap_id, Context *on_finish);

  void send();

private:
  ImageCtx &m_image_ctx;
  ObjectMapView *m_object_map;
  uint64_t m_snap_id;
  Context *m_on_finish;

  CephContext *m_cct;
  uint8_t m_view_idx = 0;
  ObjectMapView m_object_map_dup;

  void switch_thread_context();
  void handle_swtich_thread_context(int r);

  void update_object_map();
  Context *handle_update_object_map(int *result);

  void finish();
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::BatchUpdateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_BATCH_UPDATE_REQUEST_H
