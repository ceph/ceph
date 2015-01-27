// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OBJECT_MAP_H
#define CEPH_LIBRBD_OBJECT_MAP_H

#include "include/int_types.h"
#include "common/bit_vector.hpp"
#include <boost/optional.hpp>

namespace librbd {

class ImageCtx;

class ObjectMap {
public:

  ObjectMap(ImageCtx &image_ctx);

  int lock();
  int unlock();

  bool object_may_exist(uint64_t object_no) const;

  int refresh();
  int resize(uint8_t default_object_state);

  int update(uint64_t object_no, uint8_t new_state,
	     const boost::optional<uint8_t> &current_state);
  int update(uint64_t start_object_no, uint64_t end_object_no,
	     uint8_t new_state, const boost::optional<uint8_t> &current_state);

private:

  ImageCtx &m_image_ctx;

  ceph::BitVector<2> object_map;

  void invalidate();

};

} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_H
