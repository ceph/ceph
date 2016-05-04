// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OBJECT_MAP_H
#define CEPH_LIBRBD_OBJECT_MAP_H

#include "include/int_types.h"
#include "include/fs_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/object_map_types.h"
#include "common/bit_vector.hpp"
#include <boost/optional.hpp>

class Context;
class RWLock;

namespace librbd {

class ImageCtx;

class ObjectMap {
public:
  ObjectMap(ImageCtx &image_ctx, uint64_t snap_id);

  static int remove(librados::IoCtx &io_ctx, const std::string &image_id);
  static std::string object_map_name(const std::string &image_id,
				     uint64_t snap_id);

  static bool is_compatible(const file_layout_t& layout, uint64_t size);

  ceph::BitVector<2u>::Reference operator[](uint64_t object_no);
  uint8_t operator[](uint64_t object_no) const;
  inline uint64_t size() const {
    return m_object_map.size();
  }

  void open(Context *on_finish);
  void close(Context *on_finish);

  bool object_may_exist(uint64_t object_no) const;

  void aio_save(Context *on_finish);
  void aio_resize(uint64_t new_size, uint8_t default_object_state,
		  Context *on_finish);
  bool aio_update(uint64_t object_no, uint8_t new_state,
		  const boost::optional<uint8_t> &current_state,
		  Context *on_finish);
  bool aio_update(uint64_t start_object_no, uint64_t end_object_no,
		  uint8_t new_state,
		  const boost::optional<uint8_t> &current_state,
		  Context *on_finish);

  void aio_update(uint64_t snap_id, uint64_t start_object_no,
                  uint64_t end_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  Context *on_finish);

  void rollback(uint64_t snap_id, Context *on_finish);
  void snapshot_add(uint64_t snap_id, Context *on_finish);
  void snapshot_remove(uint64_t snap_id, Context *on_finish);

private:
  ImageCtx &m_image_ctx;
  ceph::BitVector<2> m_object_map;
  uint64_t m_snap_id;

};

} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_H
