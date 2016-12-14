// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OBJECT_MAP_H
#define CEPH_LIBRBD_OBJECT_MAP_H

#include "include/int_types.h"
#include "include/fs_types.h"
#include "include/rbd/object_map_types.h"
#include "common/bit_vector.hpp"
#include <boost/optional.hpp>

class Context;
class RWLock;
namespace librados {
  class IoCtx;
}

namespace librbd {

class ImageCtx;

class ObjectMapView {
public:
  ObjectMapView() {}

  inline uint64_t size() const {
    return m_level0_view.size();
  }

  ceph::BitVector<2> *level0_map() {
    return &m_level0_view;
  }

  bool in_batch_mode(uint64_t object_no);
  void resize(uint64_t new_size, uint8_t object_state);
  uint32_t batch_size();

  ceph::BitVector<2u>::Reference operator[](uint64_t object_no);
  uint8_t operator[](uint64_t object_no) const;

  void sync_view(ImageCtx &image_ctx, uint64_t snap_id, uint8_t view_idx,
                 Context *on_finish);
  void update_view(uint64_t object_no, const boost::optional<uint8_t> &current_state,
                   uint8_t new_state, uint8_t view_idx);
  void apply_view();
private:
  struct View {
    View() : m_tracked(0) {}

    ceph::BitVector<1> m_lookup;
    ceph::BitVector<2> m_map_track;

    boost::optional<uint64_t> m_object_start;
    uint64_t m_object_end;

    boost::optional<uint8_t> m_current_state;
    uint8_t m_new_state;

    uint32_t m_tracked;
  };

  ceph::BitVector<2> m_level0_view;
  View m_level_view[OBJECT_MAP_VIEW_LEVELS];

  void reset_view(uint8_t view_idx);
};

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
  bool update_required(uint64_t object_no, uint8_t new_state);

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

  // batch updations
  bool aio_batch(uint64_t object_no, uint8_t new_state,
                 const boost::optional<uint8_t> &current_state, uint8_t view_idx);
  void aio_update_batch(Context *on_finish);
  uint32_t batch_size();

  void rollback(uint64_t snap_id, Context *on_finish);
  void snapshot_add(uint64_t snap_id, Context *on_finish);
  void snapshot_remove(uint64_t snap_id, Context *on_finish);

private:
  ImageCtx &m_image_ctx;
  uint64_t m_snap_id;

  ObjectMapView m_object_map;

  ceph::BitVector<2> *level0_map() {
    return m_object_map.level0_map();
  }
};

} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_H
