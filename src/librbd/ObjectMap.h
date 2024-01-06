// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_H
#define CEPH_LIBRBD_OBJECT_MAP_H

#include "include/int_types.h"
#include "include/fs_types.h"
#include "include/rados/librados_fwd.hpp"
#include "include/rbd/object_map_types.h"
#include "common/AsyncOpTracker.h"
#include "common/bit_vector.hpp"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "librbd/Utils.h"
#include <boost/optional.hpp>

class Context;
namespace ZTracer { struct Trace; }

namespace librbd {

template <typename Op> class BlockGuard;
struct BlockGuardCell;
class ImageCtx;

template <typename ImageCtxT = ImageCtx>
class ObjectMap : public RefCountedObject {
public:
  static ObjectMap *create(ImageCtxT &image_ctx, uint64_t snap_id) {
    return new ObjectMap(image_ctx, snap_id);
  }

  ObjectMap(ImageCtxT &image_ctx, uint64_t snap_id);
  ~ObjectMap();

  static int aio_remove(librados::IoCtx &io_ctx, const std::string &image_id, librados::AioCompletion *c);
  static std::string object_map_name(const std::string &image_id,
				     uint64_t snap_id);

  static bool is_compatible(const file_layout_t& layout, uint64_t size);

  uint8_t operator[](uint64_t object_no) const;
  inline uint64_t size() const {
    std::shared_lock locker{m_lock};
    return m_object_map.size();
  }

  template <typename F, typename... Args>
  auto with_object_map(F&& f, Args&&... args) const {
    std::shared_lock locker(m_lock);
    return std::forward<F>(f)(m_object_map, std::forward<Args>(args)...);
  }

  inline void set_state(uint64_t object_no, uint8_t new_state,
                        const boost::optional<uint8_t> &current_state) {
    std::unique_lock locker{m_lock};
    ceph_assert(object_no < m_object_map.size());
    if (current_state && m_object_map[object_no] != *current_state) {
      return;
    }
    m_object_map[object_no] = new_state;
  }

  void open(Context *on_finish);
  void close(Context *on_finish);
  bool set_object_map(ceph::BitVector<2> &target_object_map);
  bool object_may_exist(uint64_t object_no) const;
  bool object_may_not_exist(uint64_t object_no) const;

  void aio_save(Context *on_finish);
  void aio_resize(uint64_t new_size, uint8_t default_object_state,
		  Context *on_finish);

  template <typename T, void(T::*MF)(int) = &T::complete>
  bool aio_update(uint64_t snap_id, uint64_t start_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  const ZTracer::Trace &parent_trace, bool ignore_enoent,
                  T *callback_object) {
    return aio_update<T, MF>(snap_id, start_object_no, start_object_no + 1,
                             new_state, current_state, parent_trace,
                             ignore_enoent, callback_object);
  }

  template <typename T, void(T::*MF)(int) = &T::complete>
  bool aio_update(uint64_t snap_id, uint64_t start_object_no,
                  uint64_t end_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  const ZTracer::Trace &parent_trace, bool ignore_enoent,
                  T *callback_object) {
    ceph_assert(start_object_no < end_object_no);
    std::unique_lock locker{m_lock};

    if (snap_id == CEPH_NOSNAP) {
      end_object_no = std::min(end_object_no, m_object_map.size());
      if (start_object_no >= end_object_no) {
        return false;
      }

      auto it = m_object_map.begin() + start_object_no;
      auto end_it = m_object_map.begin() + end_object_no;
      for (; it != end_it; ++it) {
        if (update_required(it, new_state)) {
          break;
        }
      }

      if (it == end_it) {
        return false;
      }

      m_async_op_tracker.start_op();
      UpdateOperation update_operation(start_object_no, end_object_no,
                                       new_state, current_state, parent_trace,
                                       ignore_enoent,
                                       util::create_context_callback<T, MF>(
                                         callback_object));
      detained_aio_update(std::move(update_operation));
    } else {
      aio_update(snap_id, start_object_no, end_object_no, new_state,
                 current_state, parent_trace, ignore_enoent,
                 util::create_context_callback<T, MF>(callback_object));
    }
    return true;
  }

  void rollback(uint64_t snap_id, Context *on_finish);
  void snapshot_add(uint64_t snap_id, Context *on_finish);
  void snapshot_remove(uint64_t snap_id, Context *on_finish);

private:
  struct UpdateOperation {
    uint64_t start_object_no;
    uint64_t end_object_no;
    uint8_t new_state;
    boost::optional<uint8_t> current_state;
    ZTracer::Trace parent_trace;
    bool ignore_enoent;
    Context *on_finish;

    UpdateOperation(uint64_t start_object_no, uint64_t end_object_no,
                    uint8_t new_state,
                    const boost::optional<uint8_t> &current_state,
                    const ZTracer::Trace &parent_trace,
                    bool ignore_enoent, Context *on_finish)
      : start_object_no(start_object_no), end_object_no(end_object_no),
        new_state(new_state), current_state(current_state),
        parent_trace(parent_trace), ignore_enoent(ignore_enoent),
        on_finish(on_finish) {
    }
  };

  typedef BlockGuard<UpdateOperation> UpdateGuard;

  ImageCtxT &m_image_ctx;
  uint64_t m_snap_id;

  mutable ceph::shared_mutex m_lock;
  ceph::BitVector<2> m_object_map;

  AsyncOpTracker m_async_op_tracker;
  UpdateGuard *m_update_guard = nullptr;

  void detained_aio_update(UpdateOperation &&update_operation);
  void handle_detained_aio_update(BlockGuardCell *cell, int r,
                                  Context *on_finish);

  void aio_update(uint64_t snap_id, uint64_t start_object_no,
                  uint64_t end_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  const ZTracer::Trace &parent_trace, bool ignore_enoent,
                  Context *on_finish);
  bool update_required(const ceph::BitVector<2>::Iterator &it,
                       uint8_t new_state);

};

} // namespace librbd

extern template class librbd::ObjectMap<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_H
