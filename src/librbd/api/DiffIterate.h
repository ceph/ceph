// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_DIFF_ITERATE_H
#define CEPH_LIBRBD_API_DIFF_ITERATE_H

#include "include/int_types.h"
#include "common/bit_vector.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <utility>

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class DiffIterate {
public:
  typedef int (*Callback)(uint64_t, size_t, int, void *);

  static int diff_iterate(ImageCtxT *ictx,
			  const cls::rbd::SnapshotNamespace& from_snap_namespace,
			  const char *fromsnapname,
                          uint64_t off, uint64_t len, bool include_parent,
                          bool whole_object,
		          int (*cb)(uint64_t, size_t, int, void *),
		          void *arg);

private:
  ImageCtxT &m_image_ctx;
  cls::rbd::SnapshotNamespace m_from_snap_namespace;
  const char* m_from_snap_name;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_include_parent;
  bool m_whole_object;
  Callback m_callback;
  void *m_callback_arg;

  DiffIterate(ImageCtxT &image_ctx, 
	      const cls::rbd::SnapshotNamespace& from_snap_namespace,
	      const char *from_snap_name, uint64_t off, uint64_t len,
	      bool include_parent, bool whole_object, Callback callback,
	      void *callback_arg)
    : m_image_ctx(image_ctx), m_from_snap_namespace(from_snap_namespace),
      m_from_snap_name(from_snap_name), m_offset(off),
      m_length(len), m_include_parent(include_parent),
      m_whole_object(whole_object), m_callback(callback),
      m_callback_arg(callback_arg)
  {
  }

  std::pair<uint64_t, uint64_t> calc_object_diff_range();

  int execute();

  int diff_object_map(uint64_t from_snap_id, uint64_t to_snap_id,
                      BitVector<2>* object_diff_state);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::DiffIterate<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_DIFF_ITERATE_H
