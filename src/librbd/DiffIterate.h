// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_DIFF_ITERATE_H
#define CEPH_LIBRBD_DIFF_ITERATE_H

#include "include/int_types.h"
#include "common/bit_vector.hpp"

namespace librbd {

class ImageCtx;

class DiffIterate {
public:
  typedef int (*Callback)(uint64_t, size_t, int, void *);

  DiffIterate(ImageCtx &image_ctx, const char *from_snap_name, uint64_t off,
              uint64_t len, bool include_parent, bool whole_object,
              Callback callback, void *callback_arg)
    : m_image_ctx(image_ctx), m_from_snap_name(from_snap_name), m_offset(off),
      m_length(len), m_include_parent(include_parent),
      m_whole_object(whole_object), m_callback(callback),
      m_callback_arg(callback_arg)
  {
  }

  int execute();

private:
  ImageCtx &m_image_ctx;
  const char* m_from_snap_name;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_include_parent;
  bool m_whole_object;
  Callback m_callback;
  void *m_callback_arg;

  int diff_object_map(uint64_t from_snap_id, uint64_t to_snap_id,
                      BitVector<2>* object_diff_state);

  static int simple_diff_cb(uint64_t off, size_t len, int exists, void *arg);
};

} // namespace librbd

#endif // CEPH_LIBRBD_DIFF_ITERATE_H
