// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H

#include "include/int_types.h"
#include "include/rados/librados_fwd.hpp"
#include "json_spirit/json_spirit.h"
#include <string>

namespace librbd {

struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
class NativeFormat {
public:
  static std::string build_source_spec(int64_t pool_id,
                                       const std::string& pool_namespace,
                                       const std::string& image_name,
                                       const std::string& image_id);

  static bool is_source_spec(const json_spirit::mObject& source_spec_object);

  static int create_image_ctx(librados::IoCtx& dst_io_ctx,
                              const json_spirit::mObject& source_spec_object,
                              bool import_only, uint64_t src_snap_id,
                              ImageCtxT** src_image_ctx);
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::NativeFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_NATIVE_FORMAT_H
