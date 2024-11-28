// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H
#define CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H

#include "include/int_types.h"
#include <json_spirit/json_spirit.h>
#include <memory>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace migration {

struct FormatInterface;
struct SnapshotInterface;
struct StreamInterface;

template <typename ImageCtxT>
class SourceSpecBuilder {
public:
  static int parse_source_spec(const std::string& source_spec,
                               json_spirit::mObject* source_spec_object);

  SourceSpecBuilder(ImageCtxT* image_ctx) : m_image_ctx(image_ctx) {
  }

  int build_format(const json_spirit::mObject& format_object,
                   std::unique_ptr<FormatInterface>* format) const;

  int build_snapshot(const json_spirit::mObject& source_spec_object,
                     uint64_t index,
                     std::shared_ptr<SnapshotInterface>* snapshot) const;

  int build_stream(const json_spirit::mObject& source_spec_object,
                   std::shared_ptr<StreamInterface>* stream) const;

private:
  ImageCtxT* m_image_ctx;
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::SourceSpecBuilder<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H
