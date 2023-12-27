// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H
#define CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include <json_spirit/json_spirit.h>
#include <memory>
#include <optional>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
struct FormatInterface;

struct SnapshotInterface;
struct StreamInterface;

template <typename ImageCtxT>
class SourceSpecBuilder {
public:
  SourceSpecBuilder(CephContext* cct) : m_cct(cct) {
  }

  int parse_source_spec(const std::string& source_spec,
                        json_spirit::mObject* source_spec_object) const;

  int build_format(const json_spirit::mObject& format_object, bool import_only,
                   std::unique_ptr<FormatInterface<ImageCtxT>>* format) const;

  int build_snapshot(ImageCtxT* image_ctx,
                     const json_spirit::mObject& source_spec_object,
                     uint64_t index,
                     std::shared_ptr<SnapshotInterface>* snapshot) const;

  int build_stream(ImageCtxT* image_ctx,
                   const json_spirit::mObject& source_spec_object,
                   std::shared_ptr<StreamInterface>* stream) const;

private:
  CephContext* m_cct;
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::SourceSpecBuilder<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_SOURCE_SPEC_BUILDER_H
