// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/SourceSpecBuilder.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/migration/FileStream.h"
#include "librbd/migration/HttpStream.h"
#include "librbd/migration/S3Stream.h"
#include "librbd/migration/NativeFormat.h"
#include "librbd/migration/QCOWFormat.h"
#include "librbd/migration/RawFormat.h"
#include "librbd/migration/RawSnapshot.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::SourceSpecBuilder: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace migration {

namespace {

const std::string STREAM_KEY{"stream"};
const std::string TYPE_KEY{"type"};

} // anonymous namespace

template <typename I>
int SourceSpecBuilder<I>::parse_source_spec(
    const std::string& source_spec,
    json_spirit::mObject* source_spec_object) const {
  ldout(m_cct, 10) << dendl;

  json_spirit::mValue json_root;
  if(json_spirit::read(source_spec, json_root)) {
    try {
      *source_spec_object = json_root.get_obj();
      return 0;
    } catch (std::runtime_error&) {
    }
  }

  lderr(m_cct) << "invalid source-spec JSON" << dendl;
  return -EBADMSG;
}

template <typename I>
int SourceSpecBuilder<I>::build_format(
    const json_spirit::mObject& source_spec_object, bool import_only,
    std::unique_ptr<FormatInterface<I>>* format) const {
  ldout(m_cct, 10) << dendl;

  auto type_value_it = source_spec_object.find(TYPE_KEY);
  if (type_value_it == source_spec_object.end() ||
      type_value_it->second.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate format type value" << dendl;
    return -EINVAL;
  }

  auto& type = type_value_it->second.get_str();
  if (type == "native") {
    format->reset(NativeFormat<I>::create(source_spec_object,
                                          import_only));
  } else if (type == "qcow") {
    format->reset(QCOWFormat<I>::create(source_spec_object, this));
  } else if (type == "raw") {
    format->reset(RawFormat<I>::create(source_spec_object, this));
  } else {
    lderr(m_cct) << "unknown or unsupported format type '" << type << "'"
                 << dendl;
    return -ENOSYS;
  }
  return 0;
}

template <typename I>
int SourceSpecBuilder<I>::build_snapshot(
    I* image_ctx, const json_spirit::mObject& source_spec_object,
    uint64_t index, std::shared_ptr<SnapshotInterface>* snapshot) const {
  ldout(m_cct, 10) << dendl;

  auto type_value_it = source_spec_object.find(TYPE_KEY);
  if (type_value_it == source_spec_object.end() ||
      type_value_it->second.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate snapshot type value" << dendl;
    return -EINVAL;
  }

  auto& type = type_value_it->second.get_str();
  if (type == "raw") {
    snapshot->reset(RawSnapshot<I>::create(image_ctx, source_spec_object,
                                           this, index));
  } else {
    lderr(m_cct) << "unknown or unsupported format type '" << type << "'"
                 << dendl;
    return -ENOSYS;
  }
  return 0;
}

template <typename I>
int SourceSpecBuilder<I>::build_stream(
    I* image_ctx, const json_spirit::mObject& source_spec_object,
    std::shared_ptr<StreamInterface>* stream) const {
  ldout(m_cct, 10) << dendl;

  auto stream_value_it = source_spec_object.find(STREAM_KEY);
  if (stream_value_it == source_spec_object.end() ||
      stream_value_it->second.type() != json_spirit::obj_type) {
    lderr(m_cct) << "failed to locate stream object" << dendl;
    return -EINVAL;
  }

  auto& stream_obj = stream_value_it->second.get_obj();
  auto type_value_it = stream_obj.find(TYPE_KEY);
  if (type_value_it == stream_obj.end() ||
      type_value_it->second.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate stream type value" << dendl;
    return -EINVAL;
  }

  auto& type = type_value_it->second.get_str();
  if (type == "file") {
    stream->reset(FileStream<I>::create(image_ctx, stream_obj));
  } else if (type == "http") {
    stream->reset(HttpStream<I>::create(image_ctx, stream_obj));
  } else if (type == "s3") {
    stream->reset(S3Stream<I>::create(image_ctx, stream_obj));
  } else {
    lderr(m_cct) << "unknown or unsupported stream type '" << type << "'"
                 << dendl;
    return -ENOSYS;
  }

  return 0;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::SourceSpecBuilder<librbd::ImageCtx>;
