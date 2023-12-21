// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_FILE_STREAM_H
#define CEPH_LIBRBD_MIGRATION_FILE_STREAM_H

#include "include/int_types.h"
#include "librbd/migration/StreamInterface.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <json_spirit/json_spirit.h>
#include <memory>
#include <string>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
class FileStream : public StreamInterface {
public:
  static FileStream* create(ImageCtxT* image_ctx,
                            const json_spirit::mObject& json_object) {
    return new FileStream(image_ctx, json_object);
  }

  FileStream(ImageCtxT* image_ctx, const json_spirit::mObject& json_object);
  ~FileStream() override;

  FileStream(const FileStream&) = delete;
  FileStream& operator=(const FileStream&) = delete;

  void open(Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_size(uint64_t* size, Context* on_finish) override;

  void read(io::Extents&& byte_extents, bufferlist* data,
            Context* on_finish) override;

private:
  CephContext* m_cct;
  std::shared_ptr<AsioEngine> m_asio_engine;
  json_spirit::mObject m_json_object;

  boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
#ifdef BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR
  std::optional<boost::asio::posix::stream_descriptor> m_stream_descriptor;

  struct ReadRequest;

#endif // BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR

  int m_file_no = -1;
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::FileStream<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_FILE_STREAM_H
