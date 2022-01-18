// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_NBD_STREAM_H
#define CEPH_LIBRBD_MIGRATION_NBD_STREAM_H

#include "include/int_types.h"
#include "librbd/migration/StreamInterface.h"
#include <json_spirit/json_spirit.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

struct Context;

struct nbd_handle;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
class NBDStream : public StreamInterface {
public:
  static NBDStream* create(ImageCtxT* image_ctx,
                           const json_spirit::mObject& json_object) {
    return new NBDStream(image_ctx, json_object);
  }

  NBDStream(ImageCtxT* image_ctx, const json_spirit::mObject& json_object);
  ~NBDStream() override;

  NBDStream(const NBDStream&) = delete;
  NBDStream& operator=(const NBDStream&) = delete;

  void open(Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_size(uint64_t* size, Context* on_finish) override;

  void read(io::Extents&& byte_extents, bufferlist* data,
            Context* on_finish) override;

  void list_sparse_extents(io::Extents&& byte_extents,
                           io::SparseExtents* sparse_extents,
                           Context* on_finish) override;

private:
  CephContext* m_cct;
  std::shared_ptr<AsioEngine> m_asio_engine;
  json_spirit::mObject m_json_object;
  boost::asio::strand<boost::asio::io_context::executor_type> m_strand;

  struct nbd_handle* m_nbd = nullptr;

  struct ReadRequest;
  struct ListSparseExtentsRequest;
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::NBDStream<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_NBD_STREAM_H
