// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_METADATA_SET_REQUEST_H
#define CEPH_LIBRBD_OPERATION_METADATA_SET_REQUEST_H

#include "librbd/operation/Request.h"
#include "include/buffer.h"
#include <string>
#include <map>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class MetadataSetRequest : public Request<ImageCtxT> {
public:
  MetadataSetRequest(ImageCtxT &image_ctx, Context *on_finish,
                     const std::string &key, const std::string &value);

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::MetadataSetEvent(op_tid, m_key, m_value);
  }

private:
  std::string m_key;
  std::string m_value;
  std::map<std::string, bufferlist> m_data;

  void send_metadata_set();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::MetadataSetRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_METADATA_SET_REQUEST_H
