// -*- mode:C++; tab-width:8; c-basic-offremove:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_METADATA_REMOVE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_METADATA_REMOVE_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class MetadataRemoveRequest : public Request<ImageCtxT> {
public:
  MetadataRemoveRequest(ImageCtxT &image_ctx, Context *on_finish,
                        const std::string &key);

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::MetadataRemoveEvent(op_tid, m_key);
  }

private:
  std::string m_key;

  void send_metadata_remove();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::MetadataRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_METADATA_REMOVE_REQUEST_H
