// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GET_UUID_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GET_UUID_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GetUuidRequest {
public:
  static GetUuidRequest *create(librados::IoCtx& io_ctx,
                                std::string* mirror_uuid, Context* on_finish) {
    return new GetUuidRequest(io_ctx, mirror_uuid, on_finish);
  }

  GetUuidRequest(librados::IoCtx& io_ctx, std::string* mirror_uuid,
                 Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_UUID
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx m_io_ctx;
  std::string* m_mirror_uuid;
  Context* m_on_finish;

  CephContext* m_cct;

  bufferlist m_out_bl;

  void get_mirror_uuid();
  void handle_get_mirror_uuid(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GetUuidRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GET_UUID_REQUEST_H
