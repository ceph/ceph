// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H

#include "common/snap_types.h"
#include "include/buffer.h"
#include "include/common_fwd.h"
#include "include/rados/librados.hpp"
#include "librbd/Types.h"
#include "librbd/mirror/Types.h"
#include <string>

struct Context;

namespace cls { namespace rbd { struct MirrorImage; } }

namespace librbd {

struct ImageCtx;
namespace asio { struct ContextWQ; }

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GetInfoRequest {
public:
  static GetInfoRequest *create(librados::IoCtx &io_ctx,
                                asio::ContextWQ *op_work_queue,
                                const std::string &image_id,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                std::string* primary_mirror_uuid,
                                Context *on_finish) {
    return new GetInfoRequest(io_ctx, op_work_queue, image_id, mirror_image,
                              promotion_state, primary_mirror_uuid, on_finish);
  }
  static GetInfoRequest *create(ImageCtxT &image_ctx,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                std::string* primary_mirror_uuid,
                                Context *on_finish) {
    return new GetInfoRequest(image_ctx, mirror_image, promotion_state,
                              primary_mirror_uuid, on_finish);
  }

  GetInfoRequest(librados::IoCtx& io_ctx, asio::ContextWQ *op_work_queue,
                 const std::string &image_id,
                 cls::rbd::MirrorImage *mirror_image,
                 PromotionState *promotion_state,
                 std::string* primary_mirror_uuid, Context *on_finish);
  GetInfoRequest(ImageCtxT &image_ctx, cls::rbd::MirrorImage *mirror_image,
                 PromotionState *promotion_state,
                 std::string* primary_mirror_uuid, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   *                  <start>
   *                     |
   *                     v
   *              GET_MIRROR_IMAGE
   *                     |
   *  (journal /--------/ \--------\ (snapshot
   *   mode)   |                   |  mode)
   *           v                   v
   *  GET_JOURNAL_TAG_OWNER    GET_SNAPCONTEXT (skip if
   *           |                   |            cached)
   *           |                   v
   *           |               GET_SNAPSHOTS (skip if
   *           |                   |          cached)
   *           \--------\ /--------/
   *                     |
   *                     v
   *                  <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx = nullptr;
  librados::IoCtx &m_io_ctx;
  asio::ContextWQ *m_op_work_queue;
  std::string m_image_id;
  cls::rbd::MirrorImage *m_mirror_image;
  PromotionState *m_promotion_state;
  std::string* m_primary_mirror_uuid;
  Context *m_on_finish;

  CephContext *m_cct;

  bufferlist m_out_bl;
  std::string m_mirror_uuid;
  ::SnapContext m_snapc;

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void get_journal_tag_owner();
  void handle_get_journal_tag_owner(int r);

  void get_snapcontext();
  void handle_get_snapcontext(int r);

  void get_snapshots();
  void handle_get_snapshots(int r);

  void finish(int r);

  void calc_promotion_state(
    const std::map<librados::snap_t, SnapInfo> &snap_info);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GetInfoRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H

