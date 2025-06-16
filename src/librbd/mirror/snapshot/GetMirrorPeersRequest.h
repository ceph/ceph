// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_MIRROR_PEERS_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_MIRROR_PEERS_REQUEST_H

#include "include/buffer.h"
#include "include/types.h"
#include "include/rados/librados.hpp"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {


template <typename ImageCtxT = librbd::ImageCtx>
class GetMirrorPeersRequest {
public:
  static GetMirrorPeersRequest *create(ImageCtxT *image_ctx,
                                       std::set<std::string>& mirror_peer_uuids,
                                       Context *on_finish) {
    return new GetMirrorPeersRequest(image_ctx, mirror_peer_uuids, on_finish);
  }

  GetMirrorPeersRequest(ImageCtxT *image_ctx,
                        std::set<std::string>& mirror_peer_uuids,
                        Context *on_finish) 
    : m_image_ctx(image_ctx), m_mirror_peer_uuids(mirror_peer_uuids),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_PEERS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  std::set<std::string> m_mirror_peer_uuids;
  Context *m_on_finish;

  CephContext *m_cct;
  librados::IoCtx m_default_ns_ctx;

  bufferlist m_out_bl;

  void get_mirror_peers();
  void handle_get_mirror_peers(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GetMirrorPeersRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_MIRROR_PEERS_REQUEST_H
