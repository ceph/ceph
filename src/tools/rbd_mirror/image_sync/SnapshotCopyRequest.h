// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include <map>
#include <set>
#include <string>
#include <tuple>

class Context;
namespace journal { class Journaler; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class SnapshotCopyRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  static SnapshotCopyRequest* create(ImageCtxT *local_image_ctx,
                                     ImageCtxT *remote_image_ctx,
                                     SnapMap *snap_map, Journaler *journaler,
                                     librbd::journal::MirrorPeerClientMeta *client_meta,
                                     Context *on_finish) {
    return new SnapshotCopyRequest(local_image_ctx, remote_image_ctx,
                                   snap_map, journaler, client_meta, on_finish);
  }

  SnapshotCopyRequest(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
                      SnapMap *snap_map, Journaler *journaler,
                      librbd::journal::MirrorPeerClientMeta *client_meta,
                      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |   /-------\
   *    |   |       |
   *    v   v       | (repeat as needed)
   * REMOVE_SNAP <--/
   *    |
   *    |   /-------\
   *    |   |       |
   *    v   v       | (repeat as needed)
   * CREATE_SNAP <--/
   *    |
   *    v
   * UPDATE_CLIENT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  typedef std::set<librados::snap_t> SnapIdSet;
  typedef std::map<librados::snap_t, librados::snap_t> SnapSeqs;

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  SnapMap *m_snap_map;
  Journaler *m_journaler;
  librbd::journal::MirrorPeerClientMeta *m_client_meta;
  Context *m_on_finish;

  SnapIdSet m_local_snap_ids;
  SnapIdSet m_remote_snap_ids;
  SnapSeqs m_snap_seqs;

  std::string m_snap_name;

  void send_snap_remove();
  void handle_snap_remove(int r);

  void send_snap_create();
  void handle_snap_create(int r);

  void send_update_client();
  void handle_update_client(int r);

  void finish(int r);

  void compute_snap_map();

};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::SnapshotCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H
