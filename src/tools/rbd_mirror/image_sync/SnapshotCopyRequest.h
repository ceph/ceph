// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/parent_types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include <map>
#include <set>
#include <string>
#include <tuple>

class Context;
class ContextWQ;
namespace journal { class Journaler; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class SnapshotCopyRequest : public BaseRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  static SnapshotCopyRequest* create(ImageCtxT *local_image_ctx,
                                     ImageCtxT *remote_image_ctx,
                                     SnapMap *snap_map, Journaler *journaler,
                                     librbd::journal::MirrorPeerClientMeta *client_meta,
                                     ContextWQ *work_queue,
                                     Context *on_finish) {
    return new SnapshotCopyRequest(local_image_ctx, remote_image_ctx,
                                   snap_map, journaler, client_meta, work_queue,
                                   on_finish);
  }

  SnapshotCopyRequest(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
                      SnapMap *snap_map, Journaler *journaler,
                      librbd::journal::MirrorPeerClientMeta *client_meta,
                      ContextWQ *work_queue, Context *on_finish);

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * UNPROTECT_SNAP ----/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * REMOVE_SNAP -------/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * CREATE_SNAP -------/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * PROTECT_SNAP ------/
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
  ContextWQ *m_work_queue;

  SnapIdSet m_local_snap_ids;
  SnapIdSet m_remote_snap_ids;
  SnapSeqs m_snap_seqs;
  librados::snap_t m_prev_snap_id = CEPH_NOSNAP;

  std::string m_snap_name;

  librbd::parent_spec m_local_parent_spec;

  Mutex m_lock;
  bool m_canceled = false;

  void send_snap_unprotect();
  void handle_snap_unprotect(int r);

  void send_snap_remove();
  void handle_snap_remove(int r);

  void send_snap_create();
  void handle_snap_create(int r);

  void send_snap_protect();
  void handle_snap_protect(int r);

  void send_update_client();
  void handle_update_client(int r);

  bool handle_cancellation();

  void error(int r);

  void compute_snap_map();

  int validate_parent(ImageCtxT *image_ctx, librbd::parent_spec *spec);

};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::SnapshotCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_SNAPSHOT_COPY_REQUEST_H
