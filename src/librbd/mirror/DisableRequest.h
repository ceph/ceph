// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_DISABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_DISABLE_REQUEST_H

#include "include/buffer.h"
#include "common/Mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class DisableRequest {
public:
  static DisableRequest *create(ImageCtxT *image_ctx, bool force,
                                bool remove, Context *on_finish) {
    return new DisableRequest(image_ctx, force, remove, on_finish);
  }

  DisableRequest(ImageCtxT *image_ctx, bool force, bool remove,
                 Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_IMAGE * * * * * * * * * * * * * * * * * * * * * * *
   *    |                                                         *
   *    v                                                         *
   * GET_TAG_OWNER  * * * * * * * * * * * * * * * * * * * * * * * *
   *    |                                                         *
   *    v                                                         *
   * SET_MIRROR_IMAGE * * * * * * * * * * * * * * * * * * * * * * *
   *    |                                                         *
   *    v                                                         *
   * NOTIFY_MIRRORING_WATCHER                                     *
   *    |                                                         *
   *    v                                                         *
   * PROMOTE_IMAGE (skip if primary)                              *
   *    |                                                         *
   *    v                                                         *
   * GET_CLIENTS <----------------------------------------\ * * * *
   *    |     | (unregister clients)                      |       *  (on error)
   *    |     |/----------------------------\             |       *
   *    |     |                             |             |       *
   *    |     |   /-----------\ (repeat     | (repeat     | (repeat
   *    |     |   |           |  as needed) |  as needed) |  as needed)
   *    |     v   v           |             |             |       *
   *    |  REMOVE_SYNC_SNAP --/ * * * * * * | * * * * * * | * * * *
   *    |     |                             |             |       *
   *    |     v                             |             |       *
   *    |  UNREGISTER_CLIENT ---------------/-------------/ * * * *
   *    |                                                         *
   *    | (no more clients                                        *
   *    |  to unregister)                                         *
   *    v                                                         *
   * REMOVE_MIRROR_IMAGE  * * * * * * * * * * * * * * * * * * * * *
   *    |         (skip if no remove)                             *
   *    v                                                         *
   * NOTIFY_MIRRORING_WATCHER_REMOVED                             *
   *    |         (skip if not primary or no remove)              *
   *    v                                                         *
   * <finish> < * * * * * * * * * * * * * * * * * * * * * * * * * *
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  bool m_force;
  bool m_remove;
  Context *m_on_finish;

  bool m_is_primary = false;
  bufferlist m_out_bl;
  cls::rbd::MirrorImage m_mirror_image;
  std::set<cls::journal::Client> m_clients;
  std::map<std::string, int> m_ret;
  std::map<std::string, int> m_current_ops;
  int m_error_result = 0;
  mutable Mutex m_lock;

  void send_get_mirror_image();
  Context *handle_get_mirror_image(int *result);

  void send_get_tag_owner();
  Context *handle_get_tag_owner(int *result);

  void send_set_mirror_image();
  Context *handle_set_mirror_image(int *result);

  void send_notify_mirroring_watcher();
  Context *handle_notify_mirroring_watcher(int *result);

  void send_promote_image();
  Context *handle_promote_image(int *result);

  void send_get_clients();
  Context *handle_get_clients(int *result);

  void send_remove_snap(const std::string &client_id,
                        const cls::rbd::SnapshotNamespace &snap_namespace,
			const std::string &snap_name);
  Context *handle_remove_snap(int *result, const std::string &client_id);

  void send_unregister_client(const std::string &client_id);
  Context *handle_unregister_client(int *result, const std::string &client_id);

  void send_remove_mirror_image();
  Context *handle_remove_mirror_image(int *result);

  void send_notify_mirroring_watcher_removed();
  Context *handle_notify_mirroring_watcher_removed(int *result);

  Context *create_context_callback(
    Context*(DisableRequest<ImageCtxT>::*handle)(
      int*, const std::string &client_id),
    const std::string &client_id);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::DisableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_DISABLE_REQUEST_H
