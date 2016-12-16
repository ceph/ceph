// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_SNAP_SET_REQUEST_H
#define CEPH_LIBRBD_IMAGE_SNAP_SET_REQUEST_H

#include <string>

class Context;

namespace librbd {

template <typename> class ExclusiveLock;
class ImageCtx;
template <typename> class ObjectMap;

namespace image {

template <typename> class RefreshParentRequest;

template <typename ImageCtxT = ImageCtx>
class SetSnapRequest {
public:
  static SetSnapRequest *create(ImageCtxT &image_ctx,
                                const std::string &snap_name,
                                Context *on_finish) {
    return new SetSnapRequest(image_ctx, snap_name, on_finish);
  }

  ~SetSnapRequest();

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    | (set snap)
   *    |-----------> BLOCK_WRITES
   *    |                 |
   *    |                 v
   *    |             SHUTDOWN_EXCLUSIVE_LOCK (skip if lock inactive
   *    |                 |                    or disabled)
   *    |                 v
   *    |             REFRESH_PARENT (skip if no parent
   *    |                 |           or refresh not needed)
   *    |                 v
   *    |             OPEN_OBJECT_MAP (skip if map disabled)
   *    |                 |
   *    |                 v
   *    |              <apply>
   *    |                 |
   *    |                 v
   *    |             FINALIZE_REFRESH_PARENT (skip if no parent
   *    |                 |                    or refresh not needed)
   *    |                 v
   *    |             <finish>
   *    |
   *    \-----------> INIT_EXCLUSIVE_LOCK (skip if active or
   *                      |                disabled)
   *                      v
   *                  REFRESH_PARENT (skip if no parent
   *                      |           or refresh not needed)
   *                      v
   *                   <apply>
   *                      |
   *                      v
   *                  FINALIZE_REFRESH_PARENT (skip if no parent
   *                      |                    or refresh not needed)
   *                      v
   *                  <finish>
   *
   * @endverbatim
   */

  SetSnapRequest(ImageCtxT &image_ctx, const std::string &snap_name,
                Context *on_finish);

  ImageCtxT &m_image_ctx;
  std::string m_snap_name;
  Context *m_on_finish;

  uint64_t m_snap_id;
  ExclusiveLock<ImageCtxT> *m_exclusive_lock;
  ObjectMap<ImageCtxT> *m_object_map;
  RefreshParentRequest<ImageCtxT> *m_refresh_parent;

  bool m_writes_blocked;

  void send_block_writes();
  Context *handle_block_writes(int *result);

  void send_init_exclusive_lock();
  Context *handle_init_exclusive_lock(int *result);

  Context *send_shut_down_exclusive_lock(int *result);
  Context *handle_shut_down_exclusive_lock(int *result);

  Context *send_refresh_parent(int *result);
  Context *handle_refresh_parent(int *result);

  Context *send_open_object_map(int *result);
  Context *handle_open_object_map(int *result);

  Context *send_finalize_refresh_parent(int *result);
  Context *handle_finalize_refresh_parent(int *result);

  int apply();
  void finalize();
  void send_complete();
};

} // namespace image
} // namespace librbd

extern template class librbd::image::SetSnapRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_SNAP_SET_REQUEST_H
