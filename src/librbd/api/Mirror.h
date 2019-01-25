// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_MIRROR_H
#define LIBRBD_API_MIRROR_H

#include "include/rbd/librbd.hpp"
#include <map>
#include <string>
#include <vector>

struct Context;

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Mirror {
  typedef std::map<std::string, std::string> Attributes;
  typedef std::map<std::string, mirror_image_status_t> IdToMirrorImageStatus;
  typedef std::map<mirror_image_status_state_t, int> MirrorImageStatusStates;

  static int mode_get(librados::IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode);
  static int mode_set(librados::IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode);

  static int peer_add(librados::IoCtx& io_ctx, std::string *uuid,
                      const std::string &cluster_name,
                      const std::string &client_name);
  static int peer_remove(librados::IoCtx& io_ctx, const std::string &uuid);
  static int peer_list(librados::IoCtx& io_ctx,
                       std::vector<mirror_peer_t> *peers);
  static int peer_set_client(librados::IoCtx& io_ctx, const std::string &uuid,
                             const std::string &client_name);
  static int peer_set_cluster(librados::IoCtx& io_ctx, const std::string &uuid,
                              const std::string &cluster_name);
  static int peer_get_attributes(librados::IoCtx& io_ctx,
                                 const std::string &uuid,
                                 Attributes* attributes);
  static int peer_set_attributes(librados::IoCtx& io_ctx,
                                 const std::string &uuid,
                                 const Attributes& attributes);

  static int image_status_list(librados::IoCtx& io_ctx,
                               const std::string &start_id, size_t max,
                               IdToMirrorImageStatus *images);
  static int image_status_summary(librados::IoCtx& io_ctx,
                                  MirrorImageStatusStates *states);
  static int image_instance_id_list(librados::IoCtx& io_ctx,
                                    const std::string &start_image_id,
                                    size_t max,
                                    std::map<std::string, std::string> *ids);

  static int image_enable(ImageCtxT *ictx, bool relax_same_pool_parent_check);
  static int image_disable(ImageCtxT *ictx, bool force);
  static int image_promote(ImageCtxT *ictx, bool force);
  static void image_promote(ImageCtxT *ictx, bool force, Context *on_finish);
  static int image_demote(ImageCtxT *ictx);
  static void image_demote(ImageCtxT *ictx, Context *on_finish);
  static int image_resync(ImageCtxT *ictx);
  static int image_get_info(ImageCtxT *ictx,
                            mirror_image_info_t *mirror_image_info);
  static void image_get_info(ImageCtxT *ictx,
                             mirror_image_info_t *mirror_image_info,
                             Context *on_finish);
  static int image_get_status(ImageCtxT *ictx, mirror_image_status_t *status);
  static void image_get_status(ImageCtxT *ictx, mirror_image_status_t *status,
                               Context *on_finish);
  static int image_get_instance_id(ImageCtxT *ictx, std::string *instance_id);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Mirror<librbd::ImageCtx>;

#endif // LIBRBD_API_MIRROR_H
