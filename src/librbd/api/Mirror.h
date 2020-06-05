// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_MIRROR_H
#define LIBRBD_API_MIRROR_H

#include "include/rbd/librbd.hpp"
#include <map>
#include <string>
#include <vector>

class ContextWQ;

struct Context;

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Mirror {
  typedef std::map<std::string, std::string> Attributes;
  typedef std::map<std::string, mirror_image_global_status_t>
    IdToMirrorImageGlobalStatus;
  typedef std::map<mirror_image_status_state_t, int> MirrorImageStatusStates;

  static int site_name_get(librados::Rados& rados, std::string* name);
  static int site_name_set(librados::Rados& rados, const std::string& name);

  static int mode_get(librados::IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode);
  static int mode_set(librados::IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode);

  static int uuid_get(librados::IoCtx& io_ctx, std::string* mirror_uuid);
  static void uuid_get(librados::IoCtx& io_ctx, std::string* mirror_uuid,
                       Context* on_finish);

  static int peer_bootstrap_create(librados::IoCtx& io_ctx, std::string* token);
  static int peer_bootstrap_import(librados::IoCtx& io_ctx,
                                   rbd_mirror_peer_direction_t direction,
                                   const std::string& token);

  static int peer_site_add(librados::IoCtx& io_ctx, std::string *uuid,
                           mirror_peer_direction_t direction,
                           const std::string &site_name,
                           const std::string &client_name);
  static int peer_site_remove(librados::IoCtx& io_ctx, const std::string &uuid);
  static int peer_site_list(librados::IoCtx& io_ctx,
                            std::vector<mirror_peer_site_t> *peers);
  static int peer_site_set_client(librados::IoCtx& io_ctx,
                                  const std::string &uuid,
                                  const std::string &client_name);
  static int peer_site_set_name(librados::IoCtx& io_ctx,
                                const std::string &uuid,
                                const std::string &site_name);
  static int peer_site_set_direction(librados::IoCtx& io_ctx,
                                     const std::string &uuid,
                                     mirror_peer_direction_t direction);
  static int peer_site_get_attributes(librados::IoCtx& io_ctx,
                                      const std::string &uuid,
                                      Attributes* attributes);
  static int peer_site_set_attributes(librados::IoCtx& io_ctx,
                                      const std::string &uuid,
                                      const Attributes& attributes);

  static int image_global_status_list(librados::IoCtx& io_ctx,
                                      const std::string &start_id, size_t max,
                                      IdToMirrorImageGlobalStatus *images);

  static int image_status_summary(librados::IoCtx& io_ctx,
                                  MirrorImageStatusStates *states);
  static int image_instance_id_list(librados::IoCtx& io_ctx,
                                    const std::string &start_image_id,
                                    size_t max,
                                    std::map<std::string, std::string> *ids);

  static int image_info_list(
      librados::IoCtx& io_ctx, mirror_image_mode_t *mode_filter,
      const std::string &start_id, size_t max,
      std::map<std::string, std::pair<mirror_image_mode_t,
                                      mirror_image_info_t>> *entries);

  static int image_enable(ImageCtxT *ictx, mirror_image_mode_t mode,
                          bool relax_same_pool_parent_check);
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
  static int image_get_info(librados::IoCtx& io_ctx,
                            ContextWQ *op_work_queue,
                            const std::string &image_id,
                            mirror_image_info_t *mirror_image_info);
  static void image_get_info(librados::IoCtx& io_ctx,
                             ContextWQ *op_work_queue,
                             const std::string &image_id,
                             mirror_image_info_t *mirror_image_info,
                             Context *on_finish);
  static int image_get_mode(ImageCtxT *ictx, mirror_image_mode_t *mode);
  static void image_get_mode(ImageCtxT *ictx, mirror_image_mode_t *mode,
                             Context *on_finish);
  static int image_get_global_status(ImageCtxT *ictx,
                                     mirror_image_global_status_t *status);
  static void image_get_global_status(ImageCtxT *ictx,
                                      mirror_image_global_status_t *status,
                                      Context *on_finish);
  static int image_get_instance_id(ImageCtxT *ictx, std::string *instance_id);

  static int image_snapshot_create(ImageCtxT *ictx, uint32_t flags,
                                   uint64_t *snap_id);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Mirror<librbd::ImageCtx>;

#endif // LIBRBD_API_MIRROR_H
