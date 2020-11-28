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
namespace asio { struct ContextWQ; }

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Mirror {
  typedef std::map<std::string, std::string> Attributes;
  typedef std::map<std::string, mirror_image_global_status_t>
    IdToMirrorImageGlobalStatus;
  typedef std::map<std::string, mirror_group_global_status_t> IdToMirrorGroupStatus;
  typedef std::map<mirror_image_status_state_t, int> MirrorImageStatusStates;
  typedef std::map<mirror_group_status_state_t, int> MirrorGroupStatusStates;

  static int site_name_get(librados::Rados& rados, std::string* name);
  static int site_name_set(librados::Rados& rados, const std::string& name);

  static int mode_get(librados::IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode);
  static int mode_set(librados::IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode);

  static int remote_namespace_get(librados::IoCtx& io_ctx,
                                  std::string* remote_namespace);
  static int remote_namespace_set(librados::IoCtx& io_ctx,
                                  const std::string& remote_namespace);

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
  static const char *pool_or_namespace(ImageCtxT *ictx);

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
  static int image_enable(ImageCtxT *ictx,
                          const std::string &group_snap_id,
                          mirror_image_mode_t mode,
                          bool relax_same_pool_parent_check,
                          uint64_t *snap_id);
  static int image_disable(ImageCtxT *ictx, bool force);
  static int image_promote(ImageCtxT *ictx, bool force);
  static void image_promote(ImageCtxT *ictx, bool force, Context *on_finish);
  static void image_promote(ImageCtxT *ictx,
                            const std::string &group_snap_id, bool force,
                            uint64_t *snap_id, Context *on_finish);
  static int image_demote(ImageCtxT *ictx);
  static void image_demote(ImageCtxT *ictx, Context *on_finish);
  static void image_demote(ImageCtxT *ictx,
                           const std::string &group_snap_id, uint64_t *snap_id,
                           Context *on_finish);
  static int image_resync(ImageCtxT *ictx);
  static int image_get_info(ImageCtxT *ictx,
                            mirror_image_info_t *mirror_image_info);
  static void image_get_info(ImageCtxT *ictx,
                             mirror_image_info_t *mirror_image_info,
                             Context *on_finish);
  static int image_get_info(librados::IoCtx& io_ctx,
                            asio::ContextWQ *op_work_queue,
                            const std::string &image_id,
                            mirror_image_info_t *mirror_image_info);
  static void image_get_info(librados::IoCtx& io_ctx,
                             asio::ContextWQ *op_work_queue,
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
  static void image_snapshot_create(ImageCtxT *ictx, uint32_t flags,
                                    uint64_t *snap_id, Context *on_finish);
  static void image_snapshot_create(ImageCtxT *ictx, uint32_t flags,
                                    const std::string &group_snap_id,
                                    uint64_t *snap_id, Context *on_finish);

  static int group_list(IoCtx &io_ctx, std::vector<std::string> *names);
  static int group_enable(IoCtx &group_ioctx, const char *group_name,
                          mirror_image_mode_t group_image_mode);
  static int group_disable(IoCtx &group_ioctx, const char *group_name,
                           bool force);
  static int group_promote(IoCtx &group_ioctx, const char *group_name,
                           bool force);
  static int group_demote(IoCtx &group_ioctx, const char *group_name);
  static int group_resync(IoCtx &group_ioctx, const char *group_name);
  static int group_snapshot_create(IoCtx& group_ioctx, const char *group_name,
                                   uint32_t flags, std::string *snap_id);

  static int group_image_add(IoCtx &group_ioctx, const std::string &group_id,
                             IoCtx &image_ioctx, const std::string &image_id);
  static int group_image_remove(IoCtx &group_ioctx, const std::string &group_id,
                                IoCtx &image_ioctx, const std::string &image_id);

  static int group_status_list(librados::IoCtx& io_ctx,
                               const std::string &start_id, size_t max,
                               IdToMirrorGroupStatus *groups);
  static int group_status_summary(librados::IoCtx& io_ctx,
                                  MirrorGroupStatusStates *states);
  static int group_instance_id_list(librados::IoCtx& io_ctx,
                                    const std::string &start_group_id,
                                    size_t max,
                                    std::map<std::string, std::string> *ids);
  static int group_info_list(librados::IoCtx& io_ctx,
                             mirror_image_mode_t *mode_filter,
                             const std::string &start_id,
                             size_t max,
                             std::map<std::string,
                             mirror_group_info_t> *entries);
  static int group_get_info(librados::IoCtx& io_ctx,
                            const std::string &group_name,
                            mirror_group_info_t *mirror_group_info);
  static int group_get_status(librados::IoCtx& io_ctx,
                              const std::string &group_name,
                              mirror_group_global_status_t *status);
  static int group_get_instance_id(librados::IoCtx& io_ctx,
                                   const std::string &group_name,
                                   std::string *instance_id);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Mirror<librbd::ImageCtx>;

#endif // LIBRBD_API_MIRROR_H
