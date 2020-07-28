// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CLS_RBD_CLIENT_H
#define CEPH_LIBRBD_CLS_RBD_CLIENT_H

#include "cls/lock/cls_lock_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/snap_types.h"
#include "include/types.h"
#include "include/rados/librados_fwd.hpp"

class Context;
namespace ceph { template <uint8_t> class BitVector; }
namespace neorados { struct WriteOp; }

namespace librbd {
namespace cls_client {

// low-level interface (mainly for testing)
void create_image(librados::ObjectWriteOperation *op, uint64_t size,
                  uint8_t order, uint64_t features,
                  const std::string &object_prefix, int64_t data_pool_id);
int create_image(librados::IoCtx *ioctx, const std::string &oid,
                 uint64_t size, uint8_t order, uint64_t features,
                 const std::string &object_prefix, int64_t data_pool_id);

void get_features_start(librados::ObjectReadOperation *op, bool read_only);
int get_features_finish(ceph::buffer::list::const_iterator *it, uint64_t *features,
                        uint64_t *incompatible_features);
int get_features(librados::IoCtx *ioctx, const std::string &oid,
                 bool read_only, uint64_t *features,
                 uint64_t *incompatible_features);
void set_features(librados::ObjectWriteOperation *op, uint64_t features,
                  uint64_t mask);
int set_features(librados::IoCtx *ioctx, const std::string &oid,
                 uint64_t features, uint64_t mask);

void get_object_prefix_start(librados::ObjectReadOperation *op);
int get_object_prefix_finish(ceph::buffer::list::const_iterator *it,
                             std::string *object_prefix);
int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
                      std::string *object_prefix);

void get_data_pool_start(librados::ObjectReadOperation *op);
int get_data_pool_finish(ceph::buffer::list::const_iterator *it, int64_t *data_pool_id);
int get_data_pool(librados::IoCtx *ioctx, const std::string &oid,
                  int64_t *data_pool_id);

void get_size_start(librados::ObjectReadOperation *op, snapid_t snap_id);
int get_size_finish(ceph::buffer::list::const_iterator *it, uint64_t *size,
                    uint8_t *order);
int get_size(librados::IoCtx *ioctx, const std::string &oid,
             snapid_t snap_id, uint64_t *size, uint8_t *order);
int set_size(librados::IoCtx *ioctx, const std::string &oid,
             uint64_t size);
void set_size(librados::ObjectWriteOperation *op, uint64_t size);

void get_flags_start(librados::ObjectReadOperation *op, snapid_t snap_id);
int get_flags_finish(ceph::buffer::list::const_iterator *it, uint64_t *flags);
int get_flags(librados::IoCtx *ioctx, const std::string &oid,
              snapid_t snap_id, uint64_t *flags);

void set_flags(librados::ObjectWriteOperation *op, snapid_t snap_id,
               uint64_t flags, uint64_t mask);

void op_features_get_start(librados::ObjectReadOperation *op);
int op_features_get_finish(ceph::buffer::list::const_iterator *it,
                           uint64_t *op_features);
int op_features_get(librados::IoCtx *ioctx, const std::string &oid,
                    uint64_t *op_features);
void op_features_set(librados::ObjectWriteOperation *op,
                     uint64_t op_features, uint64_t mask);
int op_features_set(librados::IoCtx *ioctx, const std::string &oid,
                    uint64_t op_features, uint64_t mask);

// NOTE: deprecate v1 parent APIs after mimic EOLed
void get_parent_start(librados::ObjectReadOperation *op, snapid_t snap_id);
int get_parent_finish(ceph::buffer::list::const_iterator *it,
                      cls::rbd::ParentImageSpec *pspec,
                      uint64_t *parent_overlap);
int get_parent(librados::IoCtx *ioctx, const std::string &oid,
               snapid_t snap_id, cls::rbd::ParentImageSpec *pspec,
               uint64_t *parent_overlap);
int set_parent(librados::IoCtx *ioctx, const std::string &oid,
               const cls::rbd::ParentImageSpec &pspec, uint64_t parent_overlap);
void set_parent(librados::ObjectWriteOperation *op,
                const cls::rbd::ParentImageSpec &pspec,
                uint64_t parent_overlap);
int remove_parent(librados::IoCtx *ioctx, const std::string &oid);
void remove_parent(librados::ObjectWriteOperation *op);

// v2 parent APIs
void parent_get_start(librados::ObjectReadOperation* op);
int parent_get_finish(ceph::buffer::list::const_iterator* it,
                      cls::rbd::ParentImageSpec* parent_image_spec);
int parent_get(librados::IoCtx* ioctx, const std::string &oid,
               cls::rbd::ParentImageSpec* parent_image_spec);

void parent_overlap_get_start(librados::ObjectReadOperation* op,
                              snapid_t snap_id);
int parent_overlap_get_finish(ceph::buffer::list::const_iterator* it,
                              std::optional<uint64_t>* parent_overlap);
int parent_overlap_get(librados::IoCtx* ioctx, const std::string &oid,
                       snapid_t snap_id,
                       std::optional<uint64_t>* parent_overlap);

void parent_attach(librados::ObjectWriteOperation* op,
                   const cls::rbd::ParentImageSpec& parent_image_spec,
                   uint64_t parent_overlap, bool reattach);
int parent_attach(librados::IoCtx *ioctx, const std::string &oid,
                  const cls::rbd::ParentImageSpec& parent_image_spec,
                  uint64_t parent_overlap, bool reattach);

void parent_detach(librados::ObjectWriteOperation* op);
int parent_detach(librados::IoCtx *ioctx, const std::string &oid);

int add_child(librados::IoCtx *ioctx, const std::string &oid,
              const cls::rbd::ParentImageSpec &pspec,
              const std::string &c_imageid);
void add_child(librados::ObjectWriteOperation *op,
               const cls::rbd::ParentImageSpec& pspec,
               const std::string &c_imageid);
void remove_child(librados::ObjectWriteOperation *op,
                  const cls::rbd::ParentImageSpec &pspec,
                  const std::string &c_imageid);
int remove_child(librados::IoCtx *ioctx, const std::string &oid,
                 const cls::rbd::ParentImageSpec &pspec,
                 const std::string &c_imageid);
void get_children_start(librados::ObjectReadOperation *op,
                        const cls::rbd::ParentImageSpec &pspec);
int get_children_finish(ceph::buffer::list::const_iterator *it,
                        std::set<std::string> *children);
int get_children(librados::IoCtx *ioctx, const std::string &oid,
                 const cls::rbd::ParentImageSpec& pspec, std::set<std::string>& children);

void snapshot_get_start(librados::ObjectReadOperation* op,
                        snapid_t snap_id);
int snapshot_get_finish(ceph::buffer::list::const_iterator* it,
                        cls::rbd::SnapshotInfo* snap_info);
int snapshot_get(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id, cls::rbd::SnapshotInfo* snap_info);

void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const std::string &snap_name,
                  const cls::rbd::SnapshotNamespace &snap_namespace);
void snapshot_remove(librados::ObjectWriteOperation *op, snapid_t snap_id);
void snapshot_rename(librados::ObjectWriteOperation *op,
                     snapid_t src_snap_id,
                     const std::string &dst_name);
void snapshot_trash_add(librados::ObjectWriteOperation *op,
                        snapid_t snap_id);

void get_snapcontext_start(librados::ObjectReadOperation *op);
int get_snapcontext_finish(ceph::buffer::list::const_iterator *it,
                           ::SnapContext *snapc);
int get_snapcontext(librados::IoCtx *ioctx, const std::string &oid,
                    ::SnapContext *snapc);

/// NOTE: remove after Luminous is retired
void get_snapshot_name_start(librados::ObjectReadOperation *op,
                             snapid_t snap_id);
int get_snapshot_name_finish(ceph::buffer::list::const_iterator *it,
                             std::string *name);
int get_snapshot_name(librados::IoCtx *ioctx, const std::string &oid,
                      snapid_t snap_id, std::string *name);

/// NOTE: remove after Luminous is retired
void get_snapshot_timestamp_start(librados::ObjectReadOperation *op,
                                  snapid_t snap_id);
int get_snapshot_timestamp_finish(ceph::buffer::list::const_iterator *it,
                                  utime_t *timestamp);
int get_snapshot_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                           snapid_t snap_id, utime_t *timestamp);

void get_all_features_start(librados::ObjectReadOperation *op);
int get_all_features_finish(ceph::buffer::list::const_iterator *it,
                            uint64_t *all_features);
int get_all_features(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t *all_features);

/// NOTE: remove protection after clone v1 is retired
void get_protection_status_start(librados::ObjectReadOperation *op,
                                 snapid_t snap_id);
int get_protection_status_finish(ceph::buffer::list::const_iterator *it,
                                 uint8_t *protection_status);
int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
                          snapid_t snap_id, uint8_t *protection_status);

int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
                          snapid_t snap_id, uint8_t protection_status);
void set_protection_status(librados::ObjectWriteOperation *op,
                           snapid_t snap_id, uint8_t protection_status);

void snapshot_get_limit_start(librados::ObjectReadOperation *op);
int snapshot_get_limit_finish(ceph::buffer::list::const_iterator *it, uint64_t *limit);
int snapshot_get_limit(librados::IoCtx *ioctx, const std::string &oid,
                       uint64_t *limit);
void snapshot_set_limit(librados::ObjectWriteOperation *op,
                        uint64_t limit);

void get_stripe_unit_count_start(librados::ObjectReadOperation *op);
int get_stripe_unit_count_finish(ceph::buffer::list::const_iterator *it,
                                 uint64_t *stripe_unit,
                                 uint64_t *stripe_count);
int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
                          uint64_t *stripe_unit, uint64_t *stripe_count);

void set_stripe_unit_count(librados::ObjectWriteOperation *op,
                           uint64_t stripe_unit, uint64_t stripe_count);
int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
                          uint64_t stripe_unit, uint64_t stripe_count);

void get_create_timestamp_start(librados::ObjectReadOperation *op);
int get_create_timestamp_finish(ceph::buffer::list::const_iterator *it,
                                utime_t *timestamp);
int get_create_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                         utime_t *timestamp);

void get_access_timestamp_start(librados::ObjectReadOperation *op);
int get_access_timestamp_finish(ceph::buffer::list::const_iterator *it,
                                utime_t *timestamp);
int get_access_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                         utime_t *timestamp);

void set_access_timestamp(librados::ObjectWriteOperation *op);
int set_access_timestamp(librados::IoCtx *ioctx, const std::string &oid);

void get_modify_timestamp_start(librados::ObjectReadOperation *op);
int get_modify_timestamp_finish(ceph::buffer::list::const_iterator *it,
                                utime_t *timestamp);
int get_modify_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                         utime_t *timestamp);

void set_modify_timestamp(librados::ObjectWriteOperation *op);
int set_modify_timestamp(librados::IoCtx *ioctx, const std::string &oid);

int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &start, uint64_t max_return,
                  std::map<std::string, ceph::buffer::list> *pairs);
void metadata_list_start(librados::ObjectReadOperation *op,
                         const std::string &start, uint64_t max_return);
int metadata_list_finish(ceph::buffer::list::const_iterator *it,
                         std::map<std::string, ceph::buffer::list> *pairs);
void metadata_set(librados::ObjectWriteOperation *op,
                  const std::map<std::string, ceph::buffer::list> &data);
int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                 const std::map<std::string, ceph::buffer::list> &data);
void metadata_remove(librados::ObjectWriteOperation *op,
                     const std::string &key);
int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                    const std::string &key);
void metadata_get_start(librados::ObjectReadOperation* op,
                 const std::string &key);
int metadata_get_finish(ceph::buffer::list::const_iterator *it,
                        std::string* value);
int metadata_get(librados::IoCtx *ioctx, const std::string &oid,
                 const std::string &key, std::string *v);

void child_attach(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const cls::rbd::ChildImageSpec& child_image);
int child_attach(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id,
                 const cls::rbd::ChildImageSpec& child_image);
void child_detach(librados::ObjectWriteOperation *op, snapid_t snap_id,
                  const cls::rbd::ChildImageSpec& child_image);
int child_detach(librados::IoCtx *ioctx, const std::string &oid,
                 snapid_t snap_id,
                 const cls::rbd::ChildImageSpec& child_image);
void children_list_start(librados::ObjectReadOperation *op,
                         snapid_t snap_id);
int children_list_finish(ceph::buffer::list::const_iterator *it,
                         cls::rbd::ChildImageSpecs *child_images);
int children_list(librados::IoCtx *ioctx, const std::string &oid,
                  snapid_t snap_id,
                  cls::rbd::ChildImageSpecs *child_images);
int migration_set(librados::IoCtx *ioctx, const std::string &oid,
                  const cls::rbd::MigrationSpec &migration_spec);
void migration_set(librados::ObjectWriteOperation *op,
                   const cls::rbd::MigrationSpec &migration_spec);
int migration_set_state(librados::IoCtx *ioctx, const std::string &oid,
                        cls::rbd::MigrationState state,
                        const std::string &description);
void migration_set_state(librados::ObjectWriteOperation *op,
                         cls::rbd::MigrationState state,
                         const std::string &description);
void migration_get_start(librados::ObjectReadOperation *op);
int migration_get_finish(ceph::buffer::list::const_iterator *it,
                         cls::rbd::MigrationSpec *migration_spec);
int migration_get(librados::IoCtx *ioctx, const std::string &oid,
                  cls::rbd::MigrationSpec *migration_spec);
int migration_remove(librados::IoCtx *ioctx, const std::string &oid);
void migration_remove(librados::ObjectWriteOperation *op);

// operations on rbd_id objects
void get_id_start(librados::ObjectReadOperation *op);
int get_id_finish(ceph::buffer::list::const_iterator *it, std::string *id);
int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id);

void set_id(librados::ObjectWriteOperation *op, const std::string &id);
int set_id(librados::IoCtx *ioctx, const std::string &oid, const std::string &id);

// operations on rbd_directory objects
int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
               const std::string &name, std::string *id);
void dir_get_id_start(librados::ObjectReadOperation *op,
                      const std::string &image_name);
int dir_get_id_finish(ceph::buffer::list::const_iterator *iter, std::string *image_id);
void dir_get_name_start(librados::ObjectReadOperation *op,
                        const std::string &id);
int dir_get_name_finish(ceph::buffer::list::const_iterator *it, std::string *name);
int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
                 const std::string &id, std::string *name);
void dir_list_start(librados::ObjectReadOperation *op,
                    const std::string &start, uint64_t max_return);
int dir_list_finish(ceph::buffer::list::const_iterator *it, std::map<std::string, std::string> *images);
int dir_list(librados::IoCtx *ioctx, const std::string &oid,
             const std::string &start, uint64_t max_return,
             std::map<std::string, std::string> *images);
void dir_add_image(librados::ObjectWriteOperation *op,
                   const std::string &name, const std::string &id);
int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &name, const std::string &id);
int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &name, const std::string &id);
void dir_remove_image(librados::ObjectWriteOperation *op,
                      const std::string &name, const std::string &id);
// atomic remove and add
void dir_rename_image(librados::ObjectWriteOperation *op,
                      const std::string &src, const std::string &dest,
                      const std::string &id);
void dir_state_assert(librados::ObjectOperation *op,
                      cls::rbd::DirectoryState directory_state);
int dir_state_assert(librados::IoCtx *ioctx, const std::string &oid,
                     cls::rbd::DirectoryState directory_state);
void dir_state_set(librados::ObjectWriteOperation *op,
                   cls::rbd::DirectoryState directory_state);
int dir_state_set(librados::IoCtx *ioctx, const std::string &oid,
                  cls::rbd::DirectoryState directory_state);

// operations on the rbd_object_map.$image_id object
void object_map_load_start(librados::ObjectReadOperation *op);
int object_map_load_finish(ceph::buffer::list::const_iterator *it,
                           ceph::BitVector<2> *object_map);
int object_map_load(librados::IoCtx *ioctx, const std::string &oid,
                    ceph::BitVector<2> *object_map);
void object_map_save(librados::ObjectWriteOperation *rados_op,
                     const ceph::BitVector<2> &object_map);
void object_map_resize(librados::ObjectWriteOperation *rados_op,
                       uint64_t object_count, uint8_t default_state);
void object_map_update(librados::ObjectWriteOperation *rados_op,
                       uint64_t start_object_no, uint64_t end_object_no,
                       uint8_t new_object_state,
                       const boost::optional<uint8_t> &current_object_state);
void object_map_snap_add(librados::ObjectWriteOperation *rados_op);
void object_map_snap_remove(librados::ObjectWriteOperation *rados_op,
                            const ceph::BitVector<2> &object_map);

// class operations on the old format, kept for
// backwards compatibility
void old_snapshot_add(librados::ObjectWriteOperation *rados_op,
                      snapid_t snap_id, const std::string &snap_name);
void old_snapshot_remove(librados::ObjectWriteOperation *rados_op,
                         const std::string &snap_name);
void old_snapshot_rename(librados::ObjectWriteOperation *rados_op,
                         snapid_t src_snap_id, const std::string &dst_name);

void old_snapshot_list_start(librados::ObjectReadOperation *op);
int old_snapshot_list_finish(ceph::buffer::list::const_iterator *it,
                             std::vector<std::string> *names,
                             std::vector<uint64_t> *sizes,
                             ::SnapContext *snapc);
int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
                      std::vector<std::string> *names,
                      std::vector<uint64_t> *sizes,
                      ::SnapContext *snapc);

// operations on the rbd_mirroring object
void mirror_uuid_get_start(librados::ObjectReadOperation *op);
int mirror_uuid_get_finish(ceph::buffer::list::const_iterator *it,
                           std::string *uuid);
int mirror_uuid_get(librados::IoCtx *ioctx, std::string *uuid);
int mirror_uuid_set(librados::IoCtx *ioctx, const std::string &uuid);
void mirror_mode_get_start(librados::ObjectReadOperation *op);
int mirror_mode_get_finish(ceph::buffer::list::const_iterator *it,
                           cls::rbd::MirrorMode *mirror_mode);
int mirror_mode_get(librados::IoCtx *ioctx,
                    cls::rbd::MirrorMode *mirror_mode);
int mirror_mode_set(librados::IoCtx *ioctx,
                    cls::rbd::MirrorMode mirror_mode);

int mirror_peer_ping(librados::IoCtx *ioctx,
                     const std::string& site_name,
                     const std::string& fsid);
void mirror_peer_ping(librados::ObjectWriteOperation *op,
                      const std::string& site_name,
                      const std::string& fsid);
void mirror_peer_list_start(librados::ObjectReadOperation *op);
int mirror_peer_list_finish(ceph::buffer::list::const_iterator *it,
                            std::vector<cls::rbd::MirrorPeer> *peers);
int mirror_peer_list(librados::IoCtx *ioctx,
                     std::vector<cls::rbd::MirrorPeer> *peers);
int mirror_peer_add(librados::IoCtx *ioctx,
                    const cls::rbd::MirrorPeer& mirror_peer);
void mirror_peer_add(librados::ObjectWriteOperation *op,
                     const cls::rbd::MirrorPeer& mirror_peer);
int mirror_peer_remove(librados::IoCtx *ioctx,
                       const std::string &uuid);
int mirror_peer_set_client(librados::IoCtx *ioctx,
                           const std::string &uuid,
                           const std::string &client_name);
int mirror_peer_set_cluster(librados::IoCtx *ioctx,
                            const std::string &uuid,
                            const std::string &cluster_name);
int mirror_peer_set_direction(
    librados::IoCtx *ioctx, const std::string &uuid,
    cls::rbd::MirrorPeerDirection mirror_peer_direction);

void mirror_image_list_start(librados::ObjectReadOperation *op,
                             const std::string &start, uint64_t max_return);
int mirror_image_list_finish(ceph::buffer::list::const_iterator *it,
                             std::map<std::string, std::string> *mirror_image_ids);
int mirror_image_list(librados::IoCtx *ioctx,
                      const std::string &start, uint64_t max_return,
                      std::map<std::string, std::string> *mirror_image_ids);
void mirror_image_get_image_id_start(librados::ObjectReadOperation *op,
                                     const std::string &global_image_id);
int mirror_image_get_image_id_finish(ceph::buffer::list::const_iterator *it,
                                     std::string *image_id);
int mirror_image_get_image_id(librados::IoCtx *ioctx,
                              const std::string &global_image_id,
                              std::string *image_id);
int mirror_image_get(librados::IoCtx *ioctx, const std::string &image_id,
                     cls::rbd::MirrorImage *mirror_image);
void mirror_image_get_start(librados::ObjectReadOperation *op,
                            const std::string &image_id);
int mirror_image_get_finish(ceph::buffer::list::const_iterator *iter,
                            cls::rbd::MirrorImage *mirror_image);
void mirror_image_set(librados::ObjectWriteOperation *op,
                      const std::string &image_id,
                      const cls::rbd::MirrorImage &mirror_image);
int mirror_image_set(librados::IoCtx *ioctx, const std::string &image_id,
                     const cls::rbd::MirrorImage &mirror_image);
void mirror_image_remove(librados::ObjectWriteOperation *op,
                         const std::string &image_id);
int mirror_image_remove(librados::IoCtx *ioctx,
                        const std::string &image_id);
int mirror_image_status_set(librados::IoCtx *ioctx,
                            const std::string &global_image_id,
                            const cls::rbd::MirrorImageSiteStatus &status);
void mirror_image_status_set(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id,
                             const cls::rbd::MirrorImageSiteStatus &status);
int mirror_image_status_get(librados::IoCtx *ioctx,
                            const std::string &global_image_id,
                            cls::rbd::MirrorImageStatus *status);
void mirror_image_status_get_start(librados::ObjectReadOperation *op,
                                   const std::string &global_image_id);
int mirror_image_status_get_finish(ceph::buffer::list::const_iterator *iter,
                                   cls::rbd::MirrorImageStatus *status);
int mirror_image_status_list(librados::IoCtx *ioctx,
                             const std::string &start, uint64_t max_return,
                             std::map<std::string, cls::rbd::MirrorImage> *images,
                             std::map<std::string, cls::rbd::MirrorImageStatus> *statuses);
void mirror_image_status_list_start(librados::ObjectReadOperation *op,
                                    const std::string &start,
                                    uint64_t max_return);
int mirror_image_status_list_finish(ceph::buffer::list::const_iterator *iter,
                                    std::map<std::string, cls::rbd::MirrorImage> *images,
                                    std::map<std::string, cls::rbd::MirrorImageStatus> *statuses);
int mirror_image_status_get_summary(
    librados::IoCtx *ioctx,
    const std::vector<cls::rbd::MirrorPeer>& mirror_peer_sites,
    std::map<cls::rbd::MirrorImageStatusState, int32_t> *states);
void mirror_image_status_get_summary_start(
    librados::ObjectReadOperation *op,
    const std::vector<cls::rbd::MirrorPeer>& mirror_peer_sites);
int mirror_image_status_get_summary_finish(
    ceph::buffer::list::const_iterator *iter,
    std::map<cls::rbd::MirrorImageStatusState, int32_t> *states);
int mirror_image_status_remove_down(librados::IoCtx *ioctx);
void mirror_image_status_remove_down(librados::ObjectWriteOperation *op);

int mirror_image_instance_get(librados::IoCtx *ioctx,
                              const std::string &global_image_id,
                              entity_inst_t *instance);
void mirror_image_instance_get_start(librados::ObjectReadOperation *op,
                                     const std::string &global_image_id);
int mirror_image_instance_get_finish(ceph::buffer::list::const_iterator *iter,
                                     entity_inst_t *instance);
int mirror_image_instance_list(librados::IoCtx *ioctx,
                               const std::string &start, uint64_t max_return,
                               std::map<std::string, entity_inst_t> *instances);
void mirror_image_instance_list_start(librados::ObjectReadOperation *op,
                                      const std::string &start,
                                      uint64_t max_return);
int mirror_image_instance_list_finish(ceph::buffer::list::const_iterator *iter,
                                      std::map<std::string, entity_inst_t> *instances);

void mirror_instances_list_start(librados::ObjectReadOperation *op);
int mirror_instances_list_finish(ceph::buffer::list::const_iterator *iter,
                                 std::vector<std::string> *instance_ids);
int mirror_instances_list(librados::IoCtx *ioctx,
                          std::vector<std::string> *instance_ids);
void mirror_instances_add(librados::ObjectWriteOperation *op,
                          const std::string &instance_id);
int mirror_instances_add(librados::IoCtx *ioctx,
                         const std::string &instance_id);
void mirror_instances_remove(librados::ObjectWriteOperation *op,
                             const std::string &instance_id);
int mirror_instances_remove(librados::IoCtx *ioctx,
                            const std::string &instance_id);

// image mapping related routines
void mirror_image_map_list_start(librados::ObjectReadOperation *op,
                                 const std::string &start_after,
                                 uint64_t max_read);
int mirror_image_map_list_finish(ceph::buffer::list::const_iterator *iter,
                                 std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping);
int mirror_image_map_list(librados::IoCtx *ioctx,
                          const std::string &start_after, uint64_t max_read,
                          std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping);
void mirror_image_map_update(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id,
                             const cls::rbd::MirrorImageMap &image_map);
void mirror_image_map_remove(librados::ObjectWriteOperation *op,
                             const std::string &global_image_id);

void mirror_image_snapshot_unlink_peer(librados::ObjectWriteOperation *op,
                                       snapid_t snap_id,
                                       const std::string &mirror_peer_uuid);
int mirror_image_snapshot_unlink_peer(librados::IoCtx *ioctx,
                                      const std::string &oid,
                                      snapid_t snap_id,
                                      const std::string &mirror_peer_uuid);
void mirror_image_snapshot_set_copy_progress(librados::ObjectWriteOperation *op,
                                             snapid_t snap_id, bool complete,
                                             uint64_t copy_progress);
int mirror_image_snapshot_set_copy_progress(librados::IoCtx *ioctx,
                                            const std::string &oid,
                                            snapid_t snap_id, bool complete,
                                            uint64_t copy_progress);

// Groups functions
int group_dir_list(librados::IoCtx *ioctx, const std::string &oid,
                   const std::string &start, uint64_t max_return,
                   std::map<std::string, std::string> *groups);
int group_dir_add(librados::IoCtx *ioctx, const std::string &oid,
                  const std::string &name, const std::string &id);
int group_dir_rename(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &src, const std::string &dest,
                     const std::string &id);
int group_dir_remove(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &name, const std::string &id);
int group_image_remove(librados::IoCtx *ioctx, const std::string &oid,
                       const cls::rbd::GroupImageSpec &spec);
int group_image_list(librados::IoCtx *ioctx, const std::string &oid,
                     const cls::rbd::GroupImageSpec &start,
                     uint64_t max_return,
                     std::vector<cls::rbd::GroupImageStatus> *images);
int group_image_set(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupImageStatus &st);
int image_group_add(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupSpec &group_spec);
int image_group_remove(librados::IoCtx *ioctx, const std::string &oid,
                       const cls::rbd::GroupSpec &group_spec);
void image_group_get_start(librados::ObjectReadOperation *op);
int image_group_get_finish(ceph::buffer::list::const_iterator *iter,
                           cls::rbd::GroupSpec *group_spec);
int image_group_get(librados::IoCtx *ioctx, const std::string &oid,
                    cls::rbd::GroupSpec *group_spec);
int group_snap_set(librados::IoCtx *ioctx, const std::string &oid,
                   const cls::rbd::GroupSnapshot &snapshot);
int group_snap_remove(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &snap_id);
int group_snap_get_by_id(librados::IoCtx *ioctx, const std::string &oid,
                         const std::string &snap_id,
                         cls::rbd::GroupSnapshot *snapshot);
int group_snap_list(librados::IoCtx *ioctx, const std::string &oid,
                    const cls::rbd::GroupSnapshot &start,
                    uint64_t max_return,
                    std::vector<cls::rbd::GroupSnapshot> *snapshots);

// operations on rbd_trash object
void trash_add(librados::ObjectWriteOperation *op,
               const std::string &id,
               const cls::rbd::TrashImageSpec &trash_spec);
int trash_add(librados::IoCtx *ioctx, const std::string &id,
              const cls::rbd::TrashImageSpec &trash_spec);
void trash_remove(librados::ObjectWriteOperation *op,
                  const std::string &id);
int trash_remove(librados::IoCtx *ioctx, const std::string &id);
void trash_list_start(librados::ObjectReadOperation *op,
                      const std::string &start, uint64_t max_return);
int trash_list_finish(ceph::buffer::list::const_iterator *it,
                      std::map<std::string, cls::rbd::TrashImageSpec> *entries);
int trash_list(librados::IoCtx *ioctx,
               const std::string &start, uint64_t max_return,
               std::map<std::string, cls::rbd::TrashImageSpec> *entries);
void trash_get_start(librados::ObjectReadOperation *op,
                     const std::string &id);
int trash_get_finish(ceph::buffer::list::const_iterator *it,
                     cls::rbd::TrashImageSpec *trash_spec);
int trash_get(librados::IoCtx *ioctx, const std::string &id,
              cls::rbd::TrashImageSpec *trash_spec);
void trash_state_set(librados::ObjectWriteOperation *op,
                     const std::string &id,
                     const cls::rbd::TrashImageState &trash_state,
                     const cls::rbd::TrashImageState &expect_state);
int trash_state_set(librados::IoCtx *ioctx, const std::string &id,
                    const cls::rbd::TrashImageState &trash_state,
                    const cls::rbd::TrashImageState &expect_state);

// operations on rbd_namespace object
void namespace_add(librados::ObjectWriteOperation *op,
                   const std::string &name);
int namespace_add(librados::IoCtx *ioctx, const std::string &name);
void namespace_remove(librados::ObjectWriteOperation *op,
                      const std::string &name);
int namespace_remove(librados::IoCtx *ioctx, const std::string &name);
void namespace_list_start(librados::ObjectReadOperation *op,
                          const std::string &start, uint64_t max_return);
int namespace_list_finish(ceph::buffer::list::const_iterator *it,
                          std::list<std::string> *entries);
int namespace_list(librados::IoCtx *ioctx,
                   const std::string &start, uint64_t max_return,
                   std::list<std::string> *entries);

// operations on data objects
void assert_snapc_seq(neorados::WriteOp* op,
                      uint64_t snapc_seq,
                      cls::rbd::AssertSnapcSeqState state);
void assert_snapc_seq(librados::ObjectWriteOperation *op,
                      uint64_t snapc_seq,
                      cls::rbd::AssertSnapcSeqState state);
int assert_snapc_seq(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t snapc_seq,
                     cls::rbd::AssertSnapcSeqState state);

void copyup(neorados::WriteOp* op, ceph::buffer::list data);
void copyup(librados::ObjectWriteOperation *op, ceph::buffer::list data);
int copyup(librados::IoCtx *ioctx, const std::string &oid,
           ceph::buffer::list data);

void sparse_copyup(neorados::WriteOp* op,
                   const std::map<uint64_t, uint64_t> &extent_map,
                   ceph::buffer::list data);
void sparse_copyup(librados::ObjectWriteOperation *op,
                   const std::map<uint64_t, uint64_t> &extent_map,
                   ceph::buffer::list data);
int sparse_copyup(librados::IoCtx *ioctx, const std::string &oid,
                  const std::map<uint64_t, uint64_t> &extent_map,
                  ceph::buffer::list data);

void sparsify(librados::ObjectWriteOperation *op, size_t sparse_size,
              bool remove_empty);
int sparsify(librados::IoCtx *ioctx, const std::string &oid, size_t sparse_size,
             bool remove_empty);

} // namespace cls_client
} // namespace librbd

#endif // CEPH_LIBRBD_CLS_RBD_CLIENT_H
