#pragma once

#include "include/rados/cls_traits.h"

namespace cls::rbd {
struct ClassId {
  static constexpr auto name = "rbd";
};
namespace method {
constexpr auto create = ClsMethod<RdWrTag, ClassId>("create");
constexpr auto get_features = ClsMethod<RdTag, ClassId>("get_features");
constexpr auto set_features = ClsMethod<RdWrTag, ClassId>("set_features");
constexpr auto get_size = ClsMethod<RdTag, ClassId>("get_size");
constexpr auto set_size = ClsMethod<RdWrTag, ClassId>("set_size");
constexpr auto get_snapcontext = ClsMethod<RdTag, ClassId>("get_snapcontext");
constexpr auto get_object_prefix = ClsMethod<RdTag, ClassId>("get_object_prefix");
constexpr auto get_data_pool = ClsMethod<RdTag, ClassId>("get_data_pool");
constexpr auto get_snapshot_name = ClsMethod<RdTag, ClassId>("get_snapshot_name");
constexpr auto get_snapshot_timestamp = ClsMethod<RdTag, ClassId>("get_snapshot_timestamp");
constexpr auto snapshot_get = ClsMethod<RdTag, ClassId>("snapshot_get");
constexpr auto snapshot_add = ClsMethod<RdWrTag, ClassId>("snapshot_add");
constexpr auto snapshot_remove = ClsMethod<RdWrTag, ClassId>("snapshot_remove");
constexpr auto snapshot_rename = ClsMethod<RdWrTag, ClassId>("snapshot_rename");
constexpr auto snapshot_trash_add = ClsMethod<RdWrTag, ClassId>("snapshot_trash_add");
constexpr auto get_all_features = ClsMethod<RdTag, ClassId>("get_all_features");

// NOTE: deprecate v1 parent APIs after mimic EOLed
constexpr auto get_parent = ClsMethod<RdTag, ClassId>("get_parent");
constexpr auto set_parent = ClsMethod<RdWrTag, ClassId>("set_parent");
constexpr auto remove_parent = ClsMethod<RdWrTag, ClassId>("remove_parent");

// Parent/Layering (V2 APIs)
constexpr auto parent_get = ClsMethod<RdTag, ClassId>("parent_get");
constexpr auto parent_overlap_get = ClsMethod<RdTag, ClassId>("parent_overlap_get");
constexpr auto parent_attach = ClsMethod<RdWrTag, ClassId>("parent_attach");
constexpr auto parent_detach = ClsMethod<RdWrTag, ClassId>("parent_detach");

constexpr auto set_protection_status = ClsMethod<RdWrTag, ClassId>("set_protection_status");
constexpr auto get_protection_status = ClsMethod<RdTag, ClassId>("get_protection_status");
constexpr auto get_stripe_unit_count = ClsMethod<RdTag, ClassId>("get_stripe_unit_count");
constexpr auto set_stripe_unit_count = ClsMethod<RdWrTag, ClassId>("set_stripe_unit_count");
constexpr auto get_create_timestamp = ClsMethod<RdTag, ClassId>("get_create_timestamp");
constexpr auto get_access_timestamp = ClsMethod<RdTag, ClassId>("get_access_timestamp");
constexpr auto get_modify_timestamp = ClsMethod<RdTag, ClassId>("get_modify_timestamp");
constexpr auto get_flags = ClsMethod<RdTag, ClassId>("get_flags");
constexpr auto set_flags = ClsMethod<RdWrTag, ClassId>("set_flags");
constexpr auto op_features_get = ClsMethod<RdTag, ClassId>("op_features_get");
constexpr auto op_features_set = ClsMethod<RdWrTag, ClassId>("op_features_set");
constexpr auto metadata_list = ClsMethod<RdTag, ClassId>("metadata_list");
constexpr auto metadata_set = ClsMethod<RdWrTag, ClassId>("metadata_set");
constexpr auto metadata_remove = ClsMethod<RdWrTag, ClassId>("metadata_remove");
constexpr auto metadata_get = ClsMethod<RdTag, ClassId>("metadata_get");
constexpr auto snapshot_get_limit = ClsMethod<RdTag, ClassId>("snapshot_get_limit");
constexpr auto snapshot_set_limit = ClsMethod<RdWrTag, ClassId>("snapshot_set_limit");
constexpr auto child_attach = ClsMethod<RdWrTag, ClassId>("child_attach");
constexpr auto child_detach = ClsMethod<RdWrTag, ClassId>("child_detach");
constexpr auto children_list = ClsMethod<RdTag, ClassId>("children_list");
constexpr auto migration_set = ClsMethod<RdWrTag, ClassId>("migration_set");
constexpr auto migration_set_state = ClsMethod<RdWrTag, ClassId>("migration_set_state");
constexpr auto migration_get = ClsMethod<RdTag, ClassId>("migration_get");
constexpr auto migration_remove = ClsMethod<RdWrTag, ClassId>("migration_remove");

constexpr auto set_modify_timestamp = ClsMethod<RdWrTag, ClassId>("set_modify_timestamp");

constexpr auto set_access_timestamp = ClsMethod<RdWrTag, ClassId>("set_access_timestamp");

constexpr auto add_child = ClsMethod<RdWrTag, ClassId>("add_child");
constexpr auto remove_child = ClsMethod<RdWrTag, ClassId>("remove_child");
constexpr auto get_children = ClsMethod<RdTag, ClassId>("get_children");

/* methods for the rbd_id.$image_name objects */
constexpr auto get_id = ClsMethod<RdTag, ClassId>("get_id");
constexpr auto set_id = ClsMethod<RdWrTag, ClassId>("set_id");

/* methods for the rbd_directory object */
constexpr auto dir_get_id = ClsMethod<RdTag, ClassId>("dir_get_id");
constexpr auto dir_get_name = ClsMethod<RdTag, ClassId>("dir_get_name");
constexpr auto dir_list = ClsMethod<RdTag, ClassId>("dir_list");
constexpr auto dir_add_image = ClsMethod<RdWrTag, ClassId>("dir_add_image");
constexpr auto dir_remove_image = ClsMethod<RdWrTag, ClassId>("dir_remove_image");
constexpr auto dir_rename_image = ClsMethod<RdWrTag, ClassId>("dir_rename_image");
constexpr auto dir_state_assert = ClsMethod<RdTag, ClassId>("dir_state_assert");
constexpr auto dir_state_set = ClsMethod<RdWrTag, ClassId>("dir_state_set");

/* methods for the rbd_object_map.$image_id object */
constexpr auto object_map_load = ClsMethod<RdTag, ClassId>("object_map_load");
constexpr auto object_map_save = ClsMethod<RdWrTag, ClassId>("object_map_save");
constexpr auto object_map_resize = ClsMethod<RdWrTag, ClassId>("object_map_resize");
constexpr auto object_map_update = ClsMethod<RdWrTag, ClassId>("object_map_update");
constexpr auto object_map_snap_add = ClsMethod<RdWrTag, ClassId>("object_map_snap_add");
constexpr auto object_map_snap_remove = ClsMethod<RdWrTag, ClassId>("object_map_snap_remove");

/* methods for the old format */
constexpr auto snap_list = ClsMethod<RdTag, ClassId>("snap_list");
constexpr auto snap_add = ClsMethod<RdWrTag, ClassId>("snap_add");
constexpr auto snap_remove = ClsMethod<RdWrTag, ClassId>("snap_remove");
constexpr auto snap_rename = ClsMethod<RdWrTag, ClassId>("snap_rename");

/* methods for the rbd_mirroring object */
constexpr auto mirror_uuid_get = ClsMethod<RdTag, ClassId>("mirror_uuid_get");
constexpr auto mirror_uuid_set = ClsMethod<RdWrTag, ClassId>("mirror_uuid_set");
constexpr auto mirror_mode_get = ClsMethod<RdTag, ClassId>("mirror_mode_get");
constexpr auto mirror_mode_set = ClsMethod<RdWrTag, ClassId>("mirror_mode_set");
constexpr auto mirror_remote_namespace_get = ClsMethod<RdTag, ClassId>("mirror_remote_namespace_get");
constexpr auto mirror_remote_namespace_set = ClsMethod<RdWrTag, ClassId>("mirror_remote_namespace_set");
constexpr auto mirror_peer_ping = ClsMethod<RdWrTag, ClassId>("mirror_peer_ping");
constexpr auto mirror_peer_list = ClsMethod<RdTag, ClassId>("mirror_peer_list");
constexpr auto mirror_peer_add = ClsMethod<RdWrTag, ClassId>("mirror_peer_add");
constexpr auto mirror_peer_remove = ClsMethod<RdWrTag, ClassId>("mirror_peer_remove");
constexpr auto mirror_peer_set_client = ClsMethod<RdWrTag, ClassId>("mirror_peer_set_client");
constexpr auto mirror_peer_set_cluster = ClsMethod<RdWrTag, ClassId>("mirror_peer_set_cluster");
constexpr auto mirror_peer_set_direction = ClsMethod<RdWrTag, ClassId>("mirror_peer_set_direction");
constexpr auto mirror_image_list = ClsMethod<RdTag, ClassId>("mirror_image_list");
constexpr auto mirror_image_get_image_id = ClsMethod<RdTag, ClassId>("mirror_image_get_image_id");
constexpr auto mirror_image_get = ClsMethod<RdTag, ClassId>("mirror_image_get");
constexpr auto mirror_image_set = ClsMethod<RdWrTag, ClassId>("mirror_image_set");
constexpr auto mirror_image_remove = ClsMethod<RdWrTag, ClassId>("mirror_image_remove");
constexpr auto mirror_image_status_set = ClsMethod<RdWrPromoteTag, ClassId>("mirror_image_status_set");
constexpr auto mirror_image_status_remove = ClsMethod<RdWrTag, ClassId>("mirror_image_status_remove");
constexpr auto mirror_image_status_get = ClsMethod<RdTag, ClassId>("mirror_image_status_get");
constexpr auto mirror_image_status_list = ClsMethod<RdTag, ClassId>("mirror_image_status_list");
constexpr auto mirror_image_status_get_summary = ClsMethod<RdTag, ClassId>("mirror_image_status_get_summary");
constexpr auto mirror_image_status_remove_down = ClsMethod<RdWrTag, ClassId>("mirror_image_status_remove_down");
constexpr auto mirror_image_instance_get = ClsMethod<RdTag, ClassId>("mirror_image_instance_get");
constexpr auto mirror_image_instance_list = ClsMethod<RdTag, ClassId>("mirror_image_instance_list");
constexpr auto mirror_instances_list = ClsMethod<RdTag, ClassId>("mirror_instances_list");
constexpr auto mirror_instances_add = ClsMethod<RdWrPromoteTag, ClassId>("mirror_instances_add");
constexpr auto mirror_instances_remove = ClsMethod<RdWrTag, ClassId>("mirror_instances_remove");
constexpr auto mirror_image_map_list = ClsMethod<RdTag, ClassId>("mirror_image_map_list");
constexpr auto mirror_image_map_update = ClsMethod<WrTag, ClassId>("mirror_image_map_update");
constexpr auto mirror_image_map_remove = ClsMethod<WrTag, ClassId>("mirror_image_map_remove");
constexpr auto mirror_image_snapshot_unlink_peer = ClsMethod<RdWrTag, ClassId>("mirror_image_snapshot_unlink_peer");
constexpr auto mirror_image_snapshot_set_copy_progress = ClsMethod<RdWrTag, ClassId>("mirror_image_snapshot_set_copy_progress");

/* methods for the groups feature */
constexpr auto group_dir_list = ClsMethod<RdTag, ClassId>("group_dir_list");
constexpr auto group_dir_add = ClsMethod<RdWrTag, ClassId>("group_dir_add");
constexpr auto group_dir_remove = ClsMethod<RdWrTag, ClassId>("group_dir_remove");
constexpr auto group_dir_rename = ClsMethod<RdWrTag, ClassId>("group_dir_rename");
constexpr auto group_image_remove = ClsMethod<RdWrTag, ClassId>("group_image_remove");
constexpr auto group_image_list = ClsMethod<RdTag, ClassId>("group_image_list");
constexpr auto group_image_set = ClsMethod<RdWrTag, ClassId>("group_image_set");
constexpr auto image_group_add = ClsMethod<RdWrTag, ClassId>("image_group_add");
constexpr auto image_group_remove = ClsMethod<RdWrTag, ClassId>("image_group_remove");
constexpr auto image_group_get = ClsMethod<RdTag, ClassId>("image_group_get");
constexpr auto group_snap_set = ClsMethod<RdWrTag, ClassId>("group_snap_set");
constexpr auto group_snap_remove = ClsMethod<RdWrTag, ClassId>("group_snap_remove");
constexpr auto group_snap_get_by_id = ClsMethod<RdTag, ClassId>("group_snap_get_by_id");
constexpr auto group_snap_list = ClsMethod<RdTag, ClassId>("group_snap_list");
constexpr auto group_snap_list_order = ClsMethod<RdTag, ClassId>("group_snap_list_order");

/* rbd_trash object methods */
constexpr auto trash_add = ClsMethod<RdWrTag, ClassId>("trash_add");
constexpr auto trash_remove = ClsMethod<RdWrTag, ClassId>("trash_remove");
constexpr auto trash_list = ClsMethod<RdTag, ClassId>("trash_list");
constexpr auto trash_get = ClsMethod<RdTag, ClassId>("trash_get");
constexpr auto trash_state_set = ClsMethod<RdWrTag, ClassId>("trash_state_set");

/* rbd_namespace object methods */
constexpr auto namespace_add = ClsMethod<RdWrTag, ClassId>("namespace_add");
constexpr auto namespace_remove = ClsMethod<RdWrTag, ClassId>("namespace_remove");
constexpr auto namespace_list = ClsMethod<RdTag, ClassId>("namespace_list");

/* data object methods */
constexpr auto copyup = ClsMethod<RdWrTag, ClassId>("copyup");
constexpr auto sparse_copyup = ClsMethod<RdWrTag, ClassId>("sparse_copyup");
constexpr auto assert_snapc_seq = ClsMethod<RdWrTag, ClassId>("assert_snapc_seq");
constexpr auto sparsify = ClsMethod<RdWrTag, ClassId>("sparsify");
}
}