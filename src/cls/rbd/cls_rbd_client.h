// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CLS_RBD_CLIENT_H
#define CEPH_LIBRBD_CLS_RBD_CLIENT_H

#include "cls/lock/cls_lock_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/bit_vector.hpp"
#include "common/snap_types.h"
#include "include/types.h"
#include "librbd/Types.h"

class Context;
namespace librados {
  class ObjectReadOperation;
  class IoCtx;
  class ObjectWriteOperation;
}

namespace librbd {
  namespace cls_client {
    // high-level interface to the header
    void get_immutable_metadata_start(librados::ObjectReadOperation *op);
    int get_immutable_metadata_finish(bufferlist::iterator *it,
                                      std::string *object_prefix,
                                      uint8_t *order);
    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order);

    void get_mutable_metadata_start(librados::ObjectReadOperation *op,
                                    bool read_only);
    int get_mutable_metadata_finish(bufferlist::iterator *it,
                                    uint64_t *size, uint64_t *features,
                                    uint64_t *incompatible_features,
                                    std::map<rados::cls::lock::locker_id_t,
                                             rados::cls::lock::locker_info_t> *lockers,
                                    bool *exclusive_lock, std::string *lock_tag,
				    ::SnapContext *snapc, ParentInfo *parent);
    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     bool read_only, uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
			     bool *exclusive_lock,
			     std::string *lock_tag,
			     ::SnapContext *snapc,
			     ParentInfo *parent);

    // low-level interface (mainly for testing)
    void create_image(librados::ObjectWriteOperation *op, uint64_t size,
                      uint8_t order, uint64_t features,
                      const std::string &object_prefix, int64_t data_pool_id);
    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix, int64_t data_pool_id);
    int get_features(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, uint64_t *features);
    void set_features(librados::ObjectWriteOperation *op, uint64_t features,
                     uint64_t mask);
    int set_features(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t features, uint64_t mask);
    int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
			  std::string *object_prefix);
    void get_data_pool_start(librados::ObjectReadOperation *op);
    int get_data_pool_finish(bufferlist::iterator *it, int64_t *data_pool_id);
    int get_data_pool(librados::IoCtx *ioctx, const std::string &oid,
                      int64_t *data_pool_id);
    int get_size(librados::IoCtx *ioctx, const std::string &oid,
		 snapid_t snap_id, uint64_t *size, uint8_t *order);
    int set_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t size);
    void set_size(librados::ObjectWriteOperation *op, uint64_t size);
    int get_parent(librados::IoCtx *ioctx, const std::string &oid,
		   snapid_t snap_id, ParentSpec *pspec,
		   uint64_t *parent_overlap);
    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   const ParentSpec &pspec, uint64_t parent_overlap);
    void set_parent(librados::ObjectWriteOperation *op,
                    const ParentSpec &pspec, uint64_t parent_overlap);
    void get_flags_start(librados::ObjectReadOperation *op,
                         const std::vector<snapid_t> &snap_ids);
    int get_flags_finish(bufferlist::iterator *it, uint64_t *flags,
                         const std::vector<snapid_t> &snap_ids,
                         std::vector<uint64_t> *snap_flags);
    int get_flags(librados::IoCtx *ioctx, const std::string &oid,
		  uint64_t *flags, const std::vector<snapid_t> &snap_ids,
		  vector<uint64_t> *snap_flags);
    void set_flags(librados::ObjectWriteOperation *op, snapid_t snap_id,
                   uint64_t flags, uint64_t mask);
    int remove_parent(librados::IoCtx *ioctx, const std::string &oid);
    void remove_parent(librados::ObjectWriteOperation *op);
    int add_child(librados::IoCtx *ioctx, const std::string &oid,
		  const ParentSpec &pspec, const std::string &c_imageid);
    void add_child(librados::ObjectWriteOperation *op,
		  const ParentSpec pspec, const std::string &c_imageid);
    void remove_child(librados::ObjectWriteOperation *op,
		      const ParentSpec &pspec, const std::string &c_imageid);
    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     const ParentSpec &pspec, const std::string &c_imageid);
    void get_children_start(librados::ObjectReadOperation *op,
                            const ParentSpec &pspec);
    int get_children_finish(bufferlist::iterator *it,
                            std::set<string> *children);
    int get_children(librados::IoCtx *ioctx, const std::string &oid,
                      const ParentSpec &pspec, set<string>& children);
    void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
		      const std::string &snap_name,
		      const cls::rbd::SnapshotNamespace &snap_namespace);
    void snapshot_remove(librados::ObjectWriteOperation *op, snapid_t snap_id);
    void snapshot_rename(librados::ObjectWriteOperation *op,
			snapid_t src_snap_id,
			const std::string &dst_name);
    int get_snapcontext(librados::IoCtx *ioctx, const std::string &oid,
			::SnapContext *snapc);

    void snapshot_list_start(librados::ObjectReadOperation *op,
                             const std::vector<snapid_t> &ids);
    int snapshot_list_finish(bufferlist::iterator *it,
                             const std::vector<snapid_t> &ids,
                             std::vector<string> *names,
                             std::vector<uint64_t> *sizes,
                             std::vector<ParentInfo> *parents,
                             std::vector<uint8_t> *protection_statuses);
    void snapshot_timestamp_list_start(librados::ObjectReadOperation *op,
                                       const std::vector<snapid_t> &ids);

    int snapshot_timestamp_list_finish(bufferlist::iterator *it,
                                       const std::vector<snapid_t> &ids,
                                       std::vector<utime_t> *timestamps);

    int snapshot_timestamp_list(librados::IoCtx *ioctx, const std::string &oid,
                                const std::vector<snapid_t> &ids,
                                std::vector<utime_t> *timestamps);

    int snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
		      const std::vector<snapid_t> &ids,
		      std::vector<string> *names,
		      std::vector<uint64_t> *sizes,
		      std::vector<ParentInfo> *parents,
		      std::vector<uint8_t> *protection_statuses);

    void snapshot_namespace_list_start(librados::ObjectReadOperation *op,
                                       const std::vector<snapid_t> &ids);
    int snapshot_namespace_list_finish(bufferlist::iterator *it,
                                       const std::vector<snapid_t> &ids,
                                       std::vector<cls::rbd::SnapshotNamespace> *namespaces);
    int snapshot_namespace_list(librados::IoCtx *ioctx, const std::string &oid,
                                const std::vector<snapid_t> &ids,
                                std::vector<cls::rbd::SnapshotNamespace> *namespaces);

    void get_all_features_start(librados::ObjectReadOperation *op);
    int get_all_features_finish(bufferlist::iterator *it,
                                uint64_t *all_features);
    int get_all_features(librados::IoCtx *ioctx, const std::string &oid,
                         uint64_t *all_features);

    int copyup(librados::IoCtx *ioctx, const std::string &oid,
	       bufferlist data);
    int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t *protection_status);
    int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t protection_status);
    void set_protection_status(librados::ObjectWriteOperation *op,
                               snapid_t snap_id, uint8_t protection_status);
    int snapshot_get_limit(librados::IoCtx *ioctx, const std::string &oid,
			   uint64_t *limit);
    void snapshot_set_limit(librados::ObjectWriteOperation *op,
			    uint64_t limit);

    void get_stripe_unit_count_start(librados::ObjectReadOperation *op);
    int get_stripe_unit_count_finish(bufferlist::iterator *it,
                                     uint64_t *stripe_unit,
                                     uint64_t *stripe_count);
    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count);

    void set_stripe_unit_count(librados::ObjectWriteOperation *op,
			       uint64_t stripe_unit, uint64_t stripe_count);
    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count);
    void get_create_timestamp_start(librados::ObjectReadOperation *op);
    int get_create_timestamp_finish(bufferlist::iterator *it,
                                    utime_t *timestamp);
    int get_create_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                             utime_t *timestamp);
    int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &start, uint64_t max_return,
                      map<string, bufferlist> *pairs);
    void metadata_list_start(librados::ObjectReadOperation *op,
                             const std::string &start, uint64_t max_return);
    int metadata_list_finish(bufferlist::iterator *it,
                             std::map<std::string, bufferlist> *pairs);
    void metadata_set(librados::ObjectWriteOperation *op,
                      const map<std::string, bufferlist> &data);
    int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                     const map<std::string, bufferlist> &data);
    void metadata_remove(librados::ObjectWriteOperation *op,
                         const std::string &key);
    int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                        const std::string &key);
    int metadata_get(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &key, string *v);

    // operations on rbd_id objects
    void get_id_start(librados::ObjectReadOperation *op);
    int get_id_finish(bufferlist::iterator *it, std::string *id);
    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id);

    void set_id(librados::ObjectWriteOperation *op, std::string id);
    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id);

    // operations on rbd_directory objects
    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, std::string *id);
    void dir_get_id_start(librados::ObjectReadOperation *op,
                          const std::string &image_name);
    int dir_get_id_finish(bufferlist::iterator *iter, std::string *image_id);
    void dir_get_name_start(librados::ObjectReadOperation *op,
			    const std::string &id);
    int dir_get_name_finish(bufferlist::iterator *it, std::string *name);
    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name);
    void dir_list_start(librados::ObjectReadOperation *op,
                        const std::string &start, uint64_t max_return);
    int dir_list_finish(bufferlist::iterator *it, map<string, string> *images);
    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images);
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

    // operations on the rbd_object_map.$image_id object
    void object_map_load_start(librados::ObjectReadOperation *op);
    int object_map_load_finish(bufferlist::iterator *it,
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
    // backwards compatability
    void old_snapshot_add(librados::ObjectWriteOperation *rados_op,
                          snapid_t snap_id, const std::string &snap_name);
    void old_snapshot_remove(librados::ObjectWriteOperation *rados_op,
			    const std::string &snap_name);
    void old_snapshot_rename(librados::ObjectWriteOperation *rados_op,
			     snapid_t src_snap_id, const std::string &dst_name);

    void old_snapshot_list_start(librados::ObjectReadOperation *op);
    int old_snapshot_list_finish(bufferlist::iterator *it,
                                 std::vector<string> *names,
                                 std::vector<uint64_t> *sizes,
                                 ::SnapContext *snapc);
    int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
			  std::vector<string> *names,
			  std::vector<uint64_t> *sizes,
			  ::SnapContext *snapc);

    // operations on the rbd_mirroring object
    void mirror_uuid_get_start(librados::ObjectReadOperation *op);
    int mirror_uuid_get_finish(bufferlist::iterator *it,
                               std::string *uuid);
    int mirror_uuid_get(librados::IoCtx *ioctx, std::string *uuid);
    int mirror_uuid_set(librados::IoCtx *ioctx, const std::string &uuid);
    void mirror_mode_get_start(librados::ObjectReadOperation *op);
    int mirror_mode_get_finish(bufferlist::iterator *it,
			       cls::rbd::MirrorMode *mirror_mode);
    int mirror_mode_get(librados::IoCtx *ioctx,
                        cls::rbd::MirrorMode *mirror_mode);
    int mirror_mode_set(librados::IoCtx *ioctx,
                        cls::rbd::MirrorMode mirror_mode);
    int mirror_peer_list(librados::IoCtx *ioctx,
                         std::vector<cls::rbd::MirrorPeer> *peers);
    int mirror_peer_add(librados::IoCtx *ioctx, const std::string &uuid,
                        const std::string &cluster_name,
                        const std::string &client_name,
                        int64_t pool_id = -1);
    int mirror_peer_remove(librados::IoCtx *ioctx,
                           const std::string &uuid);
    int mirror_peer_set_client(librados::IoCtx *ioctx,
                               const std::string &uuid,
                               const std::string &client_name);
    int mirror_peer_set_cluster(librados::IoCtx *ioctx,
                                const std::string &uuid,
                                const std::string &cluster_name);
    void mirror_image_list_start(librados::ObjectReadOperation *op,
                                 const std::string &start, uint64_t max_return);
    int mirror_image_list_finish(bufferlist::iterator *it,
                                 std::map<string, string> *mirror_image_ids);
    int mirror_image_list(librados::IoCtx *ioctx,
		          const std::string &start, uint64_t max_return,
                          std::map<std::string, std::string> *mirror_image_ids);
    void mirror_image_get_image_id_start(librados::ObjectReadOperation *op,
                                         const std::string &global_image_id);
    int mirror_image_get_image_id_finish(bufferlist::iterator *it,
                                         std::string *image_id);
    int mirror_image_get_image_id(librados::IoCtx *ioctx,
                                  const std::string &global_image_id,
                                  std::string *image_id);
    int mirror_image_get(librados::IoCtx *ioctx, const std::string &image_id,
			 cls::rbd::MirrorImage *mirror_image);
    void mirror_image_get_start(librados::ObjectReadOperation *op,
                                const std::string &image_id);
    int mirror_image_get_finish(bufferlist::iterator *iter,
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
				const cls::rbd::MirrorImageStatus &status);
    void mirror_image_status_set(librados::ObjectWriteOperation *op,
				 const std::string &global_image_id,
				 const cls::rbd::MirrorImageStatus &status);
    int mirror_image_status_remove(librados::IoCtx *ioctx,
				   const std::string &global_image_id);
    void mirror_image_status_remove(librados::ObjectWriteOperation *op,
				    const std::string &global_image_id);
    int mirror_image_status_get(librados::IoCtx *ioctx,
				const std::string &global_image_id,
				cls::rbd::MirrorImageStatus *status);
    void mirror_image_status_get_start(librados::ObjectReadOperation *op,
				       const std::string &global_image_id);
    int mirror_image_status_get_finish(bufferlist::iterator *iter,
				       cls::rbd::MirrorImageStatus *status);
    int mirror_image_status_list(librados::IoCtx *ioctx,
	const std::string &start, uint64_t max_return,
	std::map<std::string, cls::rbd::MirrorImage> *images,
	std::map<std::string, cls::rbd::MirrorImageStatus> *statuses);
    void mirror_image_status_list_start(librados::ObjectReadOperation *op,
					const std::string &start,
					uint64_t max_return);
    int mirror_image_status_list_finish(bufferlist::iterator *iter,
	std::map<std::string, cls::rbd::MirrorImage> *images,
	std::map<std::string, cls::rbd::MirrorImageStatus> *statuses);
    int mirror_image_status_get_summary(librados::IoCtx *ioctx,
	std::map<cls::rbd::MirrorImageStatusState, int> *states);
    void mirror_image_status_get_summary_start(librados::ObjectReadOperation *op);
    int mirror_image_status_get_summary_finish(bufferlist::iterator *iter,
	std::map<cls::rbd::MirrorImageStatusState, int> *states);
    int mirror_image_status_remove_down(librados::IoCtx *ioctx);
    void mirror_image_status_remove_down(librados::ObjectWriteOperation *op);

    void mirror_instances_list_start(librados::ObjectReadOperation *op);
    int mirror_instances_list_finish(bufferlist::iterator *iter,
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

    // Consistency groups functions
    int group_create(librados::IoCtx *ioctx, const std::string &oid);
    int group_dir_list(librados::IoCtx *ioctx, const std::string &oid,
		    const std::string &start, uint64_t max_return,
		    map<string, string> *groups);
    int group_dir_add(librados::IoCtx *ioctx, const std::string &oid,
	           const std::string &name, const std::string &id);
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
    int image_add_group(librados::IoCtx *ioctx, const std::string &oid,
	                const cls::rbd::GroupSpec &group_spec);
    int image_remove_group(librados::IoCtx *ioctx, const std::string &oid,
			   const cls::rbd::GroupSpec &group_spec);
    void image_get_group_start(librados::ObjectReadOperation *op);
    int image_get_group_finish(bufferlist::iterator *iter,
                               cls::rbd::GroupSpec *group_spec);
    int image_get_group(librados::IoCtx *ioctx, const std::string &oid,
			cls::rbd::GroupSpec *group_spec);

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
    int trash_list_finish(bufferlist::iterator *it,
                          map<string, cls::rbd::TrashImageSpec> *entries);
    int trash_list(librados::IoCtx *ioctx,
                   const std::string &start, uint64_t max_return,
                   map<string, cls::rbd::TrashImageSpec> *entries);
    void trash_get_start(librados::ObjectReadOperation *op,
                         const std::string &id);
    int trash_get_finish(bufferlist::iterator *it,
                         cls::rbd::TrashImageSpec *trash_spec);
    int trash_get(librados::IoCtx *ioctx, const std::string &id,
                  cls::rbd::TrashImageSpec *trash_spec);

  } // namespace cls_client
} // namespace librbd
#endif // CEPH_LIBRBD_CLS_RBD_CLIENT_H
