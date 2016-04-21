// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CLS_RBD_CLIENT_H
#define CEPH_LIBRBD_CLS_RBD_CLIENT_H

#include "cls/lock/cls_lock_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/bit_vector.hpp"
#include "common/snap_types.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "librbd/parent_types.h"

#include <string>
#include <vector>

class Context;

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
                                    ::SnapContext *snapc, parent_info *parent);
    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     bool read_only, uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
			     bool *exclusive_lock,
			     std::string *lock_tag,
			     ::SnapContext *snapc,
			     parent_info *parent);

    // low-level interface (mainly for testing)
    int create_cg(librados::IoCtx *ioctx, const std::string &oid);
    int cg_add_image(librados::IoCtx *ioctx, const std::string &oid,
		     std::string &image_id, int64_t pool_id);
    int cg_remove_image(librados::IoCtx *ioctx, const std::string &oid,
		        std::string &image_id);
    int image_add_cg_ref(librados::IoCtx *ioctx, const std::string &oid,
		         std::string &cg_id, int64_t pool_id);
    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix);
    int get_features(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, uint64_t *features);
    int set_features(librados::IoCtx *ioctx, const std::string &oid,
                     uint64_t features, uint64_t mask);
    int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
			  std::string *object_prefix);
    int get_size(librados::IoCtx *ioctx, const std::string &oid,
		 snapid_t snap_id, uint64_t *size, uint8_t *order);
    int set_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t size);
    void set_size(librados::ObjectWriteOperation *op, uint64_t size);
    int get_parent(librados::IoCtx *ioctx, const std::string &oid,
		   snapid_t snap_id, parent_spec *pspec,
		   uint64_t *parent_overlap);
    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   parent_spec pspec, uint64_t parent_overlap);
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
		  parent_spec pspec, const std::string &c_imageid);
    void remove_child(librados::ObjectWriteOperation *op,
		      parent_spec pspec, const std::string &c_imageid);
    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, const std::string &c_imageid);
    void get_children_start(librados::ObjectReadOperation *op,
                            const parent_spec &pspec);
    int get_children_finish(bufferlist::iterator *it,
                            std::set<string> *children);
    int get_children(librados::IoCtx *ioctx, const std::string &oid,
                      parent_spec pspec, set<string>& children);
    void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
		      const std::string &snap_name);
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
                             std::vector<parent_info> *parents,
                             std::vector<uint8_t> *protection_statuses);
    int snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
		      const std::vector<snapid_t> &ids,
		      std::vector<string> *names,
		      std::vector<uint64_t> *sizes,
		      std::vector<parent_info> *parents,
		      std::vector<uint8_t> *protection_statuses);

    int copyup(librados::IoCtx *ioctx, const std::string &oid,
	       bufferlist data);
    int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t *protection_status);
    int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t protection_status);
    void set_protection_status(librados::ObjectWriteOperation *op,
                               snapid_t snap_id, uint8_t protection_status);

    void get_stripe_unit_count_start(librados::ObjectReadOperation *op);
    int get_stripe_unit_count_finish(bufferlist::iterator *it,
                                     uint64_t *stripe_unit,
                                     uint64_t *stripe_count);
    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count);

    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count);
    int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &start, uint64_t max_return,
                      map<string, bufferlist> *pairs);
    int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                     const map<std::string, bufferlist> &data);
    int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                        const std::string &key);
    int metadata_get(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &key, string *v);

    // operations on rbd_id objects
    void get_id_start(librados::ObjectReadOperation *op);
    int get_id_finish(bufferlist::iterator *it, std::string *id);
    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id);

    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id);

    // operations on rbd_directory objects
    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, std::string *id);
    void dir_get_name_start(librados::ObjectReadOperation *op,
			    const std::string &id);
    int dir_get_name_finish(bufferlist::iterator *it, std::string *name);
    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name);
    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images);
    int dir_list_cgs(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &start, uint64_t max_return,
		     map<string, string> *cgs);
    int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id);
    int dir_add_cg(librados::IoCtx *ioctx, const std::string &oid,
	           const std::string &name, const std::string &id);
    int dir_remove_cg(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id);
    int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
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
    int mirror_uuid_get(librados::IoCtx *ioctx, std::string *uuid);
    int mirror_uuid_set(librados::IoCtx *ioctx, const std::string &uuid);
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
    int mirror_image_list(librados::IoCtx *ioctx,
			  std::vector<std::string> *image_ids);
    int mirror_image_get(librados::IoCtx *ioctx, const std::string &image_id,
			 cls::rbd::MirrorImage *mirror_image);
    int mirror_image_set(librados::IoCtx *ioctx, const std::string &image_id,
			 const cls::rbd::MirrorImage &mirror_image);
    int mirror_image_remove(librados::IoCtx *ioctx,
			    const std::string &image_id);

  } // namespace cls_client
} // namespace librbd
#endif // CEPH_LIBRBD_CLS_RBD_CLIENT_H
