// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CLS_RBD_CLIENT_H
#define CEPH_LIBRBD_CLS_RBD_CLIENT_H

#include "cls/lock/cls_lock_types.h"
#include "common/snap_types.h"
#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "librbd/parent_types.h"

#include <string>
#include <vector>

namespace librbd {
  namespace cls_client {
    // high-level interface to the header
    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order);
    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
			     bool *exclusive_lock,
			     std::string *lock_tag,
			     ::SnapContext *snapc,
			     parent_info *parent);

    // low-level interface (mainly for testing)
    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix);
    int get_features(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, uint64_t *features);
    int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
			  std::string *object_prefix);
    int get_size(librados::IoCtx *ioctx, const std::string &oid,
		 snapid_t snap_id, uint64_t *size, uint8_t *order);
    int set_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t size);
    int set_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t size);
    int get_parent(librados::IoCtx *ioctx, const std::string &oid,
		   snapid_t snap_id, parent_spec *pspec,
		   uint64_t *parent_overlap);
    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   parent_spec pspec, uint64_t parent_overlap);
    int remove_parent(librados::IoCtx *ioctx, const std::string &oid);
    int add_child(librados::IoCtx *ioctx, const std::string &oid,
		  parent_spec pspec, const std::string &c_imageid);
    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, const std::string &c_imageid);
    int get_children(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, set<string>& children);
    int snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, const std::string &snap_name);
    int snapshot_remove(librados::IoCtx *ioctx, const std::string &oid,
			snapid_t snap_id);
    int get_snapcontext(librados::IoCtx *ioctx, const std::string &oid,
			::SnapContext *snapc);
    int snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
		      const std::vector<snapid_t> &ids,
		      std::vector<string> *names,
		      std::vector<uint64_t> *sizes,
		      std::vector<uint64_t> *features,
		      std::vector<parent_info> *parents,
		      std::vector<uint8_t> *protection_statuses);
    int copyup(librados::IoCtx *ioctx, const std::string &oid,
	       bufferlist data);
    int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t *protection_status);
    int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t protection_status);
    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count);
    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count);

    // operations on rbd_id objects
    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id);
    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id);

    // operations on rbd_directory objects
    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, std::string *id);
    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name);
    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images);
    int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id);
    int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &name, const std::string &id);
    // atomic remove and add
    int dir_rename_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &src, const std::string &dest,
			 const std::string &id);

    // class operations on the old format, kept for
    // backwards compatability
    int old_snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
			 snapid_t snap_id, const std::string &snap_name);
    int old_snapshot_remove(librados::IoCtx *ioctx, const std::string &oid,
			    const std::string &snap_name);
    int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
			  std::vector<string> *names,
			  std::vector<uint64_t> *sizes,
			  ::SnapContext *snapc);
  } // namespace cls_client
} // namespace librbd
#endif // CEPH_LIBRBD_CLS_RBD_CLIENT_H
