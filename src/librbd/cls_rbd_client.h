// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CLS_RBD_CLIENT_H
#define CEPH_LIBRBD_CLS_RBD_CLIENT_H

#include "common/snap_types.h"
#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"

#include <string>
#include <vector>

namespace librbd {
  namespace cls_client {

    struct parent_info {
      int64_t pool_id;
      string image_id;
      snapid_t snap_id;
      uint64_t overlap;
      parent_info() : pool_id(-1), snap_id(CEPH_NOSNAP), overlap(0) {}
    };

    // high-level interface to the header
    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order);
    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     std::set<std::pair<std::string, std::string> >* lockers,
			     bool *exclusive_lock,
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
		   snapid_t snap_id, int64_t *parent_pool,
		   std::string *parent_image, snapid_t *parent_snap_id,
		   uint64_t *parent_overlap);
    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   int64_t parent_pool, const std::string& parent_image,
		   snapid_t parent_snap_id, uint64_t parent_overlap);
    int remove_parent(librados::IoCtx *ioctx, const std::string &oid);
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
		      std::vector<parent_info> *parents);
    int assign_bid(librados::IoCtx *ioctx, const std::string &oid,
		   uint64_t *id);

    int list_locks(librados::IoCtx *ioctx, const std::string &oid,
		   std::set<std::pair<std::string, std::string> > &locks,
		   bool &exclusive);
    int lock_image_exclusive(librados::IoCtx *ioctx, const std::string &oid,
			     const std::string &cookie);
    int lock_image_shared(librados::IoCtx *ioctx, const std::string &oid,
			  const std::string &cookie);
    int unlock_image(librados::IoCtx *ioctx, const std::string& oid,
		     const std::string &cookie);
    int break_lock(librados::IoCtx *ioctx, const std::string& oid,
		   const std::string &locker, const std::string &cookie);

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
