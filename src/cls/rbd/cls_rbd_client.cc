// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_client.h"
#include "cls/lock/cls_lock_client.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rbd_types.h"
#include "include/rados/librados.hpp"

#include <errno.h>

namespace librbd {
  namespace cls_client {

    void get_immutable_metadata_start(librados::ObjectReadOperation *op) {
      bufferlist bl, empty_bl;
      snapid_t snap = CEPH_NOSNAP;
      ::encode(snap, bl);
      op->exec("rbd", "get_size", bl);
      op->exec("rbd", "get_object_prefix", empty_bl);
    }

    int get_immutable_metadata_finish(bufferlist::iterator *it,
                                      std::string *object_prefix,
                                      uint8_t *order) {
      try {
	uint64_t size;
	// get_size
	::decode(*order, *it);
	::decode(size, *it);
	// get_object_prefix
	::decode(*object_prefix, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;

    }

    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order)
    {
      librados::ObjectReadOperation op;
      get_immutable_metadata_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_immutable_metadata_finish(&it, object_prefix, order);
    }

    void get_mutable_metadata_start(librados::ObjectReadOperation *op,
                                    bool read_only) {
      snapid_t snap = CEPH_NOSNAP;
      bufferlist size_bl;
      ::encode(snap, size_bl);
      op->exec("rbd", "get_size", size_bl);

      bufferlist features_bl;
      ::encode(snap, features_bl);
      ::encode(read_only, features_bl);
      op->exec("rbd", "get_features", features_bl);

      bufferlist empty_bl;
      op->exec("rbd", "get_snapcontext", empty_bl);

      bufferlist parent_bl;
      ::encode(snap, parent_bl);
      op->exec("rbd", "get_parent", parent_bl);

      rados::cls::lock::get_lock_info_start(op, RBD_LOCK_NAME);
    }

    int get_mutable_metadata_finish(bufferlist::iterator *it,
                                    uint64_t *size, uint64_t *features,
                                    uint64_t *incompatible_features,
                                    std::map<rados::cls::lock::locker_id_t,
                                             rados::cls::lock::locker_info_t> *lockers,
                                    bool *exclusive_lock, std::string *lock_tag,
				    ::SnapContext *snapc, ParentInfo *parent) {
      assert(size);
      assert(features);
      assert(incompatible_features);
      assert(lockers);
      assert(exclusive_lock);
      assert(snapc);
      assert(parent);

      try {
	uint8_t order;
	// get_size
	::decode(order, *it);
	::decode(*size, *it);
	// get_features
	::decode(*features, *it);
	::decode(*incompatible_features, *it);
	// get_snapcontext
	::decode(*snapc, *it);
	// get_parent
	::decode(parent->spec.pool_id, *it);
	::decode(parent->spec.image_id, *it);
	::decode(parent->spec.snap_id, *it);
	::decode(parent->overlap, *it);

	// get_lock_info
	ClsLockType lock_type = LOCK_NONE;
	int r = rados::cls::lock::get_lock_info_finish(it, lockers, &lock_type,
						       lock_tag);
        if (r == -EOPNOTSUPP) {
          r = 0;
        }
        if (r == 0) {
	  *exclusive_lock = (lock_type == LOCK_EXCLUSIVE);
        }
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     bool read_only, uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
                             bool *exclusive_lock,
			     string *lock_tag,
			     ::SnapContext *snapc,
			     ParentInfo *parent)
    {
      librados::ObjectReadOperation op;
      get_mutable_metadata_start(&op, read_only);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_mutable_metadata_finish(&it, size, features,
                                         incompatible_features, lockers,
                                         exclusive_lock, lock_tag, snapc,
                                         parent);
    }

    void create_image(librados::ObjectWriteOperation *op, uint64_t size,
                      uint8_t order, uint64_t features,
                      const std::string &object_prefix, int64_t data_pool_id)
    {
      bufferlist bl;
      ::encode(size, bl);
      ::encode(order, bl);
      ::encode(features, bl);
      ::encode(object_prefix, bl);
      ::encode(data_pool_id, bl);

      op->exec("rbd", "create", bl);
    }

    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix, int64_t data_pool_id)
    {
      librados::ObjectWriteOperation op;
      create_image(&op, size, order, features, object_prefix, data_pool_id);

      return ioctx->operate(oid, &op);
    }

    int get_features(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, uint64_t *features)
    {
      bufferlist inbl, outbl;
      ::encode(snap_id, inbl);

      int r = ioctx->exec(oid, "rbd", "get_features", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*features, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    void set_features(librados::ObjectWriteOperation *op, uint64_t features,
                      uint64_t mask)
    {
      bufferlist bl;
      ::encode(features, bl);
      ::encode(mask, bl);

      op->exec("rbd", "set_features", bl);
    }

    int set_features(librados::IoCtx *ioctx, const std::string &oid,
                      uint64_t features, uint64_t mask)
    {
      librados::ObjectWriteOperation op;
      set_features(&op, features, mask);

      return ioctx->operate(oid, &op);
    }

    int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
			  std::string *object_prefix)
    {
      bufferlist inbl, outbl;
      int r = ioctx->exec(oid, "rbd", "get_object_prefix", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*object_prefix, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    void get_data_pool_start(librados::ObjectReadOperation *op) {
      bufferlist bl;
      op->exec("rbd", "get_data_pool", bl);
    }

    int get_data_pool_finish(bufferlist::iterator *it, int64_t *data_pool_id) {
      try {
	::decode(*data_pool_id, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int get_data_pool(librados::IoCtx *ioctx, const std::string &oid,
                      int64_t *data_pool_id) {
      librados::ObjectReadOperation op;
      get_data_pool_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_data_pool_finish(&it, data_pool_id);
    }

    int get_size(librados::IoCtx *ioctx, const std::string &oid,
		 snapid_t snap_id, uint64_t *size, uint8_t *order)
    {
      bufferlist inbl, outbl;
      ::encode(snap_id, inbl);

      int r = ioctx->exec(oid, "rbd", "get_size", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*order, iter);
	::decode(*size, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_size(librados::IoCtx *ioctx, const std::string &oid,
                 uint64_t size)
    {
      librados::ObjectWriteOperation op;
      set_size(&op, size);
      return ioctx->operate(oid, &op);
    }

    void set_size(librados::ObjectWriteOperation *op, uint64_t size)
    {
      bufferlist bl;
      ::encode(size, bl);
      op->exec("rbd", "set_size", bl);
    }

    int get_parent(librados::IoCtx *ioctx, const std::string &oid,
		   snapid_t snap_id, ParentSpec *pspec,
		   uint64_t *parent_overlap)
    {
      bufferlist inbl, outbl;
      ::encode(snap_id, inbl);

      int r = ioctx->exec(oid, "rbd", "get_parent", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(pspec->pool_id, iter);
	::decode(pspec->image_id, iter);
	::decode(pspec->snap_id, iter);
	::decode(*parent_overlap, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   const ParentSpec &pspec, uint64_t parent_overlap)
    {
      librados::ObjectWriteOperation op;
      set_parent(&op, pspec, parent_overlap);
      return ioctx->operate(oid, &op);
    }

    void set_parent(librados::ObjectWriteOperation *op,
                    const ParentSpec &pspec, uint64_t parent_overlap) {
      bufferlist in_bl;
      ::encode(pspec.pool_id, in_bl);
      ::encode(pspec.image_id, in_bl);
      ::encode(pspec.snap_id, in_bl);
      ::encode(parent_overlap, in_bl);

      op->exec("rbd", "set_parent", in_bl);
    }

    void get_flags_start(librados::ObjectReadOperation *op,
                         const std::vector<snapid_t> &snap_ids) {
      bufferlist in_bl;
      ::encode(static_cast<snapid_t>(CEPH_NOSNAP), in_bl);

      op->exec("rbd", "get_flags", in_bl);
      for (size_t i = 0; i < snap_ids.size(); ++i) {
        bufferlist snap_bl;
        ::encode(snap_ids[i], snap_bl);
        op->exec("rbd", "get_flags", snap_bl);
      }

    }

    int get_flags_finish(bufferlist::iterator *it, uint64_t *flags,
                         const std::vector<snapid_t> &snap_ids,
                         std::vector<uint64_t> *snap_flags) {
      snap_flags->resize(snap_ids.size());
      try {
        ::decode(*flags, *it);
	for (size_t i = 0; i < snap_flags->size(); ++i) {
	  ::decode((*snap_flags)[i], *it);
	}
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int get_flags(librados::IoCtx *ioctx, const std::string &oid,
		  uint64_t *flags, const std::vector<snapid_t> &snap_ids,
		  vector<uint64_t> *snap_flags)
    {
      librados::ObjectReadOperation op;
      get_flags_start(&op, snap_ids);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_flags_finish(&it, flags, snap_ids, snap_flags);
    }

    void set_flags(librados::ObjectWriteOperation *op, snapid_t snap_id,
                   uint64_t flags, uint64_t mask)
    {
      bufferlist inbl;
      ::encode(flags, inbl);
      ::encode(mask, inbl);
      ::encode(snap_id, inbl);
      op->exec("rbd", "set_flags", inbl);
    }

    int remove_parent(librados::IoCtx *ioctx, const std::string &oid)
    {
      librados::ObjectWriteOperation op;
      remove_parent(&op);
      return ioctx->operate(oid, &op);
    }

    void remove_parent(librados::ObjectWriteOperation *op)
    {
      bufferlist inbl;
      op->exec("rbd", "remove_parent", inbl);
    }

    int add_child(librados::IoCtx *ioctx, const std::string &oid,
		  const ParentSpec &pspec, const std::string &c_imageid)
    {
      librados::ObjectWriteOperation op;
      add_child(&op, pspec, c_imageid);
      return ioctx->operate(oid, &op);
    }

    void add_child(librados::ObjectWriteOperation *op,
		  const ParentSpec pspec, const std::string &c_imageid)
    {
      bufferlist in;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);

      op->exec("rbd", "add_child", in);
    }

    void remove_child(librados::ObjectWriteOperation *op,
		      const ParentSpec &pspec, const std::string &c_imageid)
    {
      bufferlist in;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);
      op->exec("rbd", "remove_child", in);
    }

    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     const ParentSpec &pspec, const std::string &c_imageid)
    {
      librados::ObjectWriteOperation op;
      remove_child(&op, pspec, c_imageid);
      return ioctx->operate(oid, &op);
    }

    void get_children_start(librados::ObjectReadOperation *op,
                            const ParentSpec &pspec) {
      bufferlist in_bl;
      ::encode(pspec.pool_id, in_bl);
      ::encode(pspec.image_id, in_bl);
      ::encode(pspec.snap_id, in_bl);
      op->exec("rbd", "get_children", in_bl);
    }

    int get_children_finish(bufferlist::iterator *it,
                            std::set<std::string>* children) {
      try {
        ::decode(*children, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int get_children(librados::IoCtx *ioctx, const std::string &oid,
		     const ParentSpec &pspec, set<string>& children)
    {
      librados::ObjectReadOperation op;
      get_children_start(&op, pspec);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_children_finish(&it, &children);
    }

    void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
		      const std::string &snap_name, const cls::rbd::SnapshotNamespace &snap_namespace)
    {
      bufferlist bl;
      ::encode(snap_name, bl);
      ::encode(snap_id, bl);
      ::encode(cls::rbd::SnapshotNamespaceOnDisk(snap_namespace), bl);
      op->exec("rbd", "snapshot_add", bl);
    }

    void snapshot_remove(librados::ObjectWriteOperation *op, snapid_t snap_id)
    {
      bufferlist bl;
      ::encode(snap_id, bl);
      op->exec("rbd", "snapshot_remove", bl);
    }

    void snapshot_rename(librados::ObjectWriteOperation *op,
			 snapid_t src_snap_id,
		         const std::string &dst_name)
    {
      bufferlist bl;
      ::encode(src_snap_id, bl);
      ::encode(dst_name, bl);
      op->exec("rbd", "snapshot_rename", bl);
    }

    int get_snapcontext(librados::IoCtx *ioctx, const std::string &oid,
			::SnapContext *snapc)
    {
      bufferlist inbl, outbl;

      int r = ioctx->exec(oid, "rbd", "get_snapcontext", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*snapc, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      if (!snapc->is_valid())
	return -EBADMSG;

      return 0;
    }

    void snapshot_list_start(librados::ObjectReadOperation *op,
                             const std::vector<snapid_t> &ids) {
      for (auto snap_id : ids) {
        bufferlist bl1, bl2, bl3, bl4;
        ::encode(snap_id, bl1);
        op->exec("rbd", "get_snapshot_name", bl1);
        ::encode(snap_id, bl2);
        op->exec("rbd", "get_size", bl2);
        ::encode(snap_id, bl3);
        op->exec("rbd", "get_parent", bl3);
        ::encode(snap_id, bl4);
        op->exec("rbd", "get_protection_status", bl4);
      }
    }

    int snapshot_list_finish(bufferlist::iterator *it,
                             const std::vector<snapid_t> &ids,
                             std::vector<string> *names,
                             std::vector<uint64_t> *sizes,
                             std::vector<ParentInfo> *parents,
                             std::vector<uint8_t> *protection_statuses)
    {
      names->resize(ids.size());
      sizes->resize(ids.size());
      parents->resize(ids.size());
      protection_statuses->resize(ids.size());
      try {
	for (size_t i = 0; i < names->size(); ++i) {
	  uint8_t order;
	  // get_snapshot_name
	  ::decode((*names)[i], *it);
	  // get_size
	  ::decode(order, *it);
	  ::decode((*sizes)[i], *it);
	  // get_parent
	  ::decode((*parents)[i].spec.pool_id, *it);
	  ::decode((*parents)[i].spec.image_id, *it);
	  ::decode((*parents)[i].spec.snap_id, *it);
	  ::decode((*parents)[i].overlap, *it);
	  // get_protection_status
	  ::decode((*protection_statuses)[i], *it);
	}
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
		      const std::vector<snapid_t> &ids,
		      std::vector<string> *names,
		      std::vector<uint64_t> *sizes,
		      std::vector<ParentInfo> *parents,
		      std::vector<uint8_t> *protection_statuses)
    {
      librados::ObjectReadOperation op;
      snapshot_list_start(&op, ids);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return snapshot_list_finish(&it, ids, names, sizes, parents,
                                  protection_statuses);
    }

    void snapshot_timestamp_list_start(librados::ObjectReadOperation *op,
                                       const std::vector<snapid_t> &ids)
    {
      for (auto snap_id : ids) {
        bufferlist bl;
        ::encode(snap_id, bl);
        op->exec("rbd", "get_snapshot_timestamp", bl);
      }
    }

    int snapshot_timestamp_list_finish(bufferlist::iterator *it,
                                       const std::vector<snapid_t> &ids,
                                       std::vector<utime_t> *timestamps)
    {
      timestamps->resize(ids.size());
      try {
        for (size_t i = 0; i < timestamps->size(); ++i) {
          utime_t t;
          ::decode(t, *it);
          (*timestamps)[i] = t;
        }
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int snapshot_timestamp_list(librados::IoCtx *ioctx, const std::string &oid,
                                const std::vector<snapid_t> &ids,
                                std::vector<utime_t> *timestamps)
    {
      librados::ObjectReadOperation op;
      snapshot_timestamp_list_start(&op, ids);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return snapshot_timestamp_list_finish(&it, ids, timestamps);
    }

    void snapshot_namespace_list_start(librados::ObjectReadOperation *op,
                                       const std::vector<snapid_t> &ids)
    {
      for (auto snap_id : ids) {
        bufferlist bl;
        ::encode(snap_id, bl);
        op->exec("rbd", "get_snapshot_namespace", bl);
      }
    }

    int snapshot_namespace_list_finish(bufferlist::iterator *it,
                                       const std::vector<snapid_t> &ids,
                                       std::vector<cls::rbd::SnapshotNamespace> *namespaces)
    {
      namespaces->resize(ids.size());
      try {
	for (size_t i = 0; i < namespaces->size(); ++i) {
	  cls::rbd::SnapshotNamespaceOnDisk e;
	  ::decode(e, *it);
	  (*namespaces)[i] = e.snapshot_namespace;
	}
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int snapshot_namespace_list(librados::IoCtx *ioctx, const std::string &oid,
                                const std::vector<snapid_t> &ids,
                                std::vector<cls::rbd::SnapshotNamespace> *namespaces)
    {
      librados::ObjectReadOperation op;
      snapshot_namespace_list_start(&op, ids);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return snapshot_namespace_list_finish(&it, ids, namespaces);
    }

    void old_snapshot_add(librados::ObjectWriteOperation *op,
			  snapid_t snap_id, const std::string &snap_name)
    {
      bufferlist bl;
      ::encode(snap_name, bl);
      ::encode(snap_id, bl);
      op->exec("rbd", "snap_add", bl);
    }

    void old_snapshot_remove(librados::ObjectWriteOperation *op,
			     const std::string &snap_name)
    {
      bufferlist bl;
      ::encode(snap_name, bl);
      op->exec("rbd", "snap_remove", bl);
    }

    void old_snapshot_rename(librados::ObjectWriteOperation *op,
			     snapid_t src_snap_id, const std::string &dst_name)
    {
      bufferlist bl;
      ::encode(src_snap_id, bl);
      ::encode(dst_name, bl);
      op->exec("rbd", "snap_rename", bl);
    }

    void old_snapshot_list_start(librados::ObjectReadOperation *op) {
      bufferlist in_bl;
      op->exec("rbd", "snap_list", in_bl);
    }

    int old_snapshot_list_finish(bufferlist::iterator *it,
                                 std::vector<string> *names,
                                 std::vector<uint64_t> *sizes,
                                 ::SnapContext *snapc) {
      try {
	uint32_t num_snaps;
	::decode(snapc->seq, *it);
	::decode(num_snaps, *it);

	names->resize(num_snaps);
	sizes->resize(num_snaps);
	snapc->snaps.resize(num_snaps);
	for (uint32_t i = 0; i < num_snaps; ++i) {
	  ::decode(snapc->snaps[i], *it);
	  ::decode((*sizes)[i], *it);
	  ::decode((*names)[i], *it);
	}
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
			  std::vector<string> *names,
			  std::vector<uint64_t> *sizes,
			  ::SnapContext *snapc)
    {
      librados::ObjectReadOperation op;
      old_snapshot_list_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return old_snapshot_list_finish(&it, names, sizes, snapc);
    }

    void get_all_features_start(librados::ObjectReadOperation *op) {
      bufferlist in;
      op->exec("rbd", "get_all_features", in);
    }

    int get_all_features_finish(bufferlist::iterator *it,
                                uint64_t *all_features) {
      try {
	::decode(*all_features, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int get_all_features(librados::IoCtx *ioctx, const std::string &oid,
                         uint64_t *all_features) {
      librados::ObjectReadOperation op;
      get_all_features_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_all_features_finish(&it, all_features);
    }

    int copyup(librados::IoCtx *ioctx, const std::string &oid,
	       bufferlist data) {
      bufferlist out;
      return ioctx->exec(oid, "rbd", "copyup", data, out);
    }

    int get_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t *protection_status)
    {
      bufferlist in, out;
      ::encode(snap_id.val, in);

      int r = ioctx->exec(oid, "rbd", "get_protection_status", in, out);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = out.begin();
	::decode(*protection_status, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_protection_status(librados::IoCtx *ioctx, const std::string &oid,
			      snapid_t snap_id, uint8_t protection_status)
    {
      // TODO remove
      librados::ObjectWriteOperation op;
      set_protection_status(&op, snap_id, protection_status);
      return ioctx->operate(oid, &op);
    }

    void set_protection_status(librados::ObjectWriteOperation *op,
                               snapid_t snap_id, uint8_t protection_status)
    {
      bufferlist in;
      ::encode(snap_id, in);
      ::encode(protection_status, in);
      op->exec("rbd", "set_protection_status", in);
    }

    int snapshot_get_limit(librados::IoCtx *ioctx, const std::string &oid,
			   uint64_t *limit)
    {
      bufferlist in, out;
      int r =  ioctx->exec(oid, "rbd", "snapshot_get_limit", in, out);

      if (r < 0) {
	return r;
      }

      try {
	bufferlist::iterator iter = out.begin();
	::decode(*limit, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    void snapshot_set_limit(librados::ObjectWriteOperation *op, uint64_t limit)
    {
      bufferlist in;
      ::encode(limit, in);
      op->exec("rbd", "snapshot_set_limit", in);
    }

    void get_stripe_unit_count_start(librados::ObjectReadOperation *op) {
      bufferlist empty_bl;
      op->exec("rbd", "get_stripe_unit_count", empty_bl);
    }

    int get_stripe_unit_count_finish(bufferlist::iterator *it,
                                     uint64_t *stripe_unit,
                                     uint64_t *stripe_count) {
      assert(stripe_unit);
      assert(stripe_count);

      try {
	::decode(*stripe_unit, *it);
	::decode(*stripe_count, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count)
    {
      librados::ObjectReadOperation op;
      get_stripe_unit_count_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_stripe_unit_count_finish(&it, stripe_unit, stripe_count);
    }

    void set_stripe_unit_count(librados::ObjectWriteOperation *op,
			       uint64_t stripe_unit, uint64_t stripe_count)
    {
      bufferlist bl;
      ::encode(stripe_unit, bl);
      ::encode(stripe_count, bl);

      op->exec("rbd", "set_stripe_unit_count", bl);
    }

    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count)
    {
      librados::ObjectWriteOperation op;
      set_stripe_unit_count(&op, stripe_unit, stripe_count);

      return ioctx->operate(oid, &op);
    }

    void get_create_timestamp_start(librados::ObjectReadOperation *op) {
      bufferlist empty_bl;
      op->exec("rbd", "get_create_timestamp", empty_bl);
    }

    int get_create_timestamp_finish(bufferlist::iterator *it,
                                    utime_t *timestamp) {
      assert(timestamp);

      try {
        ::decode(*timestamp, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int get_create_timestamp(librados::IoCtx *ioctx, const std::string &oid,
                             utime_t *timestamp)
    {
      librados::ObjectReadOperation op;
      get_create_timestamp_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_create_timestamp_finish(&it, timestamp);
    }

    /************************ rbd_id object methods ************************/

    void get_id_start(librados::ObjectReadOperation *op) {
      bufferlist empty_bl;
      op->exec("rbd", "get_id", empty_bl);
    }

    int get_id_finish(bufferlist::iterator *it, std::string *id) {
      try {
	::decode(*id, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id)
    {
      librados::ObjectReadOperation op;
      get_id_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return get_id_finish(&it, id);
    }

    void set_id(librados::ObjectWriteOperation *op, const std::string id)
    {
      bufferlist bl;
      ::encode(id, bl);
      op->exec("rbd", "set_id", bl);
    }

    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id)
    {
      librados::ObjectWriteOperation op;
      set_id(&op, id);

      return ioctx->operate(oid, &op);
    }

    /******************** rbd_directory object methods ********************/

    void dir_get_id_start(librados::ObjectReadOperation *op,
                          const std::string &image_name) {
      bufferlist bl;
      ::encode(image_name, bl);

      op->exec("rbd", "dir_get_id", bl);
    }

    int dir_get_id_finish(bufferlist::iterator *iter, std::string *image_id) {
      try {
        ::decode(*image_id, *iter);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }

      return 0;
    }

    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
                   const std::string &name, std::string *id) {
      librados::ObjectReadOperation op;
      dir_get_id_start(&op, name);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      return dir_get_id_finish(&iter, id);
    }

    void dir_get_name_start(librados::ObjectReadOperation *op,
			    const std::string &id) {
      bufferlist in_bl;
      ::encode(id, in_bl);
      op->exec("rbd", "dir_get_name", in_bl);
    }

    int dir_get_name_finish(bufferlist::iterator *it, std::string *name) {
      try {
	::decode(*name, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name) {
      librados::ObjectReadOperation op;
      dir_get_name_start(&op, id);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return dir_get_name_finish(&it, name);
    }

    void dir_list_start(librados::ObjectReadOperation *op,
                        const std::string &start, uint64_t max_return)
    {
      bufferlist in_bl;
      ::encode(start, in_bl);
      ::encode(max_return, in_bl);

      op->exec("rbd", "dir_list", in_bl);
    }

    int dir_list_finish(bufferlist::iterator *it, map<string, string> *images)
    {
      try {
        ::decode(*images, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images)
    {
      librados::ObjectReadOperation op;
      dir_list_start(&op, start, max_return);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      return dir_list_finish(&iter, images);
    }

    void dir_add_image(librados::ObjectWriteOperation *op,
		       const std::string &name, const std::string &id)
    {
      bufferlist bl;
      ::encode(name, bl);
      ::encode(id, bl);
      op->exec("rbd", "dir_add_image", bl);
    }

    int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id)
    {
      librados::ObjectWriteOperation op;
      dir_add_image(&op, name, id);

      return ioctx->operate(oid, &op);
    }

    int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &name, const std::string &id)
    {
      librados::ObjectWriteOperation op;
      dir_remove_image(&op, name, id);

      return ioctx->operate(oid, &op);
    }

    void dir_remove_image(librados::ObjectWriteOperation *op,
			  const std::string &name, const std::string &id)
    {
      bufferlist bl;
      ::encode(name, bl);
      ::encode(id, bl);

      op->exec("rbd", "dir_remove_image", bl);
    }

    void dir_rename_image(librados::ObjectWriteOperation *op,
			 const std::string &src, const std::string &dest,
			 const std::string &id)
    {
      bufferlist in;
      ::encode(src, in);
      ::encode(dest, in);
      ::encode(id, in);
      op->exec("rbd", "dir_rename_image", in);
    }

    void object_map_load_start(librados::ObjectReadOperation *op) {
      bufferlist in_bl;
      op->exec("rbd", "object_map_load", in_bl);
    }

    int object_map_load_finish(bufferlist::iterator *it,
                               ceph::BitVector<2> *object_map) {
      try {
        ::decode(*object_map, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int object_map_load(librados::IoCtx *ioctx, const std::string &oid,
			ceph::BitVector<2> *object_map)
    {
      librados::ObjectReadOperation op;
      object_map_load_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return object_map_load_finish(&it, object_map);
    }

    void object_map_save(librados::ObjectWriteOperation *rados_op,
                         const ceph::BitVector<2> &object_map)
    {
      ceph::BitVector<2> object_map_copy(object_map);
      object_map_copy.set_crc_enabled(false);

      bufferlist in;
      ::encode(object_map_copy, in);
      rados_op->exec("rbd", "object_map_save", in);
    }

    void object_map_resize(librados::ObjectWriteOperation *rados_op,
                           uint64_t object_count, uint8_t default_state)
    {
      bufferlist in;
      ::encode(object_count, in);
      ::encode(default_state, in);
      rados_op->exec("rbd", "object_map_resize", in);
    }

    void object_map_update(librados::ObjectWriteOperation *rados_op,
			   uint64_t start_object_no, uint64_t end_object_no,
                           uint8_t new_object_state,
			   const boost::optional<uint8_t> &current_object_state)
    {
      bufferlist in;
      ::encode(start_object_no, in);
      ::encode(end_object_no, in);
      ::encode(new_object_state, in);
      ::encode(current_object_state, in);
      rados_op->exec("rbd", "object_map_update", in);
    }

    void object_map_snap_add(librados::ObjectWriteOperation *rados_op)
    {
      bufferlist in;
      rados_op->exec("rbd", "object_map_snap_add", in);
    }

    void object_map_snap_remove(librados::ObjectWriteOperation *rados_op,
                                const ceph::BitVector<2> &object_map)
    {
      ceph::BitVector<2> object_map_copy(object_map);
      object_map_copy.set_crc_enabled(false);

      bufferlist in;
      ::encode(object_map_copy, in);
      rados_op->exec("rbd", "object_map_snap_remove", in);
    }

    void metadata_set(librados::ObjectWriteOperation *op,
                     const map<string, bufferlist> &data)
    {
      bufferlist bl;
      ::encode(data, bl);

      op->exec("rbd", "metadata_set", bl);
    }

    int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                     const map<string, bufferlist> &data)
    {
      librados::ObjectWriteOperation op;
      metadata_set(&op, data);

      return ioctx->operate(oid, &op);
    }

    void metadata_remove(librados::ObjectWriteOperation *op,
                         const std::string &key)
    {
      bufferlist bl;
      ::encode(key, bl);

      op->exec("rbd", "metadata_remove", bl);
    }

    int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &key)
    {
      librados::ObjectWriteOperation op;
      metadata_remove(&op, key);

      return ioctx->operate(oid, &op);
    }

    int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &start, uint64_t max_return,
                      map<string, bufferlist> *pairs)
    {
      librados::ObjectReadOperation op;
      metadata_list_start(&op, start, max_return);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return metadata_list_finish(&it, pairs);
    }

    void metadata_list_start(librados::ObjectReadOperation *op,
                             const std::string &start, uint64_t max_return)
    {
      bufferlist in_bl;
      ::encode(start, in_bl);
      ::encode(max_return, in_bl);
      op->exec("rbd", "metadata_list", in_bl);
    }

    int metadata_list_finish(bufferlist::iterator *it,
                             std::map<std::string, bufferlist> *pairs)
    {
      assert(pairs);
      try {
        ::decode(*pairs, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int metadata_get(librados::IoCtx *ioctx, const std::string &oid,
                     const std::string &key, string *s)
    {
      assert(s);
      bufferlist in, out;
      ::encode(key, in);
      int r = ioctx->exec(oid, "rbd", "metadata_get", in, out);
      if (r < 0)
        return r;

      bufferlist::iterator iter = out.begin();
      try {
        ::decode(*s, iter);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }

      return 0;
    }

    void mirror_uuid_get_start(librados::ObjectReadOperation *op) {
      bufferlist bl;
      op->exec("rbd", "mirror_uuid_get", bl);
    }

    int mirror_uuid_get_finish(bufferlist::iterator *it,
                               std::string *uuid) {
      try {
        ::decode(*uuid, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int mirror_uuid_get(librados::IoCtx *ioctx, std::string *uuid) {
      librados::ObjectReadOperation op;
      mirror_uuid_get_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      r = mirror_uuid_get_finish(&it, uuid);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_uuid_set(librados::IoCtx *ioctx, const std::string &uuid) {
      bufferlist in_bl;
      ::encode(uuid, in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_uuid_set", in_bl,
                          out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    void mirror_mode_get_start(librados::ObjectReadOperation *op) {
      bufferlist bl;
      op->exec("rbd", "mirror_mode_get", bl);
    }

    int mirror_mode_get_finish(bufferlist::iterator *it,
			       cls::rbd::MirrorMode *mirror_mode) {
      try {
	uint32_t mirror_mode_decode;
	::decode(mirror_mode_decode, *it);
	*mirror_mode = static_cast<cls::rbd::MirrorMode>(mirror_mode_decode);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int mirror_mode_get(librados::IoCtx *ioctx,
                        cls::rbd::MirrorMode *mirror_mode) {
      librados::ObjectReadOperation op;
      mirror_mode_get_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r == -ENOENT) {
        *mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
        return 0;
      } else if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      r = mirror_mode_get_finish(&it, mirror_mode);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_mode_set(librados::IoCtx *ioctx,
                        cls::rbd::MirrorMode mirror_mode) {
      bufferlist in_bl;
      ::encode(static_cast<uint32_t>(mirror_mode), in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_mode_set", in_bl,
                          out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_peer_list(librados::IoCtx *ioctx,
                         std::vector<cls::rbd::MirrorPeer> *peers) {
      bufferlist in_bl;
      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_list", in_bl,
                          out_bl);
      if (r < 0) {
        return r;
      }

      peers->clear();
      try {
        bufferlist::iterator bl_it = out_bl.begin();
        ::decode(*peers, bl_it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int mirror_peer_add(librados::IoCtx *ioctx, const std::string &uuid,
                        const std::string &cluster_name,
                        const std::string &client_name, int64_t pool_id) {
      cls::rbd::MirrorPeer peer(uuid, cluster_name, client_name, pool_id);
      bufferlist in_bl;
      ::encode(peer, in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_add", in_bl,
                          out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_peer_remove(librados::IoCtx *ioctx,
                           const std::string &uuid) {
      bufferlist in_bl;
      ::encode(uuid, in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_remove", in_bl,
                          out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_peer_set_client(librados::IoCtx *ioctx,
                               const std::string &uuid,
                               const std::string &client_name) {
      bufferlist in_bl;
      ::encode(uuid, in_bl);
      ::encode(client_name, in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_set_client",
                          in_bl, out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_peer_set_cluster(librados::IoCtx *ioctx,
                                const std::string &uuid,
                                const std::string &cluster_name) {
      bufferlist in_bl;
      ::encode(uuid, in_bl);
      ::encode(cluster_name, in_bl);

      bufferlist out_bl;
      int r = ioctx->exec(RBD_MIRRORING, "rbd", "mirror_peer_set_cluster",
                          in_bl, out_bl);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    void mirror_image_list_start(librados::ObjectReadOperation *op,
                                 const std::string &start, uint64_t max_return)
    {
      bufferlist in_bl;
      ::encode(start, in_bl);
      ::encode(max_return, in_bl);
      op->exec("rbd", "mirror_image_list", in_bl);
    }

    int mirror_image_list_finish(bufferlist::iterator *it,
                                 std::map<string, string> *mirror_image_ids)
    {
      try {
        ::decode(*mirror_image_ids, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int mirror_image_list(librados::IoCtx *ioctx,
		          const std::string &start, uint64_t max_return,
			  std::map<std::string, std::string> *mirror_image_ids) {
      librados::ObjectReadOperation op;
      mirror_image_list_start(&op, start, max_return);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator bl_it = out_bl.begin();
      return mirror_image_list_finish(&bl_it, mirror_image_ids);
    }

    void mirror_image_get_image_id_start(librados::ObjectReadOperation *op,
                                         const std::string &global_image_id) {
      bufferlist in_bl;
      ::encode(global_image_id, in_bl);
      op->exec( "rbd", "mirror_image_get_image_id", in_bl);
    }

    int mirror_image_get_image_id_finish(bufferlist::iterator *it,
                                         std::string *image_id) {
      try {
	::decode(*image_id, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int mirror_image_get_image_id(librados::IoCtx *ioctx,
                                  const std::string &global_image_id,
                                  std::string *image_id) {
      librados::ObjectReadOperation op;
      mirror_image_get_image_id_start(&op, global_image_id);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return mirror_image_get_image_id_finish(&it, image_id);
    }

    int mirror_image_get(librados::IoCtx *ioctx, const std::string &image_id,
			 cls::rbd::MirrorImage *mirror_image) {
      librados::ObjectReadOperation op;
      mirror_image_get_start(&op, image_id);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      r = mirror_image_get_finish(&iter, mirror_image);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    void mirror_image_get_start(librados::ObjectReadOperation *op,
                                const std::string &image_id) {
      bufferlist in_bl;
      ::encode(image_id, in_bl);

      op->exec("rbd", "mirror_image_get", in_bl);
    }

    int mirror_image_get_finish(bufferlist::iterator *iter,
			        cls::rbd::MirrorImage *mirror_image) {
      try {
        ::decode(*mirror_image, *iter);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    void mirror_image_set(librados::ObjectWriteOperation *op,
			  const std::string &image_id,
			  const cls::rbd::MirrorImage &mirror_image) {
      bufferlist bl;
      ::encode(image_id, bl);
      ::encode(mirror_image, bl);

      op->exec("rbd", "mirror_image_set", bl);
    }

    int mirror_image_set(librados::IoCtx *ioctx, const std::string &image_id,
			 const cls::rbd::MirrorImage &mirror_image) {
      librados::ObjectWriteOperation op;
      mirror_image_set(&op, image_id, mirror_image);

      int r = ioctx->operate(RBD_MIRRORING, &op);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    void mirror_image_remove(librados::ObjectWriteOperation *op,
			     const std::string &image_id) {
      bufferlist bl;
      ::encode(image_id, bl);

      op->exec("rbd", "mirror_image_remove", bl);
    }

    int mirror_image_remove(librados::IoCtx *ioctx, const std::string &image_id) {
      librados::ObjectWriteOperation op;
      mirror_image_remove(&op, image_id);

      int r = ioctx->operate(RBD_MIRRORING, &op);
      if (r < 0) {
        return r;
      }
      return 0;
    }

    int mirror_image_status_set(librados::IoCtx *ioctx,
				const std::string &global_image_id,
				const cls::rbd::MirrorImageStatus &status) {
      librados::ObjectWriteOperation op;
      mirror_image_status_set(&op, global_image_id, status);
      return ioctx->operate(RBD_MIRRORING, &op);
    }

    void mirror_image_status_set(librados::ObjectWriteOperation *op,
				 const std::string &global_image_id,
				 const cls::rbd::MirrorImageStatus &status) {
      bufferlist bl;
      ::encode(global_image_id, bl);
      ::encode(status, bl);
      op->exec("rbd", "mirror_image_status_set", bl);
    }

    int mirror_image_status_remove(librados::IoCtx *ioctx,
				   const std::string &global_image_id) {
      librados::ObjectWriteOperation op;
      mirror_image_status_remove(&op, global_image_id);
      return ioctx->operate(RBD_MIRRORING, &op);
    }

    void mirror_image_status_remove(librados::ObjectWriteOperation *op,
				    const std::string &global_image_id) {
      bufferlist bl;
      ::encode(global_image_id, bl);
      op->exec("rbd", "mirror_image_status_remove", bl);
    }

    int mirror_image_status_get(librados::IoCtx *ioctx,
				const std::string &global_image_id,
				cls::rbd::MirrorImageStatus *status) {
      librados::ObjectReadOperation op;
      mirror_image_status_get_start(&op, global_image_id);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      r = mirror_image_status_get_finish(&iter, status);
      if (r < 0) {
	return r;
      }
      return 0;
    }

    void mirror_image_status_get_start(librados::ObjectReadOperation *op,
				       const std::string &global_image_id) {
      bufferlist bl;
      ::encode(global_image_id, bl);
      op->exec("rbd", "mirror_image_status_get", bl);
    }

    int mirror_image_status_get_finish(bufferlist::iterator *iter,
				       cls::rbd::MirrorImageStatus *status) {
      try {
	::decode(*status, *iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int mirror_image_status_list(librados::IoCtx *ioctx,
	const std::string &start, uint64_t max_return,
	std::map<std::string, cls::rbd::MirrorImage> *images,
	std::map<std::string, cls::rbd::MirrorImageStatus> *statuses) {
      librados::ObjectReadOperation op;
      mirror_image_status_list_start(&op, start, max_return);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      r = mirror_image_status_list_finish(&iter, images, statuses);
      if (r < 0) {
	return r;
      }
      return 0;
    }

    void mirror_image_status_list_start(librados::ObjectReadOperation *op,
					const std::string &start,
					uint64_t max_return) {
      bufferlist bl;
      ::encode(start, bl);
      ::encode(max_return, bl);
      op->exec("rbd", "mirror_image_status_list", bl);
    }

    int mirror_image_status_list_finish(bufferlist::iterator *iter,
	std::map<std::string, cls::rbd::MirrorImage> *images,
	std::map<std::string, cls::rbd::MirrorImageStatus> *statuses) {
      images->clear();
      statuses->clear();
      try {
	::decode(*images, *iter);
	::decode(*statuses, *iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int mirror_image_status_get_summary(librados::IoCtx *ioctx,
	std::map<cls::rbd::MirrorImageStatusState, int> *states) {
      librados::ObjectReadOperation op;
      mirror_image_status_get_summary_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRRORING, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      r = mirror_image_status_get_summary_finish(&iter, states);
      if (r < 0) {
	return r;
      }
      return 0;
    }

    void mirror_image_status_get_summary_start(
      librados::ObjectReadOperation *op) {
      bufferlist bl;
      op->exec("rbd", "mirror_image_status_get_summary", bl);
    }

    int mirror_image_status_get_summary_finish(bufferlist::iterator *iter,
	std::map<cls::rbd::MirrorImageStatusState, int> *states) {
      try {
	::decode(*states, *iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int mirror_image_status_remove_down(librados::IoCtx *ioctx) {
      librados::ObjectWriteOperation op;
      mirror_image_status_remove_down(&op);
      return ioctx->operate(RBD_MIRRORING, &op);
    }

    void mirror_image_status_remove_down(librados::ObjectWriteOperation *op) {
      bufferlist bl;
      op->exec("rbd", "mirror_image_status_remove_down", bl);
    }

    void mirror_instances_list_start(librados::ObjectReadOperation *op) {
      bufferlist bl;
      op->exec("rbd", "mirror_instances_list", bl);
    }

    int mirror_instances_list_finish(bufferlist::iterator *iter,
                                     std::vector<std::string> *instance_ids) {
      instance_ids->clear();
      try {
	::decode(*instance_ids, *iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int mirror_instances_list(librados::IoCtx *ioctx,
                              std::vector<std::string> *instance_ids) {
      librados::ObjectReadOperation op;
      mirror_instances_list_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_MIRROR_LEADER, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      r = mirror_instances_list_finish(&iter, instance_ids);
      if (r < 0) {
	return r;
      }
      return 0;
    }

    void mirror_instances_add(librados::ObjectWriteOperation *op,
                              const std::string &instance_id) {
      bufferlist bl;
      ::encode(instance_id, bl);
      op->exec("rbd", "mirror_instances_add", bl);
    }

    int mirror_instances_add(librados::IoCtx *ioctx,
                             const std::string &instance_id) {
      librados::ObjectWriteOperation op;
      mirror_instances_add(&op, instance_id);
      return ioctx->operate(RBD_MIRROR_LEADER, &op);
    }

    void mirror_instances_remove(librados::ObjectWriteOperation *op,
                                 const std::string &instance_id) {
      bufferlist bl;
      ::encode(instance_id, bl);
      op->exec("rbd", "mirror_instances_remove", bl);
    }

    int mirror_instances_remove(librados::IoCtx *ioctx,
                                const std::string &instance_id) {
      librados::ObjectWriteOperation op;
      mirror_instances_remove(&op, instance_id);
      return ioctx->operate(RBD_MIRROR_LEADER, &op);
    }

    // Consistency groups functions
    int group_create(librados::IoCtx *ioctx, const std::string &oid)
    {
      bufferlist bl, bl2;

      return ioctx->exec(oid, "rbd", "group_create", bl, bl2);
    }

    int group_dir_list(librados::IoCtx *ioctx, const std::string &oid,
	             const std::string &start, uint64_t max_return,
		     map<string, string> *cgs)
    {
      bufferlist in, out;
      ::encode(start, in);
      ::encode(max_return, in);
      int r = ioctx->exec(oid, "rbd", "group_dir_list", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*cgs, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int group_dir_add(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "group_dir_add", in, out);
    }

    int group_dir_remove(librados::IoCtx *ioctx, const std::string &oid,
	              const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "group_dir_remove", in, out);
    }

    int group_image_remove(librados::IoCtx *ioctx, const std::string &oid,
			   const cls::rbd::GroupImageSpec &spec)
    {
      bufferlist bl, bl2;
      ::encode(spec, bl);

      return ioctx->exec(oid, "rbd", "group_image_remove", bl, bl2);
    }

    int group_image_list(librados::IoCtx *ioctx,
			 const std::string &oid,
                         const cls::rbd::GroupImageSpec &start,
			 uint64_t max_return,
			 std::vector<cls::rbd::GroupImageStatus> *images)
    {
      bufferlist bl, bl2;
      ::encode(start, bl);
      ::encode(max_return, bl);

      int r = ioctx->exec(oid, "rbd", "group_image_list", bl, bl2);
      if (r < 0)
	return r;

      bufferlist::iterator iter = bl2.begin();
      try {
	::decode(*images, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int group_image_set(librados::IoCtx *ioctx, const std::string &oid,
			const cls::rbd::GroupImageStatus &st)
    {
      bufferlist bl, bl2;
      ::encode(st, bl);

      return ioctx->exec(oid, "rbd", "group_image_set", bl, bl2);
    }

    int image_add_group(librados::IoCtx *ioctx, const std::string &oid,
	                const cls::rbd::GroupSpec &group_spec)
    {
      bufferlist bl, bl2;
      ::encode(group_spec, bl);

      return ioctx->exec(oid, "rbd", "image_add_group", bl, bl2);
    }

    int image_remove_group(librados::IoCtx *ioctx, const std::string &oid,
			   const cls::rbd::GroupSpec &group_spec)
    {
      bufferlist bl, bl2;
      ::encode(group_spec, bl);

      return ioctx->exec(oid, "rbd", "image_remove_group", bl, bl2);
    }

    void image_get_group_start(librados::ObjectReadOperation *op)
    {
      bufferlist in_bl;
      op->exec("rbd", "image_get_group", in_bl);
    }

    int image_get_group_finish(bufferlist::iterator *iter,
                               cls::rbd::GroupSpec *group_spec)
    {
      try {
	::decode(*group_spec, *iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int image_get_group(librados::IoCtx *ioctx, const std::string &oid,
			cls::rbd::GroupSpec *group_spec)
    {
      librados::ObjectReadOperation op;
      image_get_group_start(&op);

      bufferlist out_bl;
      int r = ioctx->operate(oid, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      return image_get_group_finish(&iter, group_spec);
    }

    // rbd_trash functions
    void trash_add(librados::ObjectWriteOperation *op,
		   const std::string &id,
                   const cls::rbd::TrashImageSpec &trash_spec)
    {
      bufferlist bl;
      ::encode(id, bl);
      ::encode(trash_spec, bl);
      op->exec("rbd", "trash_add", bl);
    }

    int trash_add(librados::IoCtx *ioctx, const std::string &id,
                  const cls::rbd::TrashImageSpec &trash_spec)
    {
      librados::ObjectWriteOperation op;
      trash_add(&op, id, trash_spec);

      return ioctx->operate(RBD_TRASH, &op);
    }

    void trash_remove(librados::ObjectWriteOperation *op,
		      const std::string &id)
    {
      bufferlist bl;
      ::encode(id, bl);
      op->exec("rbd", "trash_remove", bl);
    }

    int trash_remove(librados::IoCtx *ioctx, const std::string &id)
    {
      librados::ObjectWriteOperation op;
      trash_remove(&op, id);

      return ioctx->operate(RBD_TRASH, &op);
    }

    void trash_list_start(librados::ObjectReadOperation *op,
                          const std::string &start, uint64_t max_return)
    {
      bufferlist bl;
      ::encode(start, bl);
      ::encode(max_return, bl);
      op->exec("rbd", "trash_list", bl);
    }

    int trash_list_finish(bufferlist::iterator *it,
                          map<string, cls::rbd::TrashImageSpec> *entries)
    {
      assert(entries);

      try {
	::decode(*entries, *it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int trash_list(librados::IoCtx *ioctx,
                   const std::string &start, uint64_t max_return,
                   map<string, cls::rbd::TrashImageSpec> *entries)
    {
      librados::ObjectReadOperation op;
      trash_list_start(&op, start, max_return);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_TRASH, &op, &out_bl);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out_bl.begin();
      return trash_list_finish(&iter, entries);
    }

    void trash_get_start(librados::ObjectReadOperation *op,
		         const std::string &id)
    {
      bufferlist bl;
      ::encode(id, bl);
      op->exec("rbd", "trash_get", bl);
    }

    int trash_get_finish(bufferlist::iterator *it,
                          cls::rbd::TrashImageSpec *trash_spec) {
      assert(trash_spec);
      try {
        ::decode(*trash_spec, *it);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }

      return 0;
    }


    int trash_get(librados::IoCtx *ioctx, const std::string &id,
                  cls::rbd::TrashImageSpec *trash_spec)
    {
      librados::ObjectReadOperation op;
      trash_get_start(&op, id);

      bufferlist out_bl;
      int r = ioctx->operate(RBD_TRASH, &op, &out_bl);
      if (r < 0) {
        return r;
      }

      bufferlist::iterator it = out_bl.begin();
      return trash_get_finish(&it, trash_spec);
    }

  } // namespace cls_client
} // namespace librbd
