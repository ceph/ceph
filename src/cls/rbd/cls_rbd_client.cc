// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/lock/cls_lock_client.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rbd_types.h"

#include "cls_rbd_client.h"

#include <errno.h>

namespace librbd {
  namespace cls_client {
    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order)
    {
      assert(object_prefix);
      assert(order);

      librados::ObjectReadOperation op;
      bufferlist bl, empty;
      snapid_t snap = CEPH_NOSNAP;
      ::encode(snap, bl);
      op.exec("rbd", "get_size", bl);
      op.exec("rbd", "get_object_prefix", empty);
      

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	uint64_t size;
	// get_size
	::decode(*order, iter);
	::decode(size, iter);
	// get_object_prefix
	::decode(*object_prefix, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
                             bool *exclusive_lock,
			     string *lock_tag,
			     ::SnapContext *snapc,
			     parent_info *parent)
    {
      assert(size);
      assert(features);
      assert(incompatible_features);
      assert(lockers);
      assert(exclusive_lock);
      assert(snapc);
      assert(parent);

      librados::ObjectReadOperation op;
      bufferlist sizebl, featuresbl, parentbl, empty;
      snapid_t snap = CEPH_NOSNAP;
      ::encode(snap, sizebl);
      ::encode(snap, featuresbl);
      ::encode(snap, parentbl);
      op.exec("rbd", "get_size", sizebl);
      op.exec("rbd", "get_features", featuresbl);
      op.exec("rbd", "get_snapcontext", empty);
      op.exec("rbd", "get_parent", parentbl);
      rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	uint8_t order;
	// get_size
	::decode(order, iter);
	::decode(*size, iter);
	// get_features
	::decode(*features, iter);
	::decode(*incompatible_features, iter);
	// get_snapcontext
	::decode(*snapc, iter);
	// get_parent
	::decode(parent->spec.pool_id, iter);
	::decode(parent->spec.image_id, iter);
	::decode(parent->spec.snap_id, iter);
	::decode(parent->overlap, iter);

	// get_lock_info
	ClsLockType lock_type = LOCK_NONE;
	r = rados::cls::lock::get_lock_info_finish(&iter, lockers, &lock_type,
						   lock_tag);

	// see comment in ictx_refresh().  Ugly conflation of
	// EOPNOTSUPP and EIO.

	if (r < 0 && ((r != -EOPNOTSUPP) && (r != -EIO)))
	  return r;

	*exclusive_lock = (lock_type == LOCK_EXCLUSIVE);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix)
    {
      bufferlist bl, bl2;
      ::encode(size, bl);
      ::encode(order, bl);
      ::encode(features, bl);
      ::encode(object_prefix, (bl));

      return ioctx->exec(oid, "rbd", "create", bl, bl2);
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
      bufferlist bl, bl2;
      ::encode(size, bl);

      return ioctx->exec(oid, "rbd", "set_size", bl, bl2);
    }

    int get_parent(librados::IoCtx *ioctx, const std::string &oid,
		   snapid_t snap_id, parent_spec *pspec, 
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
		   parent_spec pspec, uint64_t parent_overlap)
    {
      bufferlist inbl, outbl;
      ::encode(pspec.pool_id, inbl);
      ::encode(pspec.image_id, inbl);
      ::encode(pspec.snap_id, inbl);
      ::encode(parent_overlap, inbl);

      return ioctx->exec(oid, "rbd", "set_parent", inbl, outbl);
    }

    int remove_parent(librados::IoCtx *ioctx, const std::string &oid)
    {
      bufferlist inbl, outbl;
      return ioctx->exec(oid, "rbd", "remove_parent", inbl, outbl);
    }

    int add_child(librados::IoCtx *ioctx, const std::string &oid,
		  parent_spec pspec, const std::string &c_imageid)
    {
      bufferlist in, out;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);

      return ioctx->exec(oid, "rbd", "add_child", in, out);
    }

    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, const std::string &c_imageid)
    {
      bufferlist in, out;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);

      return ioctx->exec(oid, "rbd", "remove_child", in, out);
    }

    int get_children(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, set<string>& children)
    {
      bufferlist in, out;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);

      int r = ioctx->exec(oid, "rbd", "get_children", in, out);
      if (r < 0)
	return r;
      bufferlist::iterator it = out.begin();
      try {
	::decode(children, it);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
		     snapid_t snap_id, const std::string &snap_name)
    {
      bufferlist bl, bl2;
      ::encode(snap_name, bl);
      ::encode(snap_id, bl);

      return ioctx->exec(oid, "rbd", "snapshot_add", bl, bl2);
    }

    int snapshot_remove(librados::IoCtx *ioctx, const std::string &oid,
			snapid_t snap_id)
    {
      bufferlist bl, bl2;
      ::encode(snap_id, bl);

      return ioctx->exec(oid, "rbd", "snapshot_remove", bl, bl2);
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

    int snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
		      const std::vector<snapid_t> &ids,
		      std::vector<string> *names,
		      std::vector<uint64_t> *sizes,
		      std::vector<uint64_t> *features,
		      std::vector<parent_info> *parents,
		      std::vector<uint8_t> *protection_statuses)
    {
      names->clear();
      names->resize(ids.size());
      sizes->clear();
      sizes->resize(ids.size());
      features->clear();
      features->resize(ids.size());
      parents->clear();
      parents->resize(ids.size());
      protection_statuses->clear();
      protection_statuses->resize(ids.size());

      librados::ObjectReadOperation op;
      for (vector<snapid_t>::const_iterator it = ids.begin();
	   it != ids.end(); ++it) {
	snapid_t snap_id = it->val;
	bufferlist bl1, bl2, bl3, bl4, bl5;
	::encode(snap_id, bl1);
	op.exec("rbd", "get_snapshot_name", bl1);
	::encode(snap_id, bl2);
	op.exec("rbd", "get_size", bl2);
	::encode(snap_id, bl3);
	op.exec("rbd", "get_features", bl3);
	::encode(snap_id, bl4);
	op.exec("rbd", "get_parent", bl4);
	::encode(snap_id, bl5);
	op.exec("rbd", "get_protection_status", bl5);
      }

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	for (size_t i = 0; i < ids.size(); ++i) {
	  uint8_t order;
	  uint64_t incompat_features;
	  // get_snapshot_name
	  ::decode((*names)[i], iter);
	  // get_size
	  ::decode(order, iter);
	  ::decode((*sizes)[i], iter);
	  // get_features
	  ::decode((*features)[i], iter);
	  ::decode(incompat_features, iter);
	  // get_parent
	  ::decode((*parents)[i].spec.pool_id, iter);
	  ::decode((*parents)[i].spec.image_id, iter);
	  ::decode((*parents)[i].spec.snap_id, iter);
	  ::decode((*parents)[i].overlap, iter);
	  // get_protection_status
	  ::decode((*protection_statuses)[i], iter);
	}
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int old_snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
			 snapid_t snap_id, const std::string &snap_name)
    {
      bufferlist bl, bl2;
      ::encode(snap_name, bl);
      ::encode(snap_id, bl);

      return ioctx->exec(oid, "rbd", "snap_add", bl, bl2);
    }

    int old_snapshot_remove(librados::IoCtx *ioctx, const std::string &oid,
			    const std::string &snap_name)
    {
      bufferlist bl, bl2;
      ::encode(snap_name, bl);

      return ioctx->exec(oid, "rbd", "snap_remove", bl, bl2);
    }

    int old_snapshot_list(librados::IoCtx *ioctx, const std::string &oid,
			  std::vector<string> *names,
			  std::vector<uint64_t> *sizes,
			  ::SnapContext *snapc)
    {
      bufferlist bl, outbl;
      int r = ioctx->exec(oid, "rbd", "snap_list", bl, outbl);
      if (r < 0)
	return r;

      bufferlist::iterator iter = outbl.begin();
      uint32_t num_snaps;
      try {
	::decode(snapc->seq, iter);
	::decode(num_snaps, iter);

	names->resize(num_snaps);
	sizes->resize(num_snaps);
	snapc->snaps.resize(num_snaps);

	for (uint32_t i = 0; i < num_snaps; ++i) {
	  ::decode(snapc->snaps[i], iter);
	  ::decode((*sizes)[i], iter);
	  ::decode((*names)[i], iter);
	}
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
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
      bufferlist in, out;
      ::encode(snap_id, in);
      ::encode(protection_status, in);
      return ioctx->exec(oid, "rbd", "set_protection_status", in, out);
    }

    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count)
    {
      assert(stripe_unit);
      assert(stripe_count);

      librados::ObjectReadOperation op;
      bufferlist empty;
      op.exec("rbd", "get_stripe_unit_count", empty);

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*stripe_unit, iter);
	::decode(*stripe_count, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count)
    {
      bufferlist in, out;
      ::encode(stripe_unit, in);
      ::encode(stripe_count, in);
      return ioctx->exec(oid, "rbd", "set_stripe_unit_count", in, out);
    }


    /************************ rbd_id object methods ************************/

    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id)
    {
      bufferlist in, out;
      int r = ioctx->exec(oid, "rbd", "get_id", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*id, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id)
    {
      bufferlist in, out;
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "set_id", in, out);
    }

    /******************** rbd_directory object methods ********************/

    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, std::string *id)
    {
      bufferlist in, out;
      ::encode(name, in);
      int r = ioctx->exec(oid, "rbd", "dir_get_id", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*id, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name)
    {
      bufferlist in, out;
      ::encode(id, in);
      int r = ioctx->exec(oid, "rbd", "dir_get_name", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*name, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images)
    {
      bufferlist in, out;
      ::encode(start, in);
      ::encode(max_return, in);
      int r = ioctx->exec(oid, "rbd", "dir_list", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*images, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_add_image", in, out);
    }

    int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_remove_image", in, out);
    }

    int dir_rename_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &src, const std::string &dest,
			 const std::string &id)
    {
      bufferlist in, out;
      ::encode(src, in);
      ::encode(dest, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_rename_image", in, out);
    }
  } // namespace cls_client
} // namespace librbd
