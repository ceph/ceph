// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"
#include "include/encoding.h"

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
                             std::set<std::pair<std::string, std::string> > *lockers,
                             bool *exclusive_lock,
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
      op.exec("rbd", "list_locks", empty);
      op.exec("rbd", "get_parent", parentbl);

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
	// list_locks
	::decode(*lockers, iter);
	::decode(*exclusive_lock, iter);
	// get_parent
	::decode(parent->pool_id, iter);
	::decode(parent->image_id, iter);
	::decode(parent->snap_id, iter);
	::decode(parent->overlap, iter);
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
		   snapid_t snap_id, int64_t *parent_pool,
		   string *parent_image, snapid_t *parent_snap_id,
		   uint64_t *parent_overlap)
    {
      bufferlist inbl, outbl;
      ::encode(snap_id, inbl);

      int r = ioctx->exec(oid, "rbd", "get_parent", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*parent_pool, iter);
	::decode(*parent_image, iter);
	::decode(*parent_snap_id, iter);
	::decode(*parent_overlap, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_parent(librados::IoCtx *ioctx, const std::string &oid,
		   int64_t parent_pool, const string& parent_image,
		   snapid_t parent_snap_id, uint64_t parent_overlap)
    {
      bufferlist inbl, outbl;
      ::encode(parent_pool, inbl);
      ::encode(parent_image, inbl);
      ::encode(parent_snap_id, inbl);
      ::encode(parent_overlap, inbl);

      return ioctx->exec(oid, "rbd", "set_parent", inbl, outbl);
    }

    int remove_parent(librados::IoCtx *ioctx, const std::string &oid)
    {
      bufferlist inbl, outbl;
      return ioctx->exec(oid, "rbd", "remove_parent", inbl, outbl);
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
		      std::vector<parent_info> *parents)
    {
      names->clear();
      names->resize(ids.size());
      sizes->clear();
      sizes->resize(ids.size());
      features->clear();
      features->resize(ids.size());
      parents->clear();
      parents->resize(ids.size());

      librados::ObjectReadOperation op;
      for (vector<snapid_t>::const_iterator it = ids.begin();
	   it != ids.end(); ++it) {
	snapid_t snap_id = it->val;
	bufferlist bl1, bl2, bl3, bl4;
	::encode(snap_id, bl1);
	op.exec("rbd", "get_snapshot_name", bl1);
	::encode(snap_id, bl2);
	op.exec("rbd", "get_size", bl2);
	::encode(snap_id, bl3);
	op.exec("rbd", "get_features", bl3);
	::encode(snap_id, bl4);
	op.exec("rbd", "get_parent", bl4);
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
	  ::decode((*parents)[i].pool_id, iter);
	  ::decode((*parents)[i].image_id, iter);
	  ::decode((*parents)[i].snap_id, iter);
	  ::decode((*parents)[i].overlap, iter);
	}
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int assign_bid(librados::IoCtx *ioctx, const std::string &oid,
		   uint64_t *id)
    {
      bufferlist bl, out;
      int r = ioctx->exec(oid, "rbd", "assign_bid", bl, out);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = out.begin();
	::decode(*id, iter);
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

    int list_locks(librados::IoCtx *ioctx, const std::string &oid,
		   std::set<std::pair<std::string, std::string> > &locks,
		   bool &exclusive)
    {
      bufferlist in, out;
      int r = ioctx->exec(oid, "rbd", "list_locks", in, out);
      if (r < 0) {
	return r;
      }

      bufferlist::iterator iter = out.begin();
      try {
	::decode(locks, iter);
	::decode(exclusive, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }
      return 0;
    }

    int lock_image_exclusive(librados::IoCtx *ioctx, const std::string &oid,
			     const std::string &cookie)
    {
      bufferlist in, out;
      ::encode(cookie, in);
      return ioctx->exec(oid, "rbd", "lock_exclusive", in, out);
    }

    int lock_image_shared(librados::IoCtx *ioctx, const std::string &oid,
			  const std::string &cookie)
    {
      bufferlist in, out;
      ::encode(cookie, in);
      return ioctx->exec(oid, "rbd", "lock_shared", in, out);
    }

    int unlock_image(librados::IoCtx *ioctx, const std::string& oid,
		     const std::string &cookie)
    {
      bufferlist in, out;
      ::encode(cookie, in);
      return ioctx->exec(oid, "rbd", "unlock_image", in, out);
    }
    int break_lock(librados::IoCtx *ioctx, const std::string& oid,
		   const std::string &locker, const std::string &cookie)
    {
      bufferlist in, out;
      ::encode(locker, in);
      ::encode(cookie, in);
      return ioctx->exec(oid, "rbd", "break_lock", in, out);
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
