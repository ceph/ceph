// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/lock/cls_lock_client.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/encoding.h"
#include "include/rbd_types.h"
#include "common/Cond.h"

#include "cls_rbd_client.h"

#include <errno.h>

namespace librbd {
  namespace cls_client {

namespace {

struct C_GetChildren : public Context {
  librados::IoCtx *ioctx;
  std::string oid;
  parent_spec pspec;
  std::set<std::string> *children;
  Context *on_finish;
  bufferlist out_bl;

  C_GetChildren(librados::IoCtx *_ioctx, const std::string &_oid,
                const parent_spec &_pspec, std::set<std::string> *_children,
                Context *_on_finish)
  : ioctx(_ioctx), oid(_oid), pspec(_pspec), children(_children),
    on_finish(_on_finish) {
  }

  void send() {
    bufferlist in_bl;
    ::encode(pspec.pool_id, in_bl);
    ::encode(pspec.image_id, in_bl);
    ::encode(pspec.snap_id, in_bl);

    librados::ObjectReadOperation op;
    op.exec("rbd", "get_children", in_bl);

    librados::AioCompletion *rados_completion =
       librados::Rados::aio_create_completion(this, rados_callback, NULL);
    int r = ioctx->aio_operate(oid, rados_completion, &op, &out_bl);
    assert(r == 0);
    rados_completion->release();
  }

  virtual void finish(int r) {
    if (r == 0) {
      try {
        bufferlist::iterator it = out_bl.begin();
        ::decode(*children, it);
      } catch (const buffer::error &err) {
        r = -EBADMSG;
      }
    }
    on_finish->complete(r);
  }

  static void rados_callback(rados_completion_t c, void *arg) {
    Context *ctx = reinterpret_cast<Context *>(arg);
    ctx->complete(rados_aio_get_return_value(c));
  }
};

} // anonymous namespace

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
			     bool read_only, uint64_t *size, uint64_t *features,
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
      op.exec("rbd", "get_size", sizebl);

      ::encode(snap, featuresbl);
      ::encode(read_only, featuresbl);
      op.exec("rbd", "get_features", featuresbl);

      op.exec("rbd", "get_snapcontext", empty);

      ::encode(snap, parentbl);
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

    int set_features(librados::IoCtx *ioctx, const std::string &oid,
                      uint64_t features, uint64_t mask)
    {
      bufferlist inbl;
      ::encode(features, inbl);
      ::encode(mask, inbl);

      librados::ObjectWriteOperation op;
      op.exec("rbd", "set_features", inbl);
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

    int get_flags(librados::IoCtx *ioctx, const std::string &oid,
		  uint64_t *flags, const std::vector<snapid_t> &snap_ids,
		  vector<uint64_t> *snap_flags)
    {
      bufferlist inbl;
      ::encode(static_cast<snapid_t>(CEPH_NOSNAP), inbl);

      librados::ObjectReadOperation op;
      op.exec("rbd", "get_flags", inbl);
      for (size_t i = 0; i < snap_ids.size(); ++i) {
	bufferlist snapbl;
	::encode(snap_ids[i], snapbl);
	op.exec("rbd", "get_flags", snapbl);
      }

      snap_flags->clear();
      snap_flags->resize(snap_ids.size());

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0) {
        return r;
      }

      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(*flags, iter);
	for (size_t i = 0; i < snap_ids.size(); ++i) {
	  ::decode((*snap_flags)[i], iter);
	}
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
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
		  parent_spec pspec, const std::string &c_imageid)
    {
      bufferlist in, out;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);

      return ioctx->exec(oid, "rbd", "add_child", in, out);
    }

    void remove_child(librados::ObjectWriteOperation *op,
		      parent_spec pspec, const std::string &c_imageid)
    {
      bufferlist in;
      ::encode(pspec.pool_id, in);
      ::encode(pspec.image_id, in);
      ::encode(pspec.snap_id, in);
      ::encode(c_imageid, in);
      op->exec("rbd", "remove_child", in);
    }

    int remove_child(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, const std::string &c_imageid)
    {
      librados::ObjectWriteOperation op;
      remove_child(&op, pspec, c_imageid);
      return ioctx->operate(oid, &op);
    }

    int get_children(librados::IoCtx *ioctx, const std::string &oid,
		     parent_spec pspec, set<string>& children)
    {
      C_SaferCond cond_ctx;
      get_children(ioctx, oid, pspec, &children, &cond_ctx);
      return cond_ctx.wait();
    }

    void get_children(librados::IoCtx *ioctx, const std::string &oid,
                      const parent_spec &pspec, std::set<string> *children,
                      Context *on_finish) {
      C_GetChildren *req = new C_GetChildren(ioctx, oid, pspec, children,
                                             on_finish);
      req->send();
    }

    void snapshot_add(librados::ObjectWriteOperation *op, snapid_t snap_id,
		      const std::string &snap_name)
    {
      bufferlist bl;
      ::encode(snap_name, bl);
      ::encode(snap_id, bl);
      op->exec("rbd", "snapshot_add", bl);
    }

    void snapshot_remove(librados::ObjectWriteOperation *op, snapid_t snap_id)
    {
      bufferlist bl;
      ::encode(snap_id, bl);
      op->exec("rbd", "snapshot_remove", bl);
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
		      std::vector<parent_info> *parents,
		      std::vector<uint8_t> *protection_statuses)
    {
      names->clear();
      names->resize(ids.size());
      sizes->clear();
      sizes->resize(ids.size());
      parents->clear();
      parents->resize(ids.size());
      protection_statuses->clear();
      protection_statuses->resize(ids.size());

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
	op.exec("rbd", "get_parent", bl3);
	::encode(snap_id, bl4);
	op.exec("rbd", "get_protection_status", bl4);
      }

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	for (size_t i = 0; i < ids.size(); ++i) {
	  uint8_t order;
	  // get_snapshot_name
	  ::decode((*names)[i], iter);
	  // get_size
	  ::decode(order, iter);
	  ::decode((*sizes)[i], iter);
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
      try {
	uint32_t num_snaps;
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

    int object_map_load(librados::IoCtx *ioctx, const std::string &oid,
			ceph::BitVector<2> *object_map)
    {
      bufferlist in;
      bufferlist out;
      int r = ioctx->exec(oid, "rbd", "object_map_load", in, out);
      if (r < 0) {
	return r;
      }

      try {
        bufferlist::iterator iter = out.begin();
        ::decode(*object_map, iter);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
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

    int metadata_set(librados::IoCtx *ioctx, const std::string &oid,
                     const map<string, bufferlist> &data)
    {
      bufferlist in;
      ::encode(data, in);
      bufferlist out;
      return ioctx->exec(oid, "rbd", "metadata_set", in, out);
    }

    int metadata_remove(librados::IoCtx *ioctx, const std::string &oid,
                        const std::string &key)
    {
      bufferlist in;
      ::encode(key, in);
      bufferlist out;
      return ioctx->exec(oid, "rbd", "metadata_remove", in, out);
    }

    int metadata_list(librados::IoCtx *ioctx, const std::string &oid,
                      const std::string &start, uint64_t max_return,
                      map<string, bufferlist> *pairs)
    {
      assert(pairs);
      bufferlist in, out;
      ::encode(start, in);
      ::encode(max_return, in);
      int r = ioctx->exec(oid, "rbd", "metadata_list", in, out);
      if (r < 0)
        return r;

      bufferlist::iterator iter = out.begin();
      try {
        ::decode(*pairs, iter);
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

  } // namespace cls_client
} // namespace librbd
