// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>
#include <inttypes.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"

#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

#define howmany(x, y)  (((x) + (y) - 1) / (y))

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;
using librados::Rados;

namespace librbd {
  const string id_obj_name(const string &name)
  {
    return RBD_ID_PREFIX + name;
  }

  const string header_name(const string &image_id)
  {
    return RBD_HEADER_PREFIX + image_id;
  }

  const string old_header_name(const string &image_name)
  {
    return image_name + RBD_SUFFIX;
  }

  int detect_format(IoCtx &io_ctx, const string &name,
		    bool *old_format, uint64_t *size)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    if (old_format)
      *old_format = true;
    int r = io_ctx.stat(old_header_name(name), size, NULL);
    if (r < 0) {
      if (old_format)
	*old_format = false;
      r = io_ctx.stat(id_obj_name(name), size, NULL);
      if (r < 0)
	return r;
    }

    ldout(cct, 20) << "detect format of " << name << " : "
		   << (old_format ? (*old_format ? "old" : "new") :
		       "don't care")  << dendl;
    return 0;
  }

  bool has_parent(int64_t parent_pool_id, uint64_t off, uint64_t overlap)
  {
    return (parent_pool_id != -1 && off <= overlap);
  }

  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
		       uint64_t size, int *order, uint64_t bid)
  {
    uint32_t hi = bid >> 32;
    uint32_t lo = bid & 0xFFFFFFFF;
    memset(&ondisk, 0, sizeof(ondisk));

    memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
    memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE,
	   sizeof(RBD_HEADER_SIGNATURE));
    memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

    snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x", hi, lo);

    ondisk.image_size = size;
    ondisk.options.order = *order;
    ondisk.options.crypt_type = RBD_CRYPT_NONE;
    ondisk.options.comp_type = RBD_COMP_NONE;
    ondisk.snap_seq = 0;
    ondisk.snap_count = 0;
    ondisk.reserved = 0;
    ondisk.snap_names_len = 0;
  }

  void image_info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    int obj_order = ictx->order;
    ictx->md_lock.Lock();
    ictx->snap_lock.Lock();
    info.size = ictx->get_image_size(ictx->snap_id);
    ictx->snap_lock.Unlock();
    ictx->md_lock.Unlock();
    info.obj_size = 1 << obj_order;
    info.num_objs = howmany(info.size, get_block_size(obj_order));
    info.order = obj_order;
    memcpy(&info.block_name_prefix, ictx->object_prefix.c_str(),
	   min((size_t)RBD_MAX_BLOCK_NAME_SIZE, ictx->object_prefix.length()));
    // clear deprecated fields
    info.parent_pool = -1L;
    info.parent_name[0] = '\0';
  }

  string get_block_oid(const string &object_prefix, uint64_t num,
		       bool old_format)
  {
    ostringstream oss;
    int width = old_format ? 12 : 16;
    oss << object_prefix << "."
	<< std::hex << std::setw(width) << std::setfill('0') << num;
    return oss.str();
  }

  uint64_t offset_of_object(const string &oid, const string &object_prefix,
			    uint8_t order)
  {
    istringstream iss(oid);
    // skip object prefix and separator
    iss.ignore(object_prefix.length() + 1);
    uint64_t num, offset;
    iss >> std::hex >> num;
    offset = num * (1 << order);
    return offset;
  }

  uint64_t get_max_block(uint64_t size, uint8_t obj_order)
  {
    uint64_t block_size = 1 << obj_order;
    uint64_t numseg = (size + block_size - 1) >> obj_order;
    return numseg;
  }

  uint64_t get_block_ofs(uint8_t order, uint64_t ofs)
  {
    uint64_t block_size = 1 << order;
    return ofs & (block_size - 1);
  }

  uint64_t get_block_size(uint8_t order)
  {
    return 1 << order;
  }

  uint64_t get_block_num(uint8_t order, uint64_t ofs)
  {
    uint64_t num = ofs >> order;

    return num;
  }

  int init_rbd_info(struct rbd_info *info)
  {
    memset(info, 0, sizeof(*info));
    return 0;
  }

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx)
  {
    assert(ictx->md_lock.is_locked());
    CephContext *cct = (CephContext *)ictx->data_ctx.cct();
    uint64_t bsize = get_block_size(ictx->order);
    uint64_t numseg = get_max_block(ictx->size, ictx->order);
    uint64_t start = get_block_num(ictx->order, newsize);

    uint64_t block_ofs = get_block_ofs(ictx->order, newsize);
    if (block_ofs) {
      ldout(cct, 2) << "trim_image object " << numseg << " truncate to "
		    << block_ofs << dendl;
      string oid = get_block_oid(ictx->object_prefix, start, ictx->old_format);
      librados::ObjectWriteOperation write_op;
      write_op.truncate(block_ofs);
      ictx->data_ctx.operate(oid, &write_op);
      start++;
    }
    if (start < numseg) {
      ldout(cct, 2) << "trim_image objects " << start << " to "
		    << (numseg - 1) << dendl;
      for (uint64_t i = start; i < numseg; ++i) {
	string oid = get_block_oid(ictx->object_prefix, i, ictx->old_format);
	ictx->data_ctx.remove(oid);
	prog_ctx.update_progress(i * bsize, (numseg - start) * bsize);
      }
    }
  }

  int read_rbd_info(IoCtx& io_ctx, const string& info_oid,
		    struct rbd_info *info)
  {
    int r;
    bufferlist bl;
    r = io_ctx.read(info_oid, bl, sizeof(*info), 0);
    if (r < 0)
      return r;
    if (r == 0) {
      return init_rbd_info(info);
    }

    if (r < (int)sizeof(*info))
      return -EIO;

    memcpy(info, bl.c_str(), r);
    return 0;
  }

  int touch_rbd_info(IoCtx& io_ctx, const string& info_oid)
  {
    bufferlist bl;
    int r = io_ctx.write(info_oid, bl, 0, 0);
    if (r < 0)
      return r;
    return 0;
  }

  int rbd_assign_bid(IoCtx& io_ctx, const string& info_oid, uint64_t *id)
  {
    int r = touch_rbd_info(io_ctx, info_oid);
    if (r < 0)
      return r;

    r = cls_client::assign_bid(&io_ctx, info_oid, id);
    if (r < 0)
      return r;

    return 0;
  }

  int read_header_bl(IoCtx& io_ctx, const string& header_oid,
		     bufferlist& header, uint64_t *ver)
  {
    int r;
    uint64_t off = 0;
#define READ_SIZE 4096
    do {
      bufferlist bl;
      r = io_ctx.read(header_oid, bl, READ_SIZE, off);
      if (r < 0)
	return r;
      header.claim_append(bl);
      off += r;
    } while (r == READ_SIZE);

    if (memcmp(RBD_HEADER_TEXT, header.c_str(), sizeof(RBD_HEADER_TEXT))) {
      CephContext *cct = (CephContext *)io_ctx.cct();
      lderr(cct) << "unrecognized header format" << dendl;
      return -ENXIO;
    }

    if (ver)
      *ver = io_ctx.get_last_version();

    return 0;
  }

  int notify_change(IoCtx& io_ctx, const string& oid, uint64_t *pver,
		    ImageCtx *ictx)
  {
    uint64_t ver;

    if (ictx) {
      assert(ictx->md_lock.is_locked());
      ictx->refresh_lock.Lock();
      ++ictx->refresh_seq;
      ictx->refresh_lock.Unlock();
    }

    if (pver)
      ver = *pver;
    else
      ver = io_ctx.get_last_version();
    bufferlist bl;
    io_ctx.notify(oid, ver, bl);
    return 0;
  }

  int read_header(IoCtx& io_ctx, const string& header_oid,
		  struct rbd_obj_header_ondisk *header, uint64_t *ver)
  {
    bufferlist header_bl;
    int r = read_header_bl(io_ctx, header_oid, header_bl, ver);
    if (r < 0)
      return r;
    if (header_bl.length() < (int)sizeof(*header))
      return -EIO;
    memcpy(header, header_bl.c_str(), sizeof(*header));

    return 0;
  }

  int write_header(IoCtx& io_ctx, const string& header_oid, bufferlist& header)
  {
    bufferlist bl;
    int r = io_ctx.write(header_oid, header, header.length(), 0);

    notify_change(io_ctx, header_oid, NULL, NULL);

    return r;
  }

  int tmap_set(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl, emptybl;
    __u8 c = CEPH_OSD_TMAP_SET;
    ::encode(c, cmdbl);
    ::encode(imgname, cmdbl);
    ::encode(emptybl, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  int tmap_rm(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl;
    __u8 c = CEPH_OSD_TMAP_RM;
    ::encode(c, cmdbl);
    ::encode(imgname, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  int rollback_image(ImageCtx *ictx, uint64_t snap_id,
		     ProgressContext& prog_ctx)
  {
    assert(ictx->md_lock.is_locked());
    uint64_t numseg = get_max_block(ictx->size, ictx->order);
    uint64_t bsize = get_block_size(ictx->order);

    for (uint64_t i = 0; i < numseg; i++) {
      int r;
      string oid = get_block_oid(ictx->object_prefix, i, ictx->old_format);
      r = ictx->data_ctx.selfmanaged_snap_rollback(oid, snap_id);
      ldout(ictx->cct, 10) << "selfmanaged_snap_rollback on " << oid << " to "
			   << snap_id << " returned " << r << dendl;
      prog_ctx.update_progress(i * bsize, numseg * bsize);
      if (r < 0 && r != -ENOENT)
	return r;
    }
    return 0;
  }

  int list(IoCtx& io_ctx, vector<string>& names)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "list " << &io_ctx << dendl;

    bufferlist bl;
    int r = io_ctx.read(RBD_DIRECTORY, bl, 0, 0);
    if (r < 0)
      return r;

    // old format images are in a tmap
    if (bl.length()) {
      bufferlist::iterator p = bl.begin();
      bufferlist header;
      map<string,bufferlist> m;
      ::decode(header, p);
      ::decode(m, p);
      for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
	names.push_back(q->first);
      }
    }

    // new format images are accessed by class methods
    int max_read = 1024;
    string last_read = "";
    do {
      map<string, string> images;
      cls_client::dir_list(&io_ctx, RBD_DIRECTORY,
			   last_read, max_read, &images);
      for (map<string, string>::const_iterator it = images.begin();
	   it != images.end(); ++it) {
	names.push_back(it->first);
      }
      if (images.size()) {
	last_read = images.rbegin()->first;
      }
    } while (r == max_read);

    return 0;
  }

  int snap_create(ImageCtx *ictx, const char *snap_name)
  {
    ldout(ictx->cct, 20) << "snap_create " << ictx << " " << snap_name << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker l(ictx->md_lock);
    r = add_snap(ictx, snap_name);

    if (r < 0)
      return r;

    notify_change(ictx->md_ctx, ictx->header_oid, NULL, ictx);

    ictx->perfcounter->inc(l_librbd_snap_create);
    return 0;
  }

  int snap_remove(ImageCtx *ictx, const char *snap_name)
  {
    ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_name << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker l(ictx->md_lock);
    ictx->snap_lock.Lock();
    snap_t snap_id = ictx->get_snap_id(snap_name);
    ictx->snap_lock.Unlock();
    if (snap_id == CEPH_NOSNAP)
      return -ENOENT;

    // If this is the last snapshot, and the base image has no parent,
    // that could be because the parent was flattened before the snapshots
    // were removed.  In any case, this is now the time we should
    // remove the child from the child list.
    if (ictx->snaps.size() == 1 && (ictx->parent_md.spec.pool_id == -1)) {
      SnapInfo *snapinfo;
      Mutex::Locker l(ictx->snap_lock);
      r = ictx->get_snapinfo(snap_id, &snapinfo);
      if (r < 0)
	return r;
      if (snapinfo->parent.spec.pool_id != -1) {
	r = cls_client::remove_child(&ictx->md_ctx, RBD_CHILDREN,
				     snapinfo->parent.spec, ictx->id);
	if (r < 0)
	  return r;
      }
    }
    r = rm_snap(ictx, snap_name);
    if (r < 0)
      return r;

    r = ictx->data_ctx.selfmanaged_snap_remove(snap_id);

    if (r < 0)
      return r;

    notify_change(ictx->md_ctx, ictx->header_oid, NULL, ictx);

    ictx->perfcounter->inc(l_librbd_snap_remove);
    return 0;
  }

  int create(IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "create " << &io_ctx << " name = " << imgname
		   << " size = " << size << " old_format = " << old_format
		   << " features = " << features << " order = " << *order
		   << dendl;


    if (features & ~RBD_FEATURES_ALL) {
      lderr(cct) << "librbd does not support requested features." << dendl;
      return -ENOSYS;
    }

    // make sure it doesn't already exist, in either format
    int r = detect_format(io_ctx, imgname, NULL, NULL);
    if (r != -ENOENT) {
      lderr(cct) << "rbd image " << imgname << " already exists" << dendl;
      return -EEXIST;
    }

    if (!order)
      return -EINVAL;

    if (*order && (*order > 255 || *order < 12)) {
      lderr(cct) << "order must be in the range [12, 255]" << dendl;
      return -EDOM;
    }

    uint64_t bid;
    string dir_info = RBD_INFO;
    r = rbd_assign_bid(io_ctx, dir_info, &bid);
    if (r < 0) {
      lderr(cct) << "failed to assign a block name for image" << dendl;
      return r;
    }


    if (!*order)
      *order = RBD_DEFAULT_OBJ_ORDER;

    if (old_format) {
      ldout(cct, 2) << "adding rbd image to directory..." << dendl;
      r = tmap_set(io_ctx, imgname);
      if (r < 0) {
	lderr(cct) << "error adding image to directory: " << cpp_strerror(r)
		   << dendl;
	return r;
      }

      ldout(cct, 2) << "creating rbd image..." << dendl;
      struct rbd_obj_header_ondisk header;
      init_rbd_header(header, size, order, bid);

      bufferlist bl;
      bl.append((const char *)&header, sizeof(header));

      string header_oid = old_header_name(imgname);
      r = io_ctx.write(header_oid, bl, bl.length(), 0);
    } else {
      string id_obj = id_obj_name(imgname);
      r = io_ctx.create(id_obj, true);
      if (r < 0) {
	lderr(cct) << "error creating rbd id object: " << cpp_strerror(r)
		   << dendl;
	return r;
      }

      ostringstream bid_ss;
      bid_ss << std::hex << bid;
      string id = bid_ss.str();
      r = cls_client::set_id(&io_ctx, id_obj, id);
      if (r < 0) {
	lderr(cct) << "error setting image id: " << cpp_strerror(r) << dendl;
	return r;
      }

      ldout(cct, 2) << "adding rbd image to directory..." << dendl;
      r = cls_client::dir_add_image(&io_ctx, RBD_DIRECTORY, imgname, id);
      if (r < 0) {
	lderr(cct) << "error adding image to directory: " << cpp_strerror(r)
		   << dendl;
	return r;
      }

      ostringstream oss;
      oss << RBD_DATA_PREFIX << std::hex << bid;
      r = cls_client::create_image(&io_ctx, header_name(id), size, *order,
				   features, oss.str());
    }

    if (r < 0) {
      lderr(cct) << "error writing header: " << cpp_strerror(r) << dendl;
      return r;
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  /*
   * Parent may be in different pool, hence different IoCtx
   */
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name,
	    uint64_t features, int *c_order)
  {
    CephContext *cct = (CephContext *)p_ioctx.cct();
    ldout(cct, 20) << "clone " << &p_ioctx << " name " << p_name << " snap "
		   << p_snap_name << "to child " << &c_ioctx << " name "
		   << c_name << " features = " << features << " order = "
		   << *c_order << dendl;

    if (features & ~RBD_FEATURES_ALL) {
      lderr(cct) << "librbd does not support requested features" << dendl;
      return -ENOSYS;
    }

    // make sure child doesn't already exist, in either format
    int r = detect_format(c_ioctx, c_name, NULL, NULL);
    if (r != -ENOENT) {
      lderr(cct) << "rbd image " << c_name << " already exists" << dendl;
      return -EEXIST;
    }

    if (p_snap_name == NULL) {
      lderr(cct) << "image to be cloned must be a snapshot" << dendl;
      return -EINVAL;
    }

    // make sure parent snapshot exists
    ImageCtx *p_imctx = new ImageCtx(p_name, "", p_snap_name, p_ioctx);
    r = open_image(p_imctx, true);
    if (r < 0) {
      lderr(cct) << "error opening parent image: "
		 << cpp_strerror(-r) << dendl;
      return r;
    }

    if (p_imctx->old_format) {
      lderr(cct) << "parent image must be in new format" << dendl;
      return -EINVAL;
    }

    p_imctx->md_lock.Lock();
    p_imctx->snap_lock.Lock();
    uint64_t p_features;
    p_imctx->get_features(p_imctx->snap_id, &p_features);
    uint64_t size = p_imctx->get_image_size(p_imctx->snap_id);
    p_imctx->snap_lock.Unlock();
    p_imctx->md_lock.Unlock();

    if ((p_features & RBD_FEATURE_LAYERING) != RBD_FEATURE_LAYERING) {
      lderr(cct) << "parent image must support layering" << dendl;
      return -EINVAL;
    }

    int order = *c_order;

    if (!*c_order) {
      order = p_imctx->order;
    }

    cls_client::parent_spec pspec(p_ioctx.get_id(), p_imctx->id,
				  p_imctx->snap_id);

    int remove_r;
    librbd::NoOpProgressContext no_op;
    ImageCtx *c_imctx = NULL;
    r = create(c_ioctx, c_name, size, false, features, &order);
    if (r < 0) {
      lderr(cct) << "error creating child: " << cpp_strerror(r) << dendl;
      goto err_close_parent;
    }

    c_imctx = new ImageCtx(c_name, "", NULL, c_ioctx);
    r = open_image(c_imctx, true);
    if (r < 0) {
      lderr(cct) << "Error opening new image: " << cpp_strerror(r) << dendl;
      goto err_remove;
    }

    r = cls_client::set_parent(&c_ioctx, c_imctx->header_oid, pspec, size);
    if (r < 0) {
      lderr(cct) << "couldn't set parent: " << r << dendl;
      goto err_close_child;
    }

    r = cls_client::add_child(&c_ioctx, RBD_CHILDREN, pspec, c_imctx->id);
    if (r < 0) {
      lderr(cct) << "couldn't add child: " << r << dendl;
      goto err_close_child;
    }
    ldout(cct, 2) << "done." << dendl;
    close_image(c_imctx);
    close_image(p_imctx);
    return 0;

  err_close_child:
    close_image(c_imctx);
  err_remove:
    remove_r = remove(c_ioctx, c_name, no_op);
    if (remove_r < 0) {
      lderr(cct) << "Error removing failed clone: "
		 << cpp_strerror(remove_r) << dendl;
    }
  err_close_parent:
    close_image(p_imctx);
    return r;
  }

  int rename(IoCtx& io_ctx, const char *srcname, const char *dstname)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "rename " << &io_ctx << " " << srcname << " -> "
		   << dstname << dendl;

    bool old_format;
    uint64_t src_size;
    int r = detect_format(io_ctx, srcname, &old_format, &src_size);
    if (r < 0) {
      lderr(cct) << "error finding source object: " << cpp_strerror(r) << dendl;
      return r;
    }

    string src_oid =
      old_format ? old_header_name(srcname) : id_obj_name(srcname);
    string dst_oid =
      old_format ? old_header_name(dstname) : id_obj_name(dstname);

    string id;
    if (!old_format) {
      r = cls_client::get_id(&io_ctx, src_oid, &id);
      if (r < 0) {
	lderr(cct) << "error reading image id: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    bufferlist databl;
    map<string, bufferlist> omap_values;
    r = io_ctx.read(src_oid, databl, src_size, 0);
    if (r < 0) {
      lderr(cct) << "error reading source object: " << src_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    int MAX_READ = 1024;
    string last_read = "";
    do {
      map<string, bufferlist> outbl;
      r = io_ctx.omap_get_vals(src_oid, last_read, MAX_READ, &outbl);
      if (r < 0) {
	lderr(cct) << "error reading source object omap values: "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      omap_values.insert(outbl.begin(), outbl.end());
      if (outbl.size() > 0)
	last_read = outbl.rbegin()->first;
    } while (r == MAX_READ);

    r = detect_format(io_ctx, dstname, NULL, NULL);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error checking for existing image called "
		 << dstname << ":" << cpp_strerror(r) << dendl;
      return r;
    }
    if (r == 0) {
      lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
      return -EEXIST;
    }

    librados::ObjectWriteOperation op;
    op.create(true);
    op.write_full(databl);
    if (omap_values.size())
      op.omap_set(omap_values);
    r = io_ctx.operate(dst_oid, &op);
    if (r < 0) {
      lderr(cct) << "error writing destination object: " << dst_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    if (old_format) {
      r = tmap_set(io_ctx, dstname);
      if (r < 0) {
	io_ctx.remove(dst_oid);
	lderr(cct) << "couldn't add " << dstname << " to directory: "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      r = tmap_rm(io_ctx, srcname);
      if (r < 0) {
	lderr(cct) << "warning: couldn't remove old entry from directory ("
		   << srcname << ")" << dendl;
      }
    } else {
      r = cls_client::dir_rename_image(&io_ctx, RBD_DIRECTORY,
				       srcname, dstname, id);
      if (r < 0) {
	lderr(cct) << "error updating directory: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    r = io_ctx.remove(src_oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "warning: couldn't remove old source object ("
		 << src_oid << ")" << dendl;
    }

    if (old_format) {
      notify_change(io_ctx, old_header_name(srcname), NULL, NULL);
    }

    return 0;
  }


  int info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    ldout(ictx->cct, 20) << "info " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    image_info(ictx, info, infosize);
    return 0;
  }

  int get_old_format(ImageCtx *ictx, uint8_t *old)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    *old = ictx->old_format;
    return 0;
  }

  int get_size(ImageCtx *ictx, uint64_t *size)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    Mutex::Locker l(ictx->md_lock);
    Mutex::Locker l2(ictx->snap_lock);
    *size = ictx->get_image_size(ictx->snap_id);
    return 0;
  }

  int get_features(ImageCtx *ictx, uint64_t *features)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    Mutex::Locker l(ictx->md_lock);
    Mutex::Locker l2(ictx->snap_lock);
    return ictx->get_features(ictx->snap_id, features);
  }

  int get_overlap(ImageCtx *ictx, uint64_t *overlap)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    Mutex::Locker l(ictx->snap_lock);
    Mutex::Locker l2(ictx->parent_lock);
    return ictx->get_parent_overlap(ictx->snap_id, overlap);
  }

  int open_parent(ImageCtx *ictx)
  {
    assert(ictx->snap_lock.is_locked());
    assert(ictx->parent_lock.is_locked());
    string pool_name;
    Rados rados(ictx->md_ctx);

    int64_t pool_id = ictx->get_parent_pool_id(ictx->snap_id);
    string parent_image_id = ictx->get_parent_image_id(ictx->snap_id);
    snap_t parent_snap_id = ictx->get_parent_snap_id(ictx->snap_id);
    assert(parent_snap_id != CEPH_NOSNAP);

    if (pool_id < 0)
      return -ENOENT;
    int r = rados.pool_reverse_lookup(pool_id, &pool_name);
    if (r < 0) {
      lderr(ictx->cct) << "error looking up name for pool id " << pool_id
		       << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    IoCtx p_ioctx;
    r = rados.ioctx_create(pool_name.c_str(), p_ioctx);
    if (r < 0) {
      lderr(ictx->cct) << "error opening pool " << pool_name << ": "
		       << cpp_strerror(r) << dendl;
      return r;
    }

    // since we don't know the image and snapshot name, set their ids and
    // reset the snap_name and snap_exists fields after we read the header
    ictx->parent = new ImageCtx("", parent_image_id, NULL, p_ioctx);
    r = open_image(ictx->parent, false);
    if (r < 0) {
      lderr(ictx->cct) << "error opening parent image: " << cpp_strerror(r)
		       << dendl;
      close_image(ictx->parent);
      ictx->parent = NULL;
      return r;
    }

    ictx->parent->snap_lock.Lock();
    r = ictx->parent->get_snap_name(parent_snap_id, &ictx->parent->snap_name);
    if (r < 0) {
      lderr(ictx->cct) << "parent snapshot does not exist" << dendl;
      ictx->parent->snap_lock.Unlock();
      close_image(ictx->parent);
      ictx->parent = NULL;
      return r;
    }
    ictx->parent->snap_set(ictx->parent->snap_name);
    ictx->parent->snap_lock.Unlock();

    return 0;
  }

  int get_parent_info(ImageCtx *ictx, string *parent_pool_name,
		      string *parent_name, string *parent_snap_name)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker l(ictx->snap_lock);
    Mutex::Locker l2(ictx->parent_lock);

    cls_client::parent_info *parent_info;

    if (ictx->snap_id == CEPH_NOSNAP) {
      if (!ictx->parent)
	return 0;
      parent_info = &(ictx->parent_md);
    } else {
      SnapInfo *snapinfo;
      r = ictx->get_snapinfo(ictx->snap_id, &snapinfo);
      if (r < 0) {
	lderr(ictx->cct) << "Can't find snapshot id" << ictx->snap_id << dendl;
	return r;
      }
      parent_info = &(snapinfo->parent);
      if (parent_info->spec.pool_id == -1)
	return 0;
    }
    if (parent_pool_name) {
      Rados rados(ictx->md_ctx);
      r = rados.pool_reverse_lookup(parent_info->spec.pool_id,
				    parent_pool_name);
      if (r < 0) {
	lderr(ictx->cct) << "error looking up pool name" << cpp_strerror(r)
			 << dendl;
      }
    }

    if (parent_snap_name) {
      Mutex::Locker l(ictx->parent->snap_lock);
      r = ictx->parent->get_snap_name(parent_info->spec.snap_id,
				      parent_snap_name);
      if (r < 0) {
	lderr(ictx->cct) << "error finding parent snap name: "
			 << cpp_strerror(r) << dendl;
	return r;
      }
    }

    if (parent_name) {
      r = cls_client::dir_get_name(&ictx->parent->md_ctx, RBD_DIRECTORY,
				   parent_info->spec.image_id, parent_name);
      if (r < 0) {
	lderr(ictx->cct) << "error getting parent image name: "
			 << cpp_strerror(r) << dendl;
	return r;
      }
    }

    return 0;
  }

  int remove(IoCtx& io_ctx, const char *imgname, ProgressContext& prog_ctx)
  {
    CephContext *cct((CephContext *)io_ctx.cct());
    ldout(cct, 20) << "remove " << &io_ctx << " " << imgname << dendl;

    string id;
    bool old_format = false;
    bool unknown_format = true;
    ImageCtx *ictx = new ImageCtx(imgname, "", NULL, io_ctx);
    int r = open_image(ictx, true);
    if (r < 0) {
      ldout(cct, 2) << "error opening image: " << cpp_strerror(-r) << dendl;
    } else {
      if (ictx->snaps.size()) {
	lderr(cct) << "image has snapshots - not removing" << dendl;
	close_image(ictx);
	return -ENOTEMPTY;
      }
      string header_oid = ictx->header_oid;
      old_format = ictx->old_format;
      unknown_format = false;
      id = ictx->id;
      ictx->md_lock.Lock();
      trim_image(ictx, 0, prog_ctx);
      ictx->md_lock.Unlock();

      ictx->parent_lock.Lock();
      // struct assignment
      cls_client::parent_info parent_info = ictx->parent_md;
      ictx->parent_lock.Unlock();
      close_image(ictx);

      if (parent_info.spec.pool_id != -1) {
	ldout(cct, 2) << "removing child from children list..." << dendl;
	int r = cls_client::remove_child(&io_ctx, RBD_CHILDREN,
					 parent_info.spec, id);
	if (r < 0) {
	  lderr(cct) << "error removing child from children list" << dendl;
	  return r;
	}
      }
      ldout(cct, 2) << "removing header..." << dendl;
      r = io_ctx.remove(header_oid);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
	return r;
      }
    }

    if (old_format || unknown_format) {
      ldout(cct, 2) << "removing rbd image from directory..." << dendl;
      r = tmap_rm(io_ctx, imgname);
      old_format = (r == 0);
      if (r < 0 && !unknown_format) {
	lderr(cct) << "error removing img from old-style directory: "
		   << cpp_strerror(-r) << dendl;
	return r;
      }
    }
    if (!old_format) {
      ldout(cct, 2) << "removing id object..." << dendl;
      r = io_ctx.remove(id_obj_name(imgname));
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error removing id object: " << cpp_strerror(r) << dendl;
	return r;
      }

      r = cls_client::dir_get_id(&io_ctx, RBD_DIRECTORY, imgname, &id);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error getting id of image" << dendl;
	return r;
      }

      ldout(cct, 2) << "removing rbd image from directory..." << dendl;
      r = cls_client::dir_remove_image(&io_ctx, RBD_DIRECTORY, imgname, id);
      if (r < 0) {
	lderr(cct) << "error removing img from new-style directory: "
		   << cpp_strerror(-r) << dendl;
	return r;
      }
    } 

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    assert(ictx->md_lock.is_locked());
    CephContext *cct = ictx->cct;

    if (size == ictx->size) {
      ldout(cct, 2) << "no change in size (" << ictx->size << " -> " << size
		    << ")" << dendl;
      return 0;
    }

    if (size > ictx->size) {
      ldout(cct, 2) << "expanding image " << ictx->size << " -> " << size
		    << dendl;
      // TODO: make ictx->set_size
    } else {
      ldout(cct, 2) << "shrinking image " << ictx->size << " -> " << size
		    << dendl;
      trim_image(ictx, size, prog_ctx);
    }
    ictx->size = size;

    int r;
    if (ictx->old_format) {
      // rewrite header
      bufferlist bl;
      ictx->header.image_size = size;
      bl.append((const char *)&(ictx->header), sizeof(ictx->header));
      r = ictx->md_ctx.write(ictx->header_oid, bl, bl.length(), 0);
    } else {
      r = cls_client::set_size(&(ictx->md_ctx), ictx->header_oid, size);
    }

    // TODO: remove this useless check
    if (r == -ERANGE)
      lderr(cct) << "operation might have conflicted with another client!"
		 << dendl;
    if (r < 0) {
      lderr(cct) << "error writing header: " << cpp_strerror(-r) << dendl;
      return r;
    } else {
      notify_change(ictx->md_ctx, ictx->header_oid, NULL, ictx);
    }

    return 0;
  }

  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "resize " << ictx << " " << ictx->size << " -> "
		   << size << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker l(ictx->md_lock);
    if (size < ictx->size && ictx->object_cacher) {
      // need to invalidate since we're deleting objects, and
      // ObjectCacher doesn't track non-existent objects
      ictx->invalidate_cache();
    }
    resize_helper(ictx, size, prog_ctx);

    ldout(cct, 2) << "done." << dendl;

    ictx->perfcounter->inc(l_librbd_resize);
    return 0;
  }

  int snap_list(ImageCtx *ictx, vector<snap_info_t>& snaps)
  {
    ldout(ictx->cct, 20) << "snap_list " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    bufferlist bl, bl2;

    Mutex::Locker l(ictx->snap_lock);
    for (map<string, SnapInfo>::iterator it = ictx->snaps_by_name.begin();
	 it != ictx->snaps_by_name.end(); ++it) {
      snap_info_t info;
      info.name = it->first;
      info.id = it->second.id;
      info.size = it->second.size;
      snaps.push_back(info);
    }

    return 0;
  }

  int add_snap(ImageCtx *ictx, const char *snap_name)
  {
    assert(ictx->md_lock.is_locked());

    uint64_t snap_id;

    int r = ictx->md_ctx.selfmanaged_snap_create(&snap_id);
    if (r < 0) {
      lderr(ictx->cct) << "failed to create snap id: " << cpp_strerror(-r)
		       << dendl;
      return r;
    }

    if (ictx->old_format) {
      r = cls_client::old_snapshot_add(&ictx->md_ctx, ictx->header_oid,
				       snap_id, snap_name);
    } else {
      r = cls_client::snapshot_add(&ictx->md_ctx, ictx->header_oid,
				   snap_id, snap_name);
    }

    if (r < 0) {
      lderr(ictx->cct) << "adding snapshot to header failed: "
		       << cpp_strerror(r) << dendl;
      return r;
    }

    return 0;
  }

  int rm_snap(ImageCtx *ictx, const char *snap_name)
  {
    assert(ictx->md_lock.is_locked());

    int r;
    if (ictx->old_format) {
      r = cls_client::old_snapshot_remove(&ictx->md_ctx,
					  ictx->header_oid, snap_name);
    } else {
      Mutex::Locker l(ictx->snap_lock);
      r = cls_client::snapshot_remove(&ictx->md_ctx,
				      ictx->header_oid,
				      ictx->get_snap_id(snap_name));
    }

    if (r < 0) {
      lderr(ictx->cct) << "removing snapshot from header failed: "
		       << cpp_strerror(r) << dendl;
      return r;
    }

    return 0;
  }

  int ictx_check(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "ictx_check " << ictx << dendl;
    ictx->refresh_lock.Lock();
    bool needs_refresh = ictx->last_refresh != ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    if (needs_refresh) {
      Mutex::Locker l(ictx->md_lock);

      int r = ictx_refresh(ictx);
      if (r < 0) {
	lderr(cct) << "Error re-reading rbd header: " << cpp_strerror(-r)
		   << dendl;
	return r;
      }
    }
    return 0;
  }

  int refresh_parent(ImageCtx *ictx) {
    assert(ictx->snap_lock.is_locked());
    assert(ictx->parent_lock.is_locked());
    // close the parent if it changed or this image no longer needs
    // to read from it
    int r;
    if (ictx->parent) {
      uint64_t overlap;
      r = ictx->get_parent_overlap(ictx->snap_id, &overlap);
      if (r < 0)
	return r;
      if (!overlap ||
	  ictx->parent->md_ctx.get_id() !=
	  ictx->get_parent_pool_id(ictx->snap_id) ||
	  ictx->parent->id != ictx->get_parent_image_id(ictx->snap_id) ||
	  ictx->parent->snap_id != ictx->get_parent_snap_id(ictx->snap_id)) {
	close_image(ictx->parent);
	ictx->parent = NULL;
      }
    }

    if (ictx->get_parent_pool_id(ictx->snap_id) > -1 && !ictx->parent) {
      r = open_parent(ictx);
      if (r < 0) {
	lderr(ictx->cct) << "error opening parent snapshot: "
			 << cpp_strerror(r) << dendl;
	return r;
      }
    }

    return 0;
  }

  int ictx_refresh(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    assert(ictx->md_lock.is_locked());
    bufferlist bl, bl2;

    ldout(cct, 20) << "ictx_refresh " << ictx << dendl;

    ictx->refresh_lock.Lock();
    int refresh_seq = ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    int r;
    ::SnapContext new_snapc;
    bool new_snap;
    vector<string> snap_names;
    vector<uint64_t> snap_sizes;
    vector<uint64_t> snap_features;
    vector<cls_client::parent_info> snap_parents;
    {
      Mutex::Locker l(ictx->snap_lock);
      {
	Mutex::Locker l2(ictx->parent_lock);
	if (ictx->old_format) {
	  r = read_header(ictx->md_ctx, ictx->header_oid, &ictx->header, NULL);
	  if (r < 0) {
	    lderr(cct) << "Error reading header: " << cpp_strerror(r) << dendl;
	    return r;
	  }
	  r = cls_client::old_snapshot_list(&ictx->md_ctx, ictx->header_oid,
					    &snap_names, &snap_sizes, &new_snapc);
	  if (r < 0) {
	    lderr(cct) << "Error listing snapshots: " << cpp_strerror(r) << dendl;
	    return r;
	  }
	  ictx->order = ictx->header.options.order;
	  ictx->size = ictx->header.image_size;
	  ictx->object_prefix = ictx->header.block_name;
	} else {
	  do {
	    uint64_t incompatible_features;
	    r = cls_client::get_mutable_metadata(&ictx->md_ctx, ictx->header_oid,
						 &ictx->size, &ictx->features,
						 &incompatible_features,
						 &ictx->locks,
						 &ictx->exclusive_locked,
						 &new_snapc,
						 &ictx->parent_md);
	    if (r < 0) {
	      lderr(cct) << "Error reading mutable metadata: " << cpp_strerror(r)
			 << dendl;
	      return r;
	    }

	    uint64_t unsupported = incompatible_features & ~RBD_FEATURES_ALL;
	    if (unsupported) {
	      lderr(ictx->cct) << "Image uses unsupported features: "
			       << unsupported << dendl;
	      return -ENOSYS;
	    }

	    r = cls_client::snapshot_list(&(ictx->md_ctx), ictx->header_oid,
					  new_snapc.snaps, &snap_names,
					  &snap_sizes, &snap_features,
					  &snap_parents);
	    // -ENOENT here means we raced with snapshot deletion
	    if (r < 0 && r != -ENOENT) {
	      lderr(ictx->cct) << "snapc = " << new_snapc << dendl;
	      lderr(ictx->cct) << "Error listing snapshots: " << cpp_strerror(r)
			       << dendl;
	      return r;
	    }
	  } while (r == -ENOENT);
	}

	for (size_t i = 0; i < new_snapc.snaps.size(); ++i) {
	  uint64_t features = ictx->old_format ? 0 : snap_features[i];
	  cls_client::parent_info parent;
	  if (!ictx->old_format)
	    parent = snap_parents[i];
	  vector<snap_t>::const_iterator it =
	    find(ictx->snaps.begin(), ictx->snaps.end(), new_snapc.snaps[i].val);
	  if (it == ictx->snaps.end()) {
	    new_snap = true;
	    ldout(cct, 20) << "new snapshot id=" << new_snapc.snaps[i].val
			   << " name=" << snap_names[i]
			   << " size=" << snap_sizes[i]
			   << " features=" << features
			   << dendl;
	  }
	}

	ictx->snaps.clear();
	ictx->snaps_by_name.clear();
	for (size_t i = 0; i < new_snapc.snaps.size(); ++i) {
	  uint64_t features = ictx->old_format ? 0 : snap_features[i];
	  cls_client::parent_info parent;
	  if (!ictx->old_format)
	    parent = snap_parents[i];
	  ictx->add_snap(snap_names[i], new_snapc.snaps[i].val,
			 snap_sizes[i], features, parent);
	}

	r = refresh_parent(ictx);
	if (r < 0)
	  return r;
      } // release parent_lock

      if (!new_snapc.is_valid()) {
	lderr(cct) << "image snap context is invalid!" << dendl;
	return -EIO;
      }

      ictx->snapc = new_snapc;

      if (ictx->snap_id != CEPH_NOSNAP &&
	  ictx->get_snap_id(ictx->snap_name) != ictx->snap_id) {
	lderr(cct) << "tried to read from a snapshot that no longer exists: "
		   << ictx->snap_name << dendl;
	ictx->snap_exists = false;
      }

      ictx->data_ctx.selfmanaged_snap_set_write_ctx(ictx->snapc.seq, ictx->snaps);
    } // release snap_lock

    if (new_snap) {
      _flush(ictx);
    }

    ictx->refresh_lock.Lock();
    ictx->last_refresh = refresh_seq;
    ictx->refresh_lock.Unlock();

    return 0;
  }

  int snap_rollback(ImageCtx *ictx, const char *snap_name,
		    ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "snap_rollback " << ictx << " snap = " << snap_name
		   << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker l(ictx->md_lock);
    Mutex::Locker l2(ictx->snap_lock);
    if (!ictx->snap_exists)
      return -ENOENT;

    if (ictx->snap_id != CEPH_NOSNAP)
      return -EROFS;

    snap_t snap_id = ictx->get_snap_id(snap_name);
    if (snap_id == CEPH_NOSNAP) {
      lderr(cct) << "No such snapshot found." << dendl;
      return -ENOENT;
    }

    // need to flush any pending writes before resizing and rolling back -
    // writes might create new snapshots. Rolling back will replace
    // the current version, so we have to invalidate that too.
    ictx->invalidate_cache();

    uint64_t new_size = ictx->get_image_size(ictx->snap_id);
    ictx->get_snap_size(snap_name, &new_size);

    ldout(cct, 2) << "resizing to snapshot size..." << dendl;
    NoOpProgressContext no_op;
    r = resize_helper(ictx, new_size, no_op);
    if (r < 0) {
      lderr(cct) << "Error resizing to snapshot size: "
		 << cpp_strerror(-r) << dendl;
      return r;
    }

    r = rollback_image(ictx, snap_id, prog_ctx);
    if (r < 0) {
      lderr(cct) << "Error rolling back image: " << cpp_strerror(-r) << dendl;
      return r;
    }

    snap_t new_snap_id = ictx->get_snap_id(snap_name);
    ldout(cct, 20) << "snap_id is " << ictx->snap_id << " new snap_id is "
		   << new_snap_id << dendl;

    notify_change(ictx->md_ctx, ictx->header_oid, NULL, ictx);

    ictx->perfcounter->inc(l_librbd_snap_rollback);
    return r;
  }

  struct CopyProgressCtx {
    CopyProgressCtx(ProgressContext &p)
      : prog_ctx(p)
    {
    }
    ImageCtx *destictx;
    uint64_t src_size;
    ProgressContext &prog_ctx;
  };

  int do_copy_extent(uint64_t offset, size_t len, const char *buf, void *data)
  {
    CopyProgressCtx *cp = reinterpret_cast<CopyProgressCtx*>(data);
    cp->prog_ctx.update_progress(offset, cp->src_size);
    int ret = 0;
    if (buf) {
      ret = write(cp->destictx, offset, len, buf);
    }
    return ret;
  }

  int copy(ImageCtx *ictx, IoCtx& dest_md_ctx, const char *destname,
	   ProgressContext &prog_ctx)
  {
    CephContext *cct = (CephContext *)dest_md_ctx.cct();
    CopyProgressCtx cp(prog_ctx);
    ictx->md_lock.Lock();
    ictx->snap_lock.Lock();
    uint64_t src_size = ictx->get_image_size(ictx->snap_id);
    ictx->snap_lock.Unlock();
    ictx->md_lock.Unlock();
    int64_t r;

    int order = ictx->order;
    r = create(dest_md_ctx, destname, src_size, ictx->old_format,
	       ictx->features, &order);
    if (r < 0) {
      lderr(cct) << "header creation failed" << dendl;
      return r;
    }

    cp.destictx = new librbd::ImageCtx(destname, "", NULL, dest_md_ctx);
    cp.src_size = src_size;
    r = open_image(cp.destictx, true);
    if (r < 0) {
      lderr(cct) << "failed to read newly created header" << dendl;
      return r;
    }

    r = read_iterate(ictx, 0, src_size, do_copy_extent, &cp);

    if (r >= 0) {
      // don't return total bytes read, which may not fit in an int
      r = 0;
      prog_ctx.update_progress(cp.src_size, cp.src_size);
    }
    close_image(cp.destictx);
    return r;
  }

  // common snap_set functionality for snap_set and open_image

  int _snap_set(ImageCtx *ictx, const char *snap_name)
  {
    Mutex::Locker l1(ictx->snap_lock);
    Mutex::Locker l2(ictx->parent_lock);
    int r;
    if ((snap_name != NULL) && (strlen(snap_name) != 0)) {
      r = ictx->snap_set(snap_name);
    } else {
      ictx->snap_unset();
      r = 0;
    }
    if (r < 0) {
      return r;
    }
    refresh_parent(ictx);
    return 0;
  }

  int snap_set(ImageCtx *ictx, const char *snap_name)
  {
    ldout(ictx->cct, 20) << "snap_set " << ictx << " snap = "
			 << (snap_name ? snap_name : "NULL") << dendl;
    // ignore return value, since we may be set to a non-existent
    // snapshot and the user is trying to fix that
    ictx_check(ictx);
    return _snap_set(ictx, snap_name);
  }

  int open_image(ImageCtx *ictx, bool watch)
  {
    ldout(ictx->cct, 20) << "open_image: ictx = " << ictx
			 << " name = '" << ictx->name
			 << " id = '" << ictx->id
			 << "' snap_name = '"
			 << ictx->snap_name << "'" << dendl;
    int r = ictx->init();
    if (r < 0)
      return r;

    ictx->md_lock.Lock();
    r = ictx_refresh(ictx);
    ictx->md_lock.Unlock();
    if (r < 0)
      return r;

    _snap_set(ictx, ictx->snap_name.c_str());

    if (watch) {
      r = ictx->register_watch();
      if (r < 0) {
	lderr(ictx->cct) << "error registering a watch: " << cpp_strerror(r)
			 << dendl;
	close_image(ictx);
	return r;
      }
    }

    return 0;
  }

  void close_image(ImageCtx *ictx)
  {
    ldout(ictx->cct, 20) << "close_image " << ictx << dendl;
    if (ictx->object_cacher)
      ictx->shutdown_cache(); // implicitly flushes
    else
      flush(ictx);

    if (ictx->parent) {
      close_image(ictx->parent);
      ictx->parent = NULL;
    }

    if (ictx->wctx)
      ictx->unregister_watch();

    delete ictx;
  }

  // copy a parent block from buf to the child ictx(offset, len)
  int copyup_block(ImageCtx *ictx, uint64_t offset, size_t len,
		   const char *buf)
  {
    uint64_t blksize = get_block_size(ictx->order);

    // len > blksize invalid, block-misaligned offset invalid
    if ((len > blksize) || (get_block_ofs(ictx->order, offset) != 0))
      return -EINVAL;

    bufferlist bl;
    string oid = get_block_oid(ictx->object_prefix,
			       get_block_num(ictx->order, offset),
			       ictx->old_format);

    bl.append(buf, len);
    return cls_client::copyup(&ictx->data_ctx, oid, bl);
  }

  // test if an entire buf is zero in 8-byte chunks
  static bool buf_is_zero(char *buf, size_t len)
  {
    size_t ofs;
    int chunk = sizeof(uint64_t);

    for (ofs = 0; ofs < len; ofs += sizeof(uint64_t)) {
      if (*(uint64_t *)(buf + ofs) != 0) {
	return false;
      }
    }
    for (ofs = (len / chunk) * chunk; ofs < len; ofs++) {
      if (buf[ofs] != '\0') {
	return false;
      }
    }
    return true;
  }

  // 'flatten' child image by copying all parent's blocks
  int flatten(ImageCtx *ictx, ProgressContext &prog_ctx)
  {
    ldout(ictx->cct, 20) << "flatten" << dendl;
    int r;
    // ictx_check also updates parent data
    if ((r = ictx_check(ictx)) < 0) {
      lderr(ictx->cct) << "ictx_check failed" << dendl;
      return r;
    }

    Mutex::Locker l(ictx->md_lock);
    Mutex::Locker l2(ictx->snap_lock);
    Mutex::Locker l3(ictx->parent_lock);
    // can't flatten a non-clone
    if (ictx->parent_md.spec.pool_id == -1) {
      lderr(ictx->cct) << "image has no parent" << dendl;
      return -EINVAL;
    }
    if (ictx->snap_id != CEPH_NOSNAP) {
      lderr(ictx->cct) << "snapshots cannot be flattened" << dendl;
      return -EROFS;
    }

    assert(ictx->parent != NULL);
    assert(ictx->parent_md.overlap <= ictx->size);

    uint64_t overlap = ictx->parent_md.overlap;
    uint64_t cblksize = get_block_size(ictx->order);
    char *buf = new char[cblksize];

    size_t ofs = 0;
    while (ofs < overlap) {
      prog_ctx.update_progress(ofs, overlap);
      size_t readsize = min(overlap - ofs, cblksize);
      if ((r = read(ictx->parent, ofs, readsize, buf)) < 0) {
	lderr(ictx->cct) << "reading from parent failed" << dendl;
	goto err;
      }

      // for actual amount read, if data is all zero, don't bother with block
      if (!buf_is_zero(buf, r)) {
	if ((r = copyup_block(ictx, ofs, r, buf)) < 0) {
	  lderr(ictx->cct) << "failed to copy block to child" << dendl;
	  goto err;
	}
      }
      ofs += cblksize;
    }

    // remove parent from this (base) image
    r = cls_client::remove_parent(&ictx->md_ctx, ictx->header_oid);
    if (r < 0) {
      lderr(ictx->cct) << "error removing parent" << dendl;
      return r;
    }

    // and if there are no snaps, remove from the children object as well
    // (if snapshots remain, they have their own parent info, and the child
    // will be removed when the last snap goes away)
    if (ictx->snaps.empty()) {
      ldout(ictx->cct, 2) << "removing child from children list..." << dendl;
      int r = cls_client::remove_child(&ictx->md_ctx, RBD_CHILDREN,
				       ictx->parent_md.spec, ictx->id);
      if (r < 0) {
	lderr(ictx->cct) << "error removing child from children list" << dendl;
	return r;
      }
    }
    notify_change(ictx->md_ctx, ictx->header_oid, NULL, ictx);

    ldout(ictx->cct, 20) << "finished flattening" << dendl;

  err:
    delete buf;
    return r;
  }

  int list_locks(ImageCtx *ictx,
		 set<pair<string, string> > &locks,
		 bool &exclusive)
  {
    ldout(ictx->cct, 20) << "list_locks on image " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    Mutex::Locker locker(ictx->md_lock);
    locks = ictx->locks;
    exclusive = ictx->exclusive_locked;
    return 0;
  }

  int lock_exclusive(ImageCtx *ictx, const string& cookie)
  {
    /**
     * If we wanted we could do something more intelligent, like local
     * checks that we think we will succeed. But for now, let's not
     * duplicate that code.
     */
    return cls_client::lock_image_exclusive(&ictx->md_ctx,
					    ictx->header_oid, cookie);
  }

  int lock_shared(ImageCtx *ictx, const string& cookie)
  {
    return cls_client::lock_image_shared(&ictx->md_ctx,
					 ictx->header_oid, cookie);
  }

  int unlock(ImageCtx *ictx, const string& cookie)
  {
    return cls_client::unlock_image(&ictx->md_ctx, ictx->header_oid, cookie);
  }

  int break_lock(ImageCtx *ictx, const string& lock_holder,
		 const string& cookie)
  {
    return cls_client::break_lock(&ictx->md_ctx, ictx->header_oid,
				  lock_holder, cookie);
  }

  void rbd_ctx_cb(completion_t cb, void *arg)
  {
    Context *ctx = reinterpret_cast<Context *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    ctx->complete(comp->get_return_value());
  }

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, size_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg)
  {
    utime_t start_time, elapsed;

    ldout(ictx->cct, 20) << "read_iterate " << ictx << " off = " << off
			 << " len = " << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    r = check_io(ictx, off, len);
    if (r < 0)
      return r;

    int64_t total_read = 0;
    uint64_t start_block = get_block_num(ictx->order, off);
    uint64_t end_block = get_block_num(ictx->order, off + len - 1);
    uint64_t block_size = get_block_size(ictx->order);
    uint64_t left = len;

    start_time = ceph_clock_now(ictx->cct);
    for (uint64_t i = start_block; i <= end_block; i++) {
      uint64_t total_off = off + total_read;
      uint64_t block_ofs = get_block_ofs(ictx->order, total_off);
      uint64_t read_len = min(block_size - block_ofs, left);
      buffer::ptr bp(read_len);
      bufferlist bl;
      bl.append(bp);

      Mutex mylock("IoCtxImpl::write::mylock");
      Cond cond;
      bool done;
      int ret;

      Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
      AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_read(ictx, total_off, read_len, bl.c_str(), c);
      if (r < 0) {
	c->release();
	delete ctx;
	return r;
      }

      mylock.Lock();
      while (!done)
	cond.Wait(mylock);
      mylock.Unlock();

      c->release();
      if (ret < 0)
	return ret;

      r = cb(total_read, ret, bl.c_str(), arg);
      if (r < 0)
	return r;

      total_read += ret;
      left -= ret;
    }

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->finc(l_librbd_rd_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_rd);
    ictx->perfcounter->inc(l_librbd_rd_bytes, len);
    return total_read;
  }

  int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
  {
    char *dest_buf = (char *)arg;
    if (buf)
      memcpy(dest_buf + ofs, buf, len);
    else
      memset(dest_buf + ofs, 0, len);

    return 0;
  }

  ssize_t read(ImageCtx *ictx, uint64_t ofs, size_t len, char *buf)
  {
    return read_iterate(ictx, ofs, len, simple_read_cb, buf);
  }

  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "write " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock("librbd::write::mylock");
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_write(ictx, off, len, buf, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    c->release();
    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->finc(l_librbd_wr_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_wr);
    ictx->perfcounter->inc(l_librbd_wr_bytes, len);
    return len;
  }

  int discard(ImageCtx *ictx, uint64_t off, uint64_t len)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "discard " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock("librbd::discard::mylock");
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_discard(ictx, off, len, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    c->release();
    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->inc(l_librbd_discard_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_discard);
    ictx->perfcounter->inc(l_librbd_discard_bytes, len);
    return len;
  }

  ssize_t handle_sparse_read(CephContext *cct,
			     bufferlist data_bl,
			     uint64_t block_ofs,
			     const map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,   // offset into buffer
			     size_t buf_len,     // length in buffer (not size of buffer!)
			     int (*cb)(uint64_t, size_t, const char *, void *),
			     void *arg)
  {
    int r;
    uint64_t bl_ofs = 0;
    size_t buf_left = buf_len;

    for (map<uint64_t, uint64_t>::const_iterator iter = data_map.begin();
	 iter != data_map.end();
	 ++iter) {
      uint64_t extent_ofs = iter->first;
      size_t extent_len = iter->second;

      ldout(cct, 10) << "extent_ofs=" << extent_ofs
		     << " extent_len=" << extent_len << dendl;
      ldout(cct, 10) << "block_ofs=" << block_ofs << dendl;

      /* a hole? */
      if (extent_ofs > block_ofs) {
	uint64_t gap = extent_ofs - block_ofs;
	ldout(cct, 10) << "<1>zeroing " << buf_ofs << "~" << gap << dendl;
	r = cb(buf_ofs, gap, NULL, arg);
	if (r < 0) {
	  return r;
	}
	buf_ofs += gap;
	buf_left -= gap;
	block_ofs = extent_ofs;
      } else if (extent_ofs < block_ofs) {
	assert(0 == "osd returned data prior to what we asked for");
	return -EIO;
      }

      if (bl_ofs + extent_len > (buf_ofs + buf_left)) {
	assert(0 == "osd returned more data than we asked for");
	return -EIO;
      }

      /* data */
      ldout(cct, 10) << "<2>copying " << buf_ofs << "~" << extent_len
		     << " from ofs=" << bl_ofs << dendl;
      r = cb(buf_ofs, extent_len, data_bl.c_str() + bl_ofs, arg);
      if (r < 0) {
	return r;
      }
      bl_ofs += extent_len;
      buf_ofs += extent_len;
      assert(buf_left >= extent_len);
      buf_left -= extent_len;
      block_ofs += extent_len;
    }

    /* last hole */
    if (buf_left > 0) {
      ldout(cct, 10) << "<3>zeroing " << buf_ofs << "~" << buf_left << dendl;
      r = cb(buf_ofs, buf_left, NULL, arg);
      if (r < 0) {
	return r;
      }
    }

    return buf_len;
  }

  void rados_req_cb(rados_completion_t c, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    req->complete(rados_aio_get_return_value(c));
  }

  int check_io(ImageCtx *ictx, uint64_t off, uint64_t len)
  {
    ictx->md_lock.Lock();
    ictx->snap_lock.Lock();
    uint64_t image_size = ictx->get_image_size(ictx->snap_id);
    bool snap_exists = ictx->snap_exists;
    ictx->snap_lock.Unlock();
    ictx->md_lock.Unlock();

    if (!snap_exists)
      return -ENOENT;

    if ((uint64_t)(off + len) > image_size)
      return -EINVAL;
    return 0;
  }

  int flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "flush " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    return _flush(ictx);
  }

  int _flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    int r;
    // flush any outstanding writes
    if (ictx->object_cacher) {
      r = ictx->flush_cache();
    } else {
      r = ictx->data_ctx.aio_flush();
    }

    if (r)
      lderr(cct) << "_flush " << ictx << " r = " << r << dendl;

    return r;
  }

  int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
		AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_write " << ictx << " off = " << off << " len = "
		   << len << " buf = " << &buf << dendl;

    if (!len)
      return 0;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    size_t total_write = 0;
    uint64_t start_block = get_block_num(ictx->order, off);
    uint64_t end_block = get_block_num(ictx->order, off + len - 1);
    uint64_t block_size = get_block_size(ictx->order);
    ictx->snap_lock.Lock();
    snapid_t snap_id = ictx->snap_id;
    ::SnapContext snapc = ictx->snapc;
    ictx->parent_lock.Lock();
    int64_t parent_pool_id = ictx->get_parent_pool_id(ictx->snap_id);
    uint64_t overlap = 0;
    ictx->get_parent_overlap(ictx->snap_id, &overlap);
    ictx->parent_lock.Unlock();
    ictx->snap_lock.Unlock();
    uint64_t left = len;

    r = check_io(ictx, off, len);
    if (r < 0)
      return r;

    if (snap_id != CEPH_NOSNAP)
      return -EROFS;

    c->get();
    c->init_time(ictx, AIO_TYPE_WRITE);
    for (uint64_t i = start_block; i <= end_block; i++) {
      string oid = get_block_oid(ictx->object_prefix, i, ictx->old_format);
      ldout(cct, 20) << "oid = '" << oid << "' i = " << i << dendl;
      uint64_t total_off = off + total_write;
      uint64_t block_ofs = get_block_ofs(ictx->order, total_off);
      uint64_t write_len = min(block_size - block_ofs, left);

      bufferlist bl;
      bl.append(buf + total_write, write_len);
      if (ictx->object_cacher) {
	// may block
	ictx->write_to_cache(oid, bl, write_len, block_ofs);
      } else {
	C_AioWrite *req_comp = new C_AioWrite(cct, c);
	bool parent_exists = has_parent(parent_pool_id, total_off - block_ofs, overlap);
	ldout(ictx->cct, 20) << "has_parent(pool=" << parent_pool_id
			     << ", off=" << total_off
			     << ", overlap=" << overlap << ") = "
			     << parent_exists << dendl;
	AioWrite *req = new AioWrite(ictx, oid, total_off, bl, snapc, snap_id,
				     parent_exists, req_comp);
	c->add_request();
	r = req->send();
	if (r < 0)
	  goto done;
      }
      total_write += write_len;
      left -= write_len;
    }
  done:
    c->finish_adding_requests();
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_wr);
    ictx->perfcounter->inc(l_librbd_aio_wr_bytes, len);

    /* FIXME: cleanup all the allocated stuff */
    return r;
  }

  int aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_discard " << ictx << " off = " << off << " len = "
		   << len << dendl;

    if (!len)
      return 0;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    // TODO: check for snap
    size_t total_write = 0;
    uint64_t start_block = get_block_num(ictx->order, off);
    uint64_t end_block = get_block_num(ictx->order, off + len - 1);
    uint64_t block_size = get_block_size(ictx->order);
    ictx->snap_lock.Lock();
    snapid_t snap_id = ictx->snap_id;
    ::SnapContext snapc = ictx->snapc;
    ictx->parent_lock.Lock();
    int64_t parent_pool_id = ictx->get_parent_pool_id(ictx->snap_id);
    uint64_t overlap = 0;
    ictx->get_parent_overlap(ictx->snap_id, &overlap);
    ictx->parent_lock.Unlock();
    ictx->snap_lock.Unlock();
    uint64_t left = len;

    r = check_io(ictx, off, len);
    if (r < 0)
      return r;

    vector<ObjectExtent> v;
    if (ictx->object_cacher)
      v.reserve(end_block - start_block + 1);

    c->get();
    c->init_time(ictx, AIO_TYPE_DISCARD);
    for (uint64_t i = start_block; i <= end_block; i++) {
      string oid = get_block_oid(ictx->object_prefix, i, ictx->old_format);
      uint64_t total_off = off + total_write;
      uint64_t block_ofs = get_block_ofs(ictx->order, total_off);;
      uint64_t write_len = min(block_size - block_ofs, left);

      if (ictx->object_cacher) {
	v.push_back(ObjectExtent(oid, block_ofs, write_len));
	v.back().oloc.pool = ictx->data_ctx.get_id();
      }

      C_AioWrite *req_comp = new C_AioWrite(cct, c);
      AbstractWrite *req;
      c->add_request();

      bool parent_exists = has_parent(parent_pool_id, total_off - block_ofs, overlap);
      if (block_ofs == 0 && write_len == block_size) {
	req = new AioRemove(ictx, oid, total_off, snapc, snap_id,
			    parent_exists, req_comp);
      } else if (block_ofs + write_len == block_size) {
	req = new AioTruncate(ictx, oid, total_off, snapc, snap_id,
			      parent_exists, req_comp);
      } else {
	req = new AioZero(ictx, oid, total_off, write_len, snapc, snap_id,
			  parent_exists, req_comp);
      }

      r = req->send();
      if (r < 0)
	goto done;
      total_write += write_len;
      left -= write_len;
    }
    r = 0;
  done:
    if (ictx->object_cacher)
      ictx->object_cacher->discard_set(ictx->object_set, v);

    c->finish_adding_requests();
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_discard);
    ictx->perfcounter->inc(l_librbd_aio_discard_bytes, len);

    /* FIXME: cleanup all the allocated stuff */
    return r;
  }

  void rbd_req_cb(completion_t cb, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    req->complete(comp->get_return_value());
  }

  int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
	       char *buf,
	       AioCompletion *c)
  {
    ldout(ictx->cct, 20) << "aio_read " << ictx << " off = " << off << " len = "
			 << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    r = check_io(ictx, off, len);
    if (r < 0)
      return r;

    int64_t ret;
    int total_read = 0;
    uint64_t start_block = get_block_num(ictx->order, off);
    uint64_t end_block = get_block_num(ictx->order, off + len - 1);
    uint64_t block_size = get_block_size(ictx->order);
    ictx->snap_lock.Lock();
    snap_t snap_id = ictx->snap_id;
    ictx->snap_lock.Unlock();
    uint64_t left = len;

    c->get();
    c->init_time(ictx, AIO_TYPE_READ);
    for (uint64_t i = start_block; i <= end_block; i++) {
      string oid = get_block_oid(ictx->object_prefix, i, ictx->old_format);
      uint64_t block_ofs = get_block_ofs(ictx->order, off + total_read);
      uint64_t read_len = min(block_size - block_ofs, left);

      C_AioRead *req_comp = new C_AioRead(ictx->cct, c, buf + total_read);
      AioRead *req = new AioRead(ictx, oid, off + total_read,
				 read_len, snap_id, true, req_comp);
      req_comp->set_req(req);
      c->add_request();

      if (ictx->object_cacher) {
	req->ext_map()[block_ofs] = read_len;
	// cache has already handled possible reading from parent, so
	// this AioRead is just used to pass data to the
	// AioCompletion. The AioRead isn't being used as a
	// completion, so wrap the completion in a C_CacheRead to
	// delete it
	C_CacheRead *cache_comp = new C_CacheRead(req_comp, req);
	ictx->aio_read_from_cache(oid, &req->data(),
				  read_len, block_ofs, cache_comp);
      } else {
	r = req->send();
	if (r < 0 && r == -ENOENT)
	  r = 0;
	if (r < 0) {
	  ret = r;
	  goto done;
	}
      }

      total_read += read_len;
      left -= read_len;
    }
    ret = total_read;
  done:
    c->finish_adding_requests();
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_rd);
    ictx->perfcounter->inc(l_librbd_aio_rd_bytes, len);

    return ret;
  }

  AioCompletion *aio_create_completion() {
    AioCompletion *c = new AioCompletion();
    return c;
  }

  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete) {
    AioCompletion *c = new AioCompletion();
    c->set_complete_cb(cb_arg, cb_complete);
    return c;
  }

  AioCompletion *aio_create_completion_internal(void *cb_arg,
						callback_t cb_complete) {
    AioCompletion *c = aio_create_completion(cb_arg, cb_complete);
    c->rbd_comp = c;
    return c;
  }
}
