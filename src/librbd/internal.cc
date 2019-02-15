// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"

#include <errno.h>
#include <limits.h>

#include "include/types.h"
#include "include/uuid.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "common/event_socket.h"
#include "common/perf_counters.h"
#include "osdc/Striper.h"
#include "include/stringify.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/api/Image.h"
#include "librbd/exclusive_lock/AutomaticPolicy.h"
#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectRequest.h"
#include "librbd/io/ReadResult.h"
#include "librbd/journal/Types.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/mirror/EnableRequest.h"
#include "librbd/operation/TrimRequest.h"

#include "journal/Journaler.h"

#include <boost/scope_exit.hpp>
#include <boost/variant.hpp>
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

#define rbd_howmany(x, y)  (((x) + (y) - 1) / (y))

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;
// list binds to list() here, so std::list is explicitly used below

using ceph::bufferlist;
using librados::snap_t;
using librados::IoCtx;
using librados::Rados;

namespace librbd {

namespace {

int validate_pool(IoCtx &io_ctx, CephContext *cct) {
  if (!cct->_conf.get_val<bool>("rbd_validate_pool")) {
    return 0;
  }

  int r = io_ctx.stat(RBD_DIRECTORY, NULL, NULL);
  if (r == 0) {
    return 0;
  } else if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to stat RBD directory: " << cpp_strerror(r) << dendl;
    return r;
  }

  // allocate a self-managed snapshot id if this a new pool to force
  // self-managed snapshot mode
  uint64_t snap_id;
  r = io_ctx.selfmanaged_snap_create(&snap_id);
  if (r == -EINVAL) {
    lderr(cct) << "pool not configured for self-managed RBD snapshot support"
               << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "failed to allocate self-managed snapshot: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  r = io_ctx.selfmanaged_snap_remove(snap_id);
  if (r < 0) {
    lderr(cct) << "failed to release self-managed snapshot " << snap_id
               << ": " << cpp_strerror(r) << dendl;
  }
  return 0;
}

} // anonymous namespace

  int detect_format(IoCtx &io_ctx, const string &name,
		    bool *old_format, uint64_t *size)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    if (old_format)
      *old_format = true;
    int r = io_ctx.stat(util::old_header_name(name), size, NULL);
    if (r == -ENOENT) {
      if (old_format)
	*old_format = false;
      r = io_ctx.stat(util::id_obj_name(name), size, NULL);
      if (r < 0)
	return r;
    } else if (r < 0) {
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
		       uint64_t size, int order, uint64_t bid)
  {
    uint32_t hi = bid >> 32;
    uint32_t lo = bid & 0xFFFFFFFF;
    uint32_t extra = rand() % 0xFFFFFFFF;
    memset(&ondisk, 0, sizeof(ondisk));

    memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
    memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE,
	   sizeof(RBD_HEADER_SIGNATURE));
    memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

    snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x.%x",
	     hi, lo, extra);

    ondisk.image_size = size;
    ondisk.options.order = order;
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
    ictx->snap_lock.get_read();
    info.size = ictx->get_image_size(ictx->snap_id);
    ictx->snap_lock.put_read();
    info.obj_size = 1ULL << obj_order;
    info.num_objs = Striper::get_num_objects(ictx->layout, info.size);
    info.order = obj_order;
    strncpy(info.block_name_prefix, ictx->object_prefix.c_str(),
            RBD_MAX_BLOCK_NAME_SIZE);
    info.block_name_prefix[RBD_MAX_BLOCK_NAME_SIZE - 1] = '\0';

    // clear deprecated fields
    info.parent_pool = -1L;
    info.parent_name[0] = '\0';
  }

  uint64_t oid_to_object_no(const string& oid, const string& object_prefix)
  {
    istringstream iss(oid);
    // skip object prefix and separator
    iss.ignore(object_prefix.length() + 1);
    uint64_t num;
    iss >> std::hex >> num;
    return num;
  }

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx)
  {
    ceph_assert(ictx->owner_lock.is_locked());
    ceph_assert(ictx->exclusive_lock == nullptr ||
                ictx->exclusive_lock->is_lock_owner());

    C_SaferCond ctx;
    ictx->snap_lock.get_read();
    operation::TrimRequest<> *req = operation::TrimRequest<>::create(
      *ictx, &ctx, ictx->size, newsize, prog_ctx);
    ictx->snap_lock.put_read();
    req->send();

    int r = ctx.wait();
    if (r < 0) {
      lderr(ictx->cct) << "warning: failed to remove some object(s): "
		       << cpp_strerror(r) << dendl;
    }
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

    static_assert(sizeof(RBD_HEADER_TEXT) == sizeof(RBD_MIGRATE_HEADER_TEXT),
                  "length of rbd headers must be the same");

    if (header.length() < sizeof(RBD_HEADER_TEXT) ||
        (memcmp(RBD_HEADER_TEXT, header.c_str(),
                sizeof(RBD_HEADER_TEXT)) != 0 &&
         memcmp(RBD_MIGRATE_HEADER_TEXT, header.c_str(),
                sizeof(RBD_MIGRATE_HEADER_TEXT)) != 0)) {
      CephContext *cct = (CephContext *)io_ctx.cct();
      lderr(cct) << "unrecognized header format" << dendl;
      return -ENXIO;
    }

    if (ver)
      *ver = io_ctx.get_last_version();

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

  int tmap_set(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl, emptybl;
    __u8 c = CEPH_OSD_TMAP_SET;
    encode(c, cmdbl);
    encode(imgname, cmdbl);
    encode(emptybl, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  int tmap_rm(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl;
    __u8 c = CEPH_OSD_TMAP_RM;
    encode(c, cmdbl);
    encode(imgname, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  typedef boost::variant<std::string,uint64_t> image_option_value_t;
  typedef std::map<int,image_option_value_t> image_options_t;
  typedef std::shared_ptr<image_options_t> image_options_ref;

  enum image_option_type_t {
    STR,
    UINT64,
  };

  const std::map<int, image_option_type_t> IMAGE_OPTIONS_TYPE_MAPPING = {
    {RBD_IMAGE_OPTION_FORMAT, UINT64},
    {RBD_IMAGE_OPTION_FEATURES, UINT64},
    {RBD_IMAGE_OPTION_ORDER, UINT64},
    {RBD_IMAGE_OPTION_STRIPE_UNIT, UINT64},
    {RBD_IMAGE_OPTION_STRIPE_COUNT, UINT64},
    {RBD_IMAGE_OPTION_JOURNAL_ORDER, UINT64},
    {RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH, UINT64},
    {RBD_IMAGE_OPTION_JOURNAL_POOL, STR},
    {RBD_IMAGE_OPTION_FEATURES_SET, UINT64},
    {RBD_IMAGE_OPTION_FEATURES_CLEAR, UINT64},
    {RBD_IMAGE_OPTION_DATA_POOL, STR},
    {RBD_IMAGE_OPTION_FLATTEN, UINT64},
  };

  std::string image_option_name(int optname) {
    switch (optname) {
    case RBD_IMAGE_OPTION_FORMAT:
      return "format";
    case RBD_IMAGE_OPTION_FEATURES:
      return "features";
    case RBD_IMAGE_OPTION_ORDER:
      return "order";
    case RBD_IMAGE_OPTION_STRIPE_UNIT:
      return "stripe_unit";
    case RBD_IMAGE_OPTION_STRIPE_COUNT:
      return "stripe_count";
    case RBD_IMAGE_OPTION_JOURNAL_ORDER:
      return "journal_order";
    case RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH:
      return "journal_splay_width";
    case RBD_IMAGE_OPTION_JOURNAL_POOL:
      return "journal_pool";
    case RBD_IMAGE_OPTION_FEATURES_SET:
      return "features_set";
    case RBD_IMAGE_OPTION_FEATURES_CLEAR:
      return "features_clear";
    case RBD_IMAGE_OPTION_DATA_POOL:
      return "data_pool";
    case RBD_IMAGE_OPTION_FLATTEN:
      return "flatten";
    default:
      return "unknown (" + stringify(optname) + ")";
    }
  }

  void image_options_create(rbd_image_options_t* opts)
  {
    image_options_ref* opts_ = new image_options_ref(new image_options_t());

    *opts = static_cast<rbd_image_options_t>(opts_);
  }

  void image_options_create_ref(rbd_image_options_t* opts,
				rbd_image_options_t orig)
  {
    image_options_ref* orig_ = static_cast<image_options_ref*>(orig);
    image_options_ref* opts_ = new image_options_ref(*orig_);

    *opts = static_cast<rbd_image_options_t>(opts_);
  }

  void image_options_copy(rbd_image_options_t* opts,
			  const ImageOptions &orig)
  {
    image_options_ref* opts_ = new image_options_ref(new image_options_t());

    *opts = static_cast<rbd_image_options_t>(opts_);

    std::string str_val;
    uint64_t uint64_val;
    for (auto &i : IMAGE_OPTIONS_TYPE_MAPPING) {
      switch (i.second) {
      case STR:
	if (orig.get(i.first, &str_val) == 0) {
	  image_options_set(*opts, i.first, str_val);
	}
	continue;
      case UINT64:
	if (orig.get(i.first, &uint64_val) == 0) {
	  image_options_set(*opts, i.first, uint64_val);
	}
	continue;
      }
    }
  }

  void image_options_destroy(rbd_image_options_t opts)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    delete opts_;
  }

  int image_options_set(rbd_image_options_t opts, int optname,
			const std::string& optval)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    std::map<int, image_option_type_t>::const_iterator i =
      IMAGE_OPTIONS_TYPE_MAPPING.find(optname);

    if (i == IMAGE_OPTIONS_TYPE_MAPPING.end() || i->second != STR) {
      return -EINVAL;
    }

    (*opts_->get())[optname] = optval;
    return 0;
  }

  int image_options_set(rbd_image_options_t opts, int optname, uint64_t optval)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    std::map<int, image_option_type_t>::const_iterator i =
      IMAGE_OPTIONS_TYPE_MAPPING.find(optname);

    if (i == IMAGE_OPTIONS_TYPE_MAPPING.end() || i->second != UINT64) {
      return -EINVAL;
    }

    (*opts_->get())[optname] = optval;
    return 0;
  }

  int image_options_get(rbd_image_options_t opts, int optname,
			std::string* optval)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    std::map<int, image_option_type_t>::const_iterator i =
      IMAGE_OPTIONS_TYPE_MAPPING.find(optname);

    if (i == IMAGE_OPTIONS_TYPE_MAPPING.end() || i->second != STR) {
      return -EINVAL;
    }

    image_options_t::const_iterator j = (*opts_)->find(optname);

    if (j == (*opts_)->end()) {
      return -ENOENT;
    }

    *optval = boost::get<std::string>(j->second);
    return 0;
  }

  int image_options_get(rbd_image_options_t opts, int optname, uint64_t* optval)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    std::map<int, image_option_type_t>::const_iterator i =
      IMAGE_OPTIONS_TYPE_MAPPING.find(optname);

    if (i == IMAGE_OPTIONS_TYPE_MAPPING.end() || i->second != UINT64) {
      return -EINVAL;
    }

    image_options_t::const_iterator j = (*opts_)->find(optname);

    if (j == (*opts_)->end()) {
      return -ENOENT;
    }

    *optval = boost::get<uint64_t>(j->second);
    return 0;
  }

  int image_options_is_set(rbd_image_options_t opts, int optname,
                           bool* is_set)
  {
    if (IMAGE_OPTIONS_TYPE_MAPPING.find(optname) ==
          IMAGE_OPTIONS_TYPE_MAPPING.end()) {
      return -EINVAL;
    }

    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);
    *is_set = ((*opts_)->find(optname) != (*opts_)->end());
    return 0;
  }

  int image_options_unset(rbd_image_options_t opts, int optname)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    std::map<int, image_option_type_t>::const_iterator i =
      IMAGE_OPTIONS_TYPE_MAPPING.find(optname);

    if (i == IMAGE_OPTIONS_TYPE_MAPPING.end()) {
      ceph_assert((*opts_)->find(optname) == (*opts_)->end());
      return -EINVAL;
    }

    image_options_t::const_iterator j = (*opts_)->find(optname);

    if (j == (*opts_)->end()) {
      return -ENOENT;
    }

    (*opts_)->erase(j);
    return 0;
  }

  void image_options_clear(rbd_image_options_t opts)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    (*opts_)->clear();
  }

  bool image_options_is_empty(rbd_image_options_t opts)
  {
    image_options_ref* opts_ = static_cast<image_options_ref*>(opts);

    return (*opts_)->empty();
  }

  int flatten_children(ImageCtx *ictx, const char* snap_name,
                       ProgressContext& pctx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "children flatten " << ictx->name << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    RWLock::RLocker l(ictx->snap_lock);
    snap_t snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
                                       snap_name);

    cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                          ictx->md_ctx.get_namespace(),
                                          ictx->id, snap_id};
    std::vector<librbd::linked_image_spec_t> child_images;
    r = api::Image<>::list_children(ictx, parent_spec, &child_images);
    if (r < 0) {
      return r;
    }

    size_t size = child_images.size();
    if (size == 0) {
      return 0;
    }

    librados::IoCtx child_io_ctx;
    int64_t child_pool_id = -1;
    size_t i = 0;
    for (auto &child_image : child_images){
      std::string pool = child_image.pool_name;
      if (child_pool_id == -1 ||
          child_pool_id != child_image.pool_id ||
          child_io_ctx.get_namespace() != child_image.pool_namespace) {
        r = util::create_ioctx(ictx->md_ctx, "child image",
                               child_image.pool_id, child_image.pool_namespace,
                               &child_io_ctx);
        if (r < 0) {
          return r;
        }

        child_pool_id = child_image.pool_id;
      }

      ImageCtx *imctx = new ImageCtx("", child_image.image_id, nullptr,
                                     child_io_ctx, false);
      r = imctx->state->open(0);
      if (r < 0) {
        lderr(cct) << "error opening image: " << cpp_strerror(r) << dendl;
        return r;
      }

      if ((imctx->features & RBD_FEATURE_DEEP_FLATTEN) == 0 &&
          !imctx->snaps.empty()) {
        lderr(cct) << "snapshot in-use by " << pool << "/" << imctx->name
                   << dendl;
        imctx->state->close();
        return -EBUSY;
      }

      librbd::NoOpProgressContext prog_ctx;
      r = imctx->operations->flatten(prog_ctx);
      if (r < 0) {
        lderr(cct) << "error flattening image: " << pool << "/"
                   << (child_image.pool_namespace.empty() ?
                        "" : "/" + child_image.pool_namespace)
                   << child_image.image_name << cpp_strerror(r) << dendl;
        imctx->state->close();
        return r;
      }

      r = imctx->state->close();
      if (r < 0) {
        lderr(cct) << "failed to close image: " << cpp_strerror(r) << dendl;
        return r;
      }

      pctx.update_progress(++i, size);
      ceph_assert(i <= size);
    }

    return 0;
  }

  int get_snap_namespace(ImageCtx *ictx,
			 const char *snap_name,
			 cls::rbd::SnapshotNamespace *snap_namespace) {
    ldout(ictx->cct, 20) << "get_snap_namespace " << ictx << " " << snap_name
			 << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->snap_lock);
    snap_t snap_id = ictx->get_snap_id(*snap_namespace, snap_name);
    if (snap_id == CEPH_NOSNAP)
      return -ENOENT;
    r = ictx->get_snap_namespace(snap_id, snap_namespace);
    return r;
  }

  int snap_is_protected(ImageCtx *ictx, const char *snap_name, bool *is_protected)
  {
    ldout(ictx->cct, 20) << "snap_is_protected " << ictx << " " << snap_name
			 << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    RWLock::RLocker l(ictx->snap_lock);
    snap_t snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), snap_name);
    if (snap_id == CEPH_NOSNAP)
      return -ENOENT;
    bool is_unprotected;
    r = ictx->is_snap_unprotected(snap_id, &is_unprotected);
    // consider both PROTECTED or UNPROTECTING to be 'protected',
    // since in either state they can't be deleted
    *is_protected = !is_unprotected;
    return r;
  }

  int create_v1(IoCtx& io_ctx, const char *imgname, uint64_t size, int order)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();

    ldout(cct, 20) << __func__ << " "  << &io_ctx << " name = " << imgname
		   << " size = " << size << " order = " << order << dendl;
    int r = validate_pool(io_ctx, cct);
    if (r < 0) {
      return r;
    }

    if (!io_ctx.get_namespace().empty()) {
      lderr(cct) << "attempting to add v1 image to namespace" << dendl;
      return -EINVAL;
    }

    ldout(cct, 2) << "adding rbd image to directory..." << dendl;
    r = tmap_set(io_ctx, imgname);
    if (r < 0) {
      lderr(cct) << "error adding image to directory: " << cpp_strerror(r)
		 << dendl;
      return r;
    }

    Rados rados(io_ctx);
    uint64_t bid = rados.get_instance_id();

    ldout(cct, 2) << "creating rbd image..." << dendl;
    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size, order, bid);

    bufferlist bl;
    bl.append((const char *)&header, sizeof(header));

    string header_oid = util::old_header_name(imgname);
    r = io_ctx.write(header_oid, bl, bl.length(), 0);
    if (r < 0) {
      lderr(cct) << "Error writing image header: " << cpp_strerror(r)
		 << dendl;
      int remove_r = tmap_rm(io_ctx, imgname);
      if (remove_r < 0) {
	lderr(cct) << "Could not remove image from directory after "
		   << "header creation failed: "
		   << cpp_strerror(remove_r) << dendl;
      }
      return r;
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     int *order)
  {
    uint64_t order_ = *order;
    ImageOptions opts;

    int r = opts.set(RBD_IMAGE_OPTION_ORDER, order_);
    ceph_assert(r == 0);

    r = create(io_ctx, imgname, "", size, opts, "", "", false);

    int r1 = opts.get(RBD_IMAGE_OPTION_ORDER, &order_);
    ceph_assert(r1 == 0);
    *order = order_;

    return r;
  }

  int create(IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order,
	     uint64_t stripe_unit, uint64_t stripe_count)
  {
    if (!order)
      return -EINVAL;

    uint64_t order_ = *order;
    uint64_t format = old_format ? 1 : 2;
    ImageOptions opts;
    int r;

    r = opts.set(RBD_IMAGE_OPTION_FORMAT, format);
    ceph_assert(r == 0);
    r = opts.set(RBD_IMAGE_OPTION_FEATURES, features);
    ceph_assert(r == 0);
    r = opts.set(RBD_IMAGE_OPTION_ORDER, order_);
    ceph_assert(r == 0);
    r = opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
    ceph_assert(r == 0);
    r = opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
    ceph_assert(r == 0);

    r = create(io_ctx, imgname, "", size, opts, "", "", false);

    int r1 = opts.get(RBD_IMAGE_OPTION_ORDER, &order_);
    ceph_assert(r1 == 0);
    *order = order_;

    return r;
  }

  int create(IoCtx& io_ctx, const std::string &image_name,
	     const std::string &image_id, uint64_t size,
	     ImageOptions& opts,
             const std::string &non_primary_global_image_id,
             const std::string &primary_mirror_uuid,
             bool skip_mirror_enable)
  {
    std::string id(image_id);
    if (id.empty()) {
      id = util::generate_image_id(io_ctx);
    }

    CephContext *cct = (CephContext *)io_ctx.cct();
    uint64_t flatten;
    if (opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
      lderr(cct) << "create does not support 'flatten' image option" << dendl;
      return -EINVAL;
    }

    ldout(cct, 10) << __func__ << " name=" << image_name << ", "
		   << "id= " << id << ", "
		   << "size=" << size << ", opts=" << opts << dendl;

    uint64_t format;
    if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0)
      format = cct->_conf.get_val<uint64_t>("rbd_default_format");
    bool old_format = format == 1;

    // make sure it doesn't already exist, in either format
    int r = detect_format(io_ctx, image_name, NULL, NULL);
    if (r != -ENOENT) {
      if (r) {
	lderr(cct) << "Could not tell if " << image_name << " already exists"
		   << dendl;
	return r;
      }
      lderr(cct) << "rbd image " << image_name << " already exists" << dendl;
      return -EEXIST;
    }

    uint64_t order = 0;
    if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0 || order == 0) {
      order = cct->_conf.get_val<uint64_t>("rbd_default_order");
    }
    r = image::CreateRequest<>::validate_order(cct, order);
    if (r < 0) {
      return r;
    }

    if (old_format) {
      if ( !getenv("RBD_FORCE_ALLOW_V1") ) {
        lderr(cct) << "Format 1 image creation unsupported. " << dendl;
        return -EINVAL;
      }
      lderr(cct) << "Forced V1 image creation. " << dendl;
      r = create_v1(io_ctx, image_name.c_str(), size, order);
    } else {
      ThreadPool *thread_pool;
      ContextWQ *op_work_queue;
      ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);

      ConfigProxy config{cct->_conf};
      api::Config<>::apply_pool_overrides(io_ctx, &config);

      C_SaferCond cond;
      image::CreateRequest<> *req = image::CreateRequest<>::create(
        config, io_ctx, image_name, id, size, opts, non_primary_global_image_id,
        primary_mirror_uuid, skip_mirror_enable, op_work_queue, &cond);
      req->send();

      r = cond.wait();
    }

    int r1 = opts.set(RBD_IMAGE_OPTION_ORDER, order);
    ceph_assert(r1 == 0);

    return r;
  }

  /*
   * Parent may be in different pool, hence different IoCtx
   */
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name,
	    uint64_t features, int *c_order,
	    uint64_t stripe_unit, int stripe_count)
  {
    uint64_t order = *c_order;

    ImageOptions opts;
    opts.set(RBD_IMAGE_OPTION_FEATURES, features);
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);

    int r = clone(p_ioctx, nullptr, p_name, p_snap_name, c_ioctx, nullptr,
                  c_name, opts, "", "");
    opts.get(RBD_IMAGE_OPTION_ORDER, &order);
    *c_order = order;
    return r;
  }

  int clone(IoCtx& p_ioctx, const char *p_id, const char *p_name,
            const char *p_snap_name, IoCtx& c_ioctx, const char *c_id,
            const char *c_name, ImageOptions& c_opts,
            const std::string &non_primary_global_image_id,
            const std::string &primary_mirror_uuid)
  {
    ceph_assert((p_id == nullptr) ^ (p_name == nullptr));

    CephContext *cct = (CephContext *)p_ioctx.cct();
    if (p_snap_name == nullptr) {
      lderr(cct) << "image to be cloned must be a snapshot" << dendl;
      return -EINVAL;
    }

    uint64_t flatten;
    if (c_opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
      lderr(cct) << "clone does not support 'flatten' image option" << dendl;
      return -EINVAL;
    }

    int r;
    std::string parent_id;
    if (p_id == nullptr) {
      r = cls_client::dir_get_id(&p_ioctx, RBD_DIRECTORY, p_name,
                                 &parent_id);
      if (r < 0) {
        if (r != -ENOENT) {
          lderr(cct) << "failed to retrieve parent image id: "
                     << cpp_strerror(r) << dendl;
        }
        return r;
      }
    } else {
      parent_id = p_id;
    }

    std::string clone_id;
    if (c_id == nullptr) {
      clone_id = util::generate_image_id(c_ioctx);
    } else {
      clone_id = c_id;
    }

    ldout(cct, 10) << __func__ << " "
		   << "c_name=" << c_name << ", "
		   << "c_id= " << clone_id << ", "
		   << "c_opts=" << c_opts << dendl;

    ConfigProxy config{reinterpret_cast<CephContext *>(c_ioctx.cct())->_conf};
    api::Config<>::apply_pool_overrides(c_ioctx, &config);

    ThreadPool *thread_pool;
    ContextWQ *op_work_queue;
    ImageCtx::get_thread_pool_instance(cct, &thread_pool, &op_work_queue);

    C_SaferCond cond;
    auto *req = image::CloneRequest<>::create(
      config, p_ioctx, parent_id, p_snap_name, CEPH_NOSNAP, c_ioctx, c_name,
      clone_id, c_opts, non_primary_global_image_id, primary_mirror_uuid,
      op_work_queue, &cond);
    req->send();

    r = cond.wait();
    if (r < 0) {
      return r;
    }

    return 0;
  }

  int rename(IoCtx& io_ctx, const char *srcname, const char *dstname)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "rename " << &io_ctx << " " << srcname << " -> "
		   << dstname << dendl;

    ImageCtx *ictx = new ImageCtx(srcname, "", "", io_ctx, false);
    int r = ictx->state->open(0);
    if (r < 0) {
      lderr(cct) << "error opening source image: " << cpp_strerror(r) << dendl;
      return r;
    }
    BOOST_SCOPE_EXIT((ictx)) {
      ictx->state->close();
    } BOOST_SCOPE_EXIT_END

    return ictx->operations->rename(dstname);
  }

  int info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    ldout(ictx->cct, 20) << "info " << ictx << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    image_info(ictx, info, infosize);
    return 0;
  }

  int get_old_format(ImageCtx *ictx, uint8_t *old)
  {
    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;
    *old = ictx->old_format;
    return 0;
  }

  int get_size(ImageCtx *ictx, uint64_t *size)
  {
    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;
    RWLock::RLocker l2(ictx->snap_lock);
    *size = ictx->get_image_size(ictx->snap_id);
    return 0;
  }

  int get_features(ImageCtx *ictx, uint64_t *features)
  {
    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->snap_lock);
    *features = ictx->features;
    return 0;
  }

  int get_overlap(ImageCtx *ictx, uint64_t *overlap)
  {
    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->snap_lock);
    RWLock::RLocker l2(ictx->parent_lock);
    return ictx->get_parent_overlap(ictx->snap_id, overlap);
  }

  int get_flags(ImageCtx *ictx, uint64_t *flags)
  {
    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    RWLock::RLocker l2(ictx->snap_lock);
    return ictx->get_flags(ictx->snap_id, flags);
  }

  int set_image_notification(ImageCtx *ictx, int fd, int type)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << " " << ictx << " fd " << fd << " type" << type << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    if (ictx->event_socket.is_valid())
      return -EINVAL;
    return ictx->event_socket.init(fd, type);
  }

  int is_exclusive_lock_owner(ImageCtx *ictx, bool *is_owner)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << ": ictx=" << ictx << dendl;
    *is_owner = false;

    RWLock::RLocker owner_locker(ictx->owner_lock);
    if (ictx->exclusive_lock == nullptr) {
      return 0;
    }

    // might have been blacklisted by peer -- ensure we still own
    // the lock by pinging the OSD
    int r = ictx->exclusive_lock->assert_header_locked();
    if (r == -EBUSY || r == -ENOENT) {
      return 0;
    } else if (r < 0) {
      return r;
    }

    *is_owner = true;
    return 0;
  }

  int lock_acquire(ImageCtx *ictx, rbd_lock_mode_t lock_mode)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << ": ictx=" << ictx << ", "
                   << "lock_mode=" << lock_mode << dendl;

    if (lock_mode != RBD_LOCK_MODE_EXCLUSIVE) {
      return -EOPNOTSUPP;
    }

    C_SaferCond lock_ctx;
    {
      RWLock::WLocker l(ictx->owner_lock);

      if (ictx->exclusive_lock == nullptr) {
	lderr(cct) << "exclusive-lock feature is not enabled" << dendl;
	return -EINVAL;
      }

      if (ictx->get_exclusive_lock_policy()->may_auto_request_lock()) {
	ictx->set_exclusive_lock_policy(
	  new exclusive_lock::StandardPolicy(ictx));
      }

      if (ictx->exclusive_lock->is_lock_owner()) {
	return 0;
      }

      ictx->exclusive_lock->acquire_lock(&lock_ctx);
    }

    int r = lock_ctx.wait();
    if (r < 0) {
      lderr(cct) << "failed to request exclusive lock: " << cpp_strerror(r)
		 << dendl;
      return r;
    }

    RWLock::RLocker l(ictx->owner_lock);
    if (ictx->exclusive_lock == nullptr) {
      return -EINVAL;
    } else if (!ictx->exclusive_lock->is_lock_owner()) {
      lderr(cct) << "failed to acquire exclusive lock" << dendl;
      return ictx->exclusive_lock->get_unlocked_op_error();
    }

    return 0;
  }

  int lock_release(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << ": ictx=" << ictx << dendl;

    C_SaferCond lock_ctx;
    {
      RWLock::WLocker l(ictx->owner_lock);

      if (ictx->exclusive_lock == nullptr ||
	  !ictx->exclusive_lock->is_lock_owner()) {
	lderr(cct) << "not exclusive lock owner" << dendl;
	return -EINVAL;
      }

      ictx->exclusive_lock->release_lock(&lock_ctx);
    }

    int r = lock_ctx.wait();
    if (r < 0) {
      lderr(cct) << "failed to release exclusive lock: " << cpp_strerror(r)
		 << dendl;
      return r;
    }
    return 0;
  }

  int lock_get_owners(ImageCtx *ictx, rbd_lock_mode_t *lock_mode,
                      std::list<std::string> *lock_owners)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << ": ictx=" << ictx << dendl;

    if (!ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
      lderr(cct) << "exclusive-lock feature is not enabled" << dendl;
      return -EINVAL;
    }

    managed_lock::Locker locker;
    C_SaferCond get_owner_ctx;
    ExclusiveLock<>(*ictx).get_locker(&locker, &get_owner_ctx);
    int r = get_owner_ctx.wait();
    if (r == -ENOENT) {
      return r;
    } else if (r < 0) {
      lderr(cct) << "failed to determine current lock owner: "
                 << cpp_strerror(r) << dendl;
      return r;
    }

    *lock_mode = RBD_LOCK_MODE_EXCLUSIVE;
    lock_owners->clear();
    lock_owners->emplace_back(locker.address);
    return 0;
  }

  int lock_break(ImageCtx *ictx, rbd_lock_mode_t lock_mode,
                 const std::string &lock_owner) {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << ": ictx=" << ictx << ", "
                   << "lock_mode=" << lock_mode << ", "
                   << "lock_owner=" << lock_owner << dendl;

    if (lock_mode != RBD_LOCK_MODE_EXCLUSIVE) {
      return -EOPNOTSUPP;
    }

    if (ictx->read_only) {
      return -EROFS;
    }

    managed_lock::Locker locker;
    C_SaferCond get_owner_ctx;
    {
      RWLock::RLocker l(ictx->owner_lock);

      if (ictx->exclusive_lock == nullptr) {
        lderr(cct) << "exclusive-lock feature is not enabled" << dendl;
        return -EINVAL;
      }

      ictx->exclusive_lock->get_locker(&locker, &get_owner_ctx);
    }
    int r = get_owner_ctx.wait();
    if (r == -ENOENT) {
      return r;
    } else if (r < 0) {
      lderr(cct) << "failed to determine current lock owner: "
                 << cpp_strerror(r) << dendl;
      return r;
    }

    if (locker.address != lock_owner) {
      return -EBUSY;
    }

    C_SaferCond break_ctx;
    {
      RWLock::RLocker l(ictx->owner_lock);

      if (ictx->exclusive_lock == nullptr) {
        lderr(cct) << "exclusive-lock feature is not enabled" << dendl;
        return -EINVAL;
      }

      ictx->exclusive_lock->break_lock(locker, true, &break_ctx);
    }
    r = break_ctx.wait();
    if (r == -ENOENT) {
      return r;
    } else if (r < 0) {
      lderr(cct) << "failed to break lock: " << cpp_strerror(r) << dendl;
      return r;
    }
    return 0;
  }

  int snap_list(ImageCtx *ictx, vector<snap_info_t>& snaps)
  {
    ldout(ictx->cct, 20) << "snap_list " << ictx << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    RWLock::RLocker l(ictx->snap_lock);
    for (map<snap_t, SnapInfo>::iterator it = ictx->snap_info.begin();
	 it != ictx->snap_info.end(); ++it) {
      snap_info_t info;
      info.name = it->second.name;
      info.id = it->first;
      info.size = it->second.size;
      snaps.push_back(info);
    }

    return 0;
  }

  int snap_exists(ImageCtx *ictx, const cls::rbd::SnapshotNamespace& snap_namespace,
		  const char *snap_name, bool *exists)
  {
    ldout(ictx->cct, 20) << "snap_exists " << ictx << " " << snap_name << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    RWLock::RLocker l(ictx->snap_lock);
    *exists = ictx->get_snap_id(snap_namespace, snap_name) != CEPH_NOSNAP;
    return 0;
  }

  int snap_remove(ImageCtx *ictx, const char *snap_name, uint32_t flags,
		  ProgressContext& pctx)
  {
    ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_name << " flags: " << flags << dendl;

    int r = 0;

    r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    if (flags & RBD_SNAP_REMOVE_FLATTEN) {
	r = flatten_children(ictx, snap_name, pctx);
	if (r < 0) {
	  return r;
	}
    }

    bool is_protected;
    r = snap_is_protected(ictx, snap_name, &is_protected);
    if (r < 0) {
      return r;
    }

    if (is_protected && flags & RBD_SNAP_REMOVE_UNPROTECT) {
      r = ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), snap_name);
      if (r < 0) {
	lderr(ictx->cct) << "failed to unprotect snapshot: " << snap_name << dendl;
	return r;
      }

      r = snap_is_protected(ictx, snap_name, &is_protected);
      if (r < 0) {
	return r;
      }
      if (is_protected) {
	lderr(ictx->cct) << "snapshot is still protected after unprotection" << dendl;
	ceph_abort();
      }
    }

    C_SaferCond ctx;
    ictx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(), snap_name, &ctx);

    r = ctx.wait();
    return r;
  }

  int snap_get_timestamp(ImageCtx *ictx, uint64_t snap_id, struct timespec *timestamp)
  {
    std::map<librados::snap_t, SnapInfo>::iterator snap_it = ictx->snap_info.find(snap_id);
    ceph_assert(snap_it != ictx->snap_info.end());
    utime_t time = snap_it->second.timestamp;
    time.to_timespec(timestamp);
    return 0;
  }

  int snap_get_limit(ImageCtx *ictx, uint64_t *limit)
  {
    int r = cls_client::snapshot_get_limit(&ictx->md_ctx, ictx->header_oid,
                                           limit);
    if (r == -EOPNOTSUPP) {
      *limit = UINT64_MAX;
      r = 0;
    }
    return r;
  }

  int snap_set_limit(ImageCtx *ictx, uint64_t limit)
  {
    return ictx->operations->snap_set_limit(limit);
  }

  struct CopyProgressCtx {
    explicit CopyProgressCtx(ProgressContext &p)
      : destictx(NULL), src_size(0), prog_ctx(p)
    { }

    ImageCtx *destictx;
    uint64_t src_size;
    ProgressContext &prog_ctx;
  };

  int copy(ImageCtx *src, IoCtx& dest_md_ctx, const char *destname,
	   ImageOptions& opts, ProgressContext &prog_ctx, size_t sparse_size)
  {
    CephContext *cct = (CephContext *)dest_md_ctx.cct();
    uint64_t flatten;
    if (opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
      lderr(cct) << "copy does not support 'flatten' image option" << dendl;
      return -EINVAL;
    }

    ldout(cct, 20) << "copy " << src->name
		   << (src->snap_name.length() ? "@" + src->snap_name : "")
		   << " -> " << destname << " opts = " << opts << dendl;

    src->snap_lock.get_read();
    uint64_t features = src->features;
    uint64_t src_size = src->get_image_size(src->snap_id);
    src->snap_lock.put_read();
    uint64_t format = src->old_format ? 1 : 2;
    if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0) {
      opts.set(RBD_IMAGE_OPTION_FORMAT, format);
    }
    uint64_t stripe_unit = src->stripe_unit;
    if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
      opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
    }
    uint64_t stripe_count = src->stripe_count;
    if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
      opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
    }
    uint64_t order = src->order;
    if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
      opts.set(RBD_IMAGE_OPTION_ORDER, order);
    }
    if (opts.get(RBD_IMAGE_OPTION_FEATURES, &features) != 0) {
      opts.set(RBD_IMAGE_OPTION_FEATURES, features);
    }
    if (features & ~RBD_FEATURES_ALL) {
      lderr(cct) << "librbd does not support requested features" << dendl;
      return -ENOSYS;
    }

    int r = create(dest_md_ctx, destname, "", src_size, opts, "", "", false);
    if (r < 0) {
      lderr(cct) << "header creation failed" << dendl;
      return r;
    }
    opts.set(RBD_IMAGE_OPTION_ORDER, static_cast<uint64_t>(order));

    ImageCtx *dest = new librbd::ImageCtx(destname, "", nullptr, dest_md_ctx,
                                          false);
    r = dest->state->open(0);
    if (r < 0) {
      lderr(cct) << "failed to read newly created header" << dendl;
      return r;
    }

    r = copy(src, dest, prog_ctx, sparse_size);

    int close_r = dest->state->close();
    if (r == 0 && close_r < 0) {
      r = close_r;
    }
    return r;
  }

  class C_CopyWrite : public Context {
  public:
    C_CopyWrite(bufferlist *bl, Context* ctx)
      : m_bl(bl), m_ctx(ctx) {}
    void finish(int r) override {
      delete m_bl;
      m_ctx->complete(r);
    }
  private:
    bufferlist *m_bl;
    Context *m_ctx;
  };

  class C_CopyRead : public Context {
  public:
    C_CopyRead(SimpleThrottle *throttle, ImageCtx *dest, uint64_t offset,
	       bufferlist *bl, size_t sparse_size)
      : m_throttle(throttle), m_dest(dest), m_offset(offset), m_bl(bl),
      m_sparse_size(sparse_size) {
      m_throttle->start_op();
    }
    void finish(int r) override {
      if (r < 0) {
	lderr(m_dest->cct) << "error reading from source image at offset "
			   << m_offset << ": " << cpp_strerror(r) << dendl;
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }
      ceph_assert(m_bl->length() == (size_t)r);

      if (m_bl->is_zero()) {
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }

      if (!m_sparse_size) {
	m_sparse_size = (1 << m_dest->order);
      }

      auto *throttle = m_throttle;
      auto *end_op_ctx = new FunctionContext([throttle](int r) {
	throttle->end_op(r);
      });
      auto gather_ctx = new C_Gather(m_dest->cct, end_op_ctx);

      m_bl->rebuild(buffer::ptr_node::create(m_bl->length()));
      size_t write_offset = 0;
      size_t write_length = 0;
      size_t offset = 0;
      size_t length = m_bl->length();
      const auto& m_ptr = m_bl->front();
      while (offset < length) {
	if (util::calc_sparse_extent(m_ptr,
				     m_sparse_size,
				     length,
				     &write_offset,
				     &write_length,
				     &offset)) {
	  bufferlist *write_bl = new bufferlist();
	  write_bl->push_back(
	    buffer::ptr_node::create(m_ptr, write_offset, write_length));
	  Context *ctx = new C_CopyWrite(write_bl, gather_ctx->new_sub());
	  auto comp = io::AioCompletion::create(ctx);

	  // coordinate through AIO WQ to ensure lock is acquired if needed
	  m_dest->io_work_queue->aio_write(comp, m_offset + write_offset,
					   write_length,
					   std::move(*write_bl),
					   LIBRADOS_OP_FLAG_FADVISE_DONTNEED,
					   std::move(read_trace));
	  write_offset = offset;
	  write_length = 0;
	}
      }
      delete m_bl;
      ceph_assert(gather_ctx->get_sub_created_count() > 0);
      gather_ctx->activate();
    }

    ZTracer::Trace read_trace;

  private:
    SimpleThrottle *m_throttle;
    ImageCtx *m_dest;
    uint64_t m_offset;
    bufferlist *m_bl;
    size_t m_sparse_size;
  };

  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx, size_t sparse_size)
  {
    src->snap_lock.get_read();
    uint64_t src_size = src->get_image_size(src->snap_id);
    src->snap_lock.put_read();

    dest->snap_lock.get_read();
    uint64_t dest_size = dest->get_image_size(dest->snap_id);
    dest->snap_lock.put_read();

    CephContext *cct = src->cct;
    if (dest_size < src_size) {
      lderr(cct) << " src size " << src_size << " > dest size "
		 << dest_size << dendl;
      return -EINVAL;
    }
    int r;
    const uint32_t MAX_KEYS = 64;
    map<string, bufferlist> pairs;
    std::string last_key = "";
    bool more_results = true;

    while (more_results) {
      r = cls_client::metadata_list(&src->md_ctx, src->header_oid, last_key, 0, &pairs);
      if (r < 0 && r != -EOPNOTSUPP && r != -EIO) {
        lderr(cct) << "couldn't list metadata: " << cpp_strerror(r) << dendl;
        return r;
      } else if (r == 0 && !pairs.empty()) {
        r = cls_client::metadata_set(&dest->md_ctx, dest->header_oid, pairs);
        if (r < 0) {
          lderr(cct) << "couldn't set metadata: " << cpp_strerror(r) << dendl;
          return r;
        }

        last_key = pairs.rbegin()->first;
      }

      more_results = (pairs.size() == MAX_KEYS);
      pairs.clear();
    }

    ZTracer::Trace trace;
    if (src->blkin_trace_all) {
      trace.init("copy", &src->trace_endpoint);
    }

    RWLock::RLocker owner_lock(src->owner_lock);
    SimpleThrottle throttle(src->config.get_val<uint64_t>("rbd_concurrent_management_ops"), false);
    uint64_t period = src->get_stripe_period();
    unsigned fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
			     LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    uint64_t object_id = 0;
    for (uint64_t offset = 0; offset < src_size; offset += period) {
      if (throttle.pending_error()) {
        return throttle.wait_for_ret();
      }

      {
        RWLock::RLocker snap_locker(src->snap_lock);
        if (src->object_map != nullptr) {
          bool skip = true;
          // each period is related to src->stripe_count objects, check them all
          for (uint64_t i=0; i < src->stripe_count; i++) {
            if (object_id < src->object_map->size() &&
                src->object_map->object_may_exist(object_id)) {
              skip = false;
            }
            ++object_id;
          }

          if (skip) continue;
        } else {
          object_id += src->stripe_count;
        }
      }

      uint64_t len = min(period, src_size - offset);
      bufferlist *bl = new bufferlist();
      auto ctx = new C_CopyRead(&throttle, dest, offset, bl, sparse_size);
      auto comp = io::AioCompletion::create_and_start<Context>(
	ctx, src, io::AIO_TYPE_READ);

      io::ImageReadRequest<> req(*src, comp, {{offset, len}},
				 io::ReadResult{bl}, fadvise_flags,
				 std::move(trace));
      ctx->read_trace = req.get_trace();

      req.send();
      prog_ctx.update_progress(offset, src_size);
    }

    r = throttle.wait_for_ret();
    if (r >= 0)
      prog_ctx.update_progress(src_size, src_size);
    return r;
  }

  int list_lockers(ImageCtx *ictx,
		   std::list<locker_t> *lockers,
		   bool *exclusive,
		   string *tag)
  {
    ldout(ictx->cct, 20) << "list_locks on image " << ictx << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    RWLock::RLocker locker(ictx->md_lock);
    if (exclusive)
      *exclusive = ictx->exclusive_locked;
    if (tag)
      *tag = ictx->lock_tag;
    if (lockers) {
      lockers->clear();
      map<rados::cls::lock::locker_id_t,
	  rados::cls::lock::locker_info_t>::const_iterator it;
      for (it = ictx->lockers.begin(); it != ictx->lockers.end(); ++it) {
	locker_t locker;
	locker.client = stringify(it->first.locker);
	locker.cookie = it->first.cookie;
	locker.address = it->second.addr.get_legacy_str();
	lockers->push_back(locker);
      }
    }

    return 0;
  }

  int lock(ImageCtx *ictx, bool exclusive, const string& cookie,
	   const string& tag)
  {
    ldout(ictx->cct, 20) << "lock image " << ictx << " exclusive=" << exclusive
			 << " cookie='" << cookie << "' tag='" << tag << "'"
			 << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    /**
     * If we wanted we could do something more intelligent, like local
     * checks that we think we will succeed. But for now, let's not
     * duplicate that code.
     */
    {
      RWLock::RLocker locker(ictx->md_lock);
      r = rados::cls::lock::lock(&ictx->md_ctx, ictx->header_oid, RBD_LOCK_NAME,
			         exclusive ? LOCK_EXCLUSIVE : LOCK_SHARED,
			         cookie, tag, "", utime_t(), 0);
      if (r < 0) {
        return r;
      }
    }

    ictx->notify_update();
    return 0;
  }

  int unlock(ImageCtx *ictx, const string& cookie)
  {
    ldout(ictx->cct, 20) << "unlock image " << ictx
			 << " cookie='" << cookie << "'" << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    {
      RWLock::RLocker locker(ictx->md_lock);
      r = rados::cls::lock::unlock(&ictx->md_ctx, ictx->header_oid,
				   RBD_LOCK_NAME, cookie);
      if (r < 0) {
        return r;
      }
    }

    ictx->notify_update();
    return 0;
  }

  int break_lock(ImageCtx *ictx, const string& client,
		 const string& cookie)
  {
    ldout(ictx->cct, 20) << "break_lock image " << ictx << " client='" << client
			 << "' cookie='" << cookie << "'" << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    entity_name_t lock_client;
    if (!lock_client.parse(client)) {
      lderr(ictx->cct) << "Unable to parse client '" << client
		       << "'" << dendl;
      return -EINVAL;
    }

    if (ictx->config.get_val<bool>("rbd_blacklist_on_break_lock")) {
      typedef std::map<rados::cls::lock::locker_id_t,
		       rados::cls::lock::locker_info_t> Lockers;
      Lockers lockers;
      ClsLockType lock_type;
      std::string lock_tag;
      r = rados::cls::lock::get_lock_info(&ictx->md_ctx, ictx->header_oid,
                                          RBD_LOCK_NAME, &lockers, &lock_type,
                                          &lock_tag);
      if (r < 0) {
        lderr(ictx->cct) << "unable to retrieve lock info: " << cpp_strerror(r)
          	       << dendl;
        return r;
      }

      std::string client_address;
      for (Lockers::iterator it = lockers.begin();
           it != lockers.end(); ++it) {
        if (it->first.locker == lock_client) {
          client_address = it->second.addr.get_legacy_str();
          break;
        }
      }
      if (client_address.empty()) {
        return -ENOENT;
      }

      RWLock::RLocker locker(ictx->md_lock);
      librados::Rados rados(ictx->md_ctx);
      r = rados.blacklist_add(
        client_address,
        ictx->config.get_val<uint64_t>("rbd_blacklist_expire_seconds"));
      if (r < 0) {
        lderr(ictx->cct) << "unable to blacklist client: " << cpp_strerror(r)
          	       << dendl;
        return r;
      }
    }

    r = rados::cls::lock::break_lock(&ictx->md_ctx, ictx->header_oid,
				     RBD_LOCK_NAME, cookie, lock_client);
    if (r < 0)
      return r;
    ictx->notify_update();
    return 0;
  }

  void rbd_ctx_cb(completion_t cb, void *arg)
  {
    Context *ctx = reinterpret_cast<Context *>(arg);
    auto comp = reinterpret_cast<io::AioCompletion *>(cb);
    ctx->complete(comp->get_return_value());
    comp->release();
  }

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg)
  {
    coarse_mono_time start_time;
    ceph::timespan elapsed;

    ldout(ictx->cct, 20) << "read_iterate " << ictx << " off = " << off
			 << " len = " << len << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    uint64_t mylen = len;
    ictx->snap_lock.get_read();
    r = clip_io(ictx, off, &mylen);
    ictx->snap_lock.put_read();
    if (r < 0)
      return r;

    int64_t total_read = 0;
    uint64_t period = ictx->get_stripe_period();
    uint64_t left = mylen;

    ZTracer::Trace trace;
    if (ictx->blkin_trace_all) {
      trace.init("read_iterate", &ictx->trace_endpoint);
    }

    RWLock::RLocker owner_locker(ictx->owner_lock);
    start_time = coarse_mono_clock::now();
    while (left > 0) {
      uint64_t period_off = off - (off % period);
      uint64_t read_len = min(period_off + period - off, left);

      bufferlist bl;

      C_SaferCond ctx;
      auto c = io::AioCompletion::create_and_start(&ctx, ictx,
                                                   io::AIO_TYPE_READ);
      io::ImageRequest<>::aio_read(ictx, c, {{off, read_len}},
                                   io::ReadResult{&bl}, 0, std::move(trace));

      int ret = ctx.wait();
      if (ret < 0) {
        return ret;
      }

      r = cb(total_read, ret, bl.c_str(), arg);
      if (r < 0) {
	return r;
      }

      total_read += ret;
      left -= ret;
      off += ret;
    }

    elapsed = coarse_mono_clock::now() - start_time;
    ictx->perfcounter->tinc(l_librbd_rd_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_rd);
    ictx->perfcounter->inc(l_librbd_rd_bytes, mylen);
    return total_read;
  }

  // validate extent against image size; clip to image size if necessary
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len)
  {
    ceph_assert(ictx->snap_lock.is_locked());
    uint64_t image_size = ictx->get_image_size(ictx->snap_id);
    bool snap_exists = ictx->snap_exists;

    if (!snap_exists)
      return -ENOENT;

    // special-case "len == 0" requests: always valid
    if (*len == 0)
      return 0;

    // can't start past end
    if (off >= image_size)
      return -EINVAL;

    // clip requests that extend past end to just end
    if ((off + *len) > image_size)
      *len = (size_t)(image_size - off);

    return 0;
  }

  int invalidate_cache(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "invalidate_cache " << ictx << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    C_SaferCond ctx;
    {
      RWLock::RLocker owner_locker(ictx->owner_lock);
      ictx->io_object_dispatcher->invalidate_cache(&ctx);
    }
    r = ctx.wait();
    ictx->perfcounter->inc(l_librbd_invalidate_cache);
    return r;
  }

  int poll_io_events(ImageCtx *ictx, io::AioCompletion **comps, int numcomp)
  {
    if (numcomp <= 0)
      return -EINVAL;
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << __func__ << " " << ictx << " numcomp = " << numcomp
                   << dendl;
    int i = 0;
    Mutex::Locker l(ictx->completed_reqs_lock);
    numcomp = std::min(numcomp, (int)ictx->completed_reqs.size());
    while (i < numcomp) {
      comps[i++] = ictx->completed_reqs.front();
      ictx->completed_reqs.pop_front();
    }
    return i;
  }

  int metadata_get(ImageCtx *ictx, const string &key, string *value)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "metadata_get " << ictx << " key=" << key << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    return cls_client::metadata_get(&ictx->md_ctx, ictx->header_oid, key, value);
  }

  int metadata_list(ImageCtx *ictx, const string &start, uint64_t max, map<string, bufferlist> *pairs)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "metadata_list " << ictx << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      return r;
    }

    return cls_client::metadata_list(&ictx->md_ctx, ictx->header_oid, start, max, pairs);
  }

  struct C_RBD_Readahead : public Context {
    ImageCtx *ictx;
    object_t oid;
    uint64_t offset;
    uint64_t length;

    bufferlist read_data;
    io::ExtentMap extent_map;

    C_RBD_Readahead(ImageCtx *ictx, object_t oid, uint64_t offset, uint64_t length)
      : ictx(ictx), oid(oid), offset(offset), length(length) {
      ictx->readahead.inc_pending();
    }

    void finish(int r) override {
      ldout(ictx->cct, 20) << "C_RBD_Readahead on " << oid << ": "
                           << offset << "~" << length << dendl;
      ictx->readahead.dec_pending();
    }
  };

  void readahead(ImageCtx *ictx,
                 const vector<pair<uint64_t,uint64_t> >& image_extents)
  {
    uint64_t total_bytes = 0;
    for (vector<pair<uint64_t,uint64_t> >::const_iterator p = image_extents.begin();
	 p != image_extents.end();
	 ++p) {
      total_bytes += p->second;
    }

    ictx->md_lock.get_write();
    bool abort = ictx->readahead_disable_after_bytes != 0 &&
      ictx->total_bytes_read > ictx->readahead_disable_after_bytes;
    if (abort) {
      ictx->md_lock.put_write();
      return;
    }
    ictx->total_bytes_read += total_bytes;
    ictx->snap_lock.get_read();
    uint64_t image_size = ictx->get_image_size(ictx->snap_id);
    auto snap_id = ictx->snap_id;
    ictx->snap_lock.put_read();
    ictx->md_lock.put_write();

    pair<uint64_t, uint64_t> readahead_extent = ictx->readahead.update(image_extents, image_size);
    uint64_t readahead_offset = readahead_extent.first;
    uint64_t readahead_length = readahead_extent.second;

    if (readahead_length > 0) {
      ldout(ictx->cct, 20) << "(readahead logical) " << readahead_offset << "~" << readahead_length << dendl;
      map<object_t,vector<ObjectExtent> > readahead_object_extents;
      Striper::file_to_extents(ictx->cct, ictx->format_string, &ictx->layout,
			       readahead_offset, readahead_length, 0, readahead_object_extents);
      for (map<object_t,vector<ObjectExtent> >::iterator p = readahead_object_extents.begin(); p != readahead_object_extents.end(); ++p) {
	for (vector<ObjectExtent>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
	  ldout(ictx->cct, 20) << "(readahead) oid " << q->oid << " " << q->offset << "~" << q->length << dendl;

	  auto req_comp = new C_RBD_Readahead(ictx, q->oid, q->offset,
                                              q->length);
          auto req = io::ObjectDispatchSpec::create_read(
            ictx, io::OBJECT_DISPATCH_LAYER_NONE, q->oid.name, q->objectno,
            q->offset, q->length, snap_id, 0, {}, &req_comp->read_data,
            &req_comp->extent_map, req_comp);
          req->send();
	}
      }
      ictx->perfcounter->inc(l_librbd_readahead);
      ictx->perfcounter->inc(l_librbd_readahead_bytes, readahead_length);
    }
  }

  int list_watchers(ImageCtx *ictx,
		    std::list<librbd::image_watcher_t> &watchers)
  {
    int r;
    std::string header_oid;
    std::list<obj_watch_t> obj_watchers;

    if (ictx->old_format) {
      header_oid = util::old_header_name(ictx->name);
    } else {
      header_oid = util::header_name(ictx->id);
    }

    r = ictx->md_ctx.list_watchers(header_oid, &obj_watchers);
    if (r < 0) {
      return r;
    }

    for (auto i = obj_watchers.begin(); i != obj_watchers.end(); ++i) {
      librbd::image_watcher_t watcher;
      watcher.addr = i->addr;
      watcher.id = i->watcher_id;
      watcher.cookie = i->cookie;

      watchers.push_back(watcher);
    }

    return 0;
  }

}

std::ostream &operator<<(std::ostream &os, const librbd::ImageOptions &opts) {
  os << "[";

  const char *delimiter = "";
  for (auto &i : librbd::IMAGE_OPTIONS_TYPE_MAPPING) {
    if (i.second == librbd::STR) {
      std::string val;
      if (opts.get(i.first, &val) == 0) {
        os << delimiter << librbd::image_option_name(i.first) << "=" << val;
        delimiter = ", ";
      }
    } else if (i.second == librbd::UINT64) {
      uint64_t val;
      if (opts.get(i.first, &val) == 0) {
        os << delimiter << librbd::image_option_name(i.first) << "=" << val;
        delimiter = ", ";
      }
    }
  }

  os << "]";

  return os;
}
