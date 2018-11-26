// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_INTERNAL_H
#define CEPH_LIBRBD_INTERNAL_H

#include "include/int_types.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "include/buffer_fwd.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/WorkQueue.h"
#include "common/ceph_time.h"
#include "librbd/Types.h"

namespace librbd {

  struct ImageCtx;
  namespace io { struct AioCompletion; }

  class NoOpProgressContext : public ProgressContext
  {
  public:
    NoOpProgressContext()
    {
    }
    int update_progress(uint64_t offset, uint64_t src_size) override
    {
      return 0;
    }
  };

  int detect_format(librados::IoCtx &io_ctx, const std::string &name,
		    bool *old_format, uint64_t *size);

  bool has_parent(int64_t parent_pool_id, uint64_t off, uint64_t overlap);

  std::string image_option_name(int optname);
  void image_options_create(rbd_image_options_t* opts);
  void image_options_create_ref(rbd_image_options_t* opts,
				rbd_image_options_t orig);
  void image_options_copy(rbd_image_options_t *opts,
			  const ImageOptions &orig);
  void image_options_destroy(rbd_image_options_t opts);
  int image_options_set(rbd_image_options_t opts, int optname,
			const std::string& optval);
  int image_options_set(rbd_image_options_t opts, int optname, uint64_t optval);
  int image_options_get(rbd_image_options_t opts, int optname,
			std::string* optval);
  int image_options_get(rbd_image_options_t opts, int optname,
			uint64_t* optval);
  int image_options_is_set(rbd_image_options_t opts, int optname,
                           bool* is_set);
  int image_options_unset(rbd_image_options_t opts, int optname);
  void image_options_clear(rbd_image_options_t opts);
  bool image_options_is_empty(rbd_image_options_t opts);

  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     int *order);
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order,
	     uint64_t stripe_unit, uint64_t stripe_count);
  int create(IoCtx& io_ctx, const std::string &image_name,
	     const std::string &image_id, uint64_t size, ImageOptions& opts,
             const std::string &non_primary_global_image_id,
             const std::string &primary_mirror_uuid,
             bool skip_mirror_enable);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name,
	    uint64_t features, int *c_order,
	    uint64_t stripe_unit, int stripe_count);
  int clone(IoCtx& p_ioctx, const char *p_id, const char *p_name,
            const char *p_snap_name, IoCtx& c_ioctx, const char *c_id,
            const char *c_name, ImageOptions& c_opts,
            const std::string &non_primary_global_image_id,
            const std::string &primary_mirror_uuid);
  int rename(librados::IoCtx& io_ctx, const char *srcname, const char *dstname);
  int info(ImageCtx *ictx, image_info_t& info, size_t image_size);
  int get_old_format(ImageCtx *ictx, uint8_t *old);
  int get_size(ImageCtx *ictx, uint64_t *size);
  int get_features(ImageCtx *ictx, uint64_t *features);
  int get_overlap(ImageCtx *ictx, uint64_t *overlap);
  int get_flags(ImageCtx *ictx, uint64_t *flags);
  int set_image_notification(ImageCtx *ictx, int fd, int type);
  int is_exclusive_lock_owner(ImageCtx *ictx, bool *is_owner);
  int lock_acquire(ImageCtx *ictx, rbd_lock_mode_t lock_mode);
  int lock_release(ImageCtx *ictx);
  int lock_get_owners(ImageCtx *ictx, rbd_lock_mode_t *lock_mode,
                      std::list<std::string> *lock_owners);
  int lock_break(ImageCtx *ictx, rbd_lock_mode_t lock_mode,
                 const std::string &lock_owner);

  int snap_list(ImageCtx *ictx, std::vector<snap_info_t>& snaps);
  int snap_exists(ImageCtx *ictx, const cls::rbd::SnapshotNamespace& snap_namespace,
		  const char *snap_name, bool *exists);
  int snap_get_limit(ImageCtx *ictx, uint64_t *limit);
  int snap_set_limit(ImageCtx *ictx, uint64_t limit);
  int snap_get_timestamp(ImageCtx *ictx, uint64_t snap_id, struct timespec *timestamp);
  int snap_remove(ImageCtx *ictx, const char *snap_name, uint32_t flags, ProgressContext& pctx);
  int snap_is_protected(ImageCtx *ictx, const char *snap_name,
			bool *is_protected);
  int copy(ImageCtx *ictx, IoCtx& dest_md_ctx, const char *destname,
	   ImageOptions& opts, ProgressContext &prog_ctx, size_t sparse_size);
  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx, size_t sparse_size);

  /* cooperative locking */
  int list_lockers(ImageCtx *ictx,
		   std::list<locker_t> *locks,
		   bool *exclusive,
		   std::string *tag);
  int lock(ImageCtx *ictx, bool exclusive, const std::string& cookie,
	   const std::string& tag);
  int lock_shared(ImageCtx *ictx, const std::string& cookie,
		  const std::string& tag);
  int unlock(ImageCtx *ictx, const std::string& cookie);
  int break_lock(ImageCtx *ictx, const std::string& client,
		 const std::string& cookie);

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx);

  int read_header_bl(librados::IoCtx& io_ctx, const std::string& md_oid,
		     ceph::bufferlist& header, uint64_t *ver);
  int read_header(librados::IoCtx& io_ctx, const std::string& md_oid,
		  struct rbd_obj_header_ondisk *header, uint64_t *ver);
  int tmap_set(librados::IoCtx& io_ctx, const std::string& imgname);
  int tmap_rm(librados::IoCtx& io_ctx, const std::string& imgname);
  void image_info(const ImageCtx *ictx, image_info_t& info, size_t info_size);
  uint64_t oid_to_object_no(const std::string& oid,
			    const std::string& object_prefix);
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len);
  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
		       uint64_t size, int order, uint64_t bid);

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg);
  void readahead(ImageCtx *ictx,
                 const vector<pair<uint64_t,uint64_t> >& image_extents);

  int invalidate_cache(ImageCtx *ictx);
  int poll_io_events(ImageCtx *ictx, io::AioCompletion **comps, int numcomp);
  int metadata_list(ImageCtx *ictx, const string &last, uint64_t max, map<string, bufferlist> *pairs);
  int metadata_get(ImageCtx *ictx, const std::string &key, std::string *value);

  int list_watchers(ImageCtx *ictx, std::list<librbd::image_watcher_t> &watchers);
}

std::ostream &operator<<(std::ostream &os, const librbd::ImageOptions &opts);

#endif
