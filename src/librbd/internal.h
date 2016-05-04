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

enum {
  l_librbd_first = 26000,

  l_librbd_rd,               // read ops
  l_librbd_rd_bytes,         // bytes read
  l_librbd_rd_latency,       // average latency
  l_librbd_wr,
  l_librbd_wr_bytes,
  l_librbd_wr_latency,
  l_librbd_discard,
  l_librbd_discard_bytes,
  l_librbd_discard_latency,
  l_librbd_flush,

  l_librbd_aio_flush,
  l_librbd_aio_flush_latency,

  l_librbd_snap_create,
  l_librbd_snap_remove,
  l_librbd_snap_rollback,
  l_librbd_snap_rename,

  l_librbd_notify,
  l_librbd_resize,

  l_librbd_readahead,
  l_librbd_readahead_bytes,

  l_librbd_invalidate_cache,

  l_librbd_last,
};

namespace librbd {

  struct AioCompletion;
  struct ImageCtx;

  class NoOpProgressContext : public ProgressContext
  {
  public:
    NoOpProgressContext()
    {
    }
    int update_progress(uint64_t offset, uint64_t src_size)
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
  void image_options_destroy(rbd_image_options_t opts);
  int image_options_set(rbd_image_options_t opts, int optname,
			const std::string& optval);
  int image_options_set(rbd_image_options_t opts, int optname, uint64_t optval);
  int image_options_get(rbd_image_options_t opts, int optname,
			std::string* optval);
  int image_options_get(rbd_image_options_t opts, int optname,
			uint64_t* optval);
  int image_options_unset(rbd_image_options_t opts, int optname);
  void image_options_clear(rbd_image_options_t opts);
  bool image_options_is_empty(rbd_image_options_t opts);

  int snap_set(ImageCtx *ictx, const char *snap_name);
  int list_cgs(librados::IoCtx& io_ctx, std::vector<std::string>& names);
  int list(librados::IoCtx& io_ctx, std::vector<std::string>& names);
  int list_children(ImageCtx *ictx,
		    std::set<std::pair<std::string, std::string> > & names);
  int cg_add_image(librados::IoCtx& cg_ioctx, const char *cg_name,
                   librados::IoCtx& image_ioctx, const char *image_name);
  int cg_remove_image(librados::IoCtx& cg_ioctx, const char *cg_name,
                      librados::IoCtx& image_ioctx, const char *image_name);
  int create_cg(librados::IoCtx& io_ctx, const char *imgname);
  int cg_list_images(librados::IoCtx& cg_ioctx, const char *cg_name,
                    std::vector<std::pair<std::string, int64_t>>& images);
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     int *order);
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order,
	     uint64_t stripe_unit, uint64_t stripe_count);
  int create(IoCtx& io_ctx, const char *imgname, uint64_t size,
	     ImageOptions& opts);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name,
	    uint64_t features, int *c_order,
	    uint64_t stripe_unit, int stripe_count);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name, ImageOptions& c_opts);
  int rename(librados::IoCtx& io_ctx, const char *srcname, const char *dstname);
  int info(ImageCtx *ictx, image_info_t& info, size_t image_size);
  int get_old_format(ImageCtx *ictx, uint8_t *old);
  int get_size(ImageCtx *ictx, uint64_t *size);
  int get_features(ImageCtx *ictx, uint64_t *features);
  int update_features(ImageCtx *ictx, uint64_t features, bool enabled);
  int get_overlap(ImageCtx *ictx, uint64_t *overlap);
  int get_parent_info(ImageCtx *ictx, std::string *parent_pool_name,
		      std::string *parent_name, std::string *parent_snap_name);
  int get_flags(ImageCtx *ictx, uint64_t *flags);
  int set_image_notification(ImageCtx *ictx, int fd, int type);
  int is_exclusive_lock_owner(ImageCtx *ictx, bool *is_owner);

  int remove(librados::IoCtx& io_ctx, const char *imgname,
	     ProgressContext& prog_ctx);
  int snap_list(ImageCtx *ictx, std::vector<snap_info_t>& snaps);
  int snap_exists(ImageCtx *ictx, const char *snap_name, bool *exists);
  int snap_is_protected(ImageCtx *ictx, const char *snap_name,
			bool *is_protected);
  int copy(ImageCtx *ictx, IoCtx& dest_md_ctx, const char *destname,
	   ImageOptions& opts, ProgressContext &prog_ctx);
  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx);

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
  int diff_iterate(ImageCtx *ictx, const char *fromsnapname, uint64_t off,
                   uint64_t len, bool include_parent, bool whole_object,
		   int (*cb)(uint64_t, size_t, int, void *),
		   void *arg);
  void readahead(ImageCtx *ictx,
                 const vector<pair<uint64_t,uint64_t> >& image_extents);

  int flush(ImageCtx *ictx);
  int invalidate_cache(ImageCtx *ictx);
  int poll_io_events(ImageCtx *ictx, AioCompletion **comps, int numcomp);
  int metadata_list(ImageCtx *ictx, const string &last, uint64_t max, map<string, bufferlist> *pairs);
  int metadata_get(ImageCtx *ictx, const std::string &key, std::string *value);
  int metadata_set(ImageCtx *ictx, const std::string &key, const std::string &value);
  int metadata_remove(ImageCtx *ictx, const std::string &key);

  int mirror_mode_get(IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode);
  int mirror_mode_set(IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode);
  int mirror_peer_add(IoCtx& io_ctx, std::string *uuid,
                      const std::string &cluster_name,
                      const std::string &client_name);
  int mirror_peer_remove(IoCtx& io_ctx, const std::string &uuid);
  int mirror_peer_list(IoCtx& io_ctx, std::vector<mirror_peer_t> *peers);
  int mirror_peer_set_client(IoCtx& io_ctx, const std::string &uuid,
                             const std::string &client_name);
  int mirror_peer_set_cluster(IoCtx& io_ctx, const std::string &uuid,
                              const std::string &cluster_name);

  int mirror_image_enable(ImageCtx *ictx);
  int mirror_image_disable(ImageCtx *ictx, bool force);
  int mirror_image_promote(ImageCtx *ictx, bool force);
  int mirror_image_demote(ImageCtx *ictx);
  int mirror_image_resync(ImageCtx *ictx);
  int mirror_image_get_info(ImageCtx *ictx, mirror_image_info_t *mirror_image_info,
                            size_t info_size);
}

#endif
