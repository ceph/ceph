// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_INTERNAL_H
#define CEPH_LIBRBD_INTERNAL_H

#include "include/int_types.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "include/buffer.h"
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

  l_librbd_aio_rd,               // read ops
  l_librbd_aio_rd_bytes,         // bytes read
  l_librbd_aio_rd_latency,
  l_librbd_aio_wr,
  l_librbd_aio_wr_bytes,
  l_librbd_aio_wr_latency,
  l_librbd_aio_discard,
  l_librbd_aio_discard_bytes,
  l_librbd_aio_discard_latency,
  l_librbd_aio_flush,
  l_librbd_aio_flush_latency,

  l_librbd_snap_create,
  l_librbd_snap_remove,
  l_librbd_snap_rollback,

  l_librbd_notify,
  l_librbd_resize,

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

  const std::string id_obj_name(const std::string &name);
  const std::string header_name(const std::string &image_id);
  const std::string old_header_name(const std::string &image_name);

  int detect_format(librados::IoCtx &io_ctx, const std::string &name,
		    bool *old_format, uint64_t *size);

  bool has_parent(int64_t parent_pool_id, uint64_t off, uint64_t overlap);

  int snap_set(ImageCtx *ictx, const char *snap_name);
  int list(librados::IoCtx& io_ctx, std::vector<std::string>& names);
  int list_children(ImageCtx *ictx,
		    std::set<pair<std::string, std::string> > & names);
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     int *order);
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order,
	     uint64_t stripe_unit, uint64_t stripe_count);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snap_name,
	    IoCtx& c_ioctx, const char *c_name,
	    uint64_t features, int *c_order,
	    uint64_t stripe_unit, int stripe_count);
  int rename(librados::IoCtx& io_ctx, const char *srcname, const char *dstname);
  int info(ImageCtx *ictx, image_info_t& info, size_t image_size);
  int get_old_format(ImageCtx *ictx, uint8_t *old);
  int get_size(ImageCtx *ictx, uint64_t *size);
  int get_features(ImageCtx *ictx, uint64_t *features);
  int get_overlap(ImageCtx *ictx, uint64_t *overlap);
  int get_parent_info(ImageCtx *ictx, string *parent_pool_name,
		      string *parent_name, string *parent_snap_name);

  int remove(librados::IoCtx& io_ctx, const char *imgname,
	     ProgressContext& prog_ctx);
  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int snap_create(ImageCtx *ictx, const char *snap_name);
  int snap_list(ImageCtx *ictx, std::vector<snap_info_t>& snaps);
  bool snap_exists(ImageCtx *ictx, const char *snap_name);
  int snap_rollback(ImageCtx *ictx, const char *snap_name,
		    ProgressContext& prog_ctx);
  int snap_remove(ImageCtx *ictx, const char *snap_name);
  int snap_protect(ImageCtx *ictx, const char *snap_name);
  int snap_unprotect(ImageCtx *ictx, const char *snap_name);
  int snap_is_protected(ImageCtx *ictx, const char *snap_name,
			bool *is_protected);
  int add_snap(ImageCtx *ictx, const char *snap_name);
  int rm_snap(ImageCtx *ictx, const char *snap_name);
  int refresh_parent(ImageCtx *ictx);
  int ictx_check(ImageCtx *ictx);
  int ictx_refresh(ImageCtx *ictx);
  int copy(ImageCtx *ictx, IoCtx& dest_md_ctx, const char *destname,
	   ProgressContext &prog_ctx);
  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx);

  int open_parent(ImageCtx *ictx);
  int open_image(ImageCtx *ictx);
  void close_image(ImageCtx *ictx);

  int copyup_block(ImageCtx *ictx, uint64_t offset, size_t len,
		   const char *buf);
  int flatten(ImageCtx *ictx, ProgressContext &prog_ctx);

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
  int read_rbd_info(librados::IoCtx& io_ctx, const std::string& info_oid,
		    struct rbd_info *info);

  int read_header_bl(librados::IoCtx& io_ctx, const std::string& md_oid,
		     ceph::bufferlist& header, uint64_t *ver);
  int notify_change(librados::IoCtx& io_ctx, const std::string& oid,
		    uint64_t *pver, ImageCtx *ictx);
  int read_header(librados::IoCtx& io_ctx, const std::string& md_oid,
		  struct rbd_obj_header_ondisk *header, uint64_t *ver);
  int write_header(librados::IoCtx& io_ctx, const std::string& md_oid,
		   ceph::bufferlist& header);
  int tmap_set(librados::IoCtx& io_ctx, const std::string& imgname);
  int tmap_rm(librados::IoCtx& io_ctx, const std::string& imgname);
  int rollback_image(ImageCtx *ictx, uint64_t snap_id,
		     ProgressContext& prog_ctx);
  void image_info(const ImageCtx *ictx, image_info_t& info, size_t info_size);
  std::string get_block_oid(const std::string &object_prefix, uint64_t num,
			    bool old_format);
  uint64_t oid_to_object_no(const string& oid, const string& object_prefix);
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len);
  int init_rbd_info(struct rbd_info *info);
  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
		       uint64_t size, int order, uint64_t bid);

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg);
  int diff_iterate(ImageCtx *ictx, const char *fromsnapname,
		   uint64_t off, uint64_t len,
		   int (*cb)(uint64_t, size_t, int, void *),
		   void *arg);
  ssize_t read(ImageCtx *ictx, uint64_t off, size_t len, char *buf);
  ssize_t read(ImageCtx *ictx, const vector<pair<uint64_t,uint64_t> >& image_extents,
	       char *buf, bufferlist *pbl);
  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf);
  int discard(ImageCtx *ictx, uint64_t off, uint64_t len);

  void aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
		 AioCompletion *c);
  void aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c);
  void aio_read(ImageCtx *ictx, uint64_t off, size_t len,
	        char *buf, bufferlist *pbl, AioCompletion *c);
  void aio_read(ImageCtx *ictx, const vector<pair<uint64_t,uint64_t> >& image_extents,
	        char *buf, bufferlist *pbl, AioCompletion *c);
  void aio_flush(ImageCtx *ictx, AioCompletion *c);
  int flush(ImageCtx *ictx);
  int _flush(ImageCtx *ictx);
  int invalidate_cache(ImageCtx *ictx);

  ssize_t handle_sparse_read(CephContext *cct,
			     ceph::bufferlist data_bl,
			     uint64_t block_ofs,
			     const std::map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,
			     size_t buf_len,
			     char *dest_buf);

  AioCompletion *aio_create_completion();
  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete);
  AioCompletion *aio_create_completion_internal(void *cb_arg,
						callback_t cb_complete);

  // raw callbacks
  int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg);
  void rados_req_cb(rados_completion_t cb, void *arg);
  void rados_ctx_cb(rados_completion_t cb, void *arg);
  void rbd_req_cb(completion_t cb, void *arg);
  void rbd_ctx_cb(completion_t cb, void *arg);
}

#endif
