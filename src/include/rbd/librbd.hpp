// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef __LIBRBD_HPP
#define __LIBRBD_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <vector>
#include "../rados/buffer.h"
#include "../rados/librados.hpp"
#include "librbd.h"

namespace librbd {

  using librados::IoCtx;

  class Image;
  class ImageOptions;
  typedef void *image_ctx_t;
  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  typedef struct {
    uint64_t id;
    uint64_t size;
    std::string name;
  } snap_info_t;

  typedef struct {
    std::string client;
    std::string cookie;
    std::string address;
  } locker_t;

  typedef struct {
    std::string uuid;
    std::string cluster_name;
    std::string client_name;
  } mirror_peer_t;

  typedef rbd_image_info_t image_info_t;

  class CEPH_RBD_API ProgressContext
  {
  public:
    virtual ~ProgressContext();
    virtual int update_progress(uint64_t offset, uint64_t total) = 0;
  };

class CEPH_RBD_API RBD
{
public:
  RBD();
  ~RBD();

  // This must be dynamically allocated with new, and
  // must be released with release().
  // Do not use delete.
  struct AioCompletion {
    void *pc;
    AioCompletion(void *cb_arg, callback_t complete_cb);
    bool is_complete();
    int wait_for_complete();
    ssize_t get_return_value();
    void *get_arg();
    void release();
  };

  void version(int *major, int *minor, int *extra);

  int open(IoCtx& io_ctx, Image& image, const char *name);
  int open(IoCtx& io_ctx, Image& image, const char *name, const char *snapname);
  int aio_open(IoCtx& io_ctx, Image& image, const char *name,
	       const char *snapname, RBD::AioCompletion *c);
  // see librbd.h
  int open_read_only(IoCtx& io_ctx, Image& image, const char *name,
		     const char *snapname);
  int aio_open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			 const char *snapname, RBD::AioCompletion *c);
  int list(IoCtx& io_ctx, std::vector<std::string>& names);
  int create(IoCtx& io_ctx, const char *name, uint64_t size, int *order);
  int create2(IoCtx& io_ctx, const char *name, uint64_t size,
	      uint64_t features, int *order);
  int create3(IoCtx& io_ctx, const char *name, uint64_t size,
	      uint64_t features, int *order,
	      uint64_t stripe_unit, uint64_t stripe_count);
  int create4(IoCtx& io_ctx, const char *name, uint64_t size,
	      ImageOptions& opts);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snapname,
	       IoCtx& c_ioctx, const char *c_name, uint64_t features,
	       int *c_order);
  int clone2(IoCtx& p_ioctx, const char *p_name, const char *p_snapname,
	     IoCtx& c_ioctx, const char *c_name, uint64_t features,
	     int *c_order, uint64_t stripe_unit, int stripe_count);
  int clone3(IoCtx& p_ioctx, const char *p_name, const char *p_snapname,
	     IoCtx& c_ioctx, const char *c_name, ImageOptions& opts);
  int remove(IoCtx& io_ctx, const char *name);
  int remove_with_progress(IoCtx& io_ctx, const char *name, ProgressContext& pctx);
  int rename(IoCtx& src_io_ctx, const char *srcname, const char *destname);

  // RBD pool mirroring support functions
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

private:
  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);
};

class CEPH_RBD_API ImageOptions {
public:
  ImageOptions();
  ImageOptions(rbd_image_options_t opts);
  ~ImageOptions();

  int set(int optname, const std::string& optval);
  int set(int optname, uint64_t optval);
  int get(int optname, std::string* optval) const;
  int get(int optname, uint64_t* optval) const;
  int unset(int optname);
  void clear();
  bool empty() const;

private:
  friend class RBD;
  friend class Image;

  rbd_image_options_t opts;
};

class CEPH_RBD_API Image
{
public:
  Image();
  ~Image();

  int close();
  int aio_close(RBD::AioCompletion *c);

  int resize(uint64_t size);
  int resize_with_progress(uint64_t size, ProgressContext& pctx);
  int stat(image_info_t &info, size_t infosize);
  int parent_info(std::string *parent_poolname, std::string *parent_name,
		      std::string *parent_snapname);
  int old_format(uint8_t *old);
  int size(uint64_t *size);
  int features(uint64_t *features);
  int update_features(uint64_t features, bool enabled);
  int overlap(uint64_t *overlap);
  int get_flags(uint64_t *flags);
  int set_image_notification(int fd, int type);

  /* exclusive lock feature */
  int is_exclusive_lock_owner(bool *is_owner);

  /* object map feature */
  int rebuild_object_map(ProgressContext &prog_ctx);

  int copy(IoCtx& dest_io_ctx, const char *destname);
  int copy2(Image& dest);
  int copy3(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts);
  int copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
			 ProgressContext &prog_ctx);
  int copy_with_progress2(Image& dest, ProgressContext &prog_ctx);
  int copy_with_progress3(IoCtx& dest_io_ctx, const char *destname,
			  ImageOptions& opts, ProgressContext &prog_ctx);

  /* striping */
  uint64_t get_stripe_unit() const;
  uint64_t get_stripe_count() const;

  int flatten();
  int flatten_with_progress(ProgressContext &prog_ctx);
  /**
   * Returns a pair of poolname, imagename for each clone
   * of this image at the currently set snapshot.
   */
  int list_children(std::set<std::pair<std::string, std::string> > *children);

  /* advisory locking (see librbd.h for details) */
  int list_lockers(std::list<locker_t> *lockers,
		   bool *exclusive, std::string *tag);
  int lock_exclusive(const std::string& cookie);
  int lock_shared(const std::string& cookie, const std::string& tag);
  int unlock(const std::string& cookie);
  int break_lock(const std::string& client, const std::string& cookie);

  /* snapshots */
  int snap_list(std::vector<snap_info_t>& snaps);
  /* DEPRECATED; use snap_exists2 */
  bool snap_exists(const char *snapname) __attribute__ ((deprecated));
  int snap_exists2(const char *snapname, bool *exists);
  int snap_create(const char *snapname);
  int snap_remove(const char *snapname);
  int snap_rollback(const char *snap_name);
  int snap_rollback_with_progress(const char *snap_name, ProgressContext& pctx);
  int snap_protect(const char *snap_name);
  int snap_unprotect(const char *snap_name);
  int snap_is_protected(const char *snap_name, bool *is_protected);
  int snap_set(const char *snap_name);
  int snap_rename(const char *srcname, const char *dstname);

  /* I/O */
  ssize_t read(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  /* @param op_flags see librados.h constants beginning with LIBRADOS_OP_FLAG */
  ssize_t read2(uint64_t ofs, size_t len, ceph::bufferlist& bl, int op_flags);
  int64_t read_iterate(uint64_t ofs, size_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
  int read_iterate2(uint64_t ofs, uint64_t len,
		    int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
  /**
   * get difference between two versions of an image
   *
   * This will return the differences between two versions of an image
   * via a callback, which gets the offset and length and a flag
   * indicating whether the extent exists (1), or is known/defined to
   * be zeros (a hole, 0).  If the source snapshot name is NULL, we
   * interpret that as the beginning of time and return all allocated
   * regions of the image.  The end version is whatever is currently
   * selected for the image handle (either a snapshot or the writeable
   * head).
   *
   * @param fromsnapname start snapshot name, or NULL
   * @param ofs start offset
   * @param len len in bytes of region to report on
   * @param include_parent true if full history diff should include parent
   * @param whole_object 1 if diff extents should cover whole object
   * @param cb callback to call for each allocated region
   * @param arg argument to pass to the callback
   * @returns 0 on success, or negative error code on error
   */
  int diff_iterate(const char *fromsnapname,
		   uint64_t ofs, uint64_t len,
		   int (*cb)(uint64_t, size_t, int, void *), void *arg);
  int diff_iterate2(const char *fromsnapname,
		    uint64_t ofs, uint64_t len,
                    bool include_parent, bool whole_object,
		    int (*cb)(uint64_t, size_t, int, void *), void *arg);

  ssize_t write(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  /* @param op_flags see librados.h constants beginning with LIBRADOS_OP_FLAG */
  ssize_t write2(uint64_t ofs, size_t len, ceph::bufferlist& bl, int op_flags);
  int discard(uint64_t ofs, uint64_t len);

  int aio_write(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  /* @param op_flags see librados.h constants beginning with LIBRADOS_OP_FLAG */
  int aio_write2(uint64_t off, size_t len, ceph::bufferlist& bl,
		  RBD::AioCompletion *c, int op_flags);
  /**
   * read async from image
   *
   * The target bufferlist is populated with references to buffers
   * that contain the data for the given extent of the image.
   *
   * NOTE: If caching is enabled, the bufferlist will directly
   * reference buffers in the cache to avoid an unnecessary data copy.
   * As a result, if the user intends to modify the buffer contents
   * directly, they should make a copy first (unconditionally, or when
   * the reference count on ther underlying buffer is more than 1).
   *
   * @param off offset in image
   * @param len length of read
   * @param bl bufferlist to read into
   * @param c aio completion to notify when read is complete
   */
  int aio_read(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  /* @param op_flags see librados.h constants beginning with LIBRADOS_OP_FLAG */
  int aio_read2(uint64_t off, size_t len, ceph::bufferlist& bl,
		  RBD::AioCompletion *c, int op_flags);
  int aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c);

  int flush();
  /**
   * Start a flush if caching is enabled. Get a callback when
   * the currently pending writes are on disk.
   *
   * @param image the image to flush writes to
   * @param c what to call when flushing is complete
   * @returns 0 on success, negative error code on failure
   */
  int aio_flush(RBD::AioCompletion *c);

  /**
   * Drop any cached data for this image
   *
   * @returns 0 on success, negative error code on failure
   */
  int invalidate_cache();

  int poll_io_events(RBD::AioCompletion **comps, int numcomp);

  int metadata_get(const std::string &key, std::string *value);
  int metadata_set(const std::string &key, const std::string &value);
  int metadata_remove(const std::string &key);
  /**
   * Returns a pair of key/value for this image
   */
  int metadata_list(const std::string &start, uint64_t max, std::map<std::string, ceph::bufferlist> *pairs);

  int mirror_image_enable();

private:
  friend class RBD;

  Image(const Image& rhs);
  const Image& operator=(const Image& rhs);

  image_ctx_t ctx;
};

}

#endif
