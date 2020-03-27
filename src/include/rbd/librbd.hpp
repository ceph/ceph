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

#include <string>
#include <list>
#include <map>
#include <vector>
#include "../rados/buffer.h"
#include "../rados/librados.hpp"
#include "librbd.h"

#if __GNUC__ >= 4
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

namespace librbd {

  using librados::IoCtx;

  class Image;
  class ImageOptions;
  class PoolStats;
  typedef void *image_ctx_t;
  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  typedef struct {
    std::string id;
    std::string name;
  } image_spec_t;

  typedef struct {
    int64_t pool_id;
    std::string pool_name;
    std::string pool_namespace;
    std::string image_id;
    std::string image_name;
    bool trash;
  } linked_image_spec_t;

  typedef rbd_snap_namespace_type_t snap_namespace_type_t;

  typedef struct {
    uint64_t id;
    snap_namespace_type_t namespace_type;
    std::string name;
  } snap_spec_t;

  typedef struct {
    uint64_t id;
    uint64_t size;
    std::string name;
  } snap_info_t;

  typedef struct {
    int64_t group_pool;
    std::string group_name;
    std::string group_snap_name;
  } snap_group_namespace_t;

  typedef rbd_snap_mirror_state_t snap_mirror_state_t;

  typedef struct {
    snap_mirror_state_t state;
    std::set<std::string> mirror_peer_uuids;
    bool complete;
    std::string primary_mirror_uuid;
    uint64_t primary_snap_id;
    uint64_t last_copied_object_number;
  } snap_mirror_namespace_t;

  typedef struct {
    std::string client;
    std::string cookie;
    std::string address;
  } locker_t;

  typedef rbd_mirror_peer_direction_t mirror_peer_direction_t;

  typedef struct {
    std::string uuid;
    std::string cluster_name;
    std::string client_name;
  } mirror_peer_t CEPH_RBD_DEPRECATED;

  typedef struct {
    std::string uuid;
    mirror_peer_direction_t direction;
    std::string site_name;
    std::string mirror_uuid;
    std::string client_name;
    time_t last_seen;
  } mirror_peer_site_t;

  typedef rbd_mirror_image_mode_t mirror_image_mode_t;
  typedef rbd_mirror_image_state_t mirror_image_state_t;

  typedef struct {
    std::string global_id;
    mirror_image_state_t state;
    bool primary;
  } mirror_image_info_t;

  typedef rbd_mirror_image_status_state_t mirror_image_status_state_t;

  typedef struct {
    std::string name;
    mirror_image_info_t info;
    mirror_image_status_state_t state;
    std::string description;
    time_t last_update;
    bool up;
  } mirror_image_status_t CEPH_RBD_DEPRECATED;

  typedef struct {
    std::string mirror_uuid;
    mirror_image_status_state_t state;
    std::string description;
    time_t last_update;
    bool up;
  } mirror_image_site_status_t;

  typedef struct {
    std::string name;
    mirror_image_info_t info;
    std::vector<mirror_image_site_status_t> site_statuses;
  } mirror_image_global_status_t;

  typedef rbd_group_image_state_t group_image_state_t;

  typedef struct {
    std::string name;
    int64_t pool;
    group_image_state_t state;
  } group_image_info_t;

  typedef struct {
    std::string name;
    int64_t pool;
  } group_info_t;

  typedef rbd_group_snap_state_t group_snap_state_t;

  typedef struct {
    std::string name;
    group_snap_state_t state;
  } group_snap_info_t;

  typedef rbd_image_info_t image_info_t;

  class CEPH_RBD_API ProgressContext
  {
  public:
    virtual ~ProgressContext();
    virtual int update_progress(uint64_t offset, uint64_t total) = 0;
  };

  typedef struct {
    std::string id;
    std::string name;
    rbd_trash_image_source_t source;
    time_t deletion_time;
    time_t deferment_end_time;
  } trash_image_info_t;

  typedef struct {
    std::string pool_name;
    std::string image_name;
    std::string image_id;
    bool trash;
  } child_info_t;

  typedef struct {
    std::string addr;
    int64_t id;
    uint64_t cookie;
  } image_watcher_t;

  typedef rbd_image_migration_state_t image_migration_state_t;

  typedef struct {
    int64_t source_pool_id;
    std::string source_pool_namespace;
    std::string source_image_name;
    std::string source_image_id;
    int64_t dest_pool_id;
    std::string dest_pool_namespace;
    std::string dest_image_name;
    std::string dest_image_id;
    image_migration_state_t state;
    std::string state_description;
  } image_migration_status_t;

  typedef rbd_config_source_t config_source_t;

  typedef struct {
    std::string name;
    std::string value;
    config_source_t source;
  } config_option_t;

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
  int open_by_id(IoCtx& io_ctx, Image& image, const char *id);
  int open_by_id(IoCtx& io_ctx, Image& image, const char *id, const char *snapname);
  int aio_open(IoCtx& io_ctx, Image& image, const char *name,
	       const char *snapname, RBD::AioCompletion *c);
  int aio_open_by_id(IoCtx& io_ctx, Image& image, const char *id,
	             const char *snapname, RBD::AioCompletion *c);
  // see librbd.h
  int open_read_only(IoCtx& io_ctx, Image& image, const char *name,
		     const char *snapname);
  int open_by_id_read_only(IoCtx& io_ctx, Image& image, const char *id,
                           const char *snapname);
  int aio_open_read_only(IoCtx& io_ctx, Image& image, const char *name,
			 const char *snapname, RBD::AioCompletion *c);
  int aio_open_by_id_read_only(IoCtx& io_ctx, Image& image, const char *id,
                               const char *snapname, RBD::AioCompletion *c);
  int features_to_string(uint64_t features, std::string *str_features);
  int features_from_string(const std::string str_features, uint64_t *features);

  int list(IoCtx& io_ctx, std::vector<std::string>& names)
    CEPH_RBD_DEPRECATED;
  int list2(IoCtx& io_ctx, std::vector<image_spec_t>* images);

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

  int trash_move(IoCtx &io_ctx, const char *name, uint64_t delay);
  int trash_get(IoCtx &io_ctx, const char *id, trash_image_info_t *info);
  int trash_list(IoCtx &io_ctx, std::vector<trash_image_info_t> &entries);
  int trash_purge(IoCtx &io_ctx, time_t expire_ts, float threshold);
  int trash_purge_with_progress(IoCtx &io_ctx, time_t expire_ts, float threshold,
                                ProgressContext &pctx);
  int trash_remove(IoCtx &io_ctx, const char *image_id, bool force);
  int trash_remove_with_progress(IoCtx &io_ctx, const char *image_id,
                                 bool force, ProgressContext &pctx);
  int trash_restore(IoCtx &io_ctx, const char *id, const char *name);

  // Migration
  int migration_prepare(IoCtx& io_ctx, const char *image_name,
                        IoCtx& dest_io_ctx, const char *dest_image_name,
                        ImageOptions& opts);
  int migration_execute(IoCtx& io_ctx, const char *image_name);
  int migration_execute_with_progress(IoCtx& io_ctx, const char *image_name,
                                      ProgressContext &prog_ctx);
  int migration_abort(IoCtx& io_ctx, const char *image_name);
  int migration_abort_with_progress(IoCtx& io_ctx, const char *image_name,
                                    ProgressContext &prog_ctx);
  int migration_commit(IoCtx& io_ctx, const char *image_name);
  int migration_commit_with_progress(IoCtx& io_ctx, const char *image_name,
                                     ProgressContext &prog_ctx);
  int migration_status(IoCtx& io_ctx, const char *image_name,
                       image_migration_status_t *status, size_t status_size);

  // RBD pool mirroring support functions
  int mirror_site_name_get(librados::Rados& rados, std::string* site_name);
  int mirror_site_name_set(librados::Rados& rados,
                           const std::string& site_name);

  int mirror_mode_get(IoCtx& io_ctx, rbd_mirror_mode_t *mirror_mode);
  int mirror_mode_set(IoCtx& io_ctx, rbd_mirror_mode_t mirror_mode);

  int mirror_uuid_get(IoCtx& io_ctx, std::string* mirror_uuid);

  int mirror_peer_bootstrap_create(IoCtx& io_ctx, std::string* token);
  int mirror_peer_bootstrap_import(IoCtx& io_ctx,
                                   mirror_peer_direction_t direction,
                                   const std::string &token);

  int mirror_peer_site_add(IoCtx& io_ctx, std::string *uuid,
                           mirror_peer_direction_t direction,
                           const std::string &site_name,
                           const std::string &client_name);
  int mirror_peer_site_set_name(IoCtx& io_ctx, const std::string& uuid,
                                const std::string &site_name);
  int mirror_peer_site_set_client_name(IoCtx& io_ctx, const std::string& uuid,
                                       const std::string &client_name);
  int mirror_peer_site_set_direction(IoCtx& io_ctx, const std::string& uuid,
                                     mirror_peer_direction_t direction);
  int mirror_peer_site_remove(IoCtx& io_ctx, const std::string& uuid);
  int mirror_peer_site_list(IoCtx& io_ctx,
                            std::vector<mirror_peer_site_t> *peers);
  int mirror_peer_site_get_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      std::map<std::string, std::string> *key_vals);
  int mirror_peer_site_set_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      const std::map<std::string, std::string>& key_vals);

  int mirror_image_global_status_list(
      IoCtx& io_ctx, const std::string &start_id, size_t max,
      std::map<std::string, mirror_image_global_status_t> *images);
  int mirror_image_status_summary(IoCtx& io_ctx,
      std::map<mirror_image_status_state_t, int> *states);
  int mirror_image_instance_id_list(IoCtx& io_ctx, const std::string &start_id,
      size_t max, std::map<std::string, std::string> *sevice_ids);
  int mirror_image_info_list(IoCtx& io_ctx, mirror_image_mode_t *mode_filter,
      const std::string &start_id, size_t max,
      std::map<std::string, std::pair<mirror_image_mode_t,
                                      mirror_image_info_t>> *entries);

  /// mirror_peer_ commands are deprecated to mirror_peer_site_ equivalents
  int mirror_peer_add(IoCtx& io_ctx, std::string *uuid,
                      const std::string &cluster_name,
                      const std::string &client_name)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_remove(IoCtx& io_ctx, const std::string &uuid)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_list(IoCtx& io_ctx, std::vector<mirror_peer_t> *peers)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_set_client(IoCtx& io_ctx, const std::string &uuid,
                             const std::string &client_name)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_set_cluster(IoCtx& io_ctx, const std::string &uuid,
                              const std::string &cluster_name)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_get_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      std::map<std::string, std::string> *key_vals)
    CEPH_RBD_DEPRECATED;
  int mirror_peer_set_attributes(
      IoCtx& io_ctx, const std::string &uuid,
      const std::map<std::string, std::string>& key_vals)
    CEPH_RBD_DEPRECATED;

  /// mirror_image_status_list command is deprecated to
  /// mirror_image_global_status_list

  int mirror_image_status_list(
      IoCtx& io_ctx, const std::string &start_id, size_t max,
      std::map<std::string, mirror_image_status_t> *images)
    CEPH_RBD_DEPRECATED;

  // RBD groups support functions
  int group_create(IoCtx& io_ctx, const char *group_name);
  int group_remove(IoCtx& io_ctx, const char *group_name);
  int group_list(IoCtx& io_ctx, std::vector<std::string> *names);
  int group_rename(IoCtx& io_ctx, const char *src_group_name,
                   const char *dest_group_name);

  int group_image_add(IoCtx& io_ctx, const char *group_name,
		      IoCtx& image_io_ctx, const char *image_name);
  int group_image_remove(IoCtx& io_ctx, const char *group_name,
			 IoCtx& image_io_ctx, const char *image_name);
  int group_image_remove_by_id(IoCtx& io_ctx, const char *group_name,
                               IoCtx& image_io_ctx, const char *image_id);
  int group_image_list(IoCtx& io_ctx, const char *group_name,
                       std::vector<group_image_info_t> *images,
                       size_t group_image_info_size);

  int group_snap_create(IoCtx& io_ctx, const char *group_name,
			const char *snap_name);
  int group_snap_remove(IoCtx& io_ctx, const char *group_name,
			const char *snap_name);
  int group_snap_rename(IoCtx& group_ioctx, const char *group_name,
                        const char *old_snap_name, const char *new_snap_name);
  int group_snap_list(IoCtx& group_ioctx, const char *group_name,
                      std::vector<group_snap_info_t> *snaps,
                      size_t group_snap_info_size);
  int group_snap_rollback(IoCtx& io_ctx, const char *group_name,
                          const char *snap_name);
  int group_snap_rollback_with_progress(IoCtx& io_ctx, const char *group_name,
                                        const char *snap_name,
                                        ProgressContext& pctx);

  int namespace_create(IoCtx& ioctx, const char *namespace_name);
  int namespace_remove(IoCtx& ioctx, const char *namespace_name);
  int namespace_list(IoCtx& io_ctx, std::vector<std::string>* namespace_names);
  int namespace_exists(IoCtx& io_ctx, const char *namespace_name, bool *exists);

  int pool_init(IoCtx& io_ctx, bool force);
  int pool_stats_get(IoCtx& io_ctx, PoolStats *pool_stats);

  int pool_metadata_get(IoCtx &io_ctx, const std::string &key,
                        std::string *value);
  int pool_metadata_set(IoCtx &io_ctx, const std::string &key,
                        const std::string &value);
  int pool_metadata_remove(IoCtx &io_ctx, const std::string &key);
  int pool_metadata_list(IoCtx &io_ctx, const std::string &start, uint64_t max,
                         std::map<std::string, ceph::bufferlist> *pairs);

  int config_list(IoCtx& io_ctx, std::vector<config_option_t> *options);

private:
  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);
};

class CEPH_RBD_API ImageOptions {
public:
  ImageOptions();
  ImageOptions(rbd_image_options_t opts);
  ImageOptions(const ImageOptions &imgopts);
  ~ImageOptions();

  int set(int optname, const std::string& optval);
  int set(int optname, uint64_t optval);
  int get(int optname, std::string* optval) const;
  int get(int optname, uint64_t* optval) const;
  int is_set(int optname, bool* is_set);
  int unset(int optname);
  void clear();
  bool empty() const;

private:
  friend class RBD;
  friend class Image;

  rbd_image_options_t opts;
};

class CEPH_RBD_API PoolStats {
public:
  PoolStats();
  ~PoolStats();

  PoolStats(const PoolStats&) = delete;
  PoolStats& operator=(const PoolStats&) = delete;

  int add(rbd_pool_stat_option_t option, uint64_t* opt_val);

private:
  friend class RBD;

  rbd_pool_stats_t pool_stats;
};

class CEPH_RBD_API UpdateWatchCtx {
public:
  virtual ~UpdateWatchCtx() {}
  /**
   * Callback activated when we receive a notify event.
   */
  virtual void handle_notify() = 0;
};

class CEPH_RBD_API QuiesceWatchCtx {
public:
  virtual ~QuiesceWatchCtx() {}
  /**
   * Callback activated when we want to quiesce.
   */
  virtual void handle_quiesce() = 0;

  /**
   * Callback activated when we want to unquiesce.
   */
  virtual void handle_unquiesce() = 0;
};

class CEPH_RBD_API Image
{
public:
  Image();
  ~Image();

  int close();
  int aio_close(RBD::AioCompletion *c);

  int resize(uint64_t size);
  int resize2(uint64_t size, bool allow_shrink, ProgressContext& pctx);
  int resize_with_progress(uint64_t size, ProgressContext& pctx);
  int stat(image_info_t &info, size_t infosize);
  int get_name(std::string *name);
  int get_id(std::string *id);
  std::string get_block_name_prefix();
  int64_t get_data_pool_id();
  int parent_info(std::string *parent_poolname, std::string *parent_name,
		  std::string *parent_snapname)
      CEPH_RBD_DEPRECATED;
  int parent_info2(std::string *parent_poolname, std::string *parent_name,
                   std::string *parent_id, std::string *parent_snapname)
      CEPH_RBD_DEPRECATED;
  int get_parent(linked_image_spec_t *parent_image, snap_spec_t *parent_snap);

  int old_format(uint8_t *old);
  int size(uint64_t *size);
  int get_group(group_info_t *group_info, size_t group_info_size);
  int features(uint64_t *features);
  int update_features(uint64_t features, bool enabled);
  int get_op_features(uint64_t *op_features);
  int overlap(uint64_t *overlap);
  int get_flags(uint64_t *flags);
  int set_image_notification(int fd, int type);

  /* exclusive lock feature */
  int is_exclusive_lock_owner(bool *is_owner);
  int lock_acquire(rbd_lock_mode_t lock_mode);
  int lock_release();
  int lock_get_owners(rbd_lock_mode_t *lock_mode,
                      std::list<std::string> *lock_owners);
  int lock_break(rbd_lock_mode_t lock_mode, const std::string &lock_owner);

  /* object map feature */
  int rebuild_object_map(ProgressContext &prog_ctx);

  int check_object_map(ProgressContext &prog_ctx);

  int copy(IoCtx& dest_io_ctx, const char *destname);
  int copy2(Image& dest);
  int copy3(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts);
  int copy4(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts,
	    size_t sparse_size);
  int copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
			 ProgressContext &prog_ctx);
  int copy_with_progress2(Image& dest, ProgressContext &prog_ctx);
  int copy_with_progress3(IoCtx& dest_io_ctx, const char *destname,
			  ImageOptions& opts, ProgressContext &prog_ctx);
  int copy_with_progress4(IoCtx& dest_io_ctx, const char *destname,
			  ImageOptions& opts, ProgressContext &prog_ctx,
			  size_t sparse_size);

  /* deep copy */
  int deep_copy(IoCtx& dest_io_ctx, const char *destname, ImageOptions& opts);
  int deep_copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
                              ImageOptions& opts, ProgressContext &prog_ctx);

  /* striping */
  uint64_t get_stripe_unit() const;
  uint64_t get_stripe_count() const;

  int get_create_timestamp(struct timespec *timestamp);
  int get_access_timestamp(struct timespec *timestamp);
  int get_modify_timestamp(struct timespec *timestamp);

  int flatten();
  int flatten_with_progress(ProgressContext &prog_ctx);

  int sparsify(size_t sparse_size);
  int sparsify_with_progress(size_t sparse_size, ProgressContext &prog_ctx);
  /**
   * Returns a pair of poolname, imagename for each clone
   * of this image at the currently set snapshot.
   */
  int list_children(std::set<std::pair<std::string, std::string> > *children)
      CEPH_RBD_DEPRECATED;
  /**
  * Returns a structure of poolname, imagename, imageid and trash flag
  * for each clone of this image at the currently set snapshot.
  */
  int list_children2(std::vector<librbd::child_info_t> *children)
      CEPH_RBD_DEPRECATED;
  int list_children3(std::vector<linked_image_spec_t> *images);
  int list_descendants(std::vector<linked_image_spec_t> *images);

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
  bool snap_exists(const char *snapname) CEPH_RBD_DEPRECATED;
  int snap_exists2(const char *snapname, bool *exists);
  int snap_create(const char *snapname);
  int snap_remove(const char *snapname);
  int snap_remove2(const char *snapname, uint32_t flags, ProgressContext& pctx);
  int snap_remove_by_id(uint64_t snap_id);
  int snap_rollback(const char *snap_name);
  int snap_rollback_with_progress(const char *snap_name, ProgressContext& pctx);
  int snap_protect(const char *snap_name);
  int snap_unprotect(const char *snap_name);
  int snap_is_protected(const char *snap_name, bool *is_protected);
  int snap_set(const char *snap_name);
  int snap_set_by_id(uint64_t snap_id);
  int snap_get_name(uint64_t snap_id, std::string *snap_name);
  int snap_get_id(const std::string snap_name, uint64_t *snap_id);
  int snap_rename(const char *srcname, const char *dstname);
  int snap_get_limit(uint64_t *limit);
  int snap_set_limit(uint64_t limit);
  int snap_get_timestamp(uint64_t snap_id, struct timespec *timestamp);
  int snap_get_namespace_type(uint64_t snap_id,
                              snap_namespace_type_t *namespace_type);
  int snap_get_group_namespace(uint64_t snap_id,
                               snap_group_namespace_t *group_namespace,
                               size_t snap_group_namespace_size);
  int snap_get_trash_namespace(uint64_t snap_id, std::string* original_name);
  int snap_get_mirror_namespace(
      uint64_t snap_id, snap_mirror_namespace_t *mirror_namespace,
      size_t snap_mirror_namespace_size);

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
  ssize_t writesame(uint64_t ofs, size_t len, ceph::bufferlist &bl, int op_flags);
  ssize_t compare_and_write(uint64_t ofs, size_t len, ceph::bufferlist &cmp_bl,
                            ceph::bufferlist& bl, uint64_t *mismatch_off, int op_flags);

  int aio_write(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  /* @param op_flags see librados.h constants beginning with LIBRADOS_OP_FLAG */
  int aio_write2(uint64_t off, size_t len, ceph::bufferlist& bl,
		  RBD::AioCompletion *c, int op_flags);
  int aio_writesame(uint64_t off, size_t len, ceph::bufferlist& bl,
                    RBD::AioCompletion *c, int op_flags);
  int aio_compare_and_write(uint64_t off, size_t len, ceph::bufferlist& cmp_bl,
                            ceph::bufferlist& bl, RBD::AioCompletion *c,
                            uint64_t *mismatch_off, int op_flags);
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

  // RBD image mirroring support functions
  int mirror_image_enable() CEPH_RBD_DEPRECATED;
  int mirror_image_enable2(mirror_image_mode_t mode);
  int mirror_image_disable(bool force);
  int mirror_image_promote(bool force);
  int mirror_image_demote();
  int mirror_image_resync();
  int mirror_image_create_snapshot(uint64_t *snap_id);
  int mirror_image_get_info(mirror_image_info_t *mirror_image_info,
                            size_t info_size);
  int mirror_image_get_mode(mirror_image_mode_t *mode);
  int mirror_image_get_global_status(
      mirror_image_global_status_t *mirror_image_global_status,
      size_t status_size);
  int mirror_image_get_status(
      mirror_image_status_t *mirror_image_status, size_t status_size)
    CEPH_RBD_DEPRECATED;
  int mirror_image_get_instance_id(std::string *instance_id);
  int aio_mirror_image_promote(bool force, RBD::AioCompletion *c);
  int aio_mirror_image_demote(RBD::AioCompletion *c);
  int aio_mirror_image_get_info(mirror_image_info_t *mirror_image_info,
                                size_t info_size, RBD::AioCompletion *c);
  int aio_mirror_image_get_mode(mirror_image_mode_t *mode,
                                RBD::AioCompletion *c);
  int aio_mirror_image_get_global_status(
      mirror_image_global_status_t *mirror_image_global_status,
      size_t status_size, RBD::AioCompletion *c);
  int aio_mirror_image_get_status(
      mirror_image_status_t *mirror_image_status, size_t status_size,
      RBD::AioCompletion *c)
    CEPH_RBD_DEPRECATED;

  int update_watch(UpdateWatchCtx *ctx, uint64_t *handle);
  int update_unwatch(uint64_t handle);

  int list_watchers(std::list<image_watcher_t> &watchers);

  int config_list(std::vector<config_option_t> *options);

  int quiesce_watch(QuiesceWatchCtx *ctx, uint64_t *handle);
  int quiesce_unwatch(uint64_t handle);
  void quiesce_complete();

private:
  friend class RBD;

  Image(const Image& rhs);
  const Image& operator=(const Image& rhs);

  image_ctx_t ctx;
};

} // namespace librbd

#if __GNUC__ >= 4
  #pragma GCC diagnostic pop
#endif

#endif // __LIBRBD_HPP
