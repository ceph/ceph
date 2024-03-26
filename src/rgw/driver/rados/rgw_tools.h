// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <functional>
#include <string>
#include <string_view>

#include "include/types.h"
#include "include/ceph_hash.h"

#include "common/ceph_time.h"

#include "rgw_common.h"
#include "rgw_sal_fwd.h"

class RGWSI_SysObj;

class RGWRados;
struct RGWObjVersionTracker;
class optional_yield;

struct obj_version;

int rgw_init_ioctx(const DoutPrefixProvider *dpp,
                   librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx,
                   bool create = false,
                   bool mostly_omap = false,
                   bool bulk = false);

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

extern const std::string MP_META_SUFFIX;

inline int rgw_shards_max()
{
  return RGW_SHARDS_PRIME_1;
}

// only called by rgw_shard_id and rgw_bucket_shard_index
static inline int rgw_shards_mod(unsigned hval, int max_shards)
{
  if (max_shards <= RGW_SHARDS_PRIME_0) {
    return hval % RGW_SHARDS_PRIME_0 % max_shards;
  }
  return hval % RGW_SHARDS_PRIME_1 % max_shards;
}

// used for logging and tagging
inline int rgw_shard_id(const std::string& key, int max_shards)
{
  return rgw_shards_mod(ceph_str_hash_linux(key.c_str(), key.size()),
			max_shards);
}

void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& key, std::string& name, int *shard_id);
void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& section, const std::string& key, std::string& name);
void rgw_shard_name(const std::string& prefix, unsigned shard_id, std::string& name);

int rgw_put_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                       const rgw_pool& pool, const std::string& oid,
                       bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker,
                       real_time set_mtime, optional_yield y,
                       const std::map<std::string, bufferlist> *pattrs = nullptr);
int rgw_get_system_obj(RGWSI_SysObj* svc_sysobj, const rgw_pool& pool,
                       const std::string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime,
                       optional_yield y, const DoutPrefixProvider *dpp,
                       std::map<std::string, bufferlist> *pattrs = nullptr,
                       rgw_cache_entry_info *cache_info = nullptr,
		       boost::optional<obj_version> refresh_version = boost::none,
                       bool raw_attrs=false);
int rgw_delete_system_obj(const DoutPrefixProvider *dpp, 
                          RGWSI_SysObj *sysobj_svc, const rgw_pool& pool, const std::string& oid,
                          RGWObjVersionTracker *objv_tracker, optional_yield y);
int rgw_stat_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                        const rgw_pool& pool, const std::string& key,
                        RGWObjVersionTracker *objv_tracker,
                        real_time *pmtime, optional_yield y,
                        std::map<std::string, bufferlist> *pattrs = nullptr);

const char *rgw_find_mime_by_ext(std::string& ext);

void rgw_filter_attrset(std::map<std::string, bufferlist>& unfiltered_attrset, const std::string& check_prefix,
                        std::map<std::string, bufferlist> *attrset);

/// indicates whether the current thread is in boost::asio::io_context::run(),
/// used to log warnings if synchronous librados calls are made
extern thread_local bool is_asio_thread;

/// perform the rados operation, using the yield context when given
int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      optional_yield y, int flags = 0, const jspan_context* trace_info = nullptr);
int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, optional_yield y,
		      int flags = 0, const jspan_context* trace_info = nullptr);
int rgw_rados_notify(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                     bufferlist& bl, uint64_t timeout_ms, bufferlist* pbl,
                     optional_yield y);

struct rgw_rados_ref {
  librados::IoCtx ioctx;
  rgw_raw_obj obj;


  int operate(const DoutPrefixProvider* dpp, librados::ObjectReadOperation* op,
	      bufferlist* pbl, optional_yield y, int flags = 0) {
    return rgw_rados_operate(dpp, ioctx, obj.oid, op, pbl, y, flags);
  }

  int operate(const DoutPrefixProvider* dpp, librados::ObjectWriteOperation* op,
	      optional_yield y, int flags = 0) {
    return rgw_rados_operate(dpp, ioctx, obj.oid, op, y, flags);
  }

  int aio_operate(librados::AioCompletion* c,
		  librados::ObjectWriteOperation* op) {
    return ioctx.aio_operate(obj.oid, c, op);
  }

  int aio_operate(librados::AioCompletion* c, librados::ObjectReadOperation* op,
		  bufferlist *pbl) {
    return ioctx.aio_operate(obj.oid, c, op, pbl);
  }

  int watch(uint64_t* handle, librados::WatchCtx2* ctx) {
    return ioctx.watch2(obj.oid, handle, ctx);
  }

  int aio_watch(librados::AioCompletion* c, uint64_t* handle,
		librados::WatchCtx2 *ctx) {
    return ioctx.aio_watch(obj.oid, c, handle, ctx);
  }

  int unwatch(uint64_t handle) {
    return ioctx.unwatch2(handle);
  }

  int notify(const DoutPrefixProvider* dpp, bufferlist& bl, uint64_t timeout_ms,
	     bufferlist* pbl, optional_yield y) {
    return rgw_rados_notify(dpp, ioctx, obj.oid, bl, timeout_ms, pbl, y);
  }

  void notify_ack(uint64_t notify_id, uint64_t cookie, bufferlist& bl) {
    ioctx.notify_ack(obj.oid, notify_id, cookie, bl);
  }
};

inline std::ostream& operator <<(std::ostream& m, const rgw_rados_ref& ref) {
  return m << ref.obj;
}

int rgw_get_rados_ref(const DoutPrefixProvider* dpp, librados::Rados* rados,
		      rgw_raw_obj obj, rgw_rados_ref* ref);



int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct);
void rgw_tools_cleanup();

/// Complete an AioCompletion. To return error values or otherwise
/// satisfy the caller. Useful for making complicated asynchronous
/// calls and error handling.
void rgw_complete_aio_completion(librados::AioCompletion* c, int r);

/// This returns a static, non-NULL pointer, recognized only by
/// rgw_put_system_obj(). When supplied instead of the attributes, the
/// attributes will be unmodified.
///
// (Currently providing nullptr will wipe all attributes.)

std::map<std::string, ceph::buffer::list>* no_change_attrs();

bool rgw_check_secure_mon_conn(const DoutPrefixProvider *dpp);
int rgw_clog_warn(librados::Rados* h, const std::string& msg);

int rgw_list_pool(const DoutPrefixProvider *dpp,
		  librados::IoCtx& ioctx,
		  uint32_t max,
		  const rgw::AccessListFilter& filter,
		  std::string& marker,
		  std::vector<std::string> *oids,
		  bool *is_truncated);
