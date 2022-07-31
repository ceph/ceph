// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

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
		   bool mostly_omap = false);

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
                       std::map<std::string, bufferlist> *pattrs = nullptr);
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

int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct);
void rgw_tools_cleanup();

template<class H, size_t S>
class RGWEtag
{
  H hash;

public:
  RGWEtag() {
    if constexpr (std::is_same_v<H, MD5>) {
      // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
      hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    }
  }

  void update(const char *buf, size_t len) {
    hash.Update((const unsigned char *)buf, len);
  }

  void update(bufferlist& bl) {
    if (bl.length() > 0) {
      update(bl.c_str(), bl.length());
    }
  }

  void update(const std::string& s) {
    if (!s.empty()) {
      update(s.c_str(), s.size());
    }
  }
  void finish(std::string *etag) {
    char etag_buf[S];
    char etag_buf_str[S * 2 + 16];

    hash.Final((unsigned char *)etag_buf);
    buf_to_hex((const unsigned char *)etag_buf, S,
	       etag_buf_str);

    *etag = etag_buf_str;
  }
};

using RGWMD5Etag = RGWEtag<MD5, CEPH_CRYPTO_MD5_DIGESTSIZE>;

class RGWDataAccess
{
  rgw::sal::Store* store;

public:
  RGWDataAccess(rgw::sal::Store* _store);

  class Object;
  class Bucket;

  using BucketRef = std::shared_ptr<Bucket>;
  using ObjectRef = std::shared_ptr<Object>;

  class Bucket : public std::enable_shared_from_this<Bucket> {
    friend class RGWDataAccess;
    friend class Object;

    RGWDataAccess *sd{nullptr};
    RGWBucketInfo bucket_info;
    std::string tenant;
    std::string name;
    std::string bucket_id;
    ceph::real_time mtime;
    std::map<std::string, bufferlist> attrs;

    RGWAccessControlPolicy policy;
    int finish_init();
    
    Bucket(RGWDataAccess *_sd,
	   const std::string& _tenant,
	   const std::string& _name,
	   const std::string& _bucket_id) : sd(_sd),
                                       tenant(_tenant),
                                       name(_name),
				       bucket_id(_bucket_id) {}
    Bucket(RGWDataAccess *_sd) : sd(_sd) {}
    int init(const DoutPrefixProvider *dpp, optional_yield y);
    int init(const RGWBucketInfo& _bucket_info, const std::map<std::string, bufferlist>& _attrs);
  public:
    int get_object(const rgw_obj_key& key,
		   ObjectRef *obj);

  };


  class Object {
    RGWDataAccess *sd{nullptr};
    BucketRef bucket;
    rgw_obj_key key;

    ceph::real_time mtime;
    std::string etag;
    uint64_t olh_epoch{0};
    ceph::real_time delete_at;
    std::optional<std::string> user_data;

    std::optional<bufferlist> aclbl;

    Object(RGWDataAccess *_sd,
           BucketRef&& _bucket,
           const rgw_obj_key& _key) : sd(_sd),
                                      bucket(_bucket),
                                      key(_key) {}
  public:
    int put(bufferlist& data, std::map<std::string, bufferlist>& attrs, const DoutPrefixProvider *dpp, optional_yield y); /* might modify attrs */

    void set_mtime(const ceph::real_time& _mtime) {
      mtime = _mtime;
    }

    void set_etag(const std::string& _etag) {
      etag = _etag;
    }

    void set_olh_epoch(uint64_t epoch) {
      olh_epoch = epoch;
    }

    void set_delete_at(ceph::real_time _delete_at) {
      delete_at = _delete_at;
    }

    void set_user_data(const std::string& _user_data) {
      user_data = _user_data;
    }

    void set_policy(const RGWAccessControlPolicy& policy);

    friend class Bucket;
  };

  int get_bucket(const DoutPrefixProvider *dpp, 
                 const std::string& tenant,
		 const std::string name,
		 const std::string bucket_id,
		 BucketRef *bucket,
		 optional_yield y) {
    bucket->reset(new Bucket(this, tenant, name, bucket_id));
    return (*bucket)->init(dpp, y);
  }

  int get_bucket(const RGWBucketInfo& bucket_info,
		 const std::map<std::string, bufferlist>& attrs,
		 BucketRef *bucket) {
    bucket->reset(new Bucket(this));
    return (*bucket)->init(bucket_info, attrs);
  }
  friend class Bucket;
  friend class Object;
};

using RGWDataAccessRef = std::shared_ptr<RGWDataAccess>;

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
#endif
