// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "include/ceph_hash.h"

#include "common/ceph_time.h"

#include "rgw_common.h"

class RGWSI_SysObj;

class RGWRados;
class RGWSysObjectCtx;
struct RGWObjVersionTracker;
class optional_yield;
namespace rgw { namespace sal {
  class RGWRadosStore;
} }

struct obj_version;


int rgw_init_ioctx(librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx,
		   bool create = false,
		   bool mostly_omap = false);

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

extern const std::string MP_META_SUFFIX;

static inline int rgw_shards_max()
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
static inline int rgw_shard_id(const string& key, int max_shards)
{
  return rgw_shards_mod(ceph_str_hash_linux(key.c_str(), key.size()),
			max_shards);
}

void rgw_shard_name(const string& prefix, unsigned max_shards, const string& key, string& name, int *shard_id);
void rgw_shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name);
void rgw_shard_name(const string& prefix, unsigned shard_id, string& name);

struct rgw_name_to_flag {
  const char *type_name;
  uint32_t flag;
};

int rgw_parse_list_of_flags(struct rgw_name_to_flag *mapping,
			    const string& str, uint32_t *perm);

int rgw_put_system_obj(RGWSysObjectCtx& obj_ctx, const rgw_pool& pool, const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, map<string, bufferlist> *pattrs = NULL);
int rgw_put_system_obj(RGWSysObjectCtx& obj_ctx, const rgw_pool& pool, const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, optional_yield y, map<string, bufferlist> *pattrs = NULL);
int rgw_get_system_obj(RGWSysObjectCtx& obj_ctx, const rgw_pool& pool, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime, optional_yield y, map<string, bufferlist> *pattrs = NULL,
                       rgw_cache_entry_info *cache_info = NULL,
		       boost::optional<obj_version> refresh_version = boost::none);
int rgw_delete_system_obj(RGWSI_SysObj *sysobj_svc, const rgw_pool& pool, const string& oid,
                          RGWObjVersionTracker *objv_tracker);

const char *rgw_find_mime_by_ext(string& ext);

void rgw_filter_attrset(map<string, bufferlist>& unfiltered_attrset, const string& check_prefix,
                        map<string, bufferlist> *attrset);

/// indicates whether the current thread is in boost::asio::io_context::run(),
/// used to log warnings if synchronous librados calls are made
extern thread_local bool is_asio_thread;

/// perform the rados operation, using the yield context when given
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      optional_yield y);
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      int flags, optional_yield y);
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, optional_yield y);
int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, int flags, optional_yield y);
int rgw_rados_notify(librados::IoCtx& ioctx, const std::string& oid,
                     bufferlist& bl, uint64_t timeout_ms, bufferlist* pbl,
                     optional_yield y);

int rgw_tools_init(CephContext *cct);
void rgw_tools_cleanup();

template<class H, size_t S>
class RGWEtag
{
  H hash;

public:
  RGWEtag() {}

  void update(const char *buf, size_t len) {
    hash.Update((const unsigned char *)buf, len);
  }

  void update(bufferlist& bl) {
    if (bl.length() > 0) {
      update(bl.c_str(), bl.length());
    }
  }

  void update(const string& s) {
    if (!s.empty()) {
      update(s.c_str(), s.size());
    }
  }
  void finish(string *etag) {
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
  rgw::sal::RGWRadosStore *store;
  std::unique_ptr<RGWSysObjectCtx> sysobj_ctx;

public:
  RGWDataAccess(rgw::sal::RGWRadosStore *_store);

  class Object;
  class Bucket;

  using BucketRef = std::shared_ptr<Bucket>;
  using ObjectRef = std::shared_ptr<Object>;

  class Bucket : public enable_shared_from_this<Bucket> {
    friend class RGWDataAccess;
    friend class Object;

    RGWDataAccess *sd{nullptr};
    RGWBucketInfo bucket_info;
    string tenant;
    string name;
    string bucket_id;
    ceph::real_time mtime;
    map<std::string, bufferlist> attrs;

    RGWAccessControlPolicy policy;
    int finish_init();
    
    Bucket(RGWDataAccess *_sd,
	   const string& _tenant,
	   const string& _name,
	   const string& _bucket_id) : sd(_sd),
                                       tenant(_tenant),
                                       name(_name),
				       bucket_id(_bucket_id) {}
    Bucket(RGWDataAccess *_sd) : sd(_sd) {}
    int init();
    int init(const RGWBucketInfo& _bucket_info, const map<string, bufferlist>& _attrs);
  public:
    int get_object(const rgw_obj_key& key,
		   ObjectRef *obj);

  };


  class Object {
    RGWDataAccess *sd{nullptr};
    BucketRef bucket;
    rgw_obj_key key;

    ceph::real_time mtime;
    string etag;
    std::optional<uint64_t> olh_epoch;
    ceph::real_time delete_at;
    std::optional<string> user_data;

    std::optional<bufferlist> aclbl;

    Object(RGWDataAccess *_sd,
           BucketRef&& _bucket,
           const rgw_obj_key& _key) : sd(_sd),
                                      bucket(_bucket),
                                      key(_key) {}
  public:
    int put(bufferlist& data, map<string, bufferlist>& attrs, const DoutPrefixProvider *dpp, optional_yield y); /* might modify attrs */

    void set_mtime(const ceph::real_time& _mtime) {
      mtime = _mtime;
    }

    void set_etag(const string& _etag) {
      etag = _etag;
    }

    void set_olh_epoch(uint64_t epoch) {
      olh_epoch = epoch;
    }

    void set_delete_at(ceph::real_time _delete_at) {
      delete_at = _delete_at;
    }

    void set_user_data(const string& _user_data) {
      user_data = _user_data;
    }

    void set_policy(const RGWAccessControlPolicy& policy);

    friend class Bucket;
  };

  int get_bucket(const string& tenant,
		 const string name,
		 const string bucket_id,
		 BucketRef *bucket) {
    bucket->reset(new Bucket(this, tenant, name, bucket_id));
    return (*bucket)->init();
  }

  int get_bucket(const RGWBucketInfo& bucket_info,
		 const map<string, bufferlist>& attrs,
		 BucketRef *bucket) {
    bucket->reset(new Bucket(this));
    return (*bucket)->init(bucket_info, attrs);
  }
  friend class Bucket;
  friend class Object;
};

using RGWDataAccessRef = std::shared_ptr<RGWDataAccess>;

#endif
