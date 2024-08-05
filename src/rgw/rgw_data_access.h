// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include "include/types.h"
#include "common/ceph_time.h"
#include "rgw_common.h"
#include "rgw_sal_fwd.h"

class RGWDataAccess
{
  rgw::sal::Driver* driver;

public:
  RGWDataAccess(rgw::sal::Driver* _driver);

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

