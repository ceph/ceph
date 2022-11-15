// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal_filter.h"
#include "rgw_sal.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
//#include "rgw_s3_proxy.h"
#include "common/dout.h" 

namespace rgw { namespace sal {

class S3FilterStore : public FilterStore {
  private:

  public:
    S3FilterStore(Store* _next) : FilterStore(_next) 
    {
      //d4n_cache = new RGWD4NCache();
    }
    virtual ~S3FilterStore() {
      //delete d4n_cache;
    }

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
	/*
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    RGWBlockDirectory* get_block_dir() { return blk_dir; }
    cache_block* get_cache_block() { return c_blk; }
    RGWD4NCache* get_d4n_cache() { return d4n_cache; }
	*/
};

class S3FilterUser : public FilterUser {
  private:
    S3FilterStore* filter;

  public:
    S3FilterUser(std::unique_ptr<User> _next, S3FilterStore* _filter) : 
      FilterUser(std::move(_next)),
      filter(_filter) {}
    virtual ~S3FilterUser() = default;

    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
                            Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
                            bool exclusive,
                            bool obj_lock_enabled,
                            bool* existed,
                            req_info& req_info,
                            std::unique_ptr<Bucket>* bucket,
                            optional_yield y) override;
};

class S3FilterBucket : public FilterBucket {
  private:
    S3FilterStore* filter;

  public:
    S3FilterBucket(std::unique_ptr<Bucket> _next, User* _user, S3FilterStore* _filter) :
      FilterBucket(std::move(_next), _user), 
      filter(_filter) {}
    virtual ~S3FilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
};

/*
class S3FilterObject : public FilterObject {
  private:
    S3FilterStore* filter;

  public:
    struct S3FilterReadOp : FilterReadOp {
      S3FilterObject* source;

      S3FilterReadOp(std::unique_ptr<ReadOp> _next, S3FilterObject* _source) : FilterReadOp(std::move(_next)),
										 source(_source) {}
      virtual ~S3FilterReadOp() = default;

      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    };

    struct S3FilterDeleteOp : FilterDeleteOp {
      S3FilterObject* source;

      S3FilterDeleteOp(std::unique_ptr<DeleteOp> _next, S3FilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~S3FilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    S3FilterObject(std::unique_ptr<Object> _next, S3FilterStore* _filter) : FilterObject(std::move(_next)),
									      filter(_filter) {}
    S3FilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, S3FilterStore* _filter) : FilterObject(std::move(_next), _bucket),
											       filter(_filter) {}
    S3FilterObject(S3FilterObject& _o, S3FilterStore* _filter) : FilterObject(_o),
								    filter(_filter) {}
    virtual ~S3FilterObject() = default;

    virtual const std::string &get_name() const override { return next->get_name(); }
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                            rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) override;

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;
};

class D4NFilterWriter : public FilterWriter {
  private:
    D4NFilterStore* filter; 
    const DoutPrefixProvider* save_dpp;
    bool atomic;

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterStore* _filter, std::unique_ptr<Object> _head_obj, 
	const DoutPrefixProvider* _dpp) : FilterWriter(std::move(_next), std::move(_head_obj)),
					  filter(_filter),
					  save_dpp(_dpp), atomic(false) {}
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterStore* _filter, std::unique_ptr<Object> _head_obj, 
	const DoutPrefixProvider* _dpp, bool _atomic) : FilterWriter(std::move(_next), std::move(_head_obj)),
							filter(_filter),
							save_dpp(_dpp), atomic(_atomic) {}
    virtual ~D4NFilterWriter() = default;

    virtual int prepare(optional_yield y);
    virtual int process(bufferlist&& data, uint64_t offset) override;
    virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
   bool is_atomic() { return atomic; };
   const DoutPrefixProvider* dpp() { return save_dpp; }
};
*/
} } // namespace rgw::sal
