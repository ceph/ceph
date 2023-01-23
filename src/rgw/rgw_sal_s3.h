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
#include "rgw_rest_client.h"

namespace rgw { namespace sal {

vector<string> get_xml_data(string &text, string tag)
{
  vector<string> collection;
  unsigned int pos = 0, start;
  while (true){
      start = text.find( "<" + tag, pos ); if ( start >= text.length() ) return collection;
      start = text.find( ">" , start );
      start++;

      pos = text.find( "</" + tag, start );   if ( pos >= text.length() ) return collection;
      collection.push_back(text.substr( start, pos - start ) );
  }
}


class S3FilterBucket;

class RGWGetBucketCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:
  S3FilterBucket *bucket;
  vector<string> *remote_bucket_list;
  vector<rgw_bucket_dir_entry> *remote_bucket;

  RGWGetBucketCB(S3FilterBucket *_bucket, vector<string> *_remote_bucket_list, vector<rgw_bucket_dir_entry> *_remote_bucket): bucket(_bucket), remote_bucket_list(_remote_bucket_list), remote_bucket(_remote_bucket) {}

  int handle_data(bufferlist& bl, bool *pause) override;
};

class S3FilterStore : public FilterStore {
  private:

  public:
	CephContext *_cct;
    S3FilterStore(Store* _next) : FilterStore(_next) 
    {
      //d4n_cache = new RGWD4NCache();
    }
    virtual ~S3FilterStore() {
      //delete d4n_cache;
    }

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
	
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
	int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y);
	int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket);
	int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y);
	
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
	/*
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
  protected:
    //RGWBucketEnt ent;
    //RGWBucketInfo info;
  
  private:
    S3FilterStore* filter;
	//ceph::real_time mtime;
	//Attrs attrs;

  public:
    S3FilterBucket(std::unique_ptr<Bucket> _next, User* _user, S3FilterStore* _filter) :
      FilterBucket(std::move(_next), _user), 
      filter(_filter) {}
	/*
    S3FilterBucket(std::unique_ptr<Bucket> _next, const rgw_bucket& _b, User* _user, S3FilterStore* _filter):
	  FilterBucket(std::move(_next), _user), 
      filter(_filter) 
	 { ent.bucket = _b; info.bucket = _b; }
	*/	 

    virtual ~S3FilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
	//virtual RGWBucketInfo& get_info() override;
	//virtual ceph::real_time& get_modification_time() override { mtime = real_clock::now(); return mtime; }
	virtual void set_owner(rgw::sal::User* _owner) override { next->set_owner(_owner); }
	//virtual Attrs& get_attrs(void) override { return attrs; }
	//virtual int set_attrs(string field, bufferlist bl) { attrs[field] = bl; return 0;}
	virtual int set_attrs(Attrs attrVal) override { return next->set_attrs(attrVal);}
	/*
	virtual rgw_bucket& get_key() override { return ent.bucket; }
    virtual RGWBucketInfo& get_info() override { return info; }
    virtual void set_info(RGWBucketInfo _info) { this->info = _info; }
	virtual rgw_placement_rule& get_placement_rule() override { return info.placement_rule; }
    virtual void print(std::ostream& out) const override { out << info.bucket; }
    virtual bool empty() const override { return info.bucket.name.empty(); }
    virtual const std::string& get_name() const override { return info.bucket.name; }
    virtual const std::string& get_tenant() const override { return info.bucket.tenant; }
    virtual const std::string& get_marker() const override { return info.bucket.marker; }
    virtual const std::string& get_bucket_id() const override { return info.bucket.bucket_id; }
    virtual size_t get_size() const override { return ent.size; }
    virtual size_t get_size_rounded() const override { return ent.size_rounded; }
    virtual uint64_t get_count() const override { return ent.count; }
	*/

};


class S3FilterObject : public FilterObject {
  private:
    S3FilterStore* filter;

  public:
    struct S3FilterReadOp : FilterReadOp {
      S3FilterObject* source;

      S3FilterReadOp(std::unique_ptr<ReadOp> _next, S3FilterObject* _source) : FilterReadOp(std::move(_next)),
										 source(_source) {}
      virtual ~S3FilterReadOp() = default;

      //virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    };

    struct S3FilterDeleteOp : FilterDeleteOp {
      S3FilterObject* source;

      S3FilterDeleteOp(std::unique_ptr<DeleteOp> _next, S3FilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~S3FilterDeleteOp() = default;

      //virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    S3FilterObject(std::unique_ptr<Object> _next, S3FilterStore* _filter) : FilterObject(std::move(_next)),
									      filter(_filter) {}
    S3FilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, S3FilterStore* _filter) : FilterObject(std::move(_next), _bucket),
											       filter(_filter) {}
    S3FilterObject(S3FilterObject& _o, S3FilterStore* _filter) : FilterObject(_o),
								    filter(_filter) {}
    virtual ~S3FilterObject() = default;

    virtual const std::string &get_name() const override { return next->get_name(); }
    //virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
    //                        Attrs* delattrs, optional_yield y) override;
    //virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
    //                        rgw_obj* target_obj = NULL) override;
    //virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
    //                           optional_yield y, const DoutPrefixProvider* dpp) override;
    //virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
    //                           optional_yield y) override;

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;
};

class S3FilterWriter : public FilterWriter {
private:
  S3FilterStore* filter; 
  const DoutPrefixProvider* save_dpp;
  bool atomic;

public:
  S3FilterWriter(std::unique_ptr<Writer> _next, S3FilterStore* _filter, std::unique_ptr<Object> _head_obj, 
					  const DoutPrefixProvider* _dpp) : FilterWriter(std::move(_next), std::move(_head_obj)),
					  filter(_filter),
					  save_dpp(_dpp), atomic(false) {}
  S3FilterWriter(std::unique_ptr<Writer> _next, S3FilterStore* _filter, std::unique_ptr<Object> _head_obj, 
					  const DoutPrefixProvider* _dpp, bool _atomic) : FilterWriter(std::move(_next), std::move(_head_obj)),
					  filter(_filter),
					  save_dpp(_dpp), atomic(_atomic) {}

  virtual ~S3FilterWriter() = default;

  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
};



} } // namespace rgw::sal
