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
#include "rgw_role.h"
#include "common/dout.h" 

#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_datacache.h"

namespace rgw { namespace sal {

class D4NFilterDriver : public FilterDriver {
  private:
    RGWBlockDirectory* blk_dir;
    cache_block* c_blk;
    RGWD4NCache* d4n_cache;

  public:
    D4NFilterDriver(Driver* _next) : FilterDriver(_next) 
    {
      blk_dir = new RGWBlockDirectory(); /* Initialize directory address with cct */
      c_blk = new cache_block();
      d4n_cache = new RGWD4NCache();
    }
    virtual ~D4NFilterDriver() {
      delete blk_dir; 
      delete c_blk;
      delete d4n_cache;
    }

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    RGWBlockDirectory* get_block_dir() { return blk_dir; }
    cache_block* get_cache_block() { return c_blk; }
    RGWD4NCache* get_d4n_cache() { return d4n_cache; }
};

class D4NFilterUser : public FilterUser {
  private:
    D4NFilterDriver* filter;

  public:
    D4NFilterUser(std::unique_ptr<User> _next, D4NFilterDriver* _filter) : 
      FilterUser(std::move(_next)),
      filter(_filter) {}
    virtual ~D4NFilterUser() = default;
};

class D4NFilterBucket : public FilterBucket {
  private:
    D4NFilterDriver* filter;

  public:
    D4NFilterBucket(std::unique_ptr<Bucket> _next, D4NFilterDriver* _filter) :
      FilterBucket(std::move(_next)),
      filter(_filter) {}
    virtual ~D4NFilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) override;
};

class D4NFilterObject : public FilterObject {
  private:
    D4NFilterDriver* filter;

  public:
    struct D4NFilterReadOp : FilterReadOp {
      D4NFilterObject* source;

      D4NFilterReadOp(std::unique_ptr<ReadOp> _next, D4NFilterObject* _source) : FilterReadOp(std::move(_next)),
										 source(_source) {}
      virtual ~D4NFilterReadOp() = default;

      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    };

    struct D4NFilterDeleteOp : FilterDeleteOp {
      D4NFilterObject* source;

      D4NFilterDeleteOp(std::unique_ptr<DeleteOp> _next, D4NFilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~D4NFilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
    };

    D4NFilterObject(std::unique_ptr<Object> _next, D4NFilterDriver* _filter) : FilterObject(std::move(_next)),
									      filter(_filter) {}
    D4NFilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, D4NFilterDriver* _filter) : FilterObject(std::move(_next), _bucket),
											       filter(_filter) {}
    D4NFilterObject(D4NFilterObject& _o, D4NFilterDriver* _filter) : FilterObject(_o),
								    filter(_filter) {}
    virtual ~D4NFilterObject() = default;

    virtual int copy_object(const ACLOwner& owner,
               const rgw_user& remote_user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
               boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual const std::string &get_name() const override { return next->get_name(); }
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags) override;
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
    D4NFilterDriver* filter; 
    const DoutPrefixProvider* save_dpp;
    bool atomic;

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _filter, Object* _obj, 
	const DoutPrefixProvider* _dpp) : FilterWriter(std::move(_next), _obj),
					  filter(_filter),
					  save_dpp(_dpp), atomic(false) {}
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _filter, Object* _obj, 
	const DoutPrefixProvider* _dpp, bool _atomic) : FilterWriter(std::move(_next), _obj),
							filter(_filter),
							save_dpp(_dpp), atomic(_atomic) {}
    virtual ~D4NFilterWriter() = default;

    virtual int prepare(optional_yield y);
    virtual int process(bufferlist&& data, uint64_t offset) override;
    virtual int complete(size_t accounted_size, const std::string& etag,
			 ceph::real_time *mtime, ceph::real_time set_mtime,
			 std::map<std::string, bufferlist>& attrs,
			 const std::optional<rgw::cksum::Cksum>& cksum,
			 ceph::real_time delete_at,
			 const char *if_match, const char *if_nomatch,
			 const std::string *user_data,
			 rgw_zone_set *zones_trace, bool *canceled,
			 const req_context& rctx,
			 uint32_t flags) override;
   bool is_atomic() { return atomic; };
   const DoutPrefixProvider* dpp() { return save_dpp; }
};

} } // namespace rgw::sal
