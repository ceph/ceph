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
#include "rgw_directory.h"
#include "common/dout.h" 
#include "rgw_sal_d4n.h"

namespace rgw { namespace sal {

class D4NFilterStore : public FilterStore {
  private:
    RGWBlockDirectory* blk_dir; 
    cache_block* c_blk;

  public:
    D4NFilterStore(Store* _next) : FilterStore(_next) 
    {
      blk_dir = new RGWBlockDirectory("127.0.0.1", 6379); // Change so it's not hardcoded -Sam
      c_blk = new cache_block();
    }
    virtual ~D4NFilterStore() {
      delete blk_dir; 
      delete c_blk;
    }

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
};

class D4NFilterObject : public FilterObject {
  private:
    D4NFilterStore* filter;

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

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    D4NFilterObject(std::unique_ptr<Object> _next, D4NFilterStore* _filter) : FilterObject(std::move(_next)),
									     filter(_filter) {}
    D4NFilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, D4NFilterStore* _filter) : FilterObject(std::move(_next), _bucket),
											       filter(_filter) {}
    D4NFilterObject(D4NFilterObject& _o, D4NFilterStore* _filter) : FilterObject(_o),
								    filter(_filter) {}
    virtual ~D4NFilterObject() = default;

    virtual const std::string &get_name() const override { return next->get_name(); }

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;
};

class D4NFilterWriter : public FilterWriter {
  private:
    D4NFilterStore* filter;
    std::unique_ptr<rgw::sal::Object> head_obj;
    const DoutPrefixProvider* save_dpp;

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, const DoutPrefixProvider* _dpp) : FilterWriter(std::move(_next)),
										     save_dpp(_dpp) {} 
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterStore* _filter, std::unique_ptr<Object> _head_obj, const DoutPrefixProvider* _dpp) : 
										     FilterWriter(std::move(_next)),
										     filter(_filter), 
										     head_obj(std::move(_head_obj)),
										     save_dpp(_dpp) {}
    virtual ~D4NFilterWriter() = default;

    virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
   const DoutPrefixProvider* dpp() { return save_dpp; }
};

} } // namespace rgw::sal
