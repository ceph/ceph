// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Red Hat, Inc.
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
#include "rgw_aio_throttle.h"


#include "driver/d4n/d4n_policy.h"
#include "driver/d4n/d4n_policy.h"


#include "driver/fdb/d4n_fdb_directory.h"

#include "driver/redis/rgw_redis_driver.h"

#include "driver/ssd/rgw_ssd_driver.h"

#include <boost/intrusive/list.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include <fmt/core.h>

namespace rgw::d4n {
  class PolicyDriver;
}

namespace rgw { namespace sal {

class D4NFilterDriver_FDB : public FilterDriver {
  private:
//JFW:    inline static ceph::libfdb::database_handle dbh;

//   std::shared_ptr<connection> conn;
/*
    std::unique_ptr<rgw::cache::CacheDriver> cacheDriver;
    std::unique_ptr<rgw::d4n::ObjectDirectory> objDir;
    std::unique_ptr<rgw::d4n::BlockDirectory> blockDir;
    std::unique_ptr<rgw::d4n::BucketDirectory> bucketDir;
    std::unique_ptr<rgw::d4n::PolicyDriver> policyDriver;
*/

/*
    boost::asio::io_context& io_context;
    optional_yield y;
*/

/*
    // Redis connection pool
    std::shared_ptr<rgw::d4n::RedisPool> redis_pool;
*/
  CephContext *cct = nullptr;

  public:
    D4NFilterDriver_FDB(CephContext *cct_, Driver* next_)
     : FilterDriver(next_),
       cct(cct_)
{
// The regular flavor D4N FilterDriver sets up L1 read cache here, with the SSD Driver.
}

    ~D4NFilterDriver_FDB() = default;

    int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override
try
{
cct = cct; // dunno why we get this twice, but ok!

// JFW: I guess make the FDB connection here?

// JFW: can we keep a pointer to this dpp thing?
ldpp_dout(dpp, 1) << "JFW: D4NFilterDriver_FDB: initialized OK." << dendl;

return 1;
}
catch(const std::exception& e)
{
 ldpp_dout(dpp, -1) << "JFW: D4NFilterDriver_FDB: failed to initialize: " << e.what() << dendl;
 return -1;
}

    std::unique_ptr<User> get_user(const rgw_user& u) override
{
//ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: get_user()." << dendl;
 return {};
}

    std::unique_ptr<Object> get_object(const rgw_obj_key& k) override
{
//ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: get_object()." << dendl;
 return {};
}

    std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override
{
//ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: get_bucket()." << dendl;
 return {};
}

    int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                  std::unique_ptr<Bucket>* bucket, optional_yield y) override
{
ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: load_bucket()." << dendl;
 return -1;
}

    void shutdown() override
{
//ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: shut down." << dendl;
}

////////
    std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
                                  optional_yield y,
                                  rgw::sal::Object* obj,
                                  const ACLOwner& owner,
                                  const rgw_placement_rule *ptail_placement_rule,
                                  uint64_t olh_epoch,
                                  const std::string& unique_tag) override
{
ldpp_dout(dpp, 0) << "JFW: D4NFilterDriver_FDB: get_atomic_writer()." << dendl;
 return {};
}

/*
////////
    rgw::cache::CacheDriver* get_cache_driver() { return cacheDriver.get(); }
    rgw::d4n::ObjectDirectory* get_obj_dir() { return objDir.get(); }
    rgw::d4n::BlockDirectory* get_block_dir() { return blockDir.get(); }
    rgw::d4n::BucketDirectory* get_bucket_dir() { return bucketDir.get(); }
    rgw::d4n::PolicyDriver* get_policy_driver() { return policyDriver.get(); }

////////
    std::shared_ptr<connection> get_conn() { return conn; }
    std::shared_ptr<rgw::d4n::RedisPool> get_redis_pool() { return redis_pool; }

////////
    void save_y(optional_yield y) { this->y = y; }
*/;
};


class D4NFilterBucket_FDB : public FilterBucket {
  private:
    struct rgw_bucket_list_entries{
      rgw_obj_key key;
      uint16_t flags;
    };
    D4NFilterDriver* filter;

  public:
    D4NFilterBucket_FDB(std::unique_ptr<Bucket> _next, D4NFilterDriver* _filter) :
      FilterBucket(std::move(_next)),
      filter(_filter) {}

  public:
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override
{ return {}; }

    virtual int list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		   ListResults& results, optional_yield y) override
{ return {}; }

    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) override
{ return {}; }

    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override
{ return {}; }

};

} } // namespace rgw::sal
