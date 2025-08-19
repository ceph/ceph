// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
                         
/*                       
 * Ceph - scalable distributed file system 
 *
 * Copyright (C) 2025 International Business Machines, Inc. (IBM)
 *                           
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
  
#ifndef CEPH_RGW_SAL_D4N_FDB
 #define CEPH_RGW_SAL_D4N_FDB

#include "rgw_sal_filter.h"

/*JFW:
#include "rgw_sal.h"
#include "rgw_role.h"
#include "rgw_aio_throttle.h"

#include "driver/ssd/rgw_ssd_driver.h"
#include "driver/redis/rgw_redis_driver.h"

#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_policy.h"
*/

#include "common/dout.h" 

#include "rgw/rgw_fdb_conversion.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include <chrono>
#include <vector>

namespace rgw:sal {

/* JFW: it's a little unclear to me what to actually implement from FilterDriver, so I'm
 * implementing whatever I see from the Redis D4N driver, at least "more or less": */
struct D4N_FDB_FilterDriver : FilterDriver
{
/*
    std::shared_ptr<connection> conn;
    std::unique_ptr<rgw::cache::CacheDriver> cacheDriver;
    std::unique_ptr<rgw::d4n::ObjectDirectory> objDir;
    std::unique_ptr<rgw::d4n::BlockDirectory> blockDir;
    std::unique_ptr<rgw::d4n::BucketDirectory> bucketDir;
    std::unique_ptr<rgw::d4n::PolicyDriver> policyDriver;
*/
  boost::asio::io_context& io_context;
  optional_yield y;

  public:
  D4N_FDB_FilterDriver(Driver* _next, boost::asio::io_context& io_context, bool admin);

  public:
  int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;

  std::unique_ptr<User> get_user(const rgw_user& u) override;
  std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override;

  int load_bucket(const DoutPrefixProvider* dpp, 
	const rgw_bucket& b,
        std::unique_ptr<Bucket>* bucket, 
        optional_yield y) override;

  std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
	optional_yield y,
	rgw::sal::Object* obj,
	const ACLOwner& owner,
	const rgw_placement_rule *ptail_placement_rule,
	uint64_t olh_epoch,
	const std::string& unique_tag) override;

  void shutdown() override;
};

} // namespace rgw::sal

#endif
