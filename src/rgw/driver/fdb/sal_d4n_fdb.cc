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
 
#include "sal_d4n_fdb.h"

#include "common/dout_fmt.h" 

#include "rgw/rgw_fdb.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include <chrono>
#include <vector>

extern "C" {

rgw::sal::Driver *newD4N_FDB_Filter(rgw::sal::Driver *next, void *io_context, bool admin)
{
 return new rgw::sal::D4N_FDB_FilterDriver(next, *static_cast<boost::asio::io_context *>(io_context), admin);
}

} // extern "C"

namespace {

void msg(const DoutPrefixProvider *dpp, const std::string msg = "", const std::source_location& sl = std::source_location::current())
{
 ldpp_dout_fmt(dpp, 0, "D4N-FDB [{} ({}, {})]: {}\n", sl.file_name(), sl.line(), sl.function_name(), msg);
}

} // namespace

namespace rgw::sal {

D4N_FDB_FilterDriver::D4N_FDB_FilterDriver(Driver* next_, boost::asio::io_context& io_context, bool /*JFW: admin*/) 
 : FilterDriver(next_),
   io_context { io_context },
   y { null_yield }
{
 /* JFW:
 FilterDrier
 cacheDriver
 policyDriver */
}

int D4N_FDB_FilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
 msg(dpp, "JFW about to become FULLY INITIALIZED!! No pointer left unassigned!"); 

 return 0;
}

std::unique_ptr<User> D4N_FDB_FilterDriver::get_user(const rgw_user& u)
{
 // JFW: Unclear why this isn't implemented in the base class. I'm going to pretty much just follow
 // along with the Redis D4N driver, which just calls the extant "Filter" approach:
 
 std::unique_ptr<User> user = next->get_user(u);

 return std::make_unique<D4NFilterUser>(std::move(next->get_user(u)), this);
}

std::unique_ptr<Object> D4N_FDB_FilterDriver::get_object(const rgw_obj_key& k)
{
 return std::make_unique<D4NFilterObject>(std::move(next->get_object(k)), this, filter);
}

std::unique_ptr<Bucket> D4N_FDB_FilterDriver::get_bucket(const RGWBucketInfo& i)
{
 // JFW: the original implementation does NOT move() this, I don't know why-- I have
 // moved to move:
 // return std::make_unique<D4NFilterBucket>(next->get_bucket(i), this);
 
 return std::make_unique<D4NFilterBucket>(std::move(next->get_bucket(i)), this);
}

int D4N_FDB_FilterDriver::load_bucket(const DoutPrefixProvider* dpp, 
      const rgw_bucket& b,
      std::unique_ptr<Bucket>* bucket, 
      optional_yield y)
{
// JFW: copied wholesale from sal_d4n
 std::unique_ptr<Bucket> nb;
 const int ret = next->load_bucket(dpp, b, &nb, y);
 *bucket = std::make_unique<D4NFilterBucket>(std::move(nb), this);
 return ret;
}

std::unique_ptr<Writer> D4N_FDB_FilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
      optional_yield y,
      rgw::sal::Object* obj,
      const ACLOwner& owner,
      const rgw_placement_rule *ptail_placement_rule,
      uint64_t olh_epoch,
      const std::string& unique_tag)
{
 std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

 return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true, y);
}

void D4N_FDB_FilterDriver::shutdown()
{
 // Without a dout, not much to do here... but it would be nice to be able to log something. :-/
}

} // namespace rgw::sal
          
