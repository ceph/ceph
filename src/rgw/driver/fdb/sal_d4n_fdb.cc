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

#include "common/dout.h" 

#include "rgw/rgw_fdb_conversion.h"

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
 return new rgw::sal::D4N_FDB_FilterDriver(next, io_context, admin);
}

} // extern "C"

namespace {

void msg(const DoutPrefixProvider *dpp, std::string_view msg, const std::source_location& sl = std::source_location::current())
{
 ldpp_dout(dpp, 0) << fmt::format("D4N-fdb: " << sl << ": " << msg << dendl;
}

} // namespace

namespace rgw::sal {

D4N_FDB_Filter(Driver* _next, boost::asio::io_context& io_context, bool /*JFW: admin*/) 
 : FilterDriver(next_),
   io_context { io_context },
   y { null_yield }
{
 // cacheDriver ? JFW
}

int D4N_FDB_FilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
 msg(dpp, "JFW about to become FULLY INITIALIZED!! No pointer left unassigned!"); 

 return 0;
}

std::unique_ptr<User> D4N_FDB_FilterDriver::get_user(const rgw_user& u)
{
 return {};
}

std::unique_ptr<Object> D4N_FDB_FilterDriver::get_object(const rgw_obj_key& k)
{
 return {};
}

std::unique_ptr<Bucket> D4N_FDB_FilterDriver::get_bucket(const RGWBucketInfo& i)
{
 return {};
}

int D4N_FDB_FilterDriver::load_bucket(const DoutPrefixProvider* dpp, 
      const rgw_bucket& b,
      std::unique_ptr<Bucket>* bucket, 
      optional_yield y)
{
 return {};
}

std::unique_ptr<Writer> D4N_FDB_FilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
      optional_yield y,
      rgw::sal::Object* obj,
      const ACLOwner& owner,
      const rgw_placement_rule *ptail_placement_rule,
      uint64_t olh_epoch,
      const std::string& unique_tag)
{
 return {};
}

void D4N_FDB_FilterDriver::shutdown()
{
}

} // namespace rgw::sal

#endif
