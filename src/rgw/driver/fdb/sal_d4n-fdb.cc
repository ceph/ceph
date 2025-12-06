// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "sal_d4n-fdb.h"

#include "rgw_perf_counters.h"

extern "C" {

rgw::sal::Driver* newD4N_FDB_Filter(CephContext *cct, const DoutPrefixProvider *dpp, rgw::sal::Driver *next_driver)
{
 return new rgw::sal::d4n::FDB_FilterDriver(next_driver);
}

} // extern "C"

namespace rgw::sal::d4n {

// struct FDB_FilterDriver:

std::unique_ptr<Bucket> FDB_FilterDriver::get_bucket(const RGWBucketInfo& i)
{
  // Return an instance of our own type so that callers up the hierarchy get
  // our polymorphic behavior:
  return std::make_unique<FDB_FilterBucket>(next->get_bucket(i), this);
}

/*
int FDB_FilterDriver::load_bucket(
  const DoutPrefixProvider* dpp, 
  const rgw_bucket& b,
  std::unique_ptr<Bucket>* bucket, optional_yield y)
{
}
*/

/*
 virtual std::unique_ptr<User> get_user(const rgw_user& u) override;

 virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

 virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* obj,
			  const ACLOwner& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t olh_epoch,
			  const std::string& unique_tag) override;
*/

} // namespace rgw::sal::d4n

namespace rgw::sal::d4n {

// FDB_FilterBucket:

int FDB_FilterBucket::create(const DoutPrefixProvider *dpp, const CreateParams& params, optional_yield y) 
{
 // Again, weird, but whatever:
 return next->create(dpp, params, y);
}

std::unique_ptr<Object> FDB_FilterBucket::get_object(const rgw_obj_key& key) 
{
 std::unique_ptr<Object> o = next->get_object(key);

 // Return a polymorphic version:  
 return std::make_unique<FDB_FilterObject>(std::move(o), owning_driver, this);
}

int FDB_FilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max, ListResults& results, optional_yield y) 
{
 return 0;
}

/*
virtual std::unique_ptr<MultipartUpload> FDB_FilterBucket::get_multipart_upload(
  const std::string& oid, 
  std::optional<std::string> upload_id = std::nullopt, 
  ACLOwner owner = {}, 
  ceph::real_time mtime = real_clock::now())
{
 return {};
}
*/

} // namespace rgw::sal::d4n
