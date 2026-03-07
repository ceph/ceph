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

#ifndef CEPH_RGW_SAL_D4N_FDB_H 
 #define CEPH_RGW_SAL_D4N_FDB_H

#include <fmt/core.h>

#include "fdb/fdb.h"

#include "rgw_sal.h"
#include "rgw_role.h"
#include "rgw_sal_filter.h"
#include "rgw_aio_throttle.h"

#include "common/dout_fmt.h"

namespace rgw::sal::d4n {

struct FDB_FilterDriver;
struct FDB_FilterBucket;

struct FDB_FilterDriver : rgw::sal::FilterDriver
{
 CephContext  *cct = nullptr;
 const DoutPrefixProvider *dpp = nullptr;

 ceph::libfdb::database_handle dbh;

 public:
 FDB_FilterDriver(Driver *next_driver)
  : rgw::sal::FilterDriver(next_driver)
 {
#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context
 dout_fmt(0, "JFW: FDB_FilterDriver(): ok");
 } 

 public:
 virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override
 {
#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context
 dout_fmt(0, "JFW: FDB_FilterDriver::initialize(): begin");

    cct = cct;
    dpp = dpp;

// JFW: get dbfile from config: g_conf()->fdb_...
    dbh = ceph::libfdb::create_database();

 dout_fmt(0, "JFW: FDB_FilterDriver::initialize(): ok");

    return 0;
 }

 virtual void shutdown() override 
 {
    dout_fmt(0, "JFW: FDB_FilterDriver::shutdown(): begin");

    // Appearantly this is required:
    next->shutdown();

    dout_fmt(0, "JFW: FDB_FilterDriver::shutdown(): ok");
 }

 virtual void finalize() override
 {
    dout_fmt(0, "JFW: FDB_FilterDriver::finalize(): begin");

    // ...this should happen once and only once during program lifetime:
    // JFW: I don't know that this is actually the right place to do this,
    // since other parts of Ceph may use libfdb:
    ceph::libfdb::shutdown_libfdb();

    // It seems like this is our responsibility for some reason:
    // JFW: I'm not sure, it's entirely unclear:
    next->finalize();

    dout_fmt(0, "JFW: FDB_FilterDriver::finalize(): ok");
 }

 std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override; 

/*
 int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                 std::unique_ptr<Bucket>* bucket, optional_yield y) override;
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
};

struct FDB_FilterBucket : FilterBucket
{
 FDB_FilterDriver *owning_driver;

 public:
 FDB_FilterBucket(std::unique_ptr<Bucket> bucket, FDB_FilterDriver *owning_driver)
  : FilterBucket(std::move(bucket)),
    owning_driver(owning_driver)
 {}

 public:
 virtual int create(const DoutPrefixProvider *dpp, const CreateParams& params, optional_yield y) override;


 public:
 virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;

 virtual int list(const DoutPrefixProvider* dpp, ListParams& params, int max, ListResults& results, optional_yield y) override;

/*JFW:
 public:
 virtual std::unique_ptr<MultipartUpload> get_multipart_upload(const std::string& oid, 
                                                               std::optional<std::string> upload_id = std::nullopt, 
                                                               ACLOwner owner = {}, 
                                                               ceph::real_time mtime = real_clock::now()) override;
*/
};

struct FDB_FilterObject : FilterObject
{
 FDB_FilterDriver *owner_driver = nullptr;

 FDB_FilterObject(std::unique_ptr<Object> next_object, FDB_FilterDriver *owner_driver_)
  : FilterObject(std::move(next_object)),
    owner_driver(owner_driver_)
 {}

 FDB_FilterObject(std::unique_ptr<Object> next_object, FDB_FilterDriver *owner_driver_, Bucket *bucket_ptr)
  : FilterObject(std::move(next_object), bucket_ptr), 
    owner_driver(owner_driver_)
 {}

 FDB_FilterObject(FDB_FilterObject& src, FDB_FilterDriver *owner_driver_)
  : FilterObject(src),
    owner_driver(owner_driver_)
 {}

 public:
 
};

} // namespace rgw::sal::d4n

#endif
