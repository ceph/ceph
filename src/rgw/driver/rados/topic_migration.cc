// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "topic_migration.h"
#include "services/svc_zone.h"
#include "rgw_sal_rados.h"

namespace rgwrados::topic_migration {

namespace {

// migrate v1 notification metadata for a single bucket
int migrate_notification(const DoutPrefixProvider* dpp, optional_yield y,
                         rgw::sal::RadosStore* driver, const rgw_raw_obj& obj)
{
  // parse bucket name and marker of out "pubsub.{tenant}.bucket.{name}/{marker}"
  std::string name;
  std::string marker;

  // in a for-loop that retries ECANCELED errors:
  // {
  // load the corresponding bucket by name
  // break if marker doesn't match loaded bucket's
  // break if RGW_ATTR_BUCKET_NOTIFICATION xattr already exists
  // write RGW_ATTR_BUCKET_NOTIFICATION xattr
  // }

  // delete v1 notification obj with Bucket::remove_topics()
  return 0;
}

// migrate topics for a given tenant
int migrate_topics(const DoutPrefixProvider* dpp, optional_yield y,
                   rgw::sal::RadosStore* driver,
                   const rgw_raw_obj& topics_obj)
{
  // parse tenant name out of topics_obj "pubsub.{tenant}"
  std::string tenant; // TODO

  // migrate the topics
  rgw_pubsub_topics topics;
  RGWObjVersionTracker topics_objv;
  int r = driver->read_topics(tenant, topics, &topics_objv, y, dpp);
  if (r == -ENOENT) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    ldpp_dout(dpp, 1) << "failed to read v1 topics from " << topics_obj
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  constexpr bool exclusive = true; // don't overwrite any existing v2 metadata
  for (const auto& [name, topic] : topics.topics) {
    // write the v2 topic
    RGWObjVersionTracker objv;
    objv.generate_new_write_ver(dpp->get_cct());
    r = driver->write_topic_v2(topic, exclusive, objv, y, dpp);
    if (r == -EEXIST) {
      continue; // ok, someone else already migrated
    }
    if (r < 0) {
      ldpp_dout(dpp, 1) << "v1 topic migration for " << topic.name
          << " failed: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  // remove the v1 topics metadata (this destroys the lock too)
  r = driver->remove_topics(tenant, &topics_objv, y, dpp);
  if (r == -ECANCELED) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    ldpp_dout(dpp, 1) << "failed to remove migrated v1 topics obj "
        << topics_obj << " with: " << cpp_strerror(r) << dendl;
    return r;
  }
  return r;
}

} // anonymous namespace

int migrate(const DoutPrefixProvider* dpp,
            rgw::sal::RadosStore* driver,
            boost::asio::io_context& context,
            spawn::yield_context yield)
{
  auto y = optional_yield{context, yield};

  ldpp_dout(dpp, 1) << "starting v1 topic migration.." << dendl;

  librados::Rados* rados = driver->getRados()->get_rados_handle();
  const rgw_pool& pool = driver->svc()->zone->get_zone_params().log_pool;
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, rados, pool, ioctx);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "failed to initialize log pool for listing with: "
        << cpp_strerror(r) << dendl;
    return r;
  }

  // loop over all objects with oid prefix "pubsub."
  auto filter = rgw::AccessListFilterPrefix(rgw::sal::pubsub_oid_prefix);
  constexpr uint32_t max = 100;
  std::string marker;
  bool truncated = false;

  std::vector<std::string> oids;
  do {
    oids.clear();
    int r = rgw_list_pool(dpp, ioctx, max, filter, marker, &oids, &truncated);
    if (r == -ENOENT) {
      r = 0;
      break;
    }
    if (r < 0) {
      ldpp_dout(dpp, 1) << "failed to list v1 topic metadata with: "
          << cpp_strerror(r) << dendl;
      return r;
    }

    for (const std::string& oid : oids) {
      const auto obj = rgw_raw_obj{pool, oid};
      if (oid.find(".bucket.") != oid.npos) {
        ldpp_dout(dpp, 4) << "migrating v1 topics " << oid << dendl;
        (void) migrate_notification(dpp, y, driver, obj);
      } else {
        ldpp_dout(dpp, 4) << "migrating v1 bucket notification " << oid << dendl;
        (void) migrate_topics(dpp, y, driver, obj);
      }
    }
    if (!oids.empty()) {
      marker = oids.back(); // update marker for next listing
    }
  } while (truncated);

  ldpp_dout(dpp, 1) << "finished v1 topic migration" << dendl;
  return 0;
}

} // rgwrados::topic_migration
