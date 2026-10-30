// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "rgw_object_lock.h"
#include "rgw_pubsub.h"
#include "rgw_sal_store.h"

namespace rgw { namespace sal {

/* A filesystem driver's notification, shared by the posix and nsfs
 * drivers:  matched against the bucket's notification configuration at
 * reserve, and sent to each matched topic's endpoint, inline, at commit.
 * Its record is the rados driver's populate_event()'s, field for field,
 * so a consumer reads one schema whichever driver sent it. */
class FSNotification : public StoreNotification {
protected:
  Driver* driver;
  rgw::sal::Bucket* bucket = nullptr;
  std::string user_id;
  std::string user_tenant;
  std::string req_id;
  /* the client's address, as the policy environment holds it;  empty
   * for the driver's own events */
  std::string source_ip;
  rgw_pubsub_bucket_topics bucket_topics;
  /* each topic a notification applies to, with the event type it
   * matched, as the rados driver's reservation holds them */
  std::vector<std::pair<rgw_pubsub_topic_filter, rgw::notify::EventType>> matched;
  RGWObjTags* obj_tags_ptr = nullptr;
  KeyValueMap x_meta_map;
  /* the object's retention, for an s3:ObjectRetention:Put event */
  std::optional<RGWObjectRetention> retention;
public:
  /* for a request */
  FSNotification(Driver* _driver, Object* _obj, Object* _src_obj,
		 rgw::notify::EventType _type, req_state* s);
  /* for the driver's own events */
  FSNotification(Driver* _driver, Object* _obj, Object* _src_obj,
		 const rgw::notify::EventTypeList& _types,
		 rgw::sal::Bucket* _bucket, std::string _user_id,
		 std::string _user_tenant, std::string _req_id)
    : StoreNotification(_obj, _src_obj, _types),
      driver(_driver), bucket(_bucket),
      user_id(std::move(_user_id)),
      user_tenant(std::move(_user_tenant)),
      req_id(std::move(_req_id)) {}
  ~FSNotification() = default;

  virtual int publish_reserve(const DoutPrefixProvider *dpp,
			      RGWObjTags* obj_tags = nullptr) override;
  virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			     const ceph::real_time& mtime, const std::string& etag,
			     const std::string& version) override;
  virtual void set_object_retention(const RGWObjectRetention& r) override {
    retention = r;
  }
};

} } // namespace rgw::sal
