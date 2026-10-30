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

#include "fs_notification.h"

#include <boost/algorithm/hex.hpp>

#include "rgw_arn.h"
#include "rgw_common.h"
#include "rgw_pubsub_push.h"
#include "rgw_s3_filter.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal {

FSNotification::FSNotification(Driver* _driver, Object* _obj, Object* _src_obj,
			       rgw::notify::EventType _type, req_state* s)
  : StoreNotification(_obj, _src_obj, {_type}),
    driver(_driver), bucket(s->bucket.get()),
    user_id(to_string(s->owner.id)),
    user_tenant(s->owner.id.index() == 0
		? std::get<rgw_user>(s->owner.id).tenant : ""),
    req_id(s->trans_id)
{
  /* the request's user metadata, as the rados driver's reservation
   * filters it */
  for (const auto& [k, v] : s->info.x_meta_map) {
    if (k.starts_with(RGW_AMZ_META_PREFIX)) {
      x_meta_map.emplace(k, v);
    }
  }
  if (auto i = s->env.find("aws:SourceIp"); i != s->env.end()) {
    source_ip = i->second;
  }
}

int FSNotification::publish_reserve(const DoutPrefixProvider *dpp,
				      RGWObjTags* obj_tags)
{
  obj_tags_ptr = obj_tags;

  if (!bucket) {
    return 0;
  }

  int ret = get_bucket_notifications(dpp, bucket, bucket_topics);
  if (ret < 0) {
    return ret;
  }

  const std::string obj_name = obj ? obj->get_name() : "";

  /* one entry per event type a topic matches, as the rados driver's
   * publish_reserve() makes them, so each event is named for its own
   * type */
  for (auto& [name, filter] : bucket_topics.topics) {
    if (!match(filter.s3_filter.key_filter, obj_name)) {
      continue;
    }

    if (!filter.s3_filter.metadata_filter.kv.empty()) {
      if (!match(filter.s3_filter.metadata_filter, x_meta_map)) {
	continue;
      }
    }

    if (!filter.s3_filter.tag_filter.kv.empty()) {
      KeyMultiValueMap tags;
      if (obj_tags) {
	tags = obj_tags->get_tags();
      }
      if (!match(filter.s3_filter.tag_filter, tags)) {
	continue;
      }
    }

    for (auto req_type : event_types) {
      if (match(filter.events, req_type)) {
	matched.emplace_back(filter, req_type);
      }
    }
  }

  return 0;
}

/* The object a notification's metadata and tags are read from:  a
 * copy's source, else the object, with its attributes loaded.  The rados
 * driver's get_object_with_attributes().  Null when they cannot be. */
static rgw::sal::Object* object_with_attributes(const DoutPrefixProvider* dpp,
						rgw::sal::Object* obj,
						rgw::sal::Object* src_obj,
						rgw::sal::Bucket* bucket)
{
  auto* o = src_obj ? src_obj : obj;
  if (!o) {
    return nullptr;
  }
  if (o->get_attrs().empty()) {
    if (!o->get_bucket()) {
      o->set_bucket(bucket);
    }
    if (int ret = o->get_obj_attrs(null_yield, dpp); ret < 0) {
      ldpp_dout(dpp, 20) << "failed to get attributes from object: "
	<< o->get_key() << ". ret = " << ret << dendl;
      return nullptr;
    }
  }
  return o;
}

int FSNotification::publish_commit(const DoutPrefixProvider* dpp,
				     uint64_t size,
				     const ceph::real_time& mtime,
				     const std::string& etag,
				     const std::string& version)
{
  if (matched.empty()) {
    return 0;
  }

  /* The record is the rados driver's populate_event()'s, field for
   * field, so a consumer reads one schema whichever driver sent it.
   * The metadata and tags are read once, for every topic. */
  KeyValueMap meta = x_meta_map;
  KeyMultiValueMap tags;
  if (auto* o = object_with_attributes(dpp, obj, src_obj, bucket); o) {
    for (auto& [k, v] : o->get_attrs()) {
      if (k.starts_with(RGW_ATTR_META_PREFIX)) {
	meta.emplace(k.substr(sizeof(RGW_ATTR_PREFIX) - 1), v.to_str().c_str());
      }
    }
    if (obj_tags_ptr && !obj_tags_ptr->get_tags().empty()) {
      tags = obj_tags_ptr->get_tags();
    } else if (auto t = o->get_attrs().find(RGW_ATTR_TAGS);
	       t != o->get_attrs().end()) {
      RGWObjTags obj_tags;
      try {
	auto bi = t->second.cbegin();
	decode(obj_tags, bi);
	tags = obj_tags.get_tags();
      } catch (const buffer::error&) {
	/* not able to decode tags */
      }
    }
  }
  const std::string region =
    driver->get_zone()->get_zonegroup().get_api_name();

  for (auto& [filter, event_type] : matched) {
    const auto& dest = filter.topic.dest;
    if (dest.push_endpoint.empty()) {
      continue;
    }

    rgw_pubsub_s3_event event;
    event.eventTime = mtime;
    event.eventName = rgw::notify::to_event_string(event_type);
    event.userIdentity = user_id;
    event.sourceIPAddress = source_ip;
    event.x_amz_request_id = req_id;
    event.x_amz_id_2 = driver->get_host_id();
    event.configurationId = filter.s3_id;
    event.opaque_data = filter.topic.opaque_data;
    if (bucket) {
      event.bucket_name = bucket->get_name();
      event.bucket_ownerIdentity = to_string(bucket->get_owner());
      event.bucket_id = bucket->get_bucket_id();
      rgw::ARN bucket_arn(bucket->get_key());
      if (driver->ctx()->_conf.get_val<bool>("rgw_notification_bucket_arn_region")) {
	bucket_arn.region = region;
      }
      event.bucket_arn = to_string(bucket_arn);
    }
    if (obj) {
      event.object_key = obj->get_name();
    }
    event.object_size = size;
    event.object_etag = etag;
    event.object_versionId = version;
    event.awsRegion = region;
    /* the timestamp as the key's sequence id, hex encoded */
    const utime_t ts(real_clock::now());
    boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t),
			  std::back_inserter(event.object_sequencer));
    set_event_id(event.id, etag, ts);
    event.x_meta_map = meta;
    event.tags = tags;
    if (retention) {
      event.object_retention_mode = retention->get_mode();
      event.object_retention_until = retention->get_retain_until_date();
    }

    try {
      RGWHTTPArgs args(dest.push_endpoint_args, dpp);
      auto endpoint = RGWPubSubEndpoint::create(
	  dest.push_endpoint, filter.topic.name, args,
	  dpp->get_cct());
      int r = endpoint->send(dpp, event, null_yield);
      if (r < 0) {
	ldpp_dout(dpp, 1) << "ERROR: notification endpoint send failed: "
			  << dest.push_endpoint << " ret=" << r << dendl;
      }
    } catch (const RGWPubSubEndpoint::configuration_error& e) {
      ldpp_dout(dpp, 1) << "ERROR: notification endpoint config error: "
			<< e.what() << dendl;
    }
  }

  return 0;
}

} } // namespace rgw::sal
