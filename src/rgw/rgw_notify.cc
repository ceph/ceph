// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "rgw_notify.h"
#include <memory>
#include <boost/algorithm/hex.hpp>
#include "rgw_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_perf_counters.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::notify {

// populate record from request
void populate_record_from_request(const req_state *s, 
        const rgw::sal::RGWObject* obj,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        rgw_pubsub_s3_record& record) { 
  record.eventTime = mtime;
  record.eventName = to_string(event_type);
  record.userIdentity = s->user->get_id().id;    // user that triggered the change
  record.x_amz_request_id = s->req_id;          // request ID of the original change
  record.x_amz_id_2 = s->host_id;               // RGW on which the change was made
  // configurationId is filled from notification configuration
  record.bucket_name = s->bucket_name;
  record.bucket_ownerIdentity = s->bucket_owner.get_id().id;
  record.bucket_arn = to_string(rgw::ARN(s->bucket->get_bi()));
  record.object_key = obj->get_name();
  record.object_size = obj->get_obj_size();
  record.object_etag = etag;
  record.object_versionId = obj->get_instance();
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(record.object_sequencer));
  set_event_id(record.id, etag, ts);
  record.bucket_id = s->bucket->get_bucket_id();
  // pass meta data
  record.x_meta_map = s->info.x_meta_map;
  // pass tags
  record.tags = s->tagset.get_tags();
  // opaque data will be filled from topic configuration
}

bool match(const rgw_pubsub_topic_filter& filter, const req_state* s, EventType event) {
  if (!::match(filter.events, event)) { 
    return false;
  }
  if (!::match(filter.s3_filter.key_filter, s->object->get_name())) {
    return false;
  }
  if (!::match(filter.s3_filter.metadata_filter, s->info.x_meta_map)) {
    return false;
  }
  if (!::match(filter.s3_filter.tag_filter, s->tagset.get_tags())) {
    return false;
  }
  return true;
}

int publish(const req_state* s, 
        rgw::sal::RGWObject* obj,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        rgw::sal::RGWRadosStore* store) {
    RGWUserPubSub ps_user(store, s->user->get_id());
    RGWUserPubSub::Bucket ps_bucket(&ps_user, s->bucket->get_bi());
    rgw_pubsub_bucket_topics bucket_topics;
    auto rc = ps_bucket.get_topics(&bucket_topics);
    if (rc < 0) {
        // failed to fetch bucket topics
        return rc;
    }
    rgw_pubsub_s3_record record;
    populate_record_from_request(s, obj, mtime, etag, event_type, record);
    bool event_handled = false;
    bool event_should_be_handled = false;
    for (const auto& bucket_topic : bucket_topics.topics) {
        const rgw_pubsub_topic_filter& topic_filter = bucket_topic.second;
        const rgw_pubsub_topic& topic_cfg = topic_filter.topic;
        if (!match(topic_filter, s, event_type)) {
            // topic does not apply to req_state
            continue;
        }
        event_should_be_handled = true;
        record.configurationId = topic_filter.s3_id;
        record.opaque_data = topic_cfg.opaque_data;
        ldout(s->cct, 20) << "notification: '" << topic_filter.s3_id << 
            "' on topic: '" << topic_cfg.dest.arn_topic << 
            "' and bucket: '" << s->bucket->get_name() <<
            "' (unique topic: '" << topic_cfg.name <<
            "') apply to event of type: '" << to_string(event_type) << "'" << dendl;
        try {
            // TODO add endpoint LRU cache
            const auto push_endpoint = RGWPubSubEndpoint::create(topic_cfg.dest.push_endpoint, 
                    topic_cfg.dest.arn_topic,
                    RGWHTTPArgs(topic_cfg.dest.push_endpoint_args), 
                    s->cct);
            const std::string push_endpoint_str = push_endpoint->to_str();
            ldout(s->cct, 20) << "push endpoint created: " << push_endpoint_str << dendl;
            auto rc = push_endpoint->send_to_completion_async(s->cct, record, s->yield);
            if (rc < 0) {
                // bail out on first error
                // TODO: add conf for bail out policy
                ldout(s->cct, 1) << "push to endpoint " << push_endpoint_str << " failed, with error: " << rc << dendl;
                if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
                return rc;
            }
            if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
            ldout(s->cct, 20) << "successfull push to endpoint " << push_endpoint_str << dendl;
            event_handled = true;
        } catch (const RGWPubSubEndpoint::configuration_error& e) {
            ldout(s->cct, 1) << "ERROR: failed to create push endpoint: " 
                << topic_cfg.dest.push_endpoint << " due to: " << e.what() << dendl;
            if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
            return -EINVAL;
        }
    }

    if (event_should_be_handled) {
        // not counting events with no notifications or events that are filtered
        // counting a single event, regardless of the number of notifications it sends
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_event_triggered);
        if (!event_handled) {
            // all notifications for this event failed
            if (perfcounter) perfcounter->inc(l_rgw_pubsub_event_lost);
        }
    }

    return 0;
}

}

