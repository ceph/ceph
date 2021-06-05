// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_PUBSUB_H
#define CEPH_RGW_PUBSUB_H

#include "rgw_sal.h"
#include "services/svc_sys_obj.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "rgw_notify_event_type.h"
#include <boost/container/flat_map.hpp>

class XMLObj;

struct rgw_s3_key_filter {
  std::string prefix_rule;
  std::string suffix_rule;
  std::string regex_rule;

  bool has_content() const;

  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(prefix_rule, bl);
    encode(suffix_rule, bl);
    encode(regex_rule, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(prefix_rule, bl);
    decode(suffix_rule, bl);
    decode(regex_rule, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_key_filter)

using KeyValueMap = boost::container::flat_map<std::string, std::string>;
using KeyMultiValueMap = std::multimap<std::string, std::string>;

struct rgw_s3_key_value_filter {
  KeyValueMap kv;
  
  bool has_content() const;
  
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(kv, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(kv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_key_value_filter)

struct rgw_s3_filter {
  rgw_s3_key_filter key_filter;
  rgw_s3_key_value_filter metadata_filter;
  rgw_s3_key_value_filter tag_filter;

  bool has_content() const;
  
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(key_filter, bl);
    encode(metadata_filter, bl);
    encode(tag_filter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(key_filter, bl);
    decode(metadata_filter, bl);
    if (struct_v >= 2) {
        decode(tag_filter, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_filter)

using OptionalFilter = std::optional<rgw_s3_filter>;

struct rgw_pubsub_topic_filter;
/* S3 notification configuration
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTnotification.html
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TopicConfiguration>
    <Filter>
      <S3Key>
        <FilterRule>
          <Name>suffix</Name>
          <Value>jpg</Value>
        </FilterRule>
      </S3Key>
      <S3Metadata>
        <FilterRule>
          <Name></Name>
          <Value></Value>
        </FilterRule>
      </S3Metadata>
      <S3Tags>
        <FilterRule>
          <Name></Name>
          <Value></Value>
        </FilterRule>
      </S3Tags>
    </Filter>
    <Id>notification1</Id>
    <Topic>arn:aws:sns:<region>:<account>:<topic></Topic>
    <Event>s3:ObjectCreated:*</Event>
    <Event>s3:ObjectRemoved:*</Event>
  </TopicConfiguration>
</NotificationConfiguration>
*/
struct rgw_pubsub_s3_notification {
  // notification id
  std::string id;
  // types of events
  rgw::notify::EventTypeList events;
  // topic ARN
  std::string topic_arn;
  // filter rules
  rgw_s3_filter filter;

  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;

  rgw_pubsub_s3_notification() = default;
  // construct from rgw_pubsub_topic_filter (used by get/list notifications)
  explicit rgw_pubsub_s3_notification(const rgw_pubsub_topic_filter& topic_filter);
};

// return true if the key matches the prefix/suffix/regex rules of the key filter
bool match(const rgw_s3_key_filter& filter, const std::string& key);

// return true if the key matches the metadata rules of the metadata filter
bool match(const rgw_s3_key_value_filter& filter, const KeyValueMap& kv);

// return true if the key matches the tag rules of the tag filter
bool match(const rgw_s3_key_value_filter& filter, const KeyMultiValueMap& kv);

// return true if the event type matches (equal or contained in) one of the events in the list
bool match(const rgw::notify::EventTypeList& events, rgw::notify::EventType event);

struct rgw_pubsub_s3_notifications {
  std::list<rgw_pubsub_s3_notification> list;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

/* S3 event records structure
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
{  
"Records":[  
  {
    "eventVersion":""
    "eventSource":"",
    "awsRegion":"",
    "eventTime":"",
    "eventName":"",
    "userIdentity":{  
      "principalId":""
    },
    "requestParameters":{
      "sourceIPAddress":""
    },
    "responseElements":{
      "x-amz-request-id":"",
      "x-amz-id-2":""
    },
    "s3":{
      "s3SchemaVersion":"1.0",
      "configurationId":"",
      "bucket":{
        "name":"",
        "ownerIdentity":{
          "principalId":""
        },
        "arn":""
        "id": ""
      },
      "object":{
        "key":"",
        "size": ,
        "eTag":"",
        "versionId":"",
        "sequencer": "",
        "metadata": ""
        "tags": ""
      }
    },
    "eventId":"",
  }
]
}*/

struct rgw_pubsub_s3_event {
  constexpr static const char* const json_type_plural = "Records";
  std::string eventVersion = "2.2";
  // aws:s3
  std::string eventSource = "ceph:s3";
  // zonegroup
  std::string awsRegion;
  // time of the request
  ceph::real_time eventTime;
  // type of the event
  std::string eventName;
  // user that sent the request
  std::string userIdentity;
  // IP address of source of the request (not implemented)
  std::string sourceIPAddress;
  // request ID (not implemented)
  std::string x_amz_request_id;
  // radosgw that received the request
  std::string x_amz_id_2;
  std::string s3SchemaVersion = "1.0";
  // ID received in the notification request
  std::string configurationId;
  // bucket name
  std::string bucket_name;
  // bucket owner
  std::string bucket_ownerIdentity;
  // bucket ARN
  std::string bucket_arn;
  // object key
  std::string object_key;
  // object size
  uint64_t object_size = 0;
  // object etag
  std::string object_etag;
  // object version id bucket is versioned
  std::string object_versionId;
  // hexadecimal value used to determine event order for specific key
  std::string object_sequencer;
  // this is an rgw extension (not S3 standard)
  // used to store a globally unique identifier of the event
  // that could be used for acking or any other identification of the event
  std::string id;
  // this is an rgw extension holding the internal bucket id
  std::string bucket_id;
  // meta data
  KeyValueMap x_meta_map;
  // tags
  KeyMultiValueMap tags;
  // opaque data received from the topic
  // could be used to identify the gateway
  std::string opaque_data;

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    encode(eventVersion, bl);
    encode(eventSource, bl);
    encode(awsRegion, bl);
    encode(eventTime, bl);
    encode(eventName, bl);
    encode(userIdentity, bl);
    encode(sourceIPAddress, bl);
    encode(x_amz_request_id, bl);
    encode(x_amz_id_2, bl);
    encode(s3SchemaVersion, bl);
    encode(configurationId, bl);
    encode(bucket_name, bl);
    encode(bucket_ownerIdentity, bl);
    encode(bucket_arn, bl);
    encode(object_key, bl);
    encode(object_size, bl);
    encode(object_etag, bl);
    encode(object_versionId, bl);
    encode(object_sequencer, bl);
    encode(id, bl);
    encode(bucket_id, bl);
    encode(x_meta_map, bl);
    encode(tags, bl);
    encode(opaque_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(4, bl);
    decode(eventVersion, bl);
    decode(eventSource, bl);
    decode(awsRegion, bl);
    decode(eventTime, bl);
    decode(eventName, bl);
    decode(userIdentity, bl);
    decode(sourceIPAddress, bl);
    decode(x_amz_request_id, bl);
    decode(x_amz_id_2, bl);
    decode(s3SchemaVersion, bl);
    decode(configurationId, bl);
    decode(bucket_name, bl);
    decode(bucket_ownerIdentity, bl);
    decode(bucket_arn, bl);
    decode(object_key, bl);
    decode(object_size, bl);
    decode(object_etag, bl);
    decode(object_versionId, bl);
    decode(object_sequencer, bl);
    decode(id, bl);
    if (struct_v >= 2) {
      decode(bucket_id, bl);
      decode(x_meta_map, bl);
    }
    if (struct_v >= 3) {
      decode(tags, bl);
    }
    if (struct_v >= 4) {
      decode(opaque_data, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_s3_event)

struct rgw_pubsub_event {
  constexpr static const char* const json_type_plural = "events";
  std::string id;
  std::string event_name;
  std::string source;
  ceph::real_time timestamp;
  JSONFormattable info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(event_name, bl);
    encode(source, bl);
    encode(timestamp, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(event_name, bl);
    decode(source, bl);
    decode(timestamp, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_event)

// settign a unique ID for an event based on object hash and timestamp
void set_event_id(std::string& id, const std::string& hash, const utime_t& ts);

struct rgw_pubsub_sub_dest {
  std::string bucket_name;
  std::string oid_prefix;
  std::string push_endpoint;
  std::string push_endpoint_args;
  std::string arn_topic;
  bool stored_secret = false;
  bool persistent = false;

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 1, bl);
    encode(bucket_name, bl);
    encode(oid_prefix, bl);
    encode(push_endpoint, bl);
    encode(push_endpoint_args, bl);
    encode(arn_topic, bl);
    encode(stored_secret, bl);
    encode(persistent, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(5, bl);
    decode(bucket_name, bl);
    decode(oid_prefix, bl);
    decode(push_endpoint, bl);
    if (struct_v >= 2) {
        decode(push_endpoint_args, bl);
    }
    if (struct_v >= 3) {
        decode(arn_topic, bl);
    }
    if (struct_v >= 4) {
        decode(stored_secret, bl);
    }
    if (struct_v >= 5) {
        decode(persistent, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  std::string to_json_str() const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_sub_dest)

struct rgw_pubsub_sub_config {
  rgw_user user;
  std::string name;
  std::string topic;
  rgw_pubsub_sub_dest dest;
  std::string s3_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(user, bl);
    encode(name, bl);
    encode(topic, bl);
    encode(dest, bl);
    encode(s3_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(user, bl);
    decode(name, bl);
    decode(topic, bl);
    decode(dest, bl);
    if (struct_v >= 2) {
      decode(s3_id, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_sub_config)

struct rgw_pubsub_topic {
  rgw_user user;
  std::string name;
  rgw_pubsub_sub_dest dest;
  std::string arn;
  std::string opaque_data;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(user, bl);
    encode(name, bl);
    encode(dest, bl);
    encode(arn, bl);
    encode(opaque_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(user, bl);
    decode(name, bl);
    if (struct_v >= 2) {
      decode(dest, bl);
      decode(arn, bl);
    }
    if (struct_v >= 3) {
      decode(opaque_data, bl);
    }
    DECODE_FINISH(bl);
  }

  string to_str() const {
    return user.tenant + "/" + name;
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  void dump_xml_as_attributes(Formatter *f) const;

  bool operator<(const rgw_pubsub_topic& t) const {
    return to_str().compare(t.to_str());
  }
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic)

struct rgw_pubsub_topic_subs {
  rgw_pubsub_topic topic;
  std::set<std::string> subs;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topic, bl);
    encode(subs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topic, bl);
    decode(subs, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic_subs)

struct rgw_pubsub_topic_filter {
  rgw_pubsub_topic topic;
  rgw::notify::EventTypeList events;
  std::string s3_id;
  rgw_s3_filter s3_filter;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(topic, bl);
    // events are stored as a vector of strings
    std::vector<std::string> tmp_events;
    const auto converter = s3_id.empty() ? rgw::notify::to_ceph_string : rgw::notify::to_string;
    std::transform(events.begin(), events.end(), std::back_inserter(tmp_events), converter);
    encode(tmp_events, bl);
    encode(s3_id, bl);
    encode(s3_filter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(topic, bl);
    // events are stored as a vector of strings
    events.clear();
    std::vector<std::string> tmp_events;
    decode(tmp_events, bl);
    std::transform(tmp_events.begin(), tmp_events.end(), std::back_inserter(events), rgw::notify::from_string);
    if (struct_v >= 2) {
      decode(s3_id, bl);
    }
    if (struct_v >= 3) {
      decode(s3_filter, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic_filter)

struct rgw_pubsub_bucket_topics {
  std::map<std::string, rgw_pubsub_topic_filter> topics;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topics, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_bucket_topics)

struct rgw_pubsub_topics {
  std::map<std::string, rgw_pubsub_topic_subs> topics;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topics, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topics)

static std::string pubsub_oid_prefix = "pubsub.";

class RGWPubSub
{
  friend class Bucket;

  rgw::sal::RGWRadosStore *store;
  const std::string tenant;
  RGWSysObjectCtx obj_ctx;

  rgw_raw_obj meta_obj;

  std::string meta_oid() const {
    return pubsub_oid_prefix + tenant;
  }

  std::string bucket_meta_oid(const rgw_bucket& bucket) const {
    return pubsub_oid_prefix + tenant + ".bucket." + bucket.name + "/" + bucket.marker;
  }

  std::string sub_meta_oid(const string& name) const {
    return pubsub_oid_prefix + tenant + ".sub." + name;
  }

  template <class T>
  int read(const rgw_raw_obj& obj, T* data, RGWObjVersionTracker* objv_tracker);

  template <class T>
  int write(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, const T& info,
	    RGWObjVersionTracker* obj_tracker, optional_yield y);

  int remove(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, RGWObjVersionTracker* objv_tracker,
	     optional_yield y);

  int read_topics(rgw_pubsub_topics *result, RGWObjVersionTracker* objv_tracker);
  int write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_topics& topics,
			RGWObjVersionTracker* objv_tracker, optional_yield y);

public:
  RGWPubSub(rgw::sal::RGWRadosStore *_store, const std::string& tenant);

  class Bucket {
    friend class RGWPubSub;
    RGWPubSub *ps;
    rgw_bucket bucket;
    rgw_raw_obj bucket_meta_obj;

    // read the list of topics associated with a bucket and populate into result
    // use version tacker to enforce atomicity between read/write
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int read_topics(rgw_pubsub_bucket_topics *result, RGWObjVersionTracker* objv_tracker);
    // set the list of topics associated with a bucket
    // use version tacker to enforce atomicity between read/write
    // return 0 on success, error code otherwise
    int write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_bucket_topics& topics,
		     RGWObjVersionTracker* objv_tracker, optional_yield y);
  public:
    Bucket(RGWPubSub *_ps, const rgw_bucket& _bucket) : ps(_ps), bucket(_bucket) {
      ps->get_bucket_meta_obj(bucket, &bucket_meta_obj);
    }

    // read the list of topics associated with a bucket and populate into result
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int get_topics(rgw_pubsub_bucket_topics *result);
    // adds a topic + filter (event list, and possibly name metadata or tags filters) to a bucket
    // assigning a notification name is optional (needed for S3 compatible notifications)
    // if the topic already exist on the bucket, the filter event list may be updated
    // for S3 compliant notifications the version with: s3_filter and notif_name should be used
    // return -ENOENT if the topic does not exists
    // return 0 on success, error code otherwise
    int create_notification(const DoutPrefixProvider *dpp, const string& topic_name, const rgw::notify::EventTypeList& events, optional_yield y);
    int create_notification(const DoutPrefixProvider *dpp, const string& topic_name, const rgw::notify::EventTypeList& events, OptionalFilter s3_filter, const std::string& notif_name, optional_yield y);
    // remove a topic and filter from bucket
    // if the topic does not exists on the bucket it is a no-op (considered success)
    // return -ENOENT if the topic does not exists
    // return 0 on success, error code otherwise
    int remove_notification(const DoutPrefixProvider *dpp, const string& topic_name, optional_yield y);
    // remove all notifications (and autogenerated topics) associated with the bucket
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int remove_notifications(const DoutPrefixProvider *dpp, optional_yield y);
  };

  // base class for subscription
  class Sub {
    friend class RGWPubSub;
  protected:
    RGWPubSub* const ps;
    const std::string sub;
    rgw_raw_obj sub_meta_obj;

    int read_sub(rgw_pubsub_sub_config *result, RGWObjVersionTracker* objv_tracker);
    int write_sub(const DoutPrefixProvider *dpp, const rgw_pubsub_sub_config& sub_conf,
		  RGWObjVersionTracker* objv_tracker, optional_yield y);
    int remove_sub(const DoutPrefixProvider *dpp, RGWObjVersionTracker* objv_tracker, optional_yield y);
  public:
    Sub(RGWPubSub *_ps, const std::string& _sub) : ps(_ps), sub(_sub) {
      ps->get_sub_meta_obj(sub, &sub_meta_obj);
    }

    virtual ~Sub() = default;

    int subscribe(const DoutPrefixProvider *dpp, const string& topic_name, const rgw_pubsub_sub_dest& dest, optional_yield y,
		  const std::string& s3_id="");
    int unsubscribe(const DoutPrefixProvider *dpp, const string& topic_name, optional_yield y);
    int get_conf(rgw_pubsub_sub_config* result);
    
    static const int DEFAULT_MAX_EVENTS = 100;
    // followint virtual methods should only be called in derived
    virtual int list_events(const DoutPrefixProvider *dpp, const string& marker, int max_events) {ceph_assert(false);}
    virtual int remove_event(const DoutPrefixProvider *dpp, const string& event_id) {ceph_assert(false);}
    virtual void dump(Formatter* f) const {ceph_assert(false);}
  };

  // subscription with templated list of events to support both S3 compliant and Ceph specific events
  template<typename EventType>
  class SubWithEvents : public Sub {
  private:
    struct list_events_result {
      std::string next_marker;
      bool is_truncated{false};
      void dump(Formatter *f) const;
      std::vector<EventType> events;
    } list;

  public:
    SubWithEvents(RGWPubSub *_ps, const string& _sub) : Sub(_ps, _sub) {}

    virtual ~SubWithEvents() = default;
    
    int list_events(const DoutPrefixProvider *dpp, const string& marker, int max_events) override;
    int remove_event(const DoutPrefixProvider *dpp, const string& event_id) override;
    void dump(Formatter* f) const override;
  };

  using BucketRef = std::shared_ptr<Bucket>;
  using SubRef = std::shared_ptr<Sub>;

  BucketRef get_bucket(const rgw_bucket& bucket) {
    return std::make_shared<Bucket>(this, bucket);
  }

  SubRef get_sub(const string& sub) {
    return std::make_shared<Sub>(this, sub);
  }
  
  SubRef get_sub_with_events(const string& sub) {
    auto tmpsub = Sub(this, sub);
    rgw_pubsub_sub_config conf;
    if (tmpsub.get_conf(&conf) < 0) {
      return nullptr;
    }
    if (conf.s3_id.empty()) {
      return std::make_shared<SubWithEvents<rgw_pubsub_event>>(this, sub);
    }
    return std::make_shared<SubWithEvents<rgw_pubsub_s3_event>>(this, sub);
  }

  void get_meta_obj(rgw_raw_obj *obj) const;
  void get_bucket_meta_obj(const rgw_bucket& bucket, rgw_raw_obj *obj) const;

  void get_sub_meta_obj(const string& name, rgw_raw_obj *obj) const;

  // get all topics (per tenant, if used)) and populate them into "result"
  // return 0 on success or if no topics exist, error code otherwise
  int get_topics(rgw_pubsub_topics *result);
  // get a topic with its subscriptions by its name and populate it into "result"
  // return -ENOENT if the topic does not exists 
  // return 0 on success, error code otherwise
  int get_topic(const string& name, rgw_pubsub_topic_subs *result);
  // get a topic with by its name and populate it into "result"
  // return -ENOENT if the topic does not exists 
  // return 0 on success, error code otherwise
  int get_topic(const string& name, rgw_pubsub_topic *result);
  // create a topic with a name only
  // if the topic already exists it is a no-op (considered success)
  // return 0 on success, error code otherwise
  int create_topic(const DoutPrefixProvider *dpp, const string& name, optional_yield y);
  // create a topic with push destination information and ARN
  // if the topic already exists the destination and ARN values may be updated (considered succsess)
  // return 0 on success, error code otherwise
  int create_topic(const DoutPrefixProvider *dpp, const string& name, const rgw_pubsub_sub_dest& dest, const std::string& arn, const std::string& opaque_data, optional_yield y);
  // remove a topic according to its name
  // if the topic does not exists it is a no-op (considered success)
  // return 0 on success, error code otherwise
  int remove_topic(const DoutPrefixProvider *dpp, const string& name, optional_yield y);
};


template <class T>
int RGWPubSub::read(const rgw_raw_obj& obj, T* result, RGWObjVersionTracker* objv_tracker)
{
  bufferlist bl;
  int ret = rgw_get_system_obj(obj_ctx,
                               obj.pool, obj.oid,
                               bl,
                               objv_tracker,
                               nullptr, null_yield, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*result, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

template <class T>
int RGWPubSub::write(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, const T& info,
			 RGWObjVersionTracker* objv_tracker, optional_yield y)
{
  bufferlist bl;
  encode(info, bl);

  int ret = rgw_put_system_obj(dpp, obj_ctx, obj.pool, obj.oid,
			       bl, false, objv_tracker,
			       real_time(), y);
  if (ret < 0) {
    return ret;
  }

  obj_ctx.invalidate(obj);
  return 0;
}

#endif
