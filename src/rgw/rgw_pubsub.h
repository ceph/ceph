#ifndef CEPH_RGW_PUBSUB_H
#define CEPH_RGW_PUBSUB_H

#include "rgw_common.h"


enum RGWPubSubEventType {
  EVENT_UNKNOWN        = 0,
  OBJECT_CREATE        = 1,
  OBJECT_DELETE        = 2,
  DELETE_MARKER_CREATE = 3,
};

struct rgw_pubsub_event {
  string id;
  rgw_bucket bucket;
  rgw_obj_key key;
  ceph::real_time mtime;

  RGWPubSubEventType event;
  ceph::real_time timestamp;

  std::vector<std::pair<std::string, std::string> > attrs;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(bucket, bl);
    encode(key, bl);
    encode(mtime, bl);
    uint32_t e = (uint32_t)event;
    encode(e, bl);
    encode(timestamp, bl);
    encode(attrs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(bucket, bl);
    decode(key, bl);
    decode(mtime, bl);
    uint32_t e;
    decode(e, bl);
    event = (RGWPubSubEventType)e;
    decode(timestamp, bl);
    decode(attrs, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_event)

struct rgw_pubsub_user_sub_dest {
  string bucket_name;
  string oid_prefix;
  string push_endpoint;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket_name, bl);
    encode(oid_prefix, bl);
    encode(push_endpoint, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket_name, bl);
    decode(oid_prefix, bl);
    decode(push_endpoint, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_user_sub_dest)

struct rgw_pubsub_user_sub_config {
  rgw_user user;
  string name;
  string topic;
  rgw_pubsub_user_sub_dest dest;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(user, bl);
    encode(name, bl);
    encode(topic, bl);
    encode(dest, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(user, bl);
    decode(name, bl);
    decode(topic, bl);
    decode(dest, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_user_sub_config)

struct rgw_pubsub_user_topic {
  string name;
  rgw_bucket bucket;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(bucket, bl);
    DECODE_FINISH(bl);
  }

  const string& to_str() const {
    return name;
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_user_topic)

struct rgw_pubsub_user_topic_info {
  rgw_user user;
  rgw_pubsub_user_topic topic;
  set<string> subs;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(user, bl);
    encode(topic, bl);
    encode(subs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(user, bl);
    decode(topic, bl);
    decode(subs, bl);
    DECODE_FINISH(bl);
  }

  string to_str() const {
    return user.to_str() + "/" + topic.name;
  }

  void dump(Formatter *f) const;

  bool operator<(const rgw_pubsub_user_topic& t) const {
    return to_str().compare(t.to_str());
  }
};
WRITE_CLASS_ENCODER(rgw_pubsub_user_topic_info)

struct rgw_pubsub_user_topics {
  map<string, rgw_pubsub_user_topic_info> topics;

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
WRITE_CLASS_ENCODER(rgw_pubsub_user_topics)

#endif
