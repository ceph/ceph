#ifndef CEPH_RGW_PUBSUB_H
#define CEPH_RGW_PUBSUB_H

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

#include "services/svc_sys_obj.h"


struct rgw_pubsub_event {
  string id;
  string event;
  string source;
  ceph::real_time timestamp;
  JSONFormattable info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(event, bl);
    encode(source, bl);
    encode(timestamp, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(event, bl);
    decode(source, bl);
    decode(timestamp, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_event)

struct rgw_pubsub_sub_dest {
  string bucket_name;
  string oid_prefix;
  string push_endpoint;
  string push_endpoint_args;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket_name, bl);
    encode(oid_prefix, bl);
    encode(push_endpoint, bl);
    encode(push_endpoint_args, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(bucket_name, bl);
    decode(oid_prefix, bl);
    decode(push_endpoint, bl);
    if (struct_v >= 2) {
        decode(push_endpoint_args, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_sub_dest)

struct rgw_pubsub_sub_config {
  rgw_user user;
  string name;
  string topic;
  rgw_pubsub_sub_dest dest;

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
WRITE_CLASS_ENCODER(rgw_pubsub_sub_config)

struct rgw_pubsub_topic {
  rgw_user user;
  string name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(user, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(user, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }

  string to_str() const {
    return user.to_str() + "/" + name;
  }

  void dump(Formatter *f) const;

  bool operator<(const rgw_pubsub_topic& t) const {
    return to_str().compare(t.to_str());
  }
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic)

struct rgw_pubsub_topic_subs {
  rgw_pubsub_topic topic;
  set<string> subs;

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
  set<string, ltstr_nocase> events;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topic, bl);
    encode(events, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topic, bl);
    decode(events, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic_filter)

struct rgw_pubsub_bucket_topics {
  map<string, rgw_pubsub_topic_filter> topics;

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

struct rgw_pubsub_user_topics {
  map<string, rgw_pubsub_topic_subs> topics;

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

static string pubsub_user_oid_prefix = "pubsub.user.";

class RGWUserPubSub
{
  friend class Bucket;

  RGWRados *store;
  rgw_user user;
  RGWSysObjectCtx obj_ctx;

  rgw_raw_obj user_meta_obj;

  string user_meta_oid() const {
    return pubsub_user_oid_prefix + user.to_str();
  }

  string bucket_meta_oid(const rgw_bucket& bucket) const {
    return pubsub_user_oid_prefix + user.to_str() + ".bucket." + bucket.name + "/" + bucket.bucket_id;
  }

  string sub_meta_oid(const string& name) const {
    return pubsub_user_oid_prefix + user.to_str() + ".sub." + name;
  }

  template <class T>
  int read(const rgw_raw_obj& obj, T *data, RGWObjVersionTracker *objv_tracker);

  template <class T>
  int write(const rgw_raw_obj& obj, const T& info, RGWObjVersionTracker *obj_tracker);

  int remove(const rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker);

  int read_user_topics(rgw_pubsub_user_topics *result, RGWObjVersionTracker *objv_tracker);
  int write_user_topics(const rgw_pubsub_user_topics& topics, RGWObjVersionTracker *objv_tracker);
public:
  RGWUserPubSub(RGWRados *_store, const rgw_user& _user) : store(_store),
                                                           user(_user),
                                                           obj_ctx(store->svc.sysobj->init_obj_ctx()) {
    get_user_meta_obj(&user_meta_obj);
  }

  class Bucket {
    friend class RGWUserPubSub;
    RGWUserPubSub *ps;
    rgw_bucket bucket;
    rgw_raw_obj bucket_meta_obj;

    int read_topics(rgw_pubsub_bucket_topics *result, RGWObjVersionTracker *objv_tracker);
    int write_topics(const rgw_pubsub_bucket_topics& topics, RGWObjVersionTracker *objv_tracker);
  public:
    Bucket(RGWUserPubSub *_ps, const rgw_bucket& _bucket) : ps(_ps), bucket(_bucket) {
      ps->get_bucket_meta_obj(bucket, &bucket_meta_obj);
    }

    int get_topics(rgw_pubsub_bucket_topics *result);
    int create_notification(const string& topic_name, const set<string, ltstr_nocase>& events);
    int remove_notification(const string& topic_name);
  };

  class Sub {
    friend class RGWUserPubSub;
    RGWUserPubSub *ps;
    string sub;
    rgw_raw_obj sub_meta_obj;

    int read_sub(rgw_pubsub_sub_config *result, RGWObjVersionTracker *objv_tracker);
    int write_sub(const rgw_pubsub_sub_config& sub_conf, RGWObjVersionTracker *objv_tracker);
    int remove_sub(RGWObjVersionTracker *objv_tracker);
  public:
    Sub(RGWUserPubSub *_ps, const string& _sub) : ps(_ps), sub(_sub) {
      ps->get_sub_meta_obj(sub, &sub_meta_obj);
    }

    int subscribe(const string& topic_name, const rgw_pubsub_sub_dest& dest);
    int unsubscribe(const string& topic_name);
    int get_conf(rgw_pubsub_sub_config *result);

    struct list_events_result {
      string next_marker;
      bool is_truncated{false};
      std::vector<rgw_pubsub_event> events;

      void dump(Formatter *f) const;
    };

    int list_events(const string& marker, int max_events, list_events_result *result);
    int remove_event(const string& event_id);
  };

  using BucketRef = std::shared_ptr<Bucket>;
  using SubRef = std::shared_ptr<Sub>;

  BucketRef get_bucket(const rgw_bucket& bucket) {
    return std::make_shared<Bucket>(this, bucket);
  }

  SubRef get_sub(const string& sub) {
    return std::make_shared<Sub>(this, sub);
  }

  void get_user_meta_obj(rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->svc.zone->get_zone_params().log_pool, user_meta_oid());
  }

  void get_bucket_meta_obj(const rgw_bucket& bucket, rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->svc.zone->get_zone_params().log_pool, bucket_meta_oid(bucket));
  }

  void get_sub_meta_obj(const string& name, rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->svc.zone->get_zone_params().log_pool, sub_meta_oid(name));
  }

  int get_user_topics(rgw_pubsub_user_topics *result);
  int get_topic(const string& name, rgw_pubsub_topic_subs *result);
  int create_topic(const string& name);
  int remove_topic(const string& name);
};

template <class T>
int RGWUserPubSub::read(const rgw_raw_obj& obj, T *result, RGWObjVersionTracker *objv_tracker)
{
  bufferlist bl;
  int ret = rgw_get_system_obj(store, obj_ctx,
                               obj.pool, obj.oid,
                               bl,
                               objv_tracker,
                               nullptr, nullptr, nullptr);
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
int RGWUserPubSub::write(const rgw_raw_obj& obj, const T& info, RGWObjVersionTracker *objv_tracker)
{
  bufferlist bl;
  encode(info, bl);

  int ret = rgw_put_system_obj(store, obj.pool, obj.oid,
                           bl, false, objv_tracker,
                           real_time());
  if (ret < 0) {
    return ret;
  }

  return 0;
}

#endif
