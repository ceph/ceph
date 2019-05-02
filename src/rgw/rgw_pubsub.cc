// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_b64.h"
#include "rgw_rados.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"
#include "rgw_xml.h"
#include "rgw_arn.h"
#include "rgw_pubsub_push.h"

#define dout_subsys ceph_subsys_rgw

bool rgw_pubsub_s3_notification::decode_xml(XMLObj *obj) {
  const auto throw_if_missing = true;
  RGWXMLDecoder::decode_xml("Id", id, obj, throw_if_missing);
  
  RGWXMLDecoder::decode_xml("Topic", topic_arn, obj, throw_if_missing);

  do_decode_xml_obj(events, "Event", obj);
  if (events.empty()) {
    // if no events are provided, we assume all events
    events.push_back("s3:ObjectCreated:*");
    events.push_back("s3:ObjectRemoved:*");
  }
  return true;
}

void rgw_pubsub_s3_notification::dump_xml(Formatter *f) const {
  ::encode_xml("Id", id, f);
  ::encode_xml("Topic", topic_arn.c_str(), f);
  for (const auto& event : events) {
    ::encode_xml("Event", event, f);
  }
}

bool rgw_pubsub_s3_notifications::decode_xml(XMLObj *obj) {
  do_decode_xml_obj(list, "TopicConfiguration", obj);
  if (list.empty()) {
    throw RGWXMLDecoder::err("at least one 'TopicConfiguration' must exist");
  }
  return true;
}

void rgw_pubsub_s3_notifications::dump_xml(Formatter *f) const {
  do_encode_xml("NotificationConfiguration", list, "TopicConfiguration", f);
}

void rgw_pubsub_s3_record::dump(Formatter *f) const {
  encode_json("eventVersion", eventVersion, f);
  encode_json("eventSource", eventSource, f);
  encode_json("awsRegion", awsRegion, f);
  utime_t ut(eventTime);
  encode_json("eventTime", ut, f);
  if (eventName == "OBJECT_CREATE") {
    encode_json("eventName", "ObjectCreated", f);
  }
  else if (eventName == "OBJECT_DELETE") {
    encode_json("eventName", "ObjectRemoved", f);
  } else {
    encode_json("eventName", "UNKNOWN_EVENT", f);
  }
  {
    Formatter::ObjectSection s(*f, "userIdentity");
    encode_json("principalId", userIdentity, f);
  }
  {
    Formatter::ObjectSection s(*f, "requestParameters");
    encode_json("sourceIPAddress", sourceIPAddress, f);
  }
  {
    Formatter::ObjectSection s(*f, "responseElements");
    encode_json("x-amz-request-id", x_amz_request_id, f);
    encode_json("x-amz-id-2", x_amz_id_2, f);
  }
  {
    Formatter::ObjectSection s(*f, "s3");
    encode_json("s3SchemaVersion", s3SchemaVersion, f);
    encode_json("configurationId", configurationId, f);
    {
        Formatter::ObjectSection sub_s(*f, "bucket");
        encode_json("name", bucket_name, f);
        {
            Formatter::ObjectSection sub_sub_s(*f, "ownerIdentity");
            encode_json("principalId", bucket_ownerIdentity, f);
        }
        encode_json("arn", bucket_arn, f);
    }
    {
        Formatter::ObjectSection sub_s(*f, "object");
        encode_json("key", object_key, f);
        encode_json("size", object_size, f);
        encode_json("etag", object_etag, f);
        encode_json("versionId", object_versionId, f);
        encode_json("sequencer", object_sequencer, f);
    }
  }
  encode_json("eventId", id, f);
}

void rgw_pubsub_event::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("event", event, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
  encode_json("info", info, f);
}

void rgw_pubsub_topic::dump(Formatter *f) const
{
  encode_json("user", user, f);
  encode_json("name", name, f);
  encode_json("dest", dest, f);
  encode_json("arn", arn, f);
}

void rgw_pubsub_topic_filter::dump(Formatter *f) const
{
  encode_json("topic", topic, f);
  encode_json("events", events, f);
}

void rgw_pubsub_topic_subs::dump(Formatter *f) const
{
  encode_json("topic", topic, f);
  encode_json("subs", subs, f);
}

void rgw_pubsub_bucket_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "topics");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_user_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "topics");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_sub_dest::dump(Formatter *f) const
{
  encode_json("bucket_name", bucket_name, f);
  encode_json("oid_prefix", oid_prefix, f);
  encode_json("push_endpoint", push_endpoint, f);
  encode_json("push_endpoint_args", push_endpoint_args, f);
}

void rgw_pubsub_sub_config::dump(Formatter *f) const
{
  encode_json("user", user, f);
  encode_json("name", name, f);
  encode_json("topic", topic, f);
  encode_json("dest", dest, f);
  encode_json("s3_id", s3_id, f);
}

int RGWUserPubSub::remove(const rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  int ret = rgw_delete_system_obj(store, obj.pool, obj.oid, objv_tracker);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWUserPubSub::read_user_topics(rgw_pubsub_user_topics *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = read(user_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::write_user_topics(const rgw_pubsub_user_topics& topics, RGWObjVersionTracker *objv_tracker)
{
  int ret = write(user_meta_obj, topics, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::get_user_topics(rgw_pubsub_user_topics *result)
{
  return read_user_topics(result, nullptr);
}

int RGWUserPubSub::Bucket::read_topics(rgw_pubsub_bucket_topics *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->read(bucket_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::Bucket::write_topics(const rgw_pubsub_bucket_topics& topics, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->write(bucket_meta_obj, topics, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Bucket::get_topics(rgw_pubsub_bucket_topics *result)
{
  return read_topics(result, nullptr);
}

int RGWUserPubSub::get_topic(const string& name, rgw_pubsub_topic_subs *result)
{
  rgw_pubsub_user_topics topics;
  int ret = get_user_topics(&topics);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  auto iter = topics.topics.find(name);
  if (iter == topics.topics.end()) {
    ldout(store->ctx(), 1) << "ERROR: topic not found" << dendl;
    return -ENOENT;
  }

  *result = iter->second;
  return 0;
}

int RGWUserPubSub::Bucket::create_notification(const string& topic_name, const EventTypeList& events)
{
  rgw_pubsub_topic_subs user_topic_info;
  RGWRados *store = ps->store;

  int ret = ps->get_topic(topic_name, &user_topic_info);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topic '" << topic_name << "' info: ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << "successfully read topic '" << topic_name << "' info" << dendl;

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics from bucket '" << 
      bucket.name << "': ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << "successfully read " << bucket_topics.topics.size() << " topics from bucket '" << 
    bucket.name << "'" << dendl;

  auto& topic_filter = bucket_topics.topics[topic_name];
  topic_filter.topic = user_topic_info.topic;
  topic_filter.events = events;

  ret = write_topics(bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to write topics to bucket '" << bucket.name << "': ret=" << ret << dendl;
    return ret;
  }
    
  ldout(store->ctx(), 20) << "successfully wrote " << bucket_topics.topics.size() << " topics to bucket '" << bucket.name << "'" << dendl;

  return 0;
}

int RGWUserPubSub::Bucket::remove_notification(const string& topic_name)
{
  rgw_pubsub_topic_subs user_topic_info;
  RGWRados *store = ps->store;

  int ret = ps->get_topic(topic_name, &user_topic_info);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topic info: ret=" << ret << dendl;
    return ret;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  bucket_topics.topics.erase(topic_name);

  ret = write_topics(bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::create_topic(const string& name) {
  return create_topic(name, rgw_pubsub_sub_dest(), "");
}

int RGWUserPubSub::create_topic(const string& name, const rgw_pubsub_sub_dest& dest, const std::string& arn) {
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read_user_topics(&topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_pubsub_topic_subs& new_topic = topics.topics[name];
  new_topic.topic.user = user;
  new_topic.topic.name = name;
  new_topic.topic.dest = dest;
  new_topic.topic.arn = arn;

  ret = write_user_topics(topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::remove_topic(const string& name)
{
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read_user_topics(&topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  topics.topics.erase(name);

  ret = write_user_topics(topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to remove topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::read_sub(rgw_pubsub_sub_config *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->read(sub_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::Sub::write_sub(const rgw_pubsub_sub_config& sub_conf, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->write(sub_meta_obj, sub_conf, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::remove_sub(RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->remove(sub_meta_obj, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to remove subscription info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::get_conf(rgw_pubsub_sub_config *result)
{
  return read_sub(result, nullptr);
}

int RGWUserPubSub::Sub::subscribe(const string& topic, const rgw_pubsub_sub_dest& dest, const std::string& s3_id)
{
  RGWObjVersionTracker user_objv_tracker;
  rgw_pubsub_user_topics topics;
  RGWRados *store = ps->store;

  int ret = ps->read_user_topics(&topics, &user_objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  auto iter = topics.topics.find(topic);
  if (iter == topics.topics.end()) {
    ldout(store->ctx(), 1) << "ERROR: cannot add subscription to topic: topic not found" << dendl;
    return -ENOENT;
  }

  auto& t = iter->second;

  rgw_pubsub_sub_config sub_conf;

  sub_conf.user = ps->user;
  sub_conf.name = sub;
  sub_conf.topic = topic;
  sub_conf.dest = dest;
  sub_conf.s3_id = s3_id;

  t.subs.insert(sub);

  ret = ps->write_user_topics(topics, &user_objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  ret = write_sub(sub_conf, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::Sub::unsubscribe(const string& _topic)
{
  string topic = _topic;
  RGWObjVersionTracker sobjv_tracker;
  RGWRados *store = ps->store;

  if (topic.empty()) {
    rgw_pubsub_sub_config sub_conf;
    int ret = read_sub(&sub_conf, &sobjv_tracker);
    if (ret < 0) {
      ldout(store->ctx(), 1) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
      return ret;
    }
    topic = sub_conf.topic;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = ps->read_user_topics(&topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
  }

  if (ret >= 0) {
    auto iter = topics.topics.find(topic);
    if (iter != topics.topics.end()) {
      auto& t = iter->second;

      t.subs.erase(sub);

      ret = ps->write_user_topics(topics, &objv_tracker);
      if (ret < 0) {
        ldout(store->ctx(), 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
        return ret;
      }
    }
  }

  ret = remove_sub(&sobjv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to delete subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

template<typename EventType>
void RGWUserPubSub::SubWithEvents<EventType>::list_events_result::dump(Formatter *f) const
{
  encode_json("next_marker", next_marker, f);
  encode_json("is_truncated", is_truncated, f);

  Formatter::ArraySection s(*f, EventType::json_type_plural);
  for (auto& event : events) {
    encode_json(EventType::json_type_single, event, f);
  }
}

template<typename EventType>
int RGWUserPubSub::SubWithEvents<EventType>::list_events(const string& marker, int max_events)
{
  RGWRados *store = ps->store;
  rgw_pubsub_sub_config sub_conf;
  int ret = get_conf(&sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWSysObjectCtx obj_ctx(store->svc.sysobj->init_obj_ctx());
  ret = store->get_bucket_info(obj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, null_yield, nullptr);
  if (ret == -ENOENT) {
    list.is_truncated = false;
    return 0;
  }
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read bucket info for events bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = sub_conf.dest.oid_prefix;
  list_op.params.marker = marker;

  std::vector<rgw_bucket_dir_entry> objs;

  ret = list_op.list_objects(max_events, &objs, nullptr, &list.is_truncated);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to list bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }
  if (list.is_truncated) {
    list.next_marker = list_op.get_next_marker().name;
  }

  for (auto& obj : objs) {
    bufferlist bl64;
    bufferlist bl;
    bl64.append(obj.meta.user_data);
    try {
      bl.decode_base64(bl64);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 1) << "ERROR: failed to event (not a valid base64)" << dendl;
      continue;
    }
    EventType event;

    auto iter = bl.cbegin();
    try {
      decode(event, iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 1) << "ERROR: failed to decode event" << dendl;
      continue;
    };

    list.events.push_back(event);
  }
  return 0;
}

template<typename EventType>
int RGWUserPubSub::SubWithEvents<EventType>::remove_event(const string& event_id)
{
  RGWRados *store = ps->store;
  rgw_pubsub_sub_config sub_conf;
  int ret = get_conf(&sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWSysObjectCtx sysobj_ctx(store->svc.sysobj->init_obj_ctx());
  ret = store->get_bucket_info(sysobj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, null_yield, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to read bucket info for events bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket& bucket = bucket_info.bucket;

  RGWObjectCtx obj_ctx(store);
  rgw_obj obj(bucket, sub_conf.dest.oid_prefix + event_id);

  obj_ctx.set_atomic(obj);

  RGWRados::Object del_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = bucket_info.versioning_status();

  ret = del_op.delete_obj(null_yield);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "ERROR: failed to remove event (obj=" << obj << "): ret=" << ret << dendl;
  }
  return 0;
}

template<typename EventType>
void RGWUserPubSub::SubWithEvents<EventType>::dump(Formatter* f) const {
  list.dump(f);
}

// explicit instantiation for the only two possible types
// no need to move implementation to header
template class RGWUserPubSub::SubWithEvents<rgw_pubsub_event>;
template class RGWUserPubSub::SubWithEvents<rgw_pubsub_s3_record>;

