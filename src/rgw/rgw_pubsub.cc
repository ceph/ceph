#include "rgw_b64.h"
#include "rgw_rados.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

void encode_json(const char *name, const RGWPubSubEventType& val, Formatter *f)
{
  switch (val) {
    case EVENT_UNKNOWN:
      encode_json(name, "EVENT_UNKNOWN", f);
      break;
    case OBJECT_CREATE:
      encode_json(name, "OBJECT_CREATE", f);
      break;
    case OBJECT_DELETE:
      encode_json(name, "OBJECT_DELETE", f);
      break;
    case DELETE_MARKER_CREATE:
      encode_json(name, "DELETE_MARKER_CREATE", f);
      break;
  };
}


void rgw_pubsub_event::dump(Formatter *f) const
{
  encode_json("id", id, f);
  {
    Formatter::ObjectSection s(*f, "bucket");
    encode_json("name", bucket.name, f);
    encode_json("id", bucket.bucket_id, f);
  }
  {
    Formatter::ObjectSection s(*f, "object");
    encode_json("name", key.name, f);
    encode_json("version-id", key.instance, f);
  }

  utime_t mt(mtime);
  encode_json("mtime", mt, f);
  encode_json("event", event, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);

  {
    Formatter::ObjectSection s(*f, "attrs");
    for (auto& attr : attrs) {
      encode_json(attr.first.c_str(), attr.second.c_str(), f);
    }
  }
}

void rgw_pubsub_user_topic::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("bucket", bucket, f);
}

void rgw_pubsub_user_topic_info::dump(Formatter *f) const
{
  encode_json("user", user, f);
  encode_json("topic", topic, f);
  encode_json("subs", subs, f);
}

void rgw_pubsub_user_topics::dump(Formatter *f) const
{
  Formatter::ObjectSection s(*f, "topics");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_user_sub_dest::dump(Formatter *f) const
{
  encode_json("bucket_name", bucket_name, f);
  encode_json("oid_prefix", oid_prefix, f);
  encode_json("push_endpoint", push_endpoint, f);
}

void rgw_pubsub_user_sub_config::dump(Formatter *f) const
{
  encode_json("user", user, f);
  encode_json("name", name, f);
  encode_json("topic", topic, f);
  encode_json("dest", dest, f);
}


int RGWUserPubSub::remove(const rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  int ret = rgw_delete_system_obj(store, obj.pool, obj.oid, objv_tracker);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWUserPubSub::get_topics(rgw_pubsub_user_topics *result)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  int ret = read(obj, result, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::get_bucket_topics(const rgw_bucket& bucket, rgw_pubsub_user_topics *result)
{
  rgw_raw_obj obj;
  get_bucket_meta_obj(bucket, &obj);

  RGWObjVersionTracker objv_tracker;
  int ret = read(obj, result, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::get_topic(const string& name, rgw_pubsub_user_topic_info *result)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  auto iter = topics.topics.find(name);
  if (iter == topics.topics.end()) {
    ldout(store->ctx(), 0) << "ERROR: cannot add subscription to topic: topic not found" << dendl;
    return -ENOENT;
  }

  *result = iter->second;
  return 0;
}


int RGWUserPubSub::create_topic(const string& name, const rgw_bucket& bucket)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_pubsub_user_topic_info& new_topic = topics.topics[name];
  new_topic.user = user;
  new_topic.topic.name = name;
  new_topic.topic.bucket = bucket;

  ret = write(obj, topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  ret = update_bucket(topics, bucket);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::remove_topic(const string& name)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket bucket;
  auto t = topics.topics.find(name);
  if (t != topics.topics.end()) {
    bucket = t->second.topic.bucket;
  }

  topics.topics.erase(name);

  ret = write(obj, topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  if (bucket.name.empty()) {
    return 0;
  }

  ret = update_bucket(topics, bucket);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::update_bucket(const rgw_pubsub_user_topics& topics, const rgw_bucket& bucket)
{
  rgw_pubsub_user_topics bucket_topics;
  for (auto& t : topics.topics) {
    if (t.second.topic.bucket == bucket) {
      bucket_topics.topics.insert(t);
    }
  }

  rgw_raw_obj bobj;
  get_bucket_meta_obj(bucket, &bobj);
  int ret = write(bobj, bucket_topics, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::get_sub(const string& name, rgw_pubsub_user_sub_config *result)
{
  rgw_raw_obj obj;
  get_sub_meta_obj(name, &obj);
  int ret = read(obj, result, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::add_sub(const string& name, const string& topic, const rgw_pubsub_user_sub_dest& dest)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  auto iter = topics.topics.find(topic);
  if (iter == topics.topics.end()) {
    ldout(store->ctx(), 0) << "ERROR: cannot add subscription to topic: topic not found" << dendl;
    return -ENOENT;
  }

  auto& t = iter->second;

  rgw_pubsub_user_sub_config sub_conf;

  sub_conf.user = user;
  sub_conf.name = name;
  sub_conf.topic = topic;
  sub_conf.dest = dest;

  t.subs.insert(name);

  ret = write(obj, topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  ret = update_bucket(topics, t.topic.bucket);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_raw_obj sobj;
  get_sub_meta_obj(name, &sobj);
  ret = write(sobj, sub_conf, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::remove_sub(const string& name, const string& _topic)
{
  string topic = _topic;

  RGWObjVersionTracker sobjv_tracker;
  rgw_raw_obj sobj;
  get_sub_meta_obj(name, &sobj);

  if (topic.empty()) {
    rgw_pubsub_user_sub_config sub_conf;
    int ret = read(sobj, &sub_conf, &sobjv_tracker);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
      return ret;
    }
    topic = sub_conf.topic;
  }

  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
  }

  if (ret >= 0) {
    auto iter = topics.topics.find(topic);
    if (iter == topics.topics.end()) {
      ldout(store->ctx(), 20) << "ERROR: cannot add subscription to topic: topic not found" << dendl;
    } else {
      auto& t = iter->second;

      t.subs.erase(name);

      ret = write(obj, topics, &objv_tracker);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
        return ret;
      }
    }
  }

  ret = remove(sobj, &sobjv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

void RGWUserPubSub::list_events_result::dump(Formatter *f) const
{
  encode_json("next_marker", next_marker, f);
  encode_json("is_truncated", is_truncated, f);

  Formatter::ArraySection s(*f, "events");
  for (auto& event : events) {
    encode_json("event", event, f);
  }
}

int RGWUserPubSub::list_events(const string& sub_name,
                               const string& marker, int max_events,
                               list_events_result *result)
{
  rgw_pubsub_user_sub_config sub_conf;
  int ret = get_sub(sub_name, &sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWObjectCtx obj_ctx(store);
  ret = store->get_bucket_info(obj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read bucket info for events bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = sub_conf.dest.oid_prefix;
  list_op.params.marker = marker;

  vector<rgw_bucket_dir_entry> objs;

  ret = list_op.list_objects(max_events, &objs, nullptr, &result->is_truncated);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to list bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }
  if (result->is_truncated) {
    result->next_marker = list_op.get_next_marker().name;
  }

  for (auto& obj : objs) {
    bufferlist bl64;
    bufferlist bl;
    bl64.append(obj.meta.user_data);
    try {
      bl.decode_base64(bl64);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: failed to event (not a valid base64)" << dendl;
      continue;
    }
    rgw_pubsub_event event;

    auto iter = bl.cbegin();
    try {
      decode(event, iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: failed to decode event" << dendl;
      continue;
    };

    result->events.push_back(event);
  }
  return 0;
}

int RGWUserPubSub::remove_event(const string& sub_name, const string& event_id)
{
  rgw_pubsub_user_sub_config sub_conf;
  int ret = get_sub(sub_name, &sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWObjectCtx obj_ctx(store);
  ret = store->get_bucket_info(obj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read bucket info for events bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket& bucket = bucket_info.bucket;

  rgw_obj obj(bucket, event_id);

  obj_ctx.obj.set_atomic(obj);

  RGWRados::Object del_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = bucket_info.versioning_status();

  ret = del_op.delete_obj();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to remove event (obj=" << obj << "): ret=" << ret << dendl;
  }
  return 0;
}
