#include "rgw_b64.h"
#include "rgw_rados.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw


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
}

void rgw_pubsub_sub_config::dump(Formatter *f) const
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

int RGWUserPubSub::read_user_topics(rgw_pubsub_user_topics *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = read(user_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::write_user_topics(const rgw_pubsub_user_topics& topics, RGWObjVersionTracker *objv_tracker)
{
  int ret = write(user_meta_obj, topics, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
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
    ldout(ps->store->ctx(), 0) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::Bucket::write_topics(const rgw_pubsub_bucket_topics& topics, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->write(bucket_meta_obj, topics, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 0) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
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


int RGWUserPubSub::Bucket::create_notification(const string& topic_name, const set<string, ltstr_nocase>& events)
{
  rgw_pubsub_topic_subs user_topic_info;
  RGWRados *store = ps->store;

  int ret = ps->get_topic(topic_name, &user_topic_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topic info: ret=" << ret << dendl;
    return ret;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  auto& topic_filter = bucket_topics.topics[topic_name];
  topic_filter.topic = user_topic_info.topic;
  topic_filter.events = events;

  ret = write_topics(bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Bucket::remove_notification(const string& topic_name)
{
  rgw_pubsub_topic_subs user_topic_info;
  RGWRados *store = ps->store;

  int ret = ps->get_topic(topic_name, &user_topic_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topic info: ret=" << ret << dendl;
    return ret;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  bucket_topics.topics.erase(topic_name);

  ret = write_topics(bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::create_topic(const string& name)
{
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read_user_topics(&topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_pubsub_topic_subs& new_topic = topics.topics[name];
  new_topic.topic.user = user;
  new_topic.topic.name = name;

  ret = write_user_topics(topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
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
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  topics.topics.erase(name);

  ret = write_user_topics(topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::read_sub(rgw_pubsub_sub_config *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->read(sub_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(ps->store->ctx(), 0) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::Sub::write_sub(const rgw_pubsub_sub_config& sub_conf, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->write(sub_meta_obj, sub_conf, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::remove_sub(RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->remove(sub_meta_obj, objv_tracker);
  if (ret < 0) {
    ldout(ps->store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWUserPubSub::Sub::get_conf(rgw_pubsub_sub_config *result)
{
  return read_sub(result, nullptr);
}

int RGWUserPubSub::Sub::subscribe(const string& topic, const rgw_pubsub_sub_dest& dest)
{
  RGWObjVersionTracker user_objv_tracker;
  rgw_pubsub_user_topics topics;
  RGWRados *store = ps->store;

  int ret = ps->read_user_topics(&topics, &user_objv_tracker);
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

  rgw_pubsub_sub_config sub_conf;

  sub_conf.user = ps->user;
  sub_conf.name = sub;
  sub_conf.topic = topic;
  sub_conf.dest = dest;

  t.subs.insert(sub);

  ret = ps->write_user_topics(topics, &user_objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  ret = write_sub(sub_conf, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
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
      ldout(store->ctx(), 0) << "ERROR: failed to read subscription info: ret=" << ret << dendl;
      return ret;
    }
    topic = sub_conf.topic;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = ps->read_user_topics(&topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
  }

  if (ret >= 0) {
    auto iter = topics.topics.find(topic);
    if (iter != topics.topics.end()) {
      auto& t = iter->second;

      t.subs.erase(sub);

      ret = ps->write_user_topics(topics, &objv_tracker);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
        return ret;
      }
    }
  }

  ret = remove_sub(&sobjv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to delete subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

void RGWUserPubSub::Sub::list_events_result::dump(Formatter *f) const
{
  encode_json("next_marker", next_marker, f);
  encode_json("is_truncated", is_truncated, f);

  Formatter::ArraySection s(*f, "events");
  for (auto& event : events) {
    encode_json("event", event, f);
  }
}

int RGWUserPubSub::Sub::list_events(const string& marker, int max_events,
                               list_events_result *result)
{
  RGWRados *store = ps->store;
  rgw_pubsub_sub_config sub_conf;
  int ret = get_conf(&sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWSysObjectCtx obj_ctx(store->svc.sysobj->init_obj_ctx());
  ret = store->get_bucket_info(obj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, nullptr);
  if (ret == -ENOENT) {
    result->is_truncated = false;
    return 0;
  }
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

int RGWUserPubSub::Sub::remove_event(const string& event_id)
{
  RGWRados *store = ps->store;
  rgw_pubsub_sub_config sub_conf;
  int ret = get_conf(&sub_conf);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sub config: ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo bucket_info;
  string tenant;
  RGWSysObjectCtx sysobj_ctx(store->svc.sysobj->init_obj_ctx());
  ret = store->get_bucket_info(sysobj_ctx, tenant, sub_conf.dest.bucket_name, bucket_info, nullptr, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read bucket info for events bucket: bucket=" << sub_conf.dest.bucket_name << " ret=" << ret << dendl;
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

  ret = del_op.delete_obj();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to remove event (obj=" << obj << "): ret=" << ret << dendl;
  }
  return 0;
}
