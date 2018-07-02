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

static string pubsub_user_oid_prefix = "pubsub.user.";


class RGWUserPubSub
{
  RGWRados *store;
  rgw_user user;
  RGWObjectCtx obj_ctx;

  template <class T>
  int read(const rgw_raw_obj& obj, T *data, RGWObjVersionTracker *objv_tracker);

  template <class T>
  int write(const rgw_raw_obj& obj, const T& info, RGWObjVersionTracker *obj_tracker);

  int remove(const rgw_raw_obj& obj, RGWObjVersionTracker *objv_tracker);
public:
  RGWUserPubSub(RGWRados *_store, const rgw_user& _user) : store(_store),
                                                           user(_user),
                                                           obj_ctx(store) {}

  string user_meta_oid() const {
    return pubsub_user_oid_prefix + user.to_str();
  }

  string bucket_meta_oid(const rgw_bucket& bucket) const {
    return pubsub_user_oid_prefix + user.to_str() + ".bucket." + bucket.name + "/" + bucket.bucket_id;
  }

  string sub_meta_oid(const string& name) const {
    return pubsub_user_oid_prefix + user.to_str() + ".sub." + name;
  }

  void get_user_meta_obj(rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->get_zone_params().log_pool, user_meta_oid());
  }

  void get_bucket_meta_obj(const rgw_bucket& bucket, rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->get_zone_params().log_pool, bucket_meta_oid(bucket));
  }

  void get_sub_meta_obj(const string& name, rgw_raw_obj *obj) const {
    *obj = rgw_raw_obj(store->get_zone_params().log_pool, sub_meta_oid(name));
  }

  int get_topics(rgw_pubsub_user_topics *result);
  int get_bucket_topics(const rgw_bucket& bucket, rgw_pubsub_user_topics *result);
  int create_topic(const string& name, const rgw_bucket& bucket);
  int remove_topic(const string& name);
  int add_sub(const string& name, const string& topic, const rgw_pubsub_user_sub_dest& dest);
  int remove_sub(const string& name, const string& topic, const rgw_pubsub_user_sub_dest& dest);
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
    ldout(store->ctx(), 0) << "ERROR: failed to decode info, caught buffer::error" << dendl;
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

  rgw_pubsub_user_topic_info new_topic;
  new_topic.user = user;
  new_topic.topic.name = name;
  new_topic.topic.bucket = bucket;

  topics.topics[name] = new_topic;

  ret = write(obj, topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  rgw_pubsub_user_topics bucket_topics;
  for (auto& t : topics.topics) {
    if (t.second.topic.bucket == bucket) {
      bucket_topics.topics.insert(t);
    }
  }

  rgw_raw_obj bobj;
  get_bucket_meta_obj(bucket, &bobj);
  ret = write(obj, bucket_topics, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
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

  rgw_pubsub_user_topics bucket_topics;
  for (auto& t : topics.topics) {
    if (t.second.topic.bucket == bucket) {
      bucket_topics.topics.insert(t);
    }
  }

  rgw_raw_obj bobj;
  get_bucket_meta_obj(bucket, &bobj);
  ret = write(obj, bucket_topics, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write topics info: ret=" << ret << dendl;
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

  auto iter = topics.topics.find(name);
  if (iter == topics.topics.end()) {
    ldout(store->ctx(), 20) << "ERROR: cannot add subscription to topic: topic not found" << dendl;
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

  rgw_raw_obj bobj;
  get_sub_meta_obj(name, &bobj);
  ret = write(obj, sub_conf, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWUserPubSub::remove_sub(const string& name, const string& topic, const rgw_pubsub_user_sub_dest& dest)
{
  rgw_raw_obj obj;
  get_user_meta_obj(&obj);

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_user_topics topics;

  int ret = read(obj, &topics, &objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read topics info: ret=" << ret << dendl;
  }

  if (ret >= 0) {
    auto iter = topics.topics.find(name);
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

  rgw_raw_obj sobj;
  get_sub_meta_obj(name, &sobj);
  ret = remove(obj, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to write subscription info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

