// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "services/svc_zone.h"
#include "rgw_b64.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"
#include "rgw_xml.h"
#include "rgw_arn.h"
#include "rgw_pubsub_push.h"
#include <regex>
#include <algorithm>

#define dout_subsys ceph_subsys_rgw

using namespace std;
void set_event_id(std::string& id, const std::string& hash, const utime_t& ts) {
  char buf[64];
  const auto len = snprintf(buf, sizeof(buf), "%010ld.%06ld.%s", (long)ts.sec(), (long)ts.usec(), hash.c_str());
  if (len > 0) {
    id.assign(buf, len);
  }
}

bool rgw_s3_key_filter::decode_xml(XMLObj* obj) {
  XMLObjIter iter = obj->find("FilterRule");
  XMLObj *o;

  const auto throw_if_missing = true;
  auto prefix_not_set = true;
  auto suffix_not_set = true;
  auto regex_not_set = true;
  std::string name;

  while ((o = iter.get_next())) {
    RGWXMLDecoder::decode_xml("Name", name, o, throw_if_missing);
    if (name == "prefix" && prefix_not_set) {
        prefix_not_set = false;
        RGWXMLDecoder::decode_xml("Value", prefix_rule, o, throw_if_missing);
    } else if (name == "suffix" && suffix_not_set) {
        suffix_not_set = false;
        RGWXMLDecoder::decode_xml("Value", suffix_rule, o, throw_if_missing);
    } else if (name == "regex" && regex_not_set) {
        regex_not_set = false;
        RGWXMLDecoder::decode_xml("Value", regex_rule, o, throw_if_missing);
    } else {
        throw RGWXMLDecoder::err("invalid/duplicate S3Key filter rule name: '" + name + "'");
    }
  }
  return true;
}

void rgw_s3_key_filter::dump_xml(Formatter *f) const {
  if (!prefix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "prefix", f);
    ::encode_xml("Value", prefix_rule, f);
    f->close_section();
  }
  if (!suffix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "suffix", f);
    ::encode_xml("Value", suffix_rule, f);
    f->close_section();
  }
  if (!regex_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", "regex", f);
    ::encode_xml("Value", regex_rule, f);
    f->close_section();
  }
}

bool rgw_s3_key_filter::has_content() const {
    return !(prefix_rule.empty() && suffix_rule.empty() && regex_rule.empty());
}

bool rgw_s3_key_value_filter::decode_xml(XMLObj* obj) {
  kv.clear();
  XMLObjIter iter = obj->find("FilterRule");
  XMLObj *o;

  const auto throw_if_missing = true;

  std::string key;
  std::string value;

  while ((o = iter.get_next())) {
    RGWXMLDecoder::decode_xml("Name", key, o, throw_if_missing);
    RGWXMLDecoder::decode_xml("Value", value, o, throw_if_missing);
    kv.emplace(key, value);
  }
  return true;
}

void rgw_s3_key_value_filter::dump_xml(Formatter *f) const {
  for (const auto& key_value : kv) {
    f->open_object_section("FilterRule");
    ::encode_xml("Name", key_value.first, f);
    ::encode_xml("Value", key_value.second, f);
    f->close_section();
  }
}

bool rgw_s3_key_value_filter::has_content() const {
    return !kv.empty();
}

bool rgw_s3_filter::decode_xml(XMLObj* obj) {
    RGWXMLDecoder::decode_xml("S3Key", key_filter, obj);
    RGWXMLDecoder::decode_xml("S3Metadata", metadata_filter, obj);
    RGWXMLDecoder::decode_xml("S3Tags", tag_filter, obj);
  return true;
}

void rgw_s3_filter::dump_xml(Formatter *f) const {
  if (key_filter.has_content()) {
      ::encode_xml("S3Key", key_filter, f);
  }
  if (metadata_filter.has_content()) {
      ::encode_xml("S3Metadata", metadata_filter, f);
  }
  if (tag_filter.has_content()) {
      ::encode_xml("S3Tags", tag_filter, f);
  }
}

bool rgw_s3_filter::has_content() const {
    return key_filter.has_content()  ||
           metadata_filter.has_content() ||
           tag_filter.has_content();
}

bool match(const rgw_s3_key_filter& filter, const std::string& key) {
  const auto key_size = key.size();
  const auto prefix_size = filter.prefix_rule.size();
  if (prefix_size != 0) {
    // prefix rule exists
    if (prefix_size > key_size) {
      // if prefix is longer than key, we fail
      return false;
    }
    if (!std::equal(filter.prefix_rule.begin(), filter.prefix_rule.end(), key.begin())) {
        return false;
    }
  }
  const auto suffix_size = filter.suffix_rule.size();
  if (suffix_size != 0) {
    // suffix rule exists
    if (suffix_size > key_size) {
      // if suffix is longer than key, we fail
      return false;
    }
    if (!std::equal(filter.suffix_rule.begin(), filter.suffix_rule.end(), (key.end() - suffix_size))) {
        return false;
    }
  }
  if (!filter.regex_rule.empty()) {
    // TODO add regex chaching in the filter
    const std::regex base_regex(filter.regex_rule);
    if (!std::regex_match(key, base_regex)) {
      return false;
    }
  }
  return true;
}

bool match(const rgw_s3_key_value_filter& filter, const KeyValueMap& kv) {
  // all filter pairs must exist with the same value in the object's metadata/tags
  // object metadata/tags may include items not in the filter
  return std::includes(kv.begin(), kv.end(), filter.kv.begin(), filter.kv.end());
}

bool match(const rgw_s3_key_value_filter& filter, const KeyMultiValueMap& kv) {
  // all filter pairs must exist with the same value in the object's metadata/tags
  // object metadata/tags may include items not in the filter
  for (auto& filter : filter.kv) {
    auto result = kv.equal_range(filter.first);
    if (std::any_of(result.first, result.second, [&filter](const pair<string,string>& p) { return p.second == filter.second;}))
      continue;
    else
      return false;
  }
  return true;
}

bool match(const rgw::notify::EventTypeList& events, rgw::notify::EventType event) {
  // if event list exists, and none of the events in the list matches the event type, filter the message
  if (!events.empty() && std::find(events.begin(), events.end(), event) == events.end()) {
    return false;
  }
  return true;
}

void do_decode_xml_obj(rgw::notify::EventTypeList& l, const string& name, XMLObj *obj) {
  l.clear();

  XMLObjIter iter = obj->find(name);
  XMLObj *o;

  while ((o = iter.get_next())) {
    std::string val;
    decode_xml_obj(val, o);
    l.push_back(rgw::notify::from_string(val));
  }
}

bool rgw_pubsub_s3_notification::decode_xml(XMLObj *obj) {
  const auto throw_if_missing = true;
  RGWXMLDecoder::decode_xml("Id", id, obj, throw_if_missing);
  
  RGWXMLDecoder::decode_xml("Topic", topic_arn, obj, throw_if_missing);
  
  RGWXMLDecoder::decode_xml("Filter", filter, obj);

  do_decode_xml_obj(events, "Event", obj);
  if (events.empty()) {
    // if no events are provided, we assume all events
    events.push_back(rgw::notify::ObjectCreated);
    events.push_back(rgw::notify::ObjectRemoved);
  }
  return true;
}

void rgw_pubsub_s3_notification::dump_xml(Formatter *f) const {
  ::encode_xml("Id", id, f);
  ::encode_xml("Topic", topic_arn.c_str(), f);
  if (filter.has_content()) {
      ::encode_xml("Filter", filter, f);
  }
  for (const auto& event : events) {
    ::encode_xml("Event", rgw::notify::to_string(event), f);
  }
}

bool rgw_pubsub_s3_notifications::decode_xml(XMLObj *obj) {
  do_decode_xml_obj(list, "TopicConfiguration", obj);
  return true;
}

rgw_pubsub_s3_notification::rgw_pubsub_s3_notification(const rgw_pubsub_topic_filter& topic_filter) :
    id(topic_filter.s3_id), events(topic_filter.events), topic_arn(topic_filter.topic.arn), filter(topic_filter.s3_filter) {} 

void rgw_pubsub_s3_notifications::dump_xml(Formatter *f) const {
  do_encode_xml("NotificationConfiguration", list, "TopicConfiguration", f);
}

void rgw_pubsub_s3_event::dump(Formatter *f) const {
  encode_json("eventVersion", eventVersion, f);
  encode_json("eventSource", eventSource, f);
  encode_json("awsRegion", awsRegion, f);
  utime_t ut(eventTime);
  encode_json("eventTime", ut, f);
  encode_json("eventName", eventName, f);
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
        encode_json("id", bucket_id, f);
    }
    {
        Formatter::ObjectSection sub_s(*f, "object");
        encode_json("key", object_key, f);
        encode_json("size", object_size, f);
        encode_json("eTag", object_etag, f);
        encode_json("versionId", object_versionId, f);
        encode_json("sequencer", object_sequencer, f);
        encode_json("metadata", x_meta_map, f);
        encode_json("tags", tags, f);
    }
  }
  encode_json("eventId", id, f);
  encode_json("opaqueData", opaque_data, f);
}

void rgw_pubsub_topic::dump(Formatter *f) const
{
  encode_json("user", user, f);
  encode_json("name", name, f);
  encode_json("dest", dest, f);
  encode_json("arn", arn, f);
  encode_json("opaqueData", opaque_data, f);
}

void rgw_pubsub_topic::dump_xml(Formatter *f) const
{
  encode_xml("User", user, f);
  encode_xml("Name", name, f);
  encode_xml("EndPoint", dest, f);
  encode_xml("TopicArn", arn, f);
  encode_xml("OpaqueData", opaque_data, f);
}

void encode_xml_key_value_entry(const std::string& key, const std::string& value, Formatter *f) {
  f->open_object_section("entry");
  encode_xml("key", key, f);
  encode_xml("value", value, f);
  f->close_section(); // entry
}

void rgw_pubsub_topic::dump_xml_as_attributes(Formatter *f) const
{
  f->open_array_section("Attributes");
  std::string str_user;
  user.to_str(str_user);
  encode_xml_key_value_entry("User", str_user, f);
  encode_xml_key_value_entry("Name", name, f);
  encode_xml_key_value_entry("EndPoint", dest.to_json_str(), f);
  encode_xml_key_value_entry("TopicArn", arn, f);
  encode_xml_key_value_entry("OpaqueData", opaque_data, f);
  f->close_section(); // Attributes
}

void encode_json(const char *name, const rgw::notify::EventTypeList& l, Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = l.cbegin(); iter != l.cend(); ++iter) {
    f->dump_string("obj", rgw::notify::to_string(*iter));
  }
  f->close_section();
}

void rgw_pubsub_topic_filter::dump(Formatter *f) const
{
  encode_json("topic", topic, f);
  encode_json("events", events, f);
}

void rgw_pubsub_bucket_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "topics");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "topics");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_topics::dump_xml(Formatter *f) const
{
  for (auto& t : topics) {
    encode_xml("member", t.second, f);
  }
}

void rgw_pubsub_dest::dump(Formatter *f) const
{
  encode_json("push_endpoint", push_endpoint, f);
  encode_json("push_endpoint_args", push_endpoint_args, f);
  encode_json("push_endpoint_topic", arn_topic, f);
  encode_json("stored_secret", stored_secret, f);
  encode_json("persistent", persistent, f);
}

void rgw_pubsub_dest::dump_xml(Formatter *f) const
{
  encode_xml("EndpointAddress", push_endpoint, f);
  encode_xml("EndpointArgs", push_endpoint_args, f);
  encode_xml("EndpointTopic", arn_topic, f);
  encode_xml("HasStoredSecret", stored_secret, f);
  encode_xml("Persistent", persistent, f);
}

std::string rgw_pubsub_dest::to_json_str() const
{
  JSONFormatter f;
  f.open_object_section("");
  encode_json("EndpointAddress", push_endpoint, &f);
  encode_json("EndpointArgs", push_endpoint_args, &f);
  encode_json("EndpointTopic", arn_topic, &f);
  encode_json("HasStoredSecret", stored_secret, &f);
  encode_json("Persistent", persistent, &f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  return ss.str();
}

RGWPubSub::RGWPubSub(rgw::sal::RadosStore* _store, const std::string& _tenant)
  : store(_store), tenant(_tenant), svc_sysobj(store->svc()->sysobj)
{
  get_meta_obj(&meta_obj);
}

int RGWPubSub::remove(const DoutPrefixProvider *dpp, 
                          const rgw_raw_obj& obj,
			  RGWObjVersionTracker *objv_tracker,
			  optional_yield y)
{
  int ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, obj.pool, obj.oid, objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWPubSub::read_topics(rgw_pubsub_topics *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = read(meta_obj, result, objv_tracker);
  if (ret < 0) {
    ldout(store->ctx(), 10) << "WARNING: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_topics& topics,
				     RGWObjVersionTracker *objv_tracker, optional_yield y)
{
  int ret = write(dpp, meta_obj, topics, objv_tracker, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::get_topics(rgw_pubsub_topics *result)
{
  return read_topics(result, nullptr);
}

int RGWPubSub::Bucket::read_topics(rgw_pubsub_bucket_topics *result, RGWObjVersionTracker *objv_tracker)
{
  int ret = ps->read(bucket_meta_obj, result, objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::Bucket::write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_bucket_topics& topics,
					RGWObjVersionTracker *objv_tracker,
					optional_yield y)
{
  int ret = ps->write(dpp, bucket_meta_obj, topics, objv_tracker, y);
  if (ret < 0) {
    ldout(ps->store->ctx(), 1) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::Bucket::get_topics(rgw_pubsub_bucket_topics *result)
{
  return read_topics(result, nullptr);
}

int RGWPubSub::get_topic(const string& name, rgw_pubsub_topic *result)
{
  rgw_pubsub_topics topics;
  int ret = get_topics(&topics);
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

int RGWPubSub::Bucket::create_notification(const DoutPrefixProvider *dpp, const string& topic_name, const rgw::notify::EventTypeList& events, optional_yield y) {
  return create_notification(dpp, topic_name, events, std::nullopt, "", y);
}

int RGWPubSub::Bucket::create_notification(const DoutPrefixProvider *dpp, const string& topic_name,const rgw::notify::EventTypeList& events, OptionalFilter s3_filter, const std::string& notif_name, optional_yield y) {
  rgw_pubsub_topic topic_info;

  int ret = ps->get_topic(topic_name, &topic_info);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topic '" << topic_name << "' info: ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "successfully read topic '" << topic_name << "' info" << dendl;

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics from bucket '" << 
      bucket.name << "': ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "successfully read " << bucket_topics.topics.size() << " topics from bucket '" << 
    bucket.name << "'" << dendl;

  auto& topic_filter = bucket_topics.topics[topic_name];
  topic_filter.topic = topic_info;
  topic_filter.events = events;
  topic_filter.s3_id = notif_name;
  if (s3_filter) {
    topic_filter.s3_filter = *s3_filter;
  }

  ret = write_topics(dpp, bucket_topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics to bucket '" << bucket.name << "': ret=" << ret << dendl;
    return ret;
  }
    
  ldpp_dout(dpp, 20) << "successfully wrote " << bucket_topics.topics.size() << " topics to bucket '" << bucket.name << "'" << dendl;

  return 0;
}

int RGWPubSub::Bucket::remove_notification(const DoutPrefixProvider *dpp, const string& topic_name, optional_yield y)
{
  rgw_pubsub_topic topic_info;

  int ret = ps->get_topic(topic_name, &topic_info);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topic info: ret=" << ret << dendl;
    return ret;
  }

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(&bucket_topics, &objv_tracker);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  bucket_topics.topics.erase(topic_name);

  if (bucket_topics.topics.empty()) {
    // no more topics - delete the notification object of the bucket
    ret = ps->remove(dpp, bucket_meta_obj, &objv_tracker, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 1) << "ERROR: failed to remove bucket topics: ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }

  // write back the notifications without the deleted one
  ret = write_topics(dpp, bucket_topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::Bucket::remove_notifications(const DoutPrefixProvider *dpp, optional_yield y)
{
  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  auto ret  = get_topics(&bucket_topics);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get list of topics from bucket '" << bucket.name << "', ret=" << ret << dendl;
    return ret ;
  }

  // remove all auto-genrated topics
  for (const auto& topic : bucket_topics.topics) {
    const auto& topic_name = topic.first;
    ret = ps->remove_topic(dpp, topic_name, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 5) << "WARNING: failed to remove auto-generated topic '" << topic_name << "', ret=" << ret << dendl;
    }
  }

  // delete the notification object of the bucket
  ret = ps->remove(dpp, bucket_meta_obj, nullptr, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove bucket topics: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::create_topic(const DoutPrefixProvider *dpp, const string& name, optional_yield y) {
  return create_topic(dpp, name, rgw_pubsub_dest{}, "", "", y);
}

int RGWPubSub::create_topic(const DoutPrefixProvider *dpp, const string& name, const rgw_pubsub_dest& dest, const std::string& arn, const std::string& opaque_data, optional_yield y) {
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_topics topics;

  int ret = read_topics(&topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    // its not an error if not topics exist, we create one
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
 
  rgw_pubsub_topic& new_topic = topics.topics[name];
  new_topic.user = rgw_user("", tenant);
  new_topic.name = name;
  new_topic.dest = dest;
  new_topic.arn = arn;
  new_topic.opaque_data = opaque_data;

  ret = write_topics(dpp, topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::remove_topic(const DoutPrefixProvider *dpp, const string& name, optional_yield y)
{
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_topics topics;

  int ret = read_topics(&topics, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  } else if (ret == -ENOENT) {
      // its not an error if no topics exist, just a no-op
      ldpp_dout(dpp, 10) << "WARNING: failed to read topics info, deletion is a no-op: ret=" << ret << dendl;
      return 0;
  }

  topics.topics.erase(name);

  ret = write_topics(dpp, topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWPubSub::get_meta_obj(rgw_raw_obj *obj) const {
  *obj = rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, meta_oid());
}

void RGWPubSub::get_bucket_meta_obj(const rgw_bucket& bucket, rgw_raw_obj *obj) const {
  *obj = rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, bucket_meta_oid(bucket));
}

