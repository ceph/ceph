// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "services/svc_zone.h"
#include "rgw_b64.h"
#include "rgw_sal.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"
#include "rgw_xml.h"
#include "rgw_arn.h"
#include "rgw_pubsub_push.h"
#include "rgw_bucket.h"
#include "common/errno.h"
#include "svc_topic_rados.h"
#include <regex>
#include <algorithm>

#define dout_subsys ceph_subsys_rgw

void set_event_id(std::string& id, const std::string& hash, const utime_t& ts) {
  char buf[64];
  const auto len = snprintf(buf, sizeof(buf), "%010ld.%06ld.%s", (long)ts.sec(), (long)ts.usec(), hash.c_str());
  if (len > 0) {
    id.assign(buf, len);
  }
}

void rgw_s3_key_filter::dump(Formatter *f) const {
  if (!prefix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_json("Name", "prefix", f);
    ::encode_json("Value", prefix_rule, f);
    f->close_section();
  }
  if (!suffix_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_json("Name", "suffix", f);
    ::encode_json("Value", suffix_rule, f);
    f->close_section();
  }
  if (!regex_rule.empty()) {
    f->open_object_section("FilterRule");
    ::encode_json("Name", "regex", f);
    ::encode_json("Value", regex_rule, f);
    f->close_section();
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

void rgw_s3_key_value_filter::dump(Formatter *f) const {
  for (const auto& key_value : kv) {
    f->open_object_section("FilterRule");
    ::encode_json("Name", key_value.first, f);
    ::encode_json("Value", key_value.second, f);
    f->close_section();
  }
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

void rgw_s3_filter::dump(Formatter *f) const {
  encode_json("S3Key", key_filter, f);
  encode_json("S3Metadata", metadata_filter, f);
  encode_json("S3Tags", tag_filter, f);
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
    if (std::any_of(result.first, result.second, [&filter](const std::pair<std::string, std::string>& p) { return p.second == filter.second;}))
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

void do_decode_xml_obj(rgw::notify::EventTypeList& l, const std::string& name, XMLObj *obj) {
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
  encode_json("policy", policy_text, f);
}

void rgw_pubsub_topic::dump_xml(Formatter *f) const
{
  encode_xml("User", user, f);
  encode_xml("Name", name, f);
  encode_xml("EndPoint", dest, f);
  encode_xml("TopicArn", arn, f);
  encode_xml("OpaqueData", opaque_data, f);
  encode_xml("Policy", policy_text, f);
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
  encode_xml_key_value_entry("Policy", policy_text, f);
  std::ostringstream stream;
  f->close_section(); // Attributes
}

void rgw_pubsub_topic::decode_json(JSONObj* f) {
  JSONDecoder::decode_json("user", user, f);
  JSONDecoder::decode_json("name", name, f);
  JSONDecoder::decode_json("dest", dest, f);
  JSONDecoder::decode_json("arn", arn, f);
  JSONDecoder::decode_json("opaqueData", opaque_data, f);
  JSONDecoder::decode_json("policy", policy_text, f);
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
  encode_json("TopicArn", topic.arn, f);
  encode_json("Id", s3_id, f);
  encode_json("Events", events, f);
  encode_json("Filter", s3_filter, f);
}

void rgw_pubsub_bucket_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "notifications");
  for (auto& t : topics) {
    encode_json(t.first.c_str(), t.second, f);
  }
}

void rgw_pubsub_topics::dump(Formatter *f) const
{
  Formatter::ArraySection s(*f, "topics");
  for (auto& t : topics) {
    auto& topic = t.second;
    if (topic.name == topic.dest.arn_topic) {
      encode_json(t.first.c_str(), topic, f);
    }
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
  using rgw::notify::DEFAULT_GLOBAL_VALUE;
  using rgw::notify::DEFAULT_CONFIG;
  encode_json("push_endpoint", push_endpoint, f);
  encode_json("push_endpoint_args", push_endpoint_args, f);
  encode_json("push_endpoint_topic", arn_topic, f);
  encode_json("stored_secret", stored_secret, f);
  encode_json("persistent", persistent, f);
  encode_json("time_to_live", time_to_live!=DEFAULT_GLOBAL_VALUE? std::to_string(time_to_live): DEFAULT_CONFIG, f);
  encode_json("max_retries", max_retries!=DEFAULT_GLOBAL_VALUE? std::to_string(max_retries): DEFAULT_CONFIG, f);
  encode_json("retry_sleep_duration", retry_sleep_duration!=DEFAULT_GLOBAL_VALUE? std::to_string(retry_sleep_duration): DEFAULT_CONFIG, f);
}

void rgw_pubsub_dest::dump_xml(Formatter *f) const
{
  using rgw::notify::DEFAULT_GLOBAL_VALUE;
  using rgw::notify::DEFAULT_CONFIG;
  encode_xml("EndpointAddress", push_endpoint, f);
  encode_xml("EndpointArgs", push_endpoint_args, f);
  encode_xml("EndpointTopic", arn_topic, f);
  encode_xml("HasStoredSecret", stored_secret, f);
  encode_xml("Persistent", persistent, f);
  encode_xml("TimeToLive", time_to_live!=DEFAULT_GLOBAL_VALUE? std::to_string(time_to_live): DEFAULT_CONFIG, f);
  encode_xml("MaxRetries", max_retries!=DEFAULT_GLOBAL_VALUE? std::to_string(max_retries): DEFAULT_CONFIG, f);
  encode_xml("RetrySleepDuration", retry_sleep_duration!=DEFAULT_GLOBAL_VALUE? std::to_string(retry_sleep_duration): DEFAULT_CONFIG, f);
}

std::string rgw_pubsub_dest::to_json_str() const
{
  using rgw::notify::DEFAULT_GLOBAL_VALUE;
  using rgw::notify::DEFAULT_CONFIG;
  JSONFormatter f;
  f.open_object_section("");
  encode_json("EndpointAddress", push_endpoint, &f);
  encode_json("EndpointArgs", push_endpoint_args, &f);
  encode_json("EndpointTopic", arn_topic, &f);
  encode_json("HasStoredSecret", stored_secret, &f);
  encode_json("Persistent", persistent, &f);
  encode_json("TimeToLive", time_to_live!=DEFAULT_GLOBAL_VALUE? std::to_string(time_to_live): DEFAULT_CONFIG, &f);
  encode_json("MaxRetries", max_retries!=DEFAULT_GLOBAL_VALUE? std::to_string(max_retries): DEFAULT_CONFIG, &f);
  encode_json("RetrySleepDuration", retry_sleep_duration!=DEFAULT_GLOBAL_VALUE? std::to_string(retry_sleep_duration): DEFAULT_CONFIG, &f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  return ss.str();
}

void rgw_pubsub_dest::decode_json(JSONObj* f) {
  using rgw::notify::DEFAULT_CONFIG;
  using rgw::notify::DEFAULT_GLOBAL_VALUE;
  JSONDecoder::decode_json("push_endpoint", push_endpoint, f);
  JSONDecoder::decode_json("push_endpoint_args", push_endpoint_args, f);
  JSONDecoder::decode_json("push_endpoint_topic", arn_topic, f);
  JSONDecoder::decode_json("stored_secret", stored_secret, f);
  JSONDecoder::decode_json("persistent", persistent, f);
  std::string ttl;
  JSONDecoder::decode_json("time_to_live", ttl, f);
  time_to_live = ttl == DEFAULT_CONFIG ? DEFAULT_GLOBAL_VALUE : std::stoul(ttl);

  std::string max_retry;
  JSONDecoder::decode_json("max_retries", max_retry, f);
  max_retries = max_retry == DEFAULT_CONFIG ? DEFAULT_GLOBAL_VALUE
                                            : std::stoul(max_retry);

  std::string sleep_dur;
  JSONDecoder::decode_json("retry_sleep_duration", sleep_dur, f);
  retry_sleep_duration = sleep_dur == DEFAULT_CONFIG ? DEFAULT_GLOBAL_VALUE
                                                     : std::stoul(sleep_dur);
}

RGWPubSub::RGWPubSub(rgw::sal::Driver* _driver, const std::string& _tenant)
  : driver(_driver), tenant(_tenant)
{}

RGWPubSub::RGWPubSub(rgw::sal::Driver* _driver,
                     const std::string& _tenant,
                     const rgw::SiteConfig& site)
    : driver(_driver), tenant(_tenant),
      use_notification_v2(rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2))
{
}

int RGWPubSub::get_topics(const DoutPrefixProvider* dpp,
                          const std::string& start_marker, int max_items,
                          rgw_pubsub_topics& result, std::string& next_marker,
                          optional_yield y) const
{
  if (!use_notification_v2) {
    // v1 returns all topics, ignoring marker/max_items
    return read_topics_v1(dpp, result, nullptr, y);
  }

  // TODO: prefix filter on 'tenant:'
  void* handle = NULL;
  int ret = driver->meta_list_keys_init(dpp, "topic", start_marker, &handle);
  if (ret < 0) {
    return ret;
  }
  auto g = make_scope_guard(
      [this, handle] { driver->meta_list_keys_complete(handle); });

  if (max_items > 1000) {
    max_items = 1000;
  }
  std::list<std::string> topics;
  bool truncated = false;
  ret = driver->meta_list_keys_next(dpp, handle, max_items, topics, &truncated);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
        << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  for (auto& topic_entry : topics) {
    std::string topic_name;
    std::string topic_tenant;
    parse_topic_entry(topic_entry, &topic_tenant, &topic_name);
    if (tenant != topic_tenant) {
      continue;
    }
    rgw_pubsub_topic topic;
    int r = get_topic(dpp, topic_name, topic, y, nullptr);
    if (r < 0) {
      continue;
    }
    result.topics[topic_name] = std::move(topic);
  }
  if (truncated) {
    next_marker = driver->meta_get_marker(handle);
  } else {
    next_marker.clear();
  }
  return ret;
}

int RGWPubSub::read_topics_v1(const DoutPrefixProvider *dpp, rgw_pubsub_topics& result,
                              RGWObjVersionTracker *objv_tracker, optional_yield y) const
{
  const int ret = driver->read_topics(tenant, result, objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "WARNING: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::write_topics_v1(const DoutPrefixProvider *dpp, const rgw_pubsub_topics& topics,
                               RGWObjVersionTracker *objv_tracker, optional_yield y) const
{
  const int ret = driver->write_topics(tenant, topics, objv_tracker, y, dpp);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::Bucket::read_topics(const DoutPrefixProvider *dpp, rgw_pubsub_bucket_topics& result,
    RGWObjVersionTracker *objv_tracker, optional_yield y) const
{
  const int ret = bucket->read_topics(result, objv_tracker, y, dpp);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWPubSub::Bucket::write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_bucket_topics& topics,
					RGWObjVersionTracker *objv_tracker,
					optional_yield y) const
{
  const int ret = bucket->write_topics(topics, objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write bucket topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::get_topic(const DoutPrefixProvider* dpp,
                         const std::string& name,
                         rgw_pubsub_topic& result,
                         optional_yield y,
                         std::set<std::string>* subscribed_buckets) const {
  if (use_notification_v2) {
    int ret = driver->read_topic_v2(name, tenant, result, nullptr, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to read topic info for name: " << name
                        << " tenant: " << tenant << ", ret=" << ret << dendl;
      return ret;
    }
    if (subscribed_buckets) {
      ret =
          driver->get_bucket_topic_mapping(result, *subscribed_buckets, y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 1)
            << "failed to fetch bucket topic mapping info for topic: " << name
            << " tenant: " << tenant << ", ret=" << ret << dendl;
      }
    }
    return ret;
  }
  rgw_pubsub_topics topics;
  const int ret = read_topics_v1(dpp, topics, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }

  auto iter = topics.topics.find(name);
  if (iter == topics.topics.end()) {
    ldpp_dout(dpp, 1) << "ERROR: topic not found" << dendl;
    return -ENOENT;
  }

  result = iter->second;
  return 0;
}

int get_bucket_notifications(const DoutPrefixProvider* dpp,
                             rgw::sal::Bucket* bucket,
                             rgw_pubsub_bucket_topics& bucket_topics) {
  const rgw::sal::Attrs& attrs = bucket->get_attrs();
  auto iter = attrs.find(RGW_ATTR_BUCKET_NOTIFICATION);
  if (iter == attrs.end()) {
    return 0;
  }
  try {
    const auto& bl = iter->second;
    auto biter = bl.cbegin();
    bucket_topics.decode(biter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 1) << "ERROR: failed to decode bucket topics for bucket: "
                      << bucket->get_name() << dendl;
    return -EIO;
  }
  return 0;
}

std::string topic_to_unique(const std::string& topic,
                            const std::string& notification) {
  return notification + "_" + topic;
}

// from list of bucket topics, find the one that was auto-generated by a notification
std::optional<rgw_pubsub_topic_filter> find_unique_topic(
    const rgw_pubsub_bucket_topics& bucket_topics,
    const std::string& notification_id) {
  auto it = std::find_if(bucket_topics.topics.begin(), bucket_topics.topics.end(),
                         [&](const auto& val) { return notification_id == val.second.s3_id; });
  if (it != bucket_topics.topics.end())
    return it->second;
  return std::nullopt;
}

int store_bucket_attrs_and_update_mapping(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    rgw::sal::Bucket* bucket,
    rgw_pubsub_bucket_topics& bucket_topics,
    const rgw_pubsub_topic& topic,
    optional_yield y) {
  rgw::sal::Attrs& attrs = bucket->get_attrs();
  if (!bucket_topics.topics.empty()) {
    bufferlist bl;
    bucket_topics.encode(bl);
    attrs[RGW_ATTR_BUCKET_NOTIFICATION] = std::move(bl);
  } else {
    auto it = attrs.find(RGW_ATTR_BUCKET_NOTIFICATION);
    if (it != attrs.end()) {
      attrs.erase(it);
    }
  }
  auto ret = bucket->merge_and_store_attrs(dpp, attrs, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
        << "Failed to store RGW_ATTR_BUCKET_NOTIFICATION on bucket="
        << bucket->get_name() << " returned err= " << ret << dendl;
    return ret;
  }
  if (bucket_topics.topics.empty()) {
    // remove the bucket name from  the topic-bucket omap
    auto op_ret = driver->update_bucket_topic_mapping(
        topic,
        rgw_make_bucket_entry_name(bucket->get_tenant(), bucket->get_name()),
        /*add_mapping=*/false, y, dpp);
    if (op_ret < 0) {
      // TODO: should the error be reported, as attrs are already deleted.
      // ret = op_ret;
    }
  }
  return ret;
}

int delete_notification_attrs(const DoutPrefixProvider* dpp,
                              rgw::sal::Bucket* bucket,
                              optional_yield y) {
  auto& attrs = bucket->get_attrs();
  auto iter = attrs.find(RGW_ATTR_BUCKET_NOTIFICATION);
  if (iter == attrs.end()) {
    return 0;
  }
  // delete all notifications of on a bucket
  attrs.erase(iter);
  auto ret = bucket->merge_and_store_attrs(dpp, attrs, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
        << "Failed to remove RGW_ATTR_BUCKET_NOTIFICATION attr on bucket="
        << bucket->get_name() << " ret= " << ret << dendl;
  }
  return ret;
}

int remove_notification_v2(const DoutPrefixProvider* dpp,
                           rgw::sal::Driver* driver,
                           rgw::sal::Bucket* bucket,
                           const std::string& notification_id,
                           optional_yield y) {
  rgw_pubsub_bucket_topics bucket_topics;
  auto ret = get_bucket_notifications(dpp, bucket, bucket_topics);
  if (ret < 0) {
    return -ret;
  }
  // no notifications on the bucket.
  if (bucket_topics.topics.empty()) {
    return 0;
  }
  // delete all notifications
  if (notification_id.empty()) {
    ret = delete_notification_attrs(dpp, bucket, y);
    if (ret < 0) {
      return ret;
    }
    int op_ret = driver->remove_bucket_mapping_from_topics(
        bucket_topics,
        rgw_make_bucket_entry_name(bucket->get_tenant(), bucket->get_name()), y,
        dpp);
    if (op_ret < 0) {
      // TODO: should the error be reported, as attrs are already deleted.
      // ret = op_ret;
    }
    return ret;
  }

  // delete a specific notification
  const auto unique_topic = find_unique_topic(bucket_topics, notification_id);
  if (!unique_topic) {
    // notification to be removed is not found - considered success
    ldpp_dout(dpp, 20) << "notification '" << notification_id
                       << "' already removed" << dendl;
    return 0;
  }
  const auto& topic_name = unique_topic->topic.name;
  bucket_topics.topics.erase(topic_to_unique(topic_name, notification_id));
  return store_bucket_attrs_and_update_mapping(
      dpp, driver, bucket, bucket_topics, unique_topic->topic, y);
}

int RGWPubSub::Bucket::create_notification(const DoutPrefixProvider *dpp, const std::string& topic_name,
    const rgw::notify::EventTypeList& events, OptionalFilter s3_filter, const std::string& notif_name, optional_yield y) const {
  rgw_pubsub_topic topic_info;

  int ret = ps.get_topic(dpp, topic_name, topic_info, y, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topic '" << topic_name << "' info: ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "successfully read topic '" << topic_name << "' info" << dendl;

  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  ret = read_topics(dpp, bucket_topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics from bucket '" << 
      bucket->get_name() << "': ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "successfully read " << bucket_topics.topics.size() << " topics from bucket '" << 
    bucket->get_name() << "'" << dendl;

  auto& topic_filter = bucket_topics.topics[topic_name];
  topic_filter.topic = topic_info;
  topic_filter.events = events;
  topic_filter.s3_id = notif_name;
  if (s3_filter) {
    topic_filter.s3_filter = *s3_filter;
  }

  ret = write_topics(dpp, bucket_topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics to bucket '" << bucket->get_name() << "': ret=" << ret << dendl;
    return ret;
  }
    
  ldpp_dout(dpp, 20) << "successfully wrote " << bucket_topics.topics.size() << " topics to bucket '" << bucket->get_name() << "'" << dendl;

  return 0;
}

int RGWPubSub::Bucket::remove_notification(const DoutPrefixProvider *dpp, const std::string& topic_name, optional_yield y) const
{
  return remove_notification_inner(dpp, topic_name, false, y);
}

int RGWPubSub::Bucket::remove_notification_inner(const DoutPrefixProvider *dpp, const std::string& notification_id,
                                  bool is_notification_id, optional_yield y) const
{
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_bucket_topics bucket_topics;

  auto ret = read_topics(dpp, bucket_topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read bucket topics info: ret=" << ret << dendl;
    return ret;
  }


  std::unique_ptr<std::string> topic_name = std::make_unique<std::string>(notification_id);
  if(is_notification_id) {
    auto iter = find_unique_topic(bucket_topics, notification_id);
    if (!iter) {
      ldpp_dout(dpp, 1) << "ERROR: notification was not found" << dendl;
      return -ENOENT;
    }
    topic_name = std::make_unique<std::string>(iter->topic.name);
  }

  if (bucket_topics.topics.erase(*topic_name) == 0) {
    ldpp_dout(dpp, 1) << "INFO: no need to remove, topic does not exist" << dendl;
    return 0;
  }

  if (bucket_topics.topics.empty()) {
    // no more topics - delete the notification object of the bucket
    ret = bucket->remove_topics(&objv_tracker, y, dpp);
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

int RGWPubSub::Bucket::remove_notification_by_id(const DoutPrefixProvider *dpp, const std::string& notif_id, optional_yield y) const
{
  return remove_notification_inner(dpp, notif_id, true, y);
}

int RGWPubSub::Bucket::remove_notifications(const DoutPrefixProvider *dpp, optional_yield y) const
{
  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  auto ret  = get_topics(dpp, bucket_topics, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get list of topics from bucket '" << bucket->get_name() << "', ret=" << ret << dendl;
    return ret ;
  }

  // remove all auto-generated topics
  for (const auto& topic : bucket_topics.topics) {
    const auto& topic_name = topic.first;
    ret = ps.remove_topic(dpp, topic_name, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 5) << "WARNING: failed to remove auto-generated topic '" << topic_name << "', ret=" << ret << dendl;
    }
  }

  // delete the notification object of the bucket
  ret = bucket->remove_topics(nullptr, y, dpp);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove bucket topics: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::create_topic(const DoutPrefixProvider* dpp,
                            const rgw_pubsub_topic& topic,
                            optional_yield y) const {
  RGWObjVersionTracker objv_tracker;
  auto ret = driver->write_topic_v2(topic, &objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topic info: ret=" << ret
                      << dendl;
  }
  return ret;
}

int RGWPubSub::create_topic(const DoutPrefixProvider* dpp,
                            const std::string& name,
                            const rgw_pubsub_dest& dest, const std::string& arn,
                            const std::string& opaque_data,
                            const rgw_user& user,
                            const std::string& policy_text,
                            optional_yield y) const {
  if (use_notification_v2) {
    rgw_pubsub_topic new_topic;
    new_topic.user = user;
    new_topic.name = name;
    new_topic.dest = dest;
    new_topic.arn = arn;
    new_topic.opaque_data = opaque_data;
    new_topic.policy_text = policy_text;
    return create_topic(dpp, new_topic, y);
  }
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_topics topics;

  int ret = read_topics_v1(dpp, topics, &objv_tracker, y);
  if (ret < 0 && ret != -ENOENT) {
    // its not an error if not topics exist, we create one
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  }
 
  rgw_pubsub_topic& new_topic = topics.topics[name];
  new_topic.user = user;
  new_topic.name = name;
  new_topic.dest = dest;
  new_topic.arn = arn;
  new_topic.opaque_data = opaque_data;
  new_topic.policy_text = policy_text;

  ret = write_topics_v1(dpp, topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWPubSub::remove_topic_v2(const DoutPrefixProvider* dpp,
                               const std::string& name,
                               optional_yield y) const {
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_topic topic;
  int ret = get_topic(dpp, name, topic, y, nullptr);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  } else if (ret == -ENOENT) {
    // it's not an error if no topics exist, just a no-op
    ldpp_dout(dpp, 10) << "WARNING: topic name:" << name
                       << " does not exist, deletion is a no-op: ret=" << ret
                       << dendl;
    return 0;
  }
  ret = driver->remove_topic_v2(name, tenant, &objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove topic info: ret=" << ret
                      << dendl;
    return ret;
  }
  ret = driver->delete_bucket_topic_mapping(topic, y, dpp);
  return ret;
}

int RGWPubSub::remove_topic(const DoutPrefixProvider *dpp, const std::string& name, optional_yield y) const
{
  if (use_notification_v2) {
    return remove_topic_v2(dpp, name, y);
  }
  RGWObjVersionTracker objv_tracker;
  rgw_pubsub_topics topics;

  int ret = read_topics_v1(dpp, topics, &objv_tracker, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read topics info: ret=" << ret << dendl;
    return ret;
  } else if (ret == -ENOENT) {
      // its not an error if no topics exist, just a no-op
      ldpp_dout(dpp, 10) << "WARNING: failed to read topics info, deletion is a no-op: ret=" << ret << dendl;
      return 0;
  }

  topics.topics.erase(name);

  ret = write_topics_v1(dpp, topics, &objv_tracker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove topics info: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}
