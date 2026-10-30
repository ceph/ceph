// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <boost/algorithm/string/predicate.hpp>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "acconfig.h"
#include "common/errno.h"

#include "rgw_arn.h"
#include "rgw_common.h"
#include "rgw_notify_event_type.h"
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
#endif

#include "../rgw_sal_nsfs.h"
#include "noobaa_notification_import.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

namespace {

/* An object member as text:  a string as it stands, a number or a
 * boolean as JSON writes it, as Node and node-rdkafka take them. */
std::optional<std::string> scalar(const rapidjson::Value& v)
{
  if (v.IsString()) {
    return std::string(v.GetString(), v.GetStringLength());
  }
  if (v.IsBool()) {
    return v.GetBool() ? "true" : "false";
  }
  if (v.IsNumber()) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    v.Accept(w);
    return std::string(buf.GetString(), buf.GetSize());
  }
  return std::nullopt;
}

/* `text`, an object or empty, parsed into `d`. */
bool parse_object(const std::string& text, rapidjson::Document& d)
{
  if (text.empty()) {
    d.SetObject();
    return true;
  }
  d.Parse(text.c_str(), text.size());
  return !d.HasParseError() && d.IsObject();
}

/* RGW's rule for a topic name (`validate_topic_name()`,
 * `rgw_rest_pubsub.cc`), unless `rgw_relaxed_topic_names` lifts it. */
bool topic_name_acceptable(CephContext* cct, const std::string& name)
{
  if (name.empty()) {
    return false;
  }
  if (cct->_conf.get_val<bool>("rgw_relaxed_topic_names")) {
    return true;
  }
  if (name.size() > 256) {
    return false;
  }
  return std::all_of(name.begin(), name.end(), [](unsigned char c) {
    return std::isalnum(c) || (c == '_') || (c == '-');
  });
}

void append_arg(std::string& args, const std::string& name,
                const std::string& value)
{
  if (!args.empty()) {
    args.push_back('&');
  }
  args += name + "=" + escape_endpoint_arg(value);
}

int translate_http(const NooBaaConnection& conn, bool https,
                   TranslatedTopic& out, std::string& why)
{
  rapidjson::Document agent;
  rapidjson::Document request;
  if (!parse_object(conn.agent_request_object, agent)) {
    why = "agent_request_object is not an object";
    return -EINVAL;
  }
  if (!parse_object(conn.request_options_object, request)) {
    why = "request_options_object is not an object";
    return -EINVAL;
  }

  /* Node's Agent merges its own options over the request's when it
   * connects, so an agent's host and port decide where the request
   * goes;  the request's own come from `hostname`, then `host`. */
  auto member = [](const rapidjson::Document& d, const char* name)
      -> std::optional<std::string> {
    auto i = d.FindMember(name);
    if (i == d.MemberEnd()) {
      return std::nullopt;
    }
    return scalar(i->value);
  };
  auto either = [&](const char* name) {
    if (auto v = member(agent, name)) {
      return v;
    }
    return member(request, name);
  };
  std::string host = "localhost";
  if (auto v = member(agent, "host")) {
    host = *v;
  } else if (auto v = member(request, "hostname")) {
    host = *v;
  } else if (auto v = member(request, "host")) {
    host = *v;
  }
  if (host.find(':') != std::string::npos) {
    host = "[" + host + "]";
  }
  std::string url = https ? "https://" : "http://";
  if (!conn.auth.empty()) {
    const auto colon = conn.auth.find(':');
    if (colon == std::string::npos) {
      why = "request_options_object.auth names a user and no password";
      return -EINVAL;
    }
    url += url_encode(conn.auth.substr(0, colon)) + ":" +
           url_encode(conn.auth.substr(colon + 1)) + "@";
    out.dest.stored_secret = true;
    if (!https) {
      out.warnings.push_back(
          "its credential is sent over http, as NooBaa sent it");
    }
  }
  url += host;
  if (auto v = either("port")) {
    url += ":" + *v;
  }
  if (auto v = member(request, "path")) {
    url += (v->empty() || ((*v)[0] != '/')) ? "/" + *v : *v;
  }
  out.dest.push_endpoint = url;

  std::string args;
  if (auto v = either("rejectUnauthorized"); v && (*v == "false")) {
    append_arg(args, "verify-ssl", "false");
  }
  for (const auto& [from, to] :
         {std::pair{"local_file_ca", "ca-location"},
          std::pair{"local_file_cert", "ssl-certificate-location"},
          std::pair{"local_file_key", "ssl-key-location"}}) {
    if (auto v = either(from)) {
      append_arg(args, to, *v);
    }
  }
  out.dest.push_endpoint_args = std::move(args);

  /* what is left has no counterpart in an RGW HTTP topic */
  static const std::initializer_list<std::string_view> carried = {
    "host", "hostname", "port", "path", "auth", "rejectUnauthorized",
    "local_file_ca", "local_file_cert", "local_file_key",
  };
  for (const auto& [d, which] : {std::pair{&agent, "agent_request_object"},
                                 std::pair{&request, "request_options_object"}}) {
    for (auto i = d->MemberBegin(); i != d->MemberEnd(); ++i) {
      const std::string_view name(i->name.GetString(), i->name.GetStringLength());
      if (std::find(carried.begin(), carried.end(), name) == carried.end()) {
        out.not_carried.push_back(std::string(which) + "." + std::string(name));
      }
    }
  }
  out.name = conn.name;
  return 0;
}

#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
int translate_kafka(CephContext* cct, const NooBaaConnection& conn,
                    const std::string& kafka_topic, TranslatedTopic& out,
                    std::string& why)
{
  rapidjson::Document options;
  if (!parse_object(conn.kafka_options_object, options)) {
    why = "kafka_options_object is not an object";
    return -EINVAL;
  }
  out.name = kafka_topic.empty() ? conn.topic : kafka_topic;
  if (out.name.empty()) {
    why = "no Kafka topic";
    return -EINVAL;
  }

  /* the broker list becomes the endpoint and kafka-brokers;  every
   * other setting is librdkafka's, and carried as it stands */
  std::string brokers;
  rapidjson::Document config;
  config.SetObject();
  bool has_secret = false;
  bool plaintext = true;
  for (auto i = options.MemberBegin(); i != options.MemberEnd(); ++i) {
    const std::string key(i->name.GetString(), i->name.GetStringLength());
    if ((key == "metadata.broker.list") || (key == "bootstrap.servers")) {
      if (auto v = scalar(i->value)) {
        brokers = *v;
      }
      continue;
    }
    if (!scalar(i->value)) {
      out.not_carried.push_back("kafka_options_object." + key +
                                ", which is not a string, number or boolean");
      continue;
    }
    if (key == "security.protocol") {
      const auto v = *scalar(i->value);
      plaintext = boost::iequals(v, "plaintext") || boost::iequals(v, "sasl_plaintext");
    }
    if (!rgw::kafka::allowed_key(cct, key)) {
      out.warnings.push_back(
          "kafka-config key " + key + " is not in rgw_kafka_config_allowed_keys;  "
          "delivery is refused until it is listed");
    }
    if (rgw::kafka::sensitive_key(key)) {
      has_secret = true;
    }
    rapidjson::Value name(key.c_str(), config.GetAllocator());
    rapidjson::Value value(i->value, config.GetAllocator());
    config.AddMember(name, value, config.GetAllocator());
  }
  if (brokers.empty()) {
    why = "kafka_options_object has no metadata.broker.list";
    return -EINVAL;
  }

  std::string first = brokers;
  std::string rest;
  if (const auto comma = brokers.find(','); comma != std::string::npos) {
    first = brokers.substr(0, comma);
    rest = brokers.substr(comma + 1);
  }
  out.dest.push_endpoint = "kafka://" + first;
  std::string args;
  append_arg(args, "kafka-ack-level", "broker");
  if (!rest.empty()) {
    append_arg(args, "kafka-brokers", rest);
  }
  if (config.MemberCount() > 0) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    config.Accept(w);
    append_arg(args, "kafka-config", std::string(buf.GetString(), buf.GetSize()));
  }
  out.dest.push_endpoint_args = std::move(args);
  if (has_secret) {
    out.dest.stored_secret = true;
    if (plaintext) {
      out.warnings.push_back(
          "its credential is sent to a plaintext broker, as NooBaa sent it;  "
          "delivery is refused until rgw_allow_notification_secrets_in_cleartext "
          "is set or the broker is moved");
    }
  }
  return 0;
}
#endif

bool same_destination(const rgw_pubsub_dest& a, const rgw_pubsub_dest& b)
{
  return (a.push_endpoint == b.push_endpoint) &&
         (a.push_endpoint_args == b.push_endpoint_args) &&
         (a.arn_topic == b.arn_topic) &&
         (a.persistent == b.persistent);
}

} // namespace

int translate_connection(CephContext* cct, const NooBaaConnection& conn,
                         const std::string& kafka_topic, TranslatedTopic& out,
                         std::string& why)
{
  out = TranslatedTopic{};
  int ret;
  if (boost::iequals(conn.notification_protocol, "http") ||
      boost::iequals(conn.notification_protocol, "https")) {
    ret = translate_http(conn, boost::iequals(conn.notification_protocol, "https"),
                         out, why);
  } else if (boost::iequals(conn.notification_protocol, "kafka")) {
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
    ret = translate_kafka(cct, conn, kafka_topic, out, why);
#else
    why = "this gateway is built without Kafka";
    ret = -EINVAL;
#endif
  } else {
    why = "notification_protocol " + conn.notification_protocol + " is not http, https or kafka";
    ret = -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }
  if (!topic_name_acceptable(cct, out.name)) {
    why = "topic name " + out.name + " is not one RGW's topic names can hold";
    return -EINVAL;
  }
  out.dest.arn_topic = out.name;
  return 0;
}

void translate_events(const std::vector<std::string>& names,
                      rgw::notify::EventTypeList& events,
                      std::vector<std::string>& unknown)
{
  using namespace rgw::notify;
  /* every event RGW names, wildcards included;  `to_string()` is its
   * spelling */
  static const EventType all[] = {
    ObjectCreated, ObjectCreatedPut, ObjectCreatedPost, ObjectCreatedCopy,
    ObjectCreatedCompleteMultipartUpload,
    ObjectRemoved, ObjectRemovedDelete, ObjectRemovedDeleteMarkerCreated,
    ObjectLifecycle, ObjectExpiration, ObjectExpirationCurrent,
    ObjectExpirationNoncurrent, ObjectExpirationDeleteMarker,
    ObjectExpirationAbortMPU, ObjectTransition, ObjectTransitionCurrent,
    ObjectTransitionNoncurrent,
    ObjectSynced, ObjectSyncedCreate, ObjectSyncedDelete,
    ObjectSyncedDeletionMarkerCreated,
    LifecycleExpiration, LifecycleExpirationDelete,
    LifecycleExpirationDeleteMarkerCreated, LifecycleTransition,
    Replication, ReplicationCreate, ReplicationDelete,
    ReplicationDeletionMarkerCreated,
    ObjectRestore, ObjectRestoreInitiated, ObjectRestoreCompleted,
    ObjectRestoreExpired,
    ObjectTagging, ObjectTaggingPut, ObjectTaggingDelete,
    ObjectAcl, ObjectAclPut,
  };
  for (const auto& name : names) {
    auto t = from_string(name);
    if (t == UnknownEvent) {
      for (const auto e : all) {
        if (boost::iequals(name, to_string(e))) {
          t = e;
          break;
        }
      }
    }
    if (t == UnknownEvent) {
      unknown.push_back(name);
    } else {
      events.push_back(t);
    }
  }
}

int import_bucket_notifications(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                NSFSDriver* driver,
                                const std::string& bucket_name,
                                const std::string& notifications_json,
                                const std::vector<NooBaaConnection>& connections,
                                const std::string& account,
                                Attrs& attrs,
                                NotificationImportResult& result)
{
  rapidjson::Document d;
  d.Parse(notifications_json.c_str(), notifications_json.size());
  if (d.HasParseError() || !d.IsArray()) {
    result.reported.push_back(bucket_name + ": notifications is not a list;  none carried");
    return 0;
  }
  auto report = [&](const std::string& id, const std::string& what) {
    result.reported.push_back(bucket_name + ": notification " + id + ": " + what);
  };
  /* each element holds one-element lists, as NooBaa's schema has it
   * and its delivery reads them (`notif.id[0]`, `notif.topic[0]`) */
  auto first = [](const rapidjson::Value& n, const char* name) -> std::string {
    auto i = n.FindMember(name);
    if ((i == n.MemberEnd()) || !i->value.IsArray() || i->value.Empty() ||
        !i->value[0].IsString()) {
      return {};
    }
    return std::string(i->value[0].GetString(), i->value[0].GetStringLength());
  };

  rgw_pubsub_bucket_topics bucket_topics;
  const std::string zonegroup = driver->get_zone()->get_zonegroup().get_name();
  for (const auto& n : d.GetArray()) {
    if (!n.IsObject()) {
      result.reported.push_back(bucket_name + ": a notification that is not an object;  not carried");
      continue;
    }
    const std::string id = first(n, "id");
    std::string connection = first(n, "topic");
    if (id.empty() || connection.empty()) {
      report(id.empty() ? "<no id>" : id, "no id or no topic;  not carried");
      continue;
    }
    if (account.empty()) {
      report(id, "the bucket has no owning account to own a topic;  not carried");
      continue;
    }

    /* `kafka:::topic/<connection>/<topic>` names the Kafka topic itself;
     * NooBaa wrote the account's default_connection into it when the
     * notification was configured */
    std::string kafka_topic;
    if (connection.starts_with("kafka:::topic/")) {
      const auto parts = connection.substr(sizeof("kafka:::topic/") - 1);
      const auto slash = parts.find('/');
      if (slash == std::string::npos) {
        report(id, "topic " + connection + " is not kafka:::topic/<connection>/<topic>;  not carried");
        continue;
      }
      kafka_topic = parts.substr(slash + 1);
      connection = parts.substr(0, slash);
    }
    auto c = std::find_if(connections.begin(), connections.end(),
                          [&](const NooBaaConnection& c) { return c.name == connection; });
    if (c == connections.end()) {
      report(id, "names connection " + connection + ", which the store does not hold;  not carried");
      continue;
    }

    TranslatedTopic t;
    std::string why;
    if (translate_connection(dpp->get_cct(), *c, kafka_topic, t, why) < 0) {
      report(id, "connection " + connection + ": " + why + ";  not carried");
      continue;
    }

    rgw::notify::EventTypeList events;
    std::vector<std::string> unknown;
    std::vector<std::string> names;
    if (auto e = n.FindMember("event"); (e != n.MemberEnd()) && e->value.IsArray()) {
      for (const auto& v : e->value.GetArray()) {
        if (v.IsString()) {
          names.emplace_back(v.GetString(), v.GetStringLength());
        }
      }
    }
    translate_events(names, events, unknown);
    for (const auto& u : unknown) {
      report(id, "event " + u + " is not one RGW sends;  left out");
    }
    if (!names.empty() && events.empty()) {
      /* an empty list is every event, which these did not ask for */
      report(id, "no event RGW sends;  not carried");
      continue;
    }

    rgw_pubsub_topic topic;
    topic.owner = rgw_owner{rgw_account_id{account}};
    topic.name = t.name;
    topic.dest = t.dest;
    topic.arn = rgw::ARN{rgw::Partition::aws, rgw::Service::sns, zonegroup,
                         account, t.name}.to_string();

    rgw_pubsub_topic present;
    int ret = driver->read_topic_v2(t.name, account, present, nullptr, y, dpp);
    if (ret == 0) {
      if (!same_destination(present.dest, topic.dest)) {
        report(id, "topic " + t.name + " already exists in the account with "
               "another destination (" + present.dest.push_endpoint +
               ");  not carried");
        continue;
      }
      topic = present;
    } else if (ret == -ENOENT) {
      RGWObjVersionTracker objv;
      objv.generate_new_write_ver(dpp->get_cct());
      ret = driver->write_topic_v2(topic, false, objv, y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: import: " << bucket_name
          << ": writing topic " << t.name << ": " << cpp_strerror(-ret) << dendl;
        return ret;
      }
      ++result.topics_written;
    } else {
      ldpp_dout(dpp, 0) << "ERROR: import: " << bucket_name
        << ": reading topic " << t.name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    for (const auto& m : t.not_carried) {
      report(id, "connection " + connection + ": " + m + " is not carried");
    }
    for (const auto& w : t.warnings) {
      report(id, "connection " + connection + ": " + w);
    }

    auto& filter = bucket_topics.topics[topic_to_unique(t.name, id)];
    filter.topic = topic;
    filter.events = std::move(events);
    filter.s3_id = id;
    if (std::none_of(result.topics.begin(), result.topics.end(),
                     [&](const rgw_pubsub_topic& r) { return r.name == topic.name; })) {
      result.topics.push_back(topic);
    }
    ++result.notifications_carried;
  }

  if (!bucket_topics.topics.empty()) {
    bufferlist bl;
    bucket_topics.encode(bl);
    attrs[RGW_ATTR_BUCKET_NOTIFICATION] = std::move(bl);
  }
  return 0;
}

}}} // namespace rgw::sal::nsfs
