// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <time.h>
#include <random>
#include "common/ceph_time.h"
#include "rgw_bucket_logging.h"
#include "rgw_xml.h"
#include "rgw_sal.h"
#include "rgw_op.h"
#include "rgw_auth_s3.h"
#include <boost/lexical_cast.hpp>

#define dout_subsys ceph_subsys_rgw

namespace rgw::bucketlogging {

bool configuration::decode_xml(XMLObj* obj) {
  const auto throw_if_missing = true;
  enabled = false;
  XMLObjIter iter = obj->find("LoggingEnabled");
  XMLObj* const o = iter.get_next();
  if (o) {
    enabled = true;
    RGWXMLDecoder::decode_xml("TargetBucket", target_bucket, o, throw_if_missing);
    RGWXMLDecoder::decode_xml("TargetPrefix", target_prefix, o);
    // TODO: decode grant
    RGWXMLDecoder::decode_xml("ObjectRollTime", obj_roll_time, default_obj_roll_time, o);
    std::string default_type{"Standard"};
    std::string type;
    RGWXMLDecoder::decode_xml("LoggingType", type, default_type, o);
    if (type == "Standard") {
      logging_type = LoggingType::Standard;
    } else if (type == "Journal") {
      logging_type = LoggingType::Journal;
      if (iter = o->find("Filter"); XMLObj* const filter_o = iter.get_next()) {
        RGWXMLDecoder::decode_xml("S3Key", key_filter, filter_o);
      }
    } else {
      // we don't allow for type "Any" in the configuration
      throw RGWXMLDecoder::err("invalid bucket logging record type: '" + type + "'");
    }
    RGWXMLDecoder::decode_xml("RecordsBatchSize", records_batch_size, o);
    if (iter = o->find("TargetObjectKeyFormat"); XMLObj* const oo = iter.get_next()) {
      if (iter = oo->find("PartitionedPrefix"); XMLObj* const ooo = iter.get_next()) {
        obj_key_format = KeyFormat::Partitioned;
        default_type = "DeliveryTime";
        RGWXMLDecoder::decode_xml("PartitionDateSource", type, default_type, ooo);
        if (type == "DeliveryTime") {
          date_source = PartitionDateSource::DeliveryTime;
        } else if (type == "EventTime") {
          date_source = PartitionDateSource::EventTime;
        } else {
          throw RGWXMLDecoder::err("invalid bucket logging partition date source: '" + type + "'");
        }
      } else if (iter = oo->find("SimplePrefix"); iter.get_next()) {
          obj_key_format = KeyFormat::Simple;
      } else {
        throw RGWXMLDecoder::err("TargetObjectKeyFormat must contain a format tag");
      }
    }
  }

  return true;
}

void configuration::dump_xml(Formatter *f) const {
  if (!enabled) {
    return;
  }
  f->open_object_section("LoggingEnabled");
  ::encode_xml("TargetBucket", target_bucket, f);
  ::encode_xml("TargetPrefix", target_prefix, f);
  ::encode_xml("ObjectRollTime", obj_roll_time, f);
  switch (logging_type) {
    case LoggingType::Standard:
      ::encode_xml("LoggingType", "Standard", f);
      break;
    case LoggingType::Journal:
      ::encode_xml("LoggingType", "Journal", f);
      if (key_filter.has_content()) {
        f->open_object_section("Filter");
        ::encode_xml("S3Key", key_filter, f);
        f->close_section(); // Filter
      }
      break;
    case LoggingType::Any:
      ::encode_xml("LoggingType", "", f);
      break;
  }
  ::encode_xml("RecordsBatchSize", records_batch_size, f);
  f->open_object_section("TargetObjectKeyFormat");
  switch (obj_key_format) {
    case KeyFormat::Partitioned:
      f->open_object_section("PartitionedPrefix");
      switch (date_source) {
        case PartitionDateSource::DeliveryTime:
          ::encode_xml("PartitionDateSource", "DeliveryTime", f);
          break;
        case PartitionDateSource::EventTime:
          ::encode_xml("PartitionDateSource", "EventTime", f);
          break;
      }
      f->close_section(); // PartitionedPrefix
      break;
    case KeyFormat::Simple:
      f->open_object_section("SimplePrefix"); // empty section
      f->close_section();
      break;
  }
  f->close_section(); // TargetObjectKeyFormat
  f->close_section(); // LoggingEnabled
}

void configuration::dump(Formatter *f) const {
  Formatter::ObjectSection s(*f, "bucketLoggingStatus");
  if (!enabled) {
    return;
  }
  {
    Formatter::ObjectSection s(*f, "loggingEnabled");
    encode_json("targetBucket", target_bucket, f);
    encode_json("targetPrefix", target_prefix, f);
    encode_json("objectRollTime", obj_roll_time, f);
    switch (logging_type) {
      case LoggingType::Standard:
        encode_json("loggingType", "Standard", f);
        break;
      case LoggingType::Journal:
        encode_json("loggingType", "Journal", f);
        if (key_filter.has_content()) {
          Formatter::ObjectSection s(*f, "Filter");
          encode_json("S3Key", key_filter, f);
        }
        break;
      case LoggingType::Any:
        encode_json("loggingType", "", f);
        break;
    }
    encode_json("recordsBatchSize", records_batch_size, f);
    {
      Formatter::ObjectSection s(*f, "targetObjectKeyFormat");
      switch (obj_key_format) {
        case KeyFormat::Partitioned:
        {
          Formatter::ObjectSection s(*f, "partitionedPrefix");
          switch (date_source) {
            case PartitionDateSource::DeliveryTime:
              encode_json("partitionDateSource", "DeliveryTime", f);
              break;
            case PartitionDateSource::EventTime:
              encode_json("partitionDateSource", "EventTime", f);
              break;
          }
        }
        break;
        case KeyFormat::Simple:
        {
          Formatter::ObjectSection s(*f, "simplePrefix");
        }
        break;
      }
    }
  }
}

std::string configuration::to_json_str() const {
  JSONFormatter f;
  dump(&f);
  std::stringstream ss;
  f.flush(ss);
  return ss.str();
}

// create a random string of N characters
template<size_t N>
std::string random_string() {
  static const std::string possible_characters{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXY"};
  static const auto max_possible_value = possible_characters.length() - 1;
  std::random_device rd;
  std::mt19937 engine(rd());
  std::uniform_int_distribution<> dist(0, max_possible_value);
  std::string str(N, '\0');
  std::generate_n(str.begin(), N, [&](){return possible_characters[dist(engine)];});
  return str;
}

// create a string that start with an incremenatl counter
// of INC charecters and ends with a random string of RND characters
// fallback to a random string of INC+RND characters
template<size_t INC, size_t RND>
std::string incremental_string(const DoutPrefixProvider *dpp, std::optional<std::string> old_name) {
  static const auto format = fmt::format("{{:0>{}}}{{}}", INC);
  uint32_t counter = 0;
  if (!old_name) {
    const auto random_part = random_string<RND>();
    return fmt::vformat(format, fmt::make_format_args(counter, random_part));
  }
  const auto str_counter = old_name->substr(old_name->length() - (INC+RND), INC);
  try {
    counter = boost::lexical_cast<uint32_t>(str_counter);
    // we are not concerned about overflow here, as the counter is only used to
    // distinguish between different logging objects created in the same second
    ++counter;
    const auto random_part = random_string<RND>();
    return fmt::vformat(format, fmt::make_format_args(counter, random_part));
  } catch (const boost::bad_lexical_cast& e) {
    ldpp_dout(dpp, 5) << "WARNING: failed to convert string '" << str_counter <<
      "' to counter. " << e.what() << ". will create random temporary logging file name" << dendl;
    return random_string<INC+RND>();
  }
}

constexpr size_t UniqueStringLength = 16;
// we need 10 characters for the counter (uint32_t)
constexpr size_t CounterStringLength = 10;
constexpr size_t RandomStringLength = UniqueStringLength - CounterStringLength;

ceph::coarse_real_time time_from_name(const std::string& obj_name, const DoutPrefixProvider *dpp) {
  static const auto time_format_length = std::string{"YYYY-MM-DD-hh-mm-ss"}.length();
  const auto obj_name_length = obj_name.length();
  ceph::coarse_real_time extracted_time;
  if (obj_name_length < time_format_length + UniqueStringLength + 1) {
    ldpp_dout(dpp, 1) << "ERROR: logging object name too short: " << obj_name << dendl;
    return extracted_time;
  }
  const auto time_start_pos = obj_name_length - (time_format_length + UniqueStringLength + 1);
  // note: +1 is for the dash between the timestamp and the unique string
  std::string time_str = obj_name.substr(time_start_pos, time_format_length);

  std::tm t;
  memset(&t, 0, sizeof(tm));
  if (const char* ret = strptime(time_str.c_str(), "%Y-%m-%d-%H-%M-%S", &t); ret == nullptr || *ret != '\0') {
    ldpp_dout(dpp, 1) << "ERROR: invalid time format: '" << time_str << "' in logging object name: " << obj_name << dendl;
    return extracted_time;
  }
  extracted_time = ceph::coarse_real_time::clock::from_time_t(mktime(&t));
  ldpp_dout(dpp, 20) << "INFO: time '" << extracted_time << "' extracted from logging object name: " << obj_name << dendl;
  return extracted_time;
}

std::string full_bucket_name(const std::unique_ptr<rgw::sal::Bucket>& bucket) {
  if (bucket->get_tenant().empty()) {
    return bucket->get_name();
  }
  return fmt::format("{}:{}", bucket->get_tenant(), bucket->get_name());
}

int new_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    const std::string& region,
    const std::unique_ptr<rgw::sal::Bucket>& source_bucket,
    optional_yield y,
    std::optional<std::string> old_name,
    RGWObjVersionTracker* objv_tracker) {
  const auto tt = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now());
  std::tm t{};
  localtime_r(&tt, &t);

  const auto unique = incremental_string<CounterStringLength, RandomStringLength>(dpp, old_name);

  switch (conf.obj_key_format) {
    case KeyFormat::Simple:
      // [DestinationPrefix][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
      obj_name = fmt::format("{}{:%Y-%m-%d-%H-%M-%S}-{}",
        conf.target_prefix,
        t,
        unique);
      break;
    case KeyFormat::Partitioned:
      {
        // TODO: support both EventTime and DeliveryTime
        // [DestinationPrefix][SourceAccountId]/[SourceRegion]/[SourceBucket]/[YYYY]/[MM]/[DD]/[YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
        obj_name = fmt::format("{}{}/{}/{}/{:%Y/%m/%d}/{:%Y-%m-%d-%H-%M-%S}-{}",
          conf.target_prefix,
          to_string(source_bucket->get_owner()),
          region,
          full_bucket_name(source_bucket),
          t,
          t,
          unique);
      }
      break;
  }
  const auto& target_bucket_id = target_bucket->get_key();
  int ret = target_bucket->set_logging_object_name(obj_name, conf.target_prefix, y, dpp, (old_name == std::nullopt), objv_tracker);
  if (ret == -EEXIST || ret == -ECANCELED) {
   if (ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, nullptr); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
        target_bucket_id << "' and prefix '" << conf.target_prefix << "', ret = " << ret << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << "INFO: name already set. got name of logging object '" << obj_name <<  "' of bucket '" <<
      target_bucket_id << "' and prefix '" << conf.target_prefix << "'" << dendl;
    return -ECANCELED;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write name of logging object '" << obj_name << "' of bucket '" <<
      target_bucket_id << "'. ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: wrote name of logging object '" << obj_name <<  "' of bucket '" <<
      target_bucket_id << "'" << dendl;
  return 0;
}

int commit_logging_object(const configuration& conf,
    const DoutPrefixProvider *dpp,
    rgw::sal::Driver* driver,
    const std::string& tenant_name,
    optional_yield y,
    std::string* last_committed) {
  std::string target_bucket_name;
  std::string target_tenant_name;
  int ret = rgw_parse_url_bucket(conf.target_bucket, tenant_name, target_tenant_name, target_bucket_name);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to parse target bucket '" << conf.target_bucket << "' when commiting logging object, ret = "
      << ret << dendl;
    return ret;
  }
  const rgw_bucket target_bucket_id(target_tenant_name, target_bucket_name);
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  ret = driver->load_bucket(dpp, target_bucket_id,
                               &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target logging bucket '" << target_bucket_id << "' when commiting logging object, ret = "
      << ret << dendl;
    return ret;
  }
  return commit_logging_object(conf, target_bucket, dpp, y, last_committed);
}

int commit_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    std::string* last_committed) {
  std::string obj_name;
  if (const int ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, nullptr); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      target_bucket->get_key() << "'. ret = " << ret << dendl;
    return ret;
  }
  if (const int ret = target_bucket->commit_logging_object(obj_name, y, dpp, conf.target_prefix, last_committed); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to commit logging object '" << obj_name << "' of bucket '" <<
      target_bucket->get_key() << "'. ret = " << ret << dendl;
    return ret;
  }
  return 0;
}

int rollover_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    const std::string& region,
    const std::unique_ptr<rgw::sal::Bucket>& source_bucket,
    optional_yield y,
    bool must_commit,
    RGWObjVersionTracker* objv_tracker,
    std::string* last_committed) {
  std::string target_bucket_name;
  std::string target_tenant_name;
  std::ignore = rgw_parse_url_bucket(conf.target_bucket, target_bucket->get_tenant(), target_tenant_name, target_bucket_name);
  if (target_bucket_name != target_bucket->get_name() || target_tenant_name != target_bucket->get_tenant()) {
    ldpp_dout(dpp, 1) << "ERROR: logging bucket name mismatch. conf= '" << conf.target_bucket <<
      "', bucket= '" << target_bucket->get_key() << "'" << dendl;
    return -EINVAL;
  }
  const auto old_obj = obj_name;
  const int ret = new_logging_object(conf, target_bucket, obj_name, dpp, region, source_bucket, y, old_obj, objv_tracker);
  if (ret == -ECANCELED) {
    ldpp_dout(dpp, 20) << "INFO: rollover already performed for object '" << old_obj <<  "' to logging bucket '" <<
      target_bucket->get_key() << "'. ret = " << ret << dendl;
    return 0;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to rollover object '" << old_obj << "' to logging bucket '" <<
      target_bucket->get_key() << "'. ret = " << ret << dendl;
    return ret;
  }
  if (const int ret = target_bucket->commit_logging_object(old_obj, y, dpp, conf.target_prefix, last_committed); ret < 0) {
    if (must_commit) {
      return ret;
    }
    ldpp_dout(dpp, 5) << "WARNING: failed to commit object '" << old_obj << "' to logging bucket '" <<
      target_bucket->get_key() << "'. ret = " << ret << dendl;
    // we still want to write the new records to the new object even if commit failed
    // will try to commit again next time
  }
  return 0;
}

#define dash_if_empty(S) (S).empty() ? "-" : S
#define dash_if_empty_or_null(P, S) (((P) == nullptr) || (S).empty()) ? "-" : S
#define dash_if_zero(I) (I) == 0 ? "-" : std::to_string(I)
#define dash_if_zero_or_null(P, I) (((P) == nullptr) || ((I) == 0)) ? "-" : std::to_string(I)

/* S3 bucket standard log record
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
  - bucket owner
  - bucket name
  - The time at which the request was received at UTC time. The format, as follows: [%d/%b/%Y:%H:%M:%S %z]
  - The apparent IP address of the requester
  - The canonical user ID of the requester, or a - for unauthenticated requests
  - Request ID
  - REST.HTTP_method.resource_type or S3.action.resource_type for Lifecycle and logging
  - The key (object name) part of the request (source key in case of copy)
  - The Request-URI part of the HTTP request message
  - The numeric HTTP status code of the response
  - The S3 Error code, or - if no error occurred
  - The number of response bytes sent, excluding HTTP protocol overhead, or - if zero
  - Object Size
  - Total time: milliseconds including network transmission time. from first byte received to last byte transmitted
  - turn around time: milliseconds exluding networks transmission time. from last byte received to first byte transmitted
  - The value of the HTTP Referer header, if present, or - if not
  - User Agent
  - The version ID in the request, or - if the operation doesn't take a versionId parameter
  - Host ID: x-amz-id-2
  - SigV2 or SigV4, that was used to authenticate the request or a - for unauthenticated requests
  - SSL cipher that was negotiated for an HTTPS request or a - for HTTP
  - The type of request authentication used: AuthHeader, QueryString or a - for unauthenticated requests
  - Host Header: The RGW endpoint fqdn
  - TLS version negotiated by the client: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used
  - ARN of the access point of the request. If the access point ARN is malformed or not used, the field will contain a -
  - A string that indicates whether the request required an (ACL) for authorization. If ACL is required, the string is Yes. If no ACLs were required, the string is -

S3 bucket short (ceph) log record
  - bucket owner
  - bucket name
  - The time at which the request was received at UTC time. The format, as follows: [%d/%b/%Y:%H:%M:%S %z]
  - REST.HTTP_method.resource_type or S3.action.resource_type for Lifecycle and logging
  - The key (object name) part of the request (source key in case of copy)
  - Object version in case of versioned bucket
  - Object Size
  - eTag
};*/

int log_record(rgw::sal::Driver* driver,
    const sal::Object* obj,
    req_state* s,
    const std::string& op_name,
    const std::string& etag,
    size_t size,
    const configuration& conf,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    bool async_completion,
    bool log_source_bucket) {
  if (!s->bucket) {
    ldpp_dout(dpp, 1) << "ERROR: only bucket operations are logged in bucket logging" << dendl;
    return -EINVAL;
  }
  std::string target_bucket_name;
  std::string target_tenant_name;
  int ret = rgw_parse_url_bucket(conf.target_bucket, s->bucket_tenant, target_tenant_name, target_bucket_name);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to parse target logging bucket '" << conf.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }
  const rgw_bucket target_bucket_id(target_tenant_name, target_bucket_name);
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  ret = driver->load_bucket(dpp, target_bucket_id,
                               &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target logging bucket '" << target_bucket_id << "'. ret = " << ret << dendl;
    return ret;
  }

  rgw::ARN target_resource_arn(target_bucket_id, conf.target_prefix);
  if (ret = verify_target_bucket_policy(dpp, target_bucket.get(), target_resource_arn, s); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to verify target logging bucket policy for bucket '" << target_bucket->get_key() <<
      "'. ret = " << ret << dendl;
    return -EACCES;
  }

  if (ret = verify_target_bucket_attributes(dpp, target_bucket.get()); ret < 0) {
    return ret;
  }

  const auto region = driver->get_zone()->get_zonegroup().get_api_name();
  std::string obj_name;
  RGWObjVersionTracker objv_tracker;
  ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, &objv_tracker);
  if (ret == 0) {
    const auto time_to_commit = time_from_name(obj_name, dpp) + std::chrono::seconds(conf.obj_roll_time);
    if (ceph::coarse_real_time::clock::now() > time_to_commit) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' exceeded its time, will be committed to bucket '" <<
        target_bucket_id << "'" << dendl;
      if (ret = rollover_logging_object(conf, target_bucket, obj_name, dpp, region, s->bucket, y, false, &objv_tracker, nullptr); ret < 0) {
        return ret;
      }
    } else {
      ldpp_dout(dpp, 20) << "INFO: record will be written to current logging object '" << obj_name << "'. will be comitted at: " << time_to_commit << dendl;
    }
  } else if (ret == -ENOENT) {
    // try to create the temporary log object for the first time
    ret = new_logging_object(conf, target_bucket, obj_name, dpp, region, s->bucket, y, std::nullopt, nullptr);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "INFO: first time logging for bucket '" << target_bucket_id << "' and prefix '" <<
        conf.target_prefix << "'" << dendl;
    } else if (ret == -ECANCELED) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' already exists for bucket '" << target_bucket_id << "' and prefix" <<
        conf.target_prefix << "'" << dendl;
    } else {
      ldpp_dout(dpp, 1) << "ERROR: failed to create logging object of bucket '" <<
        target_bucket_id << "' and prefix '" << conf.target_prefix << "' for the first time. ret = " << ret << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      target_bucket_id << "'. ret = " << ret << dendl;
    return ret;
  }

  std::string record;
  const auto tt = ceph::coarse_real_time::clock::to_time_t(s->time);
  std::tm t{};
  localtime_r(&tt, &t);
  auto user_or_account = s->account_name;
  if (user_or_account.empty()) {
    s->user->get_id().to_str(user_or_account);
  }
  auto fqdn = s->info.host;
  if (!s->info.domain.empty() && !fqdn.empty()) {
    fqdn.append(".").append(s->info.domain);
  }

  std::string aws_version("-");
  std::string auth_type("-");
  rgw::auth::s3::get_aws_version_and_auth_type(s, aws_version, auth_type);
  std::string bucket_owner;
  std::string bucket_name;
  if (log_source_bucket && conf.logging_type == LoggingType::Standard) {
    // log source bucket for COPY operations only in standard mode
    if (!s->src_object || !s->src_object->get_bucket()) {
      ldpp_dout(dpp, 1) << "ERROR: source object or bucket is missing when logging source bucket" << dendl;
      return -EINVAL;
    }
    bucket_owner = to_string(s->src_object->get_bucket()->get_owner());
    bucket_name = s->src_bucket_name;
  } else {
    bucket_owner = to_string(s->bucket->get_owner());
    bucket_name = full_bucket_name(s->bucket);
  }


  switch (conf.logging_type) {
    case LoggingType::Standard:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} {} {} \"{} {}{}{} HTTP/1.1\" {} {} {} {} {} {} {} \"{}\" {} {} {} {} {} {} {} {} {}",
        dash_if_empty(bucket_owner),
        dash_if_empty(bucket_name),
        t,
        s->info.env->get("REMOTE_ADDR", "-"),
        dash_if_empty(user_or_account),
        dash_if_empty(s->req_id),
        op_name,
        dash_if_empty_or_null(obj, obj->get_name()),
        s->info.method,
        s->info.request_uri,
        s->info.request_params.empty() ? "" : "?",
        s->info.request_params,
        dash_if_zero(s->err.http_ret),
        dash_if_empty(s->err.err_code),
        dash_if_zero(s->content_length),
        dash_if_zero(size),
        "-", // no total time when logging record
        std::chrono::duration_cast<std::chrono::milliseconds>(s->time_elapsed()),
        s->info.env->get("HTTP_REFERER", "-"),
        s->info.env->get("HTTP_USER_AGENT", "-"),
        dash_if_empty_or_null(obj, obj->get_instance()),
        s->info.x_meta_map.contains("x-amz-id-2") ? s->info.x_meta_map.at("x-amz-id-2") : "-",
        aws_version,
        s->info.env->get("SSL_CIPHER", "-"),
        dash_if_empty(auth_type),
        dash_if_empty(fqdn),
        s->info.env->get("TLS_VERSION", "-"),
        "-", // no access point ARN
        (s->granted_by_acl) ? "Yes" : "-");
      break;
    case LoggingType::Journal:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} {} {}",
        dash_if_empty(bucket_owner),
        dash_if_empty(bucket_name),
        t,
        op_name,
        dash_if_empty_or_null(obj, obj->get_name()),
        dash_if_zero(size),
        dash_if_empty_or_null(obj, obj->get_instance()),
        dash_if_empty(etag));
      break;
    case LoggingType::Any:
      ldpp_dout(dpp, 1) << "ERROR: failed to format record when writing to logging object '" <<
        obj_name << "' due to unsupported logging type" << dendl;
      return -EINVAL;
  }

  // get quota of the owner of the target bucket
  RGWQuota user_quota;
  if (ret = get_owner_quota_info(dpp, y, driver, target_bucket->get_owner(), user_quota); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get quota of owner of target logging bucket '" <<
      target_bucket_id << "' failed. ret = " << ret << dendl;
    return ret;
  }
  // start with system default quota
  // and combine with the user quota
  RGWQuota quota;
  driver->get_quota(quota);
  if (target_bucket->get_info().quota.enabled) {
    quota.bucket_quota = target_bucket->get_info().quota;
  } else if (user_quota.bucket_quota.enabled) {
    quota.bucket_quota = user_quota.bucket_quota;
  }
  if (user_quota.user_quota.enabled) {
    quota.user_quota = user_quota.user_quota;
  }
  // verify there is enough quota to write the record
  if (ret = target_bucket->check_quota(dpp, quota, record.length(), y); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: quota check on target logging bucket '" <<
      target_bucket_id << "' failed. ret = " << ret << dendl;
    return ret;
  }

  if (ret = target_bucket->write_logging_object(obj_name,
        record,
        y,
        dpp,
        async_completion); ret < 0 && ret != -EFBIG) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write record to logging object '" <<
      obj_name << "'. ret = " << ret << dendl;
    return ret;
  }
  if (ret == -EFBIG) {
    ldpp_dout(dpp, 5) << "WARNING: logging object '" << obj_name << "' is full, will be committed to bucket '" <<
      target_bucket->get_key() << "'" << dendl;
    if (ret = rollover_logging_object(conf, target_bucket, obj_name, dpp, region, s->bucket, y, true, nullptr, nullptr); ret < 0 ) {
      return ret;
    }
    if (ret = target_bucket->write_logging_object(obj_name,
        record,
        y,
        dpp,
        async_completion); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write record to logging object '" <<
      obj_name << "'. ret = " << ret << dendl;
    return ret;
    }
  }

  ldpp_dout(dpp, 20) << "INFO: wrote logging record: '" << record
    << "' to '" << obj_name << "'" << dendl;
  return 0;
}

std::string object_name_oid(const rgw::sal::Bucket* bucket, const std::string& prefix) {
  // TODO: do i need bucket marker in the name?
  return fmt::format("logging.{}.bucket.{}/{}", bucket->get_tenant(), bucket->get_bucket_id(), prefix);
}

int log_record(rgw::sal::Driver* driver,
    LoggingType type,
    const sal::Object* obj,
    req_state* s,
    const std::string& op_name,
    const std::string& etag,
    size_t size,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    bool async_completion,
    bool log_source_bucket) {
  if (!s->bucket) {
    ldpp_dout(dpp, 1) << "ERROR: only bucket operations are logged in bucket logging" << dendl;
    return -EINVAL;
  }
  // check if bucket logging is needed
  const auto& bucket_attrs = s->bucket->get_attrs();
  auto iter = bucket_attrs.find(RGW_ATTR_BUCKET_LOGGING);
  if (iter == bucket_attrs.end()) {
    return 0;
  }
  configuration configuration;
  try {
    configuration.enabled = true;
    auto bl_iter = iter->second.cbegin();
    decode(configuration, bl_iter);
    if (type != LoggingType::Any && configuration.logging_type != type) {
      return 0;
    }
    if (configuration.key_filter.has_content()) {
      if (!match(configuration.key_filter, obj->get_name())) {
        return 0;
      }
    }
    ldpp_dout(dpp, 20) << "INFO: found matching logging configuration of bucket '" << s->bucket->get_key() <<
      "' configuration: " << configuration.to_json_str() << dendl;
    if (const int ret = log_record(driver, obj, s, op_name, etag, size, configuration, dpp, y, async_completion, log_source_bucket); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to perform logging for bucket '" << s->bucket->get_key() <<
        "'. ret=" << ret << dendl;
      return ret;
    }
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 1) << "ERROR: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
      << "'. error: " << err.what() << dendl;
    return  -EINVAL;
  }
  return 0;
}

int get_bucket_id(const std::string& bucket_name, const std::string& tenant_name, rgw_bucket& bucket_id) {
  std::string parsed_bucket_name;
  std::string parsed_tenant_name;
  if (const int ret = rgw_parse_url_bucket(bucket_name, tenant_name, parsed_tenant_name, parsed_bucket_name); ret < 0) {
      return ret;
  }
  bucket_id = rgw_bucket{parsed_tenant_name, parsed_bucket_name};
  return 0;
}

int update_bucket_logging_sources(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver, const rgw_bucket& target_bucket_id, const rgw_bucket& src_bucket_id, bool add, optional_yield y) {
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  const int ret = driver->load_bucket(dpp, target_bucket_id, &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: failed to get target logging bucket '" << target_bucket_id  <<
      "' in order to update logging sources. ret = " << ret << dendl;
    return ret;
  }
  return update_bucket_logging_sources(dpp, target_bucket, src_bucket_id, add, y);
}

int update_bucket_logging_sources(const DoutPrefixProvider* dpp, std::unique_ptr<rgw::sal::Bucket>& bucket, const rgw_bucket& src_bucket_id, bool add, optional_yield y) {
  return retry_raced_bucket_write(dpp, bucket.get(), [dpp, &bucket, &src_bucket_id, add, y] {
    auto& attrs = bucket->get_attrs();
    auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING_SOURCES);
    if (iter == attrs.end()) {
      if (!add) {
        ldpp_dout(dpp, 20) << "INFO: no logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
          << "' for bucket '" << bucket->get_key() << "', nothing to remove" << dendl;
        return 0;
      }
      source_buckets sources{src_bucket_id};
      bufferlist bl;
      ceph::encode(sources, bl);
      attrs.insert(std::make_pair(RGW_ATTR_BUCKET_LOGGING_SOURCES, std::move(bl)));
      return bucket->merge_and_store_attrs(dpp, attrs, y);
    }
    try {
      source_buckets sources;
      ceph::decode(sources, iter->second);
      if ((add && sources.insert(src_bucket_id).second) ||
          (!add && sources.erase(src_bucket_id) > 0)) {
        bufferlist bl;
        ceph::encode(sources, bl);
        iter->second = std::move(bl);
        return bucket->merge_and_store_attrs(dpp, attrs, y);
      }
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 5) << "WARNING: failed to decode logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
        << "' for bucket '" << bucket->get_key() << "' when updating logging sources. error: " << err.what() << dendl;
    }
    ldpp_dout(dpp, 20) << "INFO: logging source '" << src_bucket_id << "' already " <<
      (add ? "added to" : "removed from") << " bucket '" << bucket->get_key() << "'" << dendl;
    return 0;
  }, y);
}


int bucket_deletion_cleanup(const DoutPrefixProvider* dpp,
                                   sal::Driver* driver,
                                   sal::Bucket* bucket,
                                   optional_yield y) {
  // if the bucket is used a log bucket, we should delete all pending log objects
  // and also delete the object holding the pending object name
  auto& attrs = bucket->get_attrs();
  if (const auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING_SOURCES); iter != attrs.end()) {
    try {
      source_buckets sources;
      ceph::decode(sources, iter->second);
      for (const auto& source : sources) {
        std::unique_ptr<rgw::sal::Bucket> src_bucket;
        if (const int ret = driver->load_bucket(dpp, source, &src_bucket, y); ret < 0) {
          ldpp_dout(dpp, 5) << "WARNING: failed to get logging source bucket '" << source << "' for logging bucket '" <<
            bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
          continue;
        }
        auto& src_attrs = src_bucket->get_attrs();
        if (const auto iter = src_attrs.find(RGW_ATTR_BUCKET_LOGGING); iter != src_attrs.end()) {
          configuration conf;
          try {
            auto bl_iter = iter->second.cbegin();
            decode(conf, bl_iter);
            std::string obj_name;
            RGWObjVersionTracker objv;
            if (const int ret = bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, &objv); ret < 0) {
              ldpp_dout(dpp, 5) << "WARNING: failed to get logging object name for logging bucket '" << bucket->get_key() <<
                "' during cleanup. ret = " << ret << dendl;
              continue;
            }
            if (const int ret = bucket->remove_logging_object(obj_name, y, dpp); ret < 0) {
              ldpp_dout(dpp, 5) << "WARNING: failed to delete pending logging object '" << obj_name << "' for logging bucket '" <<
                bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
              continue;
            }
            ldpp_dout(dpp, 20) << "INFO: successfully deleted pending logging object '" << obj_name << "' from deleted logging bucket '" <<
                bucket->get_key() << "'" << dendl;
            if (const int ret = bucket->remove_logging_object_name(conf.target_prefix, y, dpp, &objv); ret < 0) {
              ldpp_dout(dpp, 5) << "WARNING: failed to delete object holding bucket logging object name for logging bucket '" <<
                bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
              continue;
            }
            ldpp_dout(dpp, 20) << "INFO: successfully deleted object holding bucket logging object name from deleted logging bucket '" <<
              bucket->get_key() << "'" << dendl;
          } catch (buffer::error& err) {
            ldpp_dout(dpp, 5) << "WARNING: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
              << "' of bucket '" << src_bucket->get_key() << "' during cleanup. error: " << err.what() << dendl;
          }
        }
      }
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 5) << "WARNING: failed to decode logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
        << "' for logging bucket '" << bucket->get_key() << "'. error: " << err.what() << dendl;
      return -EIO;
    }
  }

  return source_bucket_cleanup(dpp, driver, bucket, false, y);
}

int source_bucket_cleanup(const DoutPrefixProvider* dpp,
                                   sal::Driver* driver,
                                   sal::Bucket* bucket,
                                   bool remove_attr,
                                   optional_yield y) {
  std::optional<configuration> conf;
  if (const int ret = retry_raced_bucket_write(dpp, bucket, [dpp, bucket, &conf, remove_attr, y] {
    auto& attrs = bucket->get_attrs();
    if (auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING); iter != attrs.end()) {
      try {
        auto bl_iter = iter->second.cbegin();
        configuration tmp_conf;
        tmp_conf.enabled = true;
        decode(tmp_conf, bl_iter);
        conf = std::move(tmp_conf);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 5) << "WARNING: failed to decode existing logging attribute '" << RGW_ATTR_BUCKET_LOGGING
          << "' of bucket '" << bucket->get_key() << "' during cleanup. error: " << err.what() << dendl;
        return -EIO;
      }
      if (remove_attr) {
        attrs.erase(iter);
        return bucket->merge_and_store_attrs(dpp, attrs, y);
      }
    }
    // nothing to remove or no need to remove
    return 0;
  }, y); ret < 0) {
    if (remove_attr) {
      ldpp_dout(dpp, 5) << "WARNING: failed to remove logging attribute '" << RGW_ATTR_BUCKET_LOGGING <<
        "' from bucket '" << bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
    }
    return ret;
  }
  if (!conf) {
    // no logging attribute found
    return 0;
  }
  const auto& info = bucket->get_info();
  if (const int ret = commit_logging_object(*conf, dpp, driver, info.bucket.tenant, y, nullptr); ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: could not commit pending logging object of bucket '" <<
      bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
  } else {
    ldpp_dout(dpp, 20) << "INFO: successfully committed pending logging object of bucket '" << bucket->get_key() << "'" << dendl;
  }
  rgw_bucket target_bucket_id;
  if (const int ret = get_bucket_id(conf->target_bucket, info.bucket.tenant, target_bucket_id); ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: failed to parse target logging bucket '" <<
      conf->target_bucket << "' during cleanup. ret = " << ret << dendl;
    return 0;
  }
  if (const int ret = update_bucket_logging_sources(dpp, driver, target_bucket_id, bucket->get_key(), false, y); ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: could not update bucket logging source '" <<
      bucket->get_key() << "' during cleanup. ret = " << ret << dendl;
    return 0;
  }
  ldpp_dout(dpp, 20) << "INFO: successfully updated bucket logging source '" <<
    bucket->get_key() << "' during cleanup"<< dendl;
  return 0;
}

int verify_target_bucket_policy(const DoutPrefixProvider* dpp,
    rgw::sal::Bucket* target_bucket,
    const rgw::ARN& target_resource_arn,
    req_state* s) {
  // verify target permissions for bucket logging
  // this is implementing the policy based permission granting from:
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html#grant-log-delivery-permissions-general
  const auto& target_bucket_id = target_bucket->get_key();
  const auto& target_attrs = target_bucket->get_attrs();
  const auto policy_it = target_attrs.find(RGW_ATTR_IAM_POLICY);
  if (policy_it == target_attrs.end()) {
    ldpp_dout(dpp, 1) << "ERROR: logging bucket '" << target_bucket_id << "' must have bucket policy to allow logging" << dendl;
    return -EACCES;
  }
  try {
    const rgw::IAM::Policy policy{s->cct, &target_bucket_id.tenant, policy_it->second.to_str(), false};
    ldpp_dout(dpp, 20) << "INFO: logging bucket '" << target_bucket_id <<
      "' policy: " << policy << dendl;
    rgw::auth::ServiceIdentity ident(rgw::bucketlogging::service_principal);
    const auto source_bucket_arn = rgw::ARN(s->bucket->get_key()).to_string();
    const auto source_account = to_string(s->bucket_owner.id);
    s->env.emplace("aws:SourceArn", source_bucket_arn);
    s->env.emplace("aws:SourceAccount", source_account);
    if (policy.eval(s->env, ident, rgw::IAM::s3PutObject, target_resource_arn) != rgw::IAM::Effect::Allow) {
      ldpp_dout(dpp, 1) << "ERROR: logging bucket: '" << target_bucket_id <<
        "' must have a bucket policy that allows logging service principal to put objects in the following resource ARN: '" <<
        target_resource_arn.to_string() << "' from source bucket ARN: '" << source_bucket_arn <<
        "' and source account: '" << source_account << "'" <<  dendl;
      return -EACCES;
    }
  } catch (const rgw::IAM::PolicyParseException& err) {
    ldpp_dout(dpp, 1) << "ERROR: failed to parse logging bucket '" << target_bucket_id <<
      "' policy. error: " << err.what() << dendl;
    return -EACCES;
  }
  return 0;
}

int verify_target_bucket_attributes(const DoutPrefixProvider* dpp, rgw::sal::Bucket* target_bucket) {
  const auto& target_info = target_bucket->get_info();
  if (target_info.requester_pays) {
    // target bucket must not have requester pays set on it
    ldpp_dout(dpp, 1) << "ERROR: logging target bucket '" << target_bucket->get_key() << "', is configured with requester pays" << dendl;
    return -EINVAL;
  }

  const auto& target_attrs = target_bucket->get_attrs();
  if (target_attrs.find(RGW_ATTR_BUCKET_LOGGING) != target_attrs.end()) {
    // target bucket must not have logging set on it
    ldpp_dout(dpp, 1) << "ERROR: logging target bucket '" << target_bucket->get_key() << "', is configured with bucket logging" << dendl;
    return -EINVAL;
  }
  if (target_attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY) != target_attrs.end()) {
    // verify target bucket does not have encryption
    ldpp_dout(dpp, 1) << "ERROR: logging target bucket '" << target_bucket->get_key() << "', is configured with encryption" << dendl;
    return -EINVAL;
  }
  return 0;
}

int get_target_and_conf_from_source(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    rgw::sal::Bucket* src_bucket,
    const std::string& tenant,
    configuration& configuration,
    std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    optional_yield y) {
  const auto src_bucket_id = src_bucket->get_key();
  const auto& bucket_attrs = src_bucket->get_attrs();
  auto iter = bucket_attrs.find(RGW_ATTR_BUCKET_LOGGING);
  if (iter == bucket_attrs.end()) {
    ldpp_dout(dpp, 1) << "WARNING: no logging configured on bucket '" << src_bucket_id << "'" << dendl;
    return -ENODATA;
  }
  try {
    configuration.enabled = true;
    auto bl_iter = iter->second.cbegin();
    decode(configuration, bl_iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 1) << "WARNING: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
      << "' for bucket '" << src_bucket_id << "', error: " << err.what() << dendl;
    return -EINVAL;
  }

  rgw_bucket target_bucket_id;
  if (const int ret = get_bucket_id(configuration.target_bucket, tenant, target_bucket_id); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to parse target bucket '" << configuration.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }

  if (const int ret = driver->load_bucket(dpp, target_bucket_id,
                               &target_bucket, y); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target bucket '" << target_bucket_id << "', ret = " << ret << dendl;
    return ret;
  }
  return 0;
}

} // namespace rgw::bucketlogging

