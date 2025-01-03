// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <time.h>
#include <random>
#include "common/ceph_time.h"
#include "rgw_bucket_logging.h"
#include "rgw_xml.h"
#include "rgw_sal.h"
#include "rgw_op.h"

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

template<size_t N>
std::string unique_string() {
  static const std::string possible_characters{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXY"};
  static const auto max_possible_value = possible_characters.length() - 1;
  std::random_device rd;
  std::mt19937 engine(rd());
  std::uniform_int_distribution<> dist(0, max_possible_value);
  std::string str(N, '\0');
  std::generate_n(str.begin(), N, [&](){return possible_characters[dist(engine)];});
  return str;
}

constexpr size_t UniqueStringLength = 16;

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

  std::tm t = {};
  if (const auto ret = strptime(time_str.c_str(), "%Y-%m-%d-%H-%M-%S", &t); ret == nullptr || *ret != '\0') {
    ldpp_dout(dpp, 1) << "ERROR: invalid time format: '" << time_str << "' in logging object name: " << obj_name << dendl;
    return extracted_time;
  }
  extracted_time = ceph::coarse_real_time::clock::from_time_t(mktime(&t));
  ldpp_dout(dpp, 20) << "INFO: time '" << extracted_time << "' extracted from logging object name: " << obj_name << dendl;
  return extracted_time;
}

int new_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    bool init_obj,
    RGWObjVersionTracker* objv_tracker) {
  const auto tt = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now());
  std::tm t{};
  localtime_r(&tt, &t);

  const auto unique = unique_string<UniqueStringLength>();
  const auto old_name = obj_name;

  switch (conf.obj_key_format) {
    case KeyFormat::Simple:
      obj_name = fmt::format("{}{:%Y-%m-%d-%H-%M-%S}-{}",
        conf.target_prefix,
        t,
        unique);
      break;
    case KeyFormat::Partitioned:
      {
        // TODO: use date_source
        const auto source_region = ""; // TODO
        obj_name = fmt::format("{}{}/{}/{}/{:%Y/%m/%d}/{:%Y-%m-%d-%H-%M-%S}-{}",
          conf.target_prefix,
          to_string(bucket->get_owner()),
          source_region,
          bucket->get_name(),
          t,
          t,
          unique);
      }
      break;
  }

  int ret = bucket->set_logging_object_name(obj_name, conf.target_prefix, y, dpp, init_obj, objv_tracker);
  if (ret == -EEXIST || ret == -ECANCELED) {
   if (ret = bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, nullptr); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
        conf.target_bucket << "'. ret = " << ret << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << "INFO: name already set. got name of logging object '" << obj_name <<  "' of bucket '" <<
      conf.target_bucket << "'" << dendl;
    return -ECANCELED;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write name of logging object '" << obj_name << "' of bucket '" <<
      conf.target_bucket << "'. ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: wrote name of logging object '" << obj_name <<  "' of bucket '" <<
      conf.target_bucket << "'" << dendl;
  return 0;
}

int rollover_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    bool must_commit,
    RGWObjVersionTracker* objv_tracker) {
  if (conf.target_bucket != bucket->get_name()) {
    ldpp_dout(dpp, 1) << "ERROR: bucket name mismatch: '" << conf.target_bucket << "' != '" << bucket->get_name() << "'" << dendl;
    return -EINVAL;
  }
  const auto old_obj = obj_name;
  const auto ret = new_logging_object(conf, bucket, obj_name, dpp, y, false, objv_tracker); 
  if (ret == -ECANCELED) {
    ldpp_dout(dpp, 20) << "INFO: rollover already performed for '" << old_obj <<  "' to bucket '" <<
      conf.target_bucket << "'. ret = " << ret << dendl;
    return 0;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to rollover logging object '" << old_obj << "' to bucket '" <<
      conf.target_bucket << "'. ret = " << ret << dendl;
    return ret;
  }
  if (const auto ret = bucket->commit_logging_object(old_obj, y, dpp); ret < 0) {
    if (must_commit) {
      return ret;
    }
    ldpp_dout(dpp, 5) << "WARNING: failed to commit logging object '" << old_obj << "' to bucket '" <<
      conf.target_bucket << "'. ret = " << ret << dendl;
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
    const req_state* s, 
    const std::string& op_name, 
    const std::string& etag, 
    size_t size,
    const configuration& conf,
    const DoutPrefixProvider *dpp, 
    optional_yield y,
    bool async_completion,
    bool log_source_bucket) {
  if (!s->bucket) {
    ldpp_dout(dpp, 1) << "ERROR: only bucket operations are logged" << dendl;
    return -EINVAL;
  }
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  auto ret = driver->load_bucket(dpp, rgw_bucket(s->bucket_tenant, conf.target_bucket),
                               &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target logging bucket '" << conf.target_bucket << "'. ret = " << ret << dendl;
    return ret;
  }
  std::string obj_name;
  RGWObjVersionTracker objv_tracker;
  ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, &objv_tracker);
  if (ret == 0) {
    const auto time_to_commit = time_from_name(obj_name, dpp) + std::chrono::seconds(conf.obj_roll_time);
    if (ceph::coarse_real_time::clock::now() > time_to_commit) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' exceeded its time, will be committed to bucket '" <<
        conf.target_bucket << "'" << dendl;
      if (ret = rollover_logging_object(conf, target_bucket, obj_name, dpp, y, false, &objv_tracker); ret < 0) {
        return ret;
      }
    } else {
      ldpp_dout(dpp, 20) << "INFO: record will be written to current logging object '" << obj_name << "'. will be comitted at: " << time_to_commit << dendl;
    }
  } else if (ret == -ENOENT) {
    // try to create the temporary log object for the first time
    ret = new_logging_object(conf, target_bucket, obj_name, dpp, y, true, nullptr);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "INFO: first time logging for bucket '" << conf.target_bucket << "'" << dendl;
    } else if (ret == -ECANCELED) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' already exists for bucket '" << conf.target_bucket << "', will be used" << dendl;
    } else {
      ldpp_dout(dpp, 1) << "ERROR: failed to create logging object of bucket '" <<
        conf.target_bucket << "' for the first time. ret = " << ret << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      conf.target_bucket << "'. ret = " << ret << dendl;
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

  std::string bucket_owner;
  std::string bucket_name;
  if (log_source_bucket) {
    if (!s->src_object || !s->src_object->get_bucket()) {
      ldpp_dout(dpp, 1) << "ERROR: source object or bucket is missing when logging source bucket" << dendl;
      return -EINVAL;
    }
    bucket_owner = to_string(s->src_object->get_bucket()->get_owner());
    bucket_name = s->src_bucket_name;
  } else {
    bucket_owner = to_string( s->bucket->get_owner());
    bucket_name = s->bucket->get_name();
  }

  switch (conf.logging_type) {
    case LoggingType::Standard:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} {} {} \"{} {}{}{} HTTP/1.1\" {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
        dash_if_empty(bucket_owner),
        dash_if_empty(bucket_name),
        t,
        "-", // no requester IP
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
        "-", // TODO: referer
        "-", // TODO: user agent
        dash_if_empty_or_null(obj, obj->get_instance()),
        s->info.x_meta_map.contains("x-amz-id-2") ? s->info.x_meta_map.at("x-amz-id-2") : "-",
        "-", // TODO: Signature Version (SigV2 or SigV4)
        "-", // TODO: SSL cipher. e.g. "ECDHE-RSA-AES128-GCM-SHA256"
        "-", // TODO: Auth type. e.g. "AuthHeader"
        dash_if_empty(fqdn),
        "-", // TODO: TLS version. e.g. "TLSv1.2" or "TLSv1.3"
        "-", // no access point ARN
        (s->has_acl_header) ? "Yes" : "-");
      break;
    case LoggingType::Journal:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} {} {}",
        dash_if_empty(to_string(s->bucket->get_owner())),
        dash_if_empty(s->bucket->get_name()),
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
    ldpp_dout(dpp, 20) << "WARNING: logging object '" << obj_name << "' is full, will be committed to bucket '" <<
      conf.target_bucket << "'" << dendl;
    if (ret = rollover_logging_object(conf, target_bucket, obj_name, dpp, y, true, nullptr); ret < 0 ) {
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
    const req_state* s, 
    const std::string& op_name, 
    const std::string& etag, 
    size_t size, 
    const DoutPrefixProvider *dpp, 
    optional_yield y, 
    bool async_completion,
    bool log_source_bucket) {
  if (!s->bucket) {
    // logging only bucket operations
    return 0;
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
    ldpp_dout(dpp, 20) << "INFO: found matching logging configuration of bucket '" << s->bucket->get_name() << 
      "' configuration: " << configuration.to_json_str() << dendl;
    if (auto ret = log_record(driver, obj, s, op_name, etag, size, configuration, dpp, y, async_completion, log_source_bucket); ret < 0) { 
      ldpp_dout(dpp, 1) << "ERROR: failed to perform logging for bucket '" << s->bucket->get_name() << 
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

} // namespace rgw::bucketlogging

