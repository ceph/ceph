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

std::string full_bucket_name(const std::unique_ptr<rgw::sal::Bucket>& bucket) {
  if (bucket->get_tenant().empty()) {
    return bucket->get_name();
  }
  return fmt::format("{}:{}", bucket->get_tenant(), bucket->get_name());
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
          full_bucket_name(bucket),
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
        conf.target_bucket << "' and prefix '" << conf.target_prefix << "', ret = " << ret << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << "INFO: name already set. got name of logging object '" << obj_name <<  "' of bucket '" <<
      conf.target_bucket << "' and prefix '" << conf.target_prefix << "'" << dendl;
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

int commit_logging_object(const configuration& conf,
    const DoutPrefixProvider *dpp,
    rgw::sal::Driver* driver,
    const std::string& tenant_name,
    optional_yield y) {
  std::string target_bucket_name;
  std::string target_tenant_name;
  auto ret = rgw_parse_url_bucket(conf.target_bucket, tenant_name, target_tenant_name, target_bucket_name);
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
  return commit_logging_object(conf, target_bucket, dpp, y);
}

int commit_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    const DoutPrefixProvider *dpp,
    optional_yield y) {
  std::string obj_name;
  if (const auto ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, nullptr); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      target_bucket->get_info().bucket << "'. ret = " << ret << dendl;
    return ret;
  }
  return target_bucket->commit_logging_object(obj_name, y, dpp);
}

int rollover_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    bool must_commit,
    RGWObjVersionTracker* objv_tracker) {
  std::string target_bucket_name;
  std::string target_tenant_name;
  std::ignore = rgw_parse_url_bucket(conf.target_bucket, bucket->get_tenant(), target_tenant_name, target_bucket_name);
  if (target_bucket_name != bucket->get_name() || target_tenant_name != bucket->get_tenant()) {
    ldpp_dout(dpp, 1) << "ERROR: bucket name mismatch. conf= '" << conf.target_bucket <<
      "', bucket= '" << bucket->get_info().bucket << "'" << dendl;
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
    bool log_source_bucket,
    boost::optional<BucketLoggingCompleter&> completer) {
  if (!s->bucket) {
    ldpp_dout(dpp, 1) << "ERROR: only bucket operations are logged" << dendl;
    return -EINVAL;
  }
  std::string target_bucket_name;
  std::string target_tenant_name;
  auto ret = rgw_parse_url_bucket(conf.target_bucket, s->bucket_tenant, target_tenant_name, target_bucket_name);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to parse target bucket '" << conf.target_bucket << "', ret = " << ret << dendl;
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
      ldpp_dout(dpp, 20) << "INFO: first time logging for bucket '" << conf.target_bucket << "' and prefix '" <<
        conf.target_prefix << "'" << dendl;
    } else if (ret == -ECANCELED) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' already exists for bucket '" << conf.target_bucket << "' and prefix" <<
        conf.target_prefix << "'" << dendl;
    } else {
      ldpp_dout(dpp, 1) << "ERROR: failed to create logging object of bucket '" <<
        conf.target_bucket << "' and prefix '" << conf.target_prefix << "' for the first time. ret = " << ret << dendl;
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
    bucket_name = full_bucket_name(s->bucket);
  }

  using namespace rgw::auth::s3;
  string aws_version("-");
  string auth_type("-");
  rgw::auth::s3::get_aws_version_and_auth_type(s, aws_version, auth_type);

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
        auth_type,
        dash_if_empty(fqdn),
        s->info.env->get("TLS_VERSION", "-"),
        "-", // no access point ARN
        (s->has_acl_header) ? "Yes" : "-");
      break;
    case LoggingType::Journal:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} {} {}",
        dash_if_empty(to_string(s->bucket->get_owner())),
        dash_if_empty(full_bucket_name(s->bucket)),
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

  boost::optional<const std::string&> trans_id;
  if (completer) {
    completer->impl = std::make_unique<BucketLoggingCompleterImpl>(completer->trans_id, obj_name, y, dpp, target_bucket.get());
    trans_id = completer->trans_id;
  }
  if (ret = target_bucket->write_logging_object(obj_name,
        record,
        y,
        dpp,
        async_completion,
        trans_id); ret < 0 && ret != -EFBIG) {
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
        async_completion,
        trans_id); ret < 0) {
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
    bool log_source_bucket,
    boost::optional<BucketLoggingCompleter&> completer) {
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
    ldpp_dout(dpp, 20) << "INFO: found matching logging configuration of bucket '" << s->bucket->get_info().bucket <<
      "' configuration: " << configuration.to_json_str() << dendl;
    if (auto ret = log_record(driver, obj, s, op_name, etag, size, configuration, dpp, y, async_completion, log_source_bucket, completer); ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to perform logging for bucket '" << s->bucket->get_info().bucket <<
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
  if (const auto ret = rgw_parse_url_bucket(bucket_name, tenant_name, parsed_tenant_name, parsed_bucket_name); ret < 0) {
      return ret;
  }
  bucket_id = rgw_bucket{parsed_tenant_name, parsed_bucket_name};
  return 0;
}

int update_bucket_logging_sources(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver, const rgw_bucket& target_bucket_id, const rgw_bucket& src_bucket_id, bool add, optional_yield y) {
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  const auto ret = driver->load_bucket(dpp, target_bucket_id, &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: failed to get target bucket '" << target_bucket_id  << "', ret = " << ret << dendl;
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
          << "' for bucket '" << bucket->get_info().bucket << "', nothing to remove" << dendl;
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
      ldpp_dout(dpp, 1) << "WARNING: failed to decode logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
        << "' for bucket '" << bucket->get_info().bucket << "', error: " << err.what() << dendl;
    }
    ldpp_dout(dpp, 20) << "INFO: logging source '" << src_bucket_id << "' already " <<
      (add ? "added to" : "removed from") << " bucket '" << bucket->get_info().bucket << "'" << dendl;
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
        if (const auto ret = driver->load_bucket(dpp, source, &src_bucket, y); ret < 0) {
          ldpp_dout(dpp, 1) << "WARNING: failed to get logging source bucket '" << source << "' for log bucket '" <<
            bucket->get_info().bucket << "', ret = " << ret << dendl;
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
            if (const auto ret = bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp, &objv); ret < 0) {
              ldpp_dout(dpp, 1) << "WARNING: failed to get logging object name for log bucket '" << bucket->get_info().bucket <<
                "', ret = " << ret << dendl;
              continue;
            }
            if (const auto ret = bucket->remove_logging_object(obj_name, y, dpp); ret < 0) {
              ldpp_dout(dpp, 1) << "WARNING: failed to delete pending logging object '" << obj_name << "' for log bucket '" <<
                bucket->get_info().bucket << "', ret = " << ret << dendl;
              continue;
            }
            ldpp_dout(dpp, 20) << "INFO: successfully deleted pending logging object '" << obj_name << "' from deleted log bucket '" <<
                bucket->get_info().bucket << "'" << dendl;
            if (const auto ret = bucket->remove_logging_object_name(conf.target_prefix, y, dpp, &objv); ret < 0) {
              ldpp_dout(dpp, 1) << "WARNING: failed to delete object holding bucket logging object name for log bucket '" <<
                bucket->get_info().bucket << "', ret = " << ret << dendl;
              continue;
            }
            ldpp_dout(dpp, 20) << "INFO: successfully deleted object holding bucket logging object name from deleted log bucket '" <<
              bucket->get_info().bucket << "'" << dendl;
          } catch (buffer::error& err) {
            ldpp_dout(dpp, 1) << "WARNING: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
              << "' of bucket '" << src_bucket->get_info().bucket << "', error: " << err.what() << dendl;
          }
        }
      }
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << "WARNING: failed to decode logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
        << "' for bucket '" << bucket->get_info().bucket << "', error: " << err.what() << dendl;
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
  const auto& info = bucket->get_info();
  if (const auto ret = retry_raced_bucket_write(dpp, bucket, [dpp, bucket, &conf, &info, remove_attr, y] {
    auto& attrs = bucket->get_attrs();
    if (auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING); iter != attrs.end()) {
      try {
        auto bl_iter = iter->second.cbegin();
        configuration tmp_conf;
        tmp_conf.enabled = true;
        decode(tmp_conf, bl_iter);
        conf = std::move(tmp_conf);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 1) << "WARNING: failed to decode existing logging attribute '" << RGW_ATTR_BUCKET_LOGGING
          << "' of bucket '" << info.bucket << "', error: " << err.what() << dendl;
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
      ldpp_dout(dpp, 1) << "ERROR: failed to remove logging attribute '" << RGW_ATTR_BUCKET_LOGGING << "' from bucket '" <<
        info.bucket << "', ret = " << ret << dendl;
    }
    return ret;
  }
  if (!conf) {
    // no logging attribute found
    return 0;
  }
  if (const auto ret = commit_logging_object(*conf, dpp, driver, info.bucket.tenant, y); ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: could not commit pending logging object of bucket '" <<
      info.bucket << "', ret = " << ret << dendl;
  } else {
    ldpp_dout(dpp, 20) << "INFO: successfully committed pending logging object of bucket '" << info.bucket << "'" << dendl;
  }
  rgw_bucket target_bucket_id;
  rgw_bucket src_bucket_id{info.bucket.tenant, info.bucket.name};
  if (const auto ret = get_bucket_id(conf->target_bucket, info.bucket.tenant, target_bucket_id); ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: failed to parse target bucket '" << conf->target_bucket << "', ret = " << ret << dendl;
    return 0;
  }
  if (const auto ret = update_bucket_logging_sources(dpp, driver, target_bucket_id, src_bucket_id, false, y); ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: could not update bucket logging source '" <<
      info.bucket << "', ret = " << ret << dendl;
    return 0;
  }
  ldpp_dout(dpp, 20) << "INFO: successfully updated bucket logging source '" <<
    info.bucket << "'"<< dendl;
  return 0;
}

class BucketLoggingCompleterImpl {
  const std::string& trans_id;
  const std::string obj_name;
  optional_yield y;
  const DoutPrefixProvider* dpp;
  std::unique_ptr<sal::Bucket> bucket;
public:
  BucketLoggingCompleterImpl(
      const std::string& _trans_id,
      const std::string& _obj_name,
      optional_yield _y,
      const DoutPrefixProvider* _dpp,
      sal::Bucket* _bucket) : trans_id(_trans_id), obj_name(_obj_name), y(_y), dpp(_dpp), bucket(_bucket->clone()) {}
  ~BucketLoggingCompleterImpl() {
    bucket->complete_logging_object_write(obj_name, y, dpp, trans_id);
  }
};

BucketLoggingCompleter::BucketLoggingCompleter(const std::string& _trans_id) : trans_id(_trans_id) {}
// default dtor will invoke complete_logging_object_write
// if impl was allocated
BucketLoggingCompleter::~BucketLoggingCompleter() {}

} // namespace rgw::bucketlogging

