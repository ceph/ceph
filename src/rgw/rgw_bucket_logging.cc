// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <time.h>
#include <random>
#include "common/ceph_time.h"
#include "fmt/chrono.h"
#include "rgw_bucket_logging.h"
#include "rgw_xml.h"
#include "rgw_sal.h"

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
    } else {
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
    ldpp_dout(dpp, 1) << "ERROR: invalid time format: '" << time_str <<"' in logging object name: " << obj_name << dendl;
    return extracted_time;
  }
  extracted_time = ceph::coarse_real_time::clock::from_time_t(mktime(&t));
  ldpp_dout(dpp, 20) << "INFO: time '" << extracted_time << "' extracted from logging object name: " << obj_name << dendl;
  return extracted_time;
}

int new_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    const std::string& rgw_id,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y) {

  const auto tt = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now());
  std::tm t{};
  localtime_r(&tt, &t);

  const auto unique = unique_string<UniqueStringLength>();

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

  int ret = bucket->set_logging_object_name(obj_name, conf.target_prefix, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write name of logging object of bucket '" <<
      conf.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: wrote name of new logging object '" << obj_name <<  "' of bucket '" <<
      conf.target_bucket << "'" << dendl;
  return 0;
}

int rollover_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    const std::string& rgw_id,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y) {
  const auto old_obj = obj_name;
  if (const auto ret = new_logging_object(conf, bucket, rgw_id, obj_name, dpp, y); ret < 0 ) {
    return ret;
  }
  if (const auto ret = bucket->commit_logging_object(old_obj, y, dpp); ret < 0) {
    ldpp_dout(dpp, 5) << "WARNING: failed to commit logging object '" << old_obj << "' to bucket '" <<
      conf.target_bucket << "', ret = " << ret << dendl;
    // we still want to write the new records to the new object even if commit failed
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
  - Object Size
  - eTag
};*/

int log_record(rgw::sal::Driver* driver, 
    const req_state* s, 
    const std::string& op_name, 
    const std::string& etag, 
    const configuration& conf,
    const DoutPrefixProvider *dpp, 
    optional_yield y,
    bool async_completion) {
  if (!s->bucket) {
    ldpp_dout(dpp, 1) << "ERROR: only bucket operations are logged" << dendl;
    return -EINVAL;
  }
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  auto ret = driver->load_bucket(dpp, rgw_bucket(s->bucket_tenant, conf.target_bucket),
                               &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target logging bucket '" << conf.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }
  std::string obj_name;
  ret = target_bucket->get_logging_object_name(obj_name, conf.target_prefix, y, dpp);
  if (ret == 0) {
    const auto time_to_commit = time_from_name(obj_name, dpp) + std::chrono::seconds(conf.obj_roll_time);
    if (ceph::coarse_real_time::clock::now() > time_to_commit) {
      ldpp_dout(dpp, 20) << "INFO: logging object '" << obj_name << "' exceeded its time, will be committed to bucket '" <<
        conf.target_bucket << "'" << dendl;
      if (ret = rollover_logging_object(conf, target_bucket, driver->get_host_id(), obj_name, dpp, y); ret < 0 ) {
        return ret;
      }
    } else {
      ldpp_dout(dpp, 20) << "INFO: record will be written to current logging object '" << obj_name << "'. will be comitted at: " << time_to_commit << dendl;
    }
  } else if (ret == -ENOENT) {
    // create the temporary log object for the first time
    ldpp_dout(dpp, 20) << "INFO: first time logging for bucket '" << conf.target_bucket << "'" << dendl;
    if (ret = new_logging_object(conf, target_bucket, driver->get_host_id(), obj_name, dpp, y); ret < 0 ) {
      return ret;
    }
  } else {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      conf.target_bucket << "', ret = " << ret << dendl;
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
  switch (conf.logging_type) {
    case LoggingType::Standard:
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {} REST.{}.{} {} \"{} {}{}{} HTTP/1.1\" {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
        dash_if_empty(to_string(s->bucket->get_owner())),
        dash_if_empty(s->bucket->get_name()),
        t,
        "-", // no requester IP
        dash_if_empty(user_or_account),
        dash_if_empty(s->req_id),
        s->info.method,
        op_name,
        dash_if_empty_or_null(s->object, s->object->get_key().name),
        s->info.method,
        s->info.request_uri,
        s->info.request_params.empty() ? "" : "?",
        s->info.request_params,
        dash_if_zero(s->err.http_ret),
        dash_if_empty(s->err.err_code),
        dash_if_zero(s->content_length),
        dash_if_zero_or_null(s->object, s->object->get_size()),
        "-", // no total time when logging record
        std::chrono::duration_cast<std::chrono::milliseconds>(s->time_elapsed()),
        "-", // TODO: referer
        "-", // TODO: user agent
        dash_if_empty_or_null(s->object, s->object->get_instance()),
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
      record = fmt::format("{} {} [{:%d/%b/%Y:%H:%M:%S %z}] {} REST.{}.{} {} {}",
        dash_if_empty(to_string(s->bucket->get_owner())),
        dash_if_empty(s->bucket->get_name()),
        t,
        dash_if_empty_or_null(s->object, s->object->get_key().name),
        s->info.method,
        op_name,
        dash_if_zero_or_null(s->object, s->object->get_size()),
        dash_if_empty(etag));
      break;
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
    if (ret = rollover_logging_object(conf, target_bucket, driver->get_host_id(), obj_name, dpp, y); ret < 0 ) {
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

} // namespace rgw::bucketlogging

