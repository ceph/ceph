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

bool rgw_bucket_logging::decode_xml(XMLObj* obj) {
  const auto throw_if_missing = true;
  enabled = false;
  XMLObjIter iter = obj->find("LoggingEnabled");
  XMLObj* const o = iter.get_next();
  if (o) {
    enabled = true;
    RGWXMLDecoder::decode_xml("TargetBucket", target_bucket, o, throw_if_missing);
    RGWXMLDecoder::decode_xml("TargetPrefix", target_prefix, o);
    // TODO: decode grant
    uint32_t default_obj_roll_time{300};
    RGWXMLDecoder::decode_xml("ObjectRollTime", obj_roll_time, default_obj_roll_time, o);
    std::string default_type{"Short"};
    std::string type;
    RGWXMLDecoder::decode_xml("RecordType", type, default_type, o);
    if (type == "Standard") {
      record_type = BucketLoggingRecordType::Standard;
    } else if (type == "Short") {
      record_type = BucketLoggingRecordType::Short;
    } else {
      throw RGWXMLDecoder::err("invalid bucket logging record type: '" + type + "'");
    }
    RGWXMLDecoder::decode_xml("RecordsBatchSize", records_batch_size, o);
    if (iter = o->find("TargetObjectKeyFormat"); iter.get_next()) {
      XMLObj* const oo = iter.get_next();
      if (iter = oo->find("PartitionedPrefix"); iter.get_next()) {
        obj_key_format = BucketLoggingKeyFormat::Partitioned;
        // TODO: support DeliveryTime when batch is non zero
      } else if (iter = oo->find("RGWPartitioned"); iter.get_next()) {
          obj_key_format = BucketLoggingKeyFormat::RGWPartitioned;
      } else if (iter = oo->find("SimplePrefix"); iter.get_next()) {
          obj_key_format = BucketLoggingKeyFormat::Simple;
      } else {
        throw RGWXMLDecoder::err("TargetObjectKeyFormat must contain a format tag");
      }
    }
    default_type = "Write";
    RGWXMLDecoder::decode_xml("EventType", type, default_type, o);
    if (type == "Read") {
      event_type = BucketLoggingEventType::Read;
    } else if (type == "Write") {
      event_type = BucketLoggingEventType::Write;
    } else if (type == "ReadWrite") {
      event_type = BucketLoggingEventType::ReadWrite;
    } else {
      throw RGWXMLDecoder::err("invalid bucket logging event type: '" + type + "'");
    }
  }

  return true;
}

void rgw_bucket_logging::dump_xml(Formatter *f) const {
  if (!enabled) {
    return;
  }
  f->open_object_section("LoggingEnabled");
  ::encode_xml("TargetBucket", target_bucket, f);
  ::encode_xml("TargetPrefix", target_prefix, f);
  ::encode_xml("ObjectRollTime", obj_roll_time, f);
  switch (record_type) {
    case BucketLoggingRecordType::Standard:
      ::encode_xml("RecordType", "Standard", f);
      break;
    case BucketLoggingRecordType::Short:
      ::encode_xml("RecordType", "Short", f);
      break;
    default:
      // do nothing
      break;
  }
  ::encode_xml("RecordsBatchSize", records_batch_size, f);
  f->open_object_section("TargetObjectKeyFormat");
  switch (obj_key_format) {
    case BucketLoggingKeyFormat::Partitioned:
      f->open_object_section("PartitionedPrefix");
      ::encode_xml("PartitionDateSource", "EventTime", f);
      // TODO: support DeliveryTime when batch is non zero
      f->close_section();
      break;
    case BucketLoggingKeyFormat::RGWPartitioned:
      f->open_object_section("RGWPartitioned"); // empty section
      f->close_section();
      break;
    case BucketLoggingKeyFormat::Simple:
      f->open_object_section("SimplePrefix"); // empty section
      f->close_section();
      break;
    default:
      // do nothing
      break;
  }
  switch (event_type) {
    case BucketLoggingEventType::Read:
      ::encode_xml("RecordType", "Read", f);
      break;
    case BucketLoggingEventType::Write:
      ::encode_xml("RecordType", "Write", f);
      break;
    case BucketLoggingEventType::ReadWrite:
      ::encode_xml("RecordType", "ReadWrite", f);
      break;
    default:
      // do nothing
      break;
  }
  f->close_section(); // TargetObjectKeyFormat
  f->close_section(); // LoggingEnabled
}

void rgw_bucket_logging::dump(Formatter *f) const {
  if (!enabled) {
    return;
  }
  Formatter::ObjectSection s(*f, "loggingEnabled");
  encode_json("targetBucket", target_bucket, f);
  encode_json("targetPrefix", target_prefix, f);
  encode_json("objectRollTime", obj_roll_time, f);
  switch (record_type) {
    case BucketLoggingRecordType::Standard:
      encode_json("recordType", "Standard", f);
      break;
    case BucketLoggingRecordType::Short:
      encode_json("recordType", "Short", f);
      break;
    default:
      // do nothing
      break;
  }
  encode_json("recordsBatchSize", records_batch_size, f);
  {
    Formatter::ObjectSection s(*f, "targetObjectKeyFormat");
    switch (obj_key_format) {
      case BucketLoggingKeyFormat::Partitioned:
      {
        Formatter::ObjectSection s(*f, "partitionedPrefix");
        encode_json("PartitionDateSource", "EventTime", f);
      }
      break;
      case BucketLoggingKeyFormat::RGWPartitioned:
      {
        Formatter::ObjectSection s(*f, "rgwPartitioned");
      }
      break;
      case BucketLoggingKeyFormat::Simple:
      {
        Formatter::ObjectSection s(*f, "simplePrefix");
      }
      break;
      default:
        // do nothing
        break;
    }
  }
  switch (event_type) {
    case BucketLoggingEventType::Read:
      encode_json("RecordType", "Read", f);
      break;
    case BucketLoggingEventType::Write:
      encode_json("RecordType", "Write", f);
      break;
    case BucketLoggingEventType::ReadWrite:
      encode_json("RecordType", "ReadWrite", f);
      break;
    default:
      // do nothing
      break;
  }
}

std::string rgw_bucket_logging::to_json_str() const {
  JSONFormatter f;
  f.open_object_section("bucketLoggingStatus");
  dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  return ss.str();
}

template<size_t N>
std::string unique_string() {
  static const std::string possible_characters{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXY"};
  static const auto possible_len = possible_characters.length();
  static std::random_device rd;
  static std::mt19937 engine(rd());
  std::uniform_int_distribution<> dist(0, possible_len);
  std::string str;
  str.resize(N);
  std::generate_n(str.begin(), N, [&](){return possible_characters[dist(engine)];});
  return str;
}

ceph::coarse_real_time time_from_name(const std::string& obj_name, const rgw_bucket_logging& configuration, const DoutPrefixProvider *dpp) {
  static const auto time_format_length = std::string{"YYYY-MM-DD-hh-mm-ss"}.length();
  std::tm t = {};
  ceph::coarse_real_time extracted_time;
  switch (configuration.obj_key_format) {
    case BucketLoggingKeyFormat::RGWPartitioned:
      // fallthrough - same as "simple"
    case BucketLoggingKeyFormat::Simple:
      {
        const auto prefix_length = configuration.target_prefix.length();
        const auto time_str = obj_name.substr(prefix_length, time_format_length);
        if (obj_name.length() < time_format_length + prefix_length) {
          ldpp_dout(dpp, 1) << "ERROR: logging object name too short: " << obj_name << dendl;
          return extracted_time;
        }
        if (!strptime(time_str.c_str(), "%Y-%m-%d-%H-%M-%S", &t)) {
          ldpp_dout(dpp, 1) << "ERROR: invalid time format in logging object name: " << time_str << dendl;
          return extracted_time;
        }
      }
      break;
    case BucketLoggingKeyFormat::Partitioned:
    default:
      ldpp_dout(dpp, 1) << "ERROR: unsupported logging key format when extracting time: " <<
        static_cast<unsigned>(configuration.obj_key_format) << dendl;
      return extracted_time;
  }
  extracted_time = ceph::coarse_real_time::clock::from_time_t(mktime(&t));
  ldpp_dout(dpp, 20) << "INFO: time '" << extracted_time << "' extracted from logging object name: " << obj_name << dendl;
  return extracted_time;
}

int new_logging_object(const rgw_bucket_logging& configuration,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    const std::string& rgw_id,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    optional_yield y) {

  const auto tt = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now());
  std::tm t{};
  localtime_r(&tt, &t);

  switch (configuration.obj_key_format) {
    case BucketLoggingKeyFormat::RGWPartitioned:
      obj_name = fmt::format("{}{:%Y-%m-%d-%H-%M-%S}/{}",
          configuration.target_prefix,
          t,
          rgw_id);
      // TODO: 
      // (1) add unique string
      // (2) move rgw id to the front
      break;
    case BucketLoggingKeyFormat::Simple:
      obj_name = fmt::format("{}{:%Y-%m-%d-%H-%M-%S}",
          configuration.target_prefix,
          t);
      // TODO: add unique string
    case BucketLoggingKeyFormat::Partitioned:
    default:
      ldpp_dout(dpp, 1) << "ERROR: unsupported logging key format when creating new object: " <<
        static_cast<unsigned>(configuration.obj_key_format) << dendl;
      return -EINVAL;
  }

  int ret = bucket->set_logging_object_name(obj_name, configuration.target_prefix, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write name of logging object of bucket '" <<
      configuration.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: wrote name of new logging object '" << obj_name <<  "' of bucket '" <<
      configuration.target_bucket << "'" << dendl;
  return 0;
}

/* S3 bucket log structure
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
  - eTag: extension to the standard
};*/

int log_record(rgw::sal::Driver* driver, const req_state* s, const std::string& op_name, const std::string& etag, const rgw_bucket_logging& configuration,
  const DoutPrefixProvider *dpp, optional_yield y) {
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  auto ret = driver->load_bucket(dpp, rgw_bucket(s->bucket_tenant, configuration.target_bucket),
                               &target_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get target logging bucket '" << configuration.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }
  std::string obj_name;
  ret = target_bucket->get_logging_object_name(obj_name, configuration.target_prefix, y, dpp);
  if (ret == 0) {
    const auto time_to_commit = time_from_name(obj_name, configuration, dpp) + std::chrono::seconds(configuration.obj_roll_time);
    if (ceph::coarse_real_time::clock::now() > time_to_commit) {
      const auto old_obj = obj_name;
      ldpp_dout(dpp, 20) << "INFO: logging object '" << old_obj << "' exceeded its time, will be committed to bucket '" <<
        configuration.target_bucket << "'" << dendl;
      if (ret = new_logging_object(configuration, target_bucket, driver->get_host_id(), obj_name, dpp, y); ret < 0 ) {
        return ret;
      }
      if (target_bucket->commit_logging_object(old_obj, y, dpp); ret < 0) {
        ldpp_dout(dpp, 5) << "WARNING: failed to commit logging object '" << old_obj << "' to bucket '" <<
          configuration.target_bucket << "', ret = " << ret << dendl;
        // we still want to write the new records to the new object even if commit failed
      }
    } else {
      ldpp_dout(dpp, 20) << "INFO: record will be written to current logging object '" << obj_name << "'. will be comitted at: " << time_to_commit << dendl;
    }
  } else if (ret == -ENOENT) {
    // create the temporary log object for the first time
    ldpp_dout(dpp, 20) << "INFO: first time logging for bucket '" << configuration.target_bucket << "'" << dendl;
    if (ret = new_logging_object(configuration, target_bucket, driver->get_host_id(), obj_name, dpp, y); ret < 0 ) {
      return ret;
    }
  } else {
    ldpp_dout(dpp, 1) << "ERROR: failed to get name of logging object of bucket '" <<
      configuration.target_bucket << "', ret = " << ret << dendl;
    return ret;
  }

  std::string record;
  const auto tt = ceph::coarse_real_time::clock::to_time_t(s->time);
  std::tm t{};
  localtime_r(&tt, &t);
  switch (configuration.record_type) {
    case BucketLoggingRecordType::Standard:
      // fallthrough - same as "short"
    case BucketLoggingRecordType::Short:
      record = fmt::format("{} [{:%d/%b/%Y:%H:%M:%S %z}] {} {} {}",
        s->bucket_tenant + "/" + s->bucket_name,
        t,
        s->object->get_key().name,
        op_name,
        etag);
      break;
    default:
      ldpp_dout(dpp, 1) << "ERROR: unsupported record format: " <<
        static_cast<unsigned>(configuration.record_type) << dendl;
      return -EINVAL;
  }

  if (ret = target_bucket->write_logging_object(obj_name, record, y, dpp); ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write record to logging object '" <<
      obj_name << "'. ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: wrote logging record: '" << record
    << "' to '" << obj_name << "'" << dendl;
  return 0;
}

std::string logging_object_name_oid(const rgw::sal::Bucket* bucket, const std::string& prefix) {
  // TODO: do i need bucket marker in the name?
  return fmt::format("logging.{}.bucket.{}/{}", bucket->get_tenant(), bucket->get_bucket_id(), prefix);
}

std::string to_temp_object_name(const rgw::sal::Bucket* bucket, const std::string& obj_name) {
  return fmt::format("{}__shadow_{}0", 
      bucket->get_bucket_id(),
      obj_name);
}

