// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_bucket_logging.h"
#include "rgw_xml.h"

#define dout_subsys ceph_subsys_rgw

bool rgw_bucket_logging::decode_xml(XMLObj* obj) {
  const auto throw_if_missing = true;
  enabled = false;
  XMLObjIter iter = obj->find("LoggingEnabled");
  XMLObj* o = iter.get_next();
  if (o) {
    enabled = true;
    RGWXMLDecoder::decode_xml("TargetBucket", target_bucket, o, throw_if_missing);
    RGWXMLDecoder::decode_xml("TargetPrefix", target_prefix, o);
    uint32_t default_obj_roll_time{600};
    RGWXMLDecoder::decode_xml("ObjectRollTime", obj_roll_time, default_obj_roll_time, o);
    std::string default_type{"Standard"};
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
    if (iter = o->find("TargetObjectKeyFormat"); (o = iter.get_next())) {
      if (iter = o->find("PartitionedPrefix"); iter.get_next()) {
        obj_key_format = BucketLoggingKeyFormat::Partitioned;
        // TODO: support DeliveryTime when batch is non zero
      } else if (iter = o->find("RGWPartitioned"); iter.get_next()) {
          obj_key_format = BucketLoggingKeyFormat::RGWPartitioned;
      } else if (iter = o->find("SimplePrefix"); iter.get_next()) {
          obj_key_format = BucketLoggingKeyFormat::Simple;
      } else {
        throw RGWXMLDecoder::err("TargetObjectKeyFormat must contain a format tag");
      }
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

