// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_tools.h"

class XMLObj;


/* S3 bucket logging configuration
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html
<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <LoggingEnabled>
      <TargetBucket>string</TargetBucket>
      <TargetGrants>
         <Grant>
            <Grantee>
               <DisplayName>string</DisplayName>
               <EmailAddress>string</EmailAddress>
               <ID>string</ID>
               <xsi:type>string</xsi:type>
               <URI>string</URI>
            </Grantee>
            <Permission>string</Permission>
         </Grant>
      </TargetGrants>
      <TargetObjectKeyFormat>
         <PartitionedPrefix>
            <PartitionDateSource>string</PartitionDateSource>
         </PartitionedPrefix>
         <SimplePrefix>
         </SimplePrefix>
      </TargetObjectKeyFormat>
      <TargetPrefix>string</TargetPrefix>
   </LoggingEnabled>
</BucketLoggingStatus>
*/

enum class BucketLoggingKeyFormat {Partitioned, RGWPartitioned, Simple};
enum class BucketLoggingRecordType {Standard, Short};

struct rgw_bucket_logging {
  bool enabled = false;
  std::string target_bucket;
  BucketLoggingKeyFormat obj_key_format = BucketLoggingKeyFormat::Simple;
  // target object key formats:
  // Partitioned: [DestinationPrefix][SourceAccountId]/[SourceRegion]/[SourceBucket]/[YYYY]/[MM]/[DD]/[YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
  // Simple: [DestinationPrefix][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
  // RGWPartitioned: [DestinationPrefix][RGWID][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
  std::string target_prefix; // a prefix for all log object keys. 
                             // useful when multiple bucket log to the same target 
                             // or when the target bucket is used for other things than logs
  uint32_t obj_roll_time; // time in seconds to move object to bucket and start another object
  BucketLoggingRecordType record_type;
  uint32_t records_batch_size = 0; // how many records to batch in memory before writing to the object
                                   // if set to zero, records are written syncronously to the object.
                                   // if obj_roll_time is reached, the batch of records will be written to the object
                                   // regardless of the number of records
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  void dump(Formatter *f) const; // json
  std::string to_json_str() const;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(target_bucket, bl);
    encode(static_cast<int>(obj_key_format), bl);
    encode(target_prefix, bl);
    encode(obj_roll_time, bl);
    encode(static_cast<int>(record_type), bl);
    encode(records_batch_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(target_bucket, bl);
    int type;
    decode(type, bl);
    obj_key_format = static_cast<BucketLoggingKeyFormat>(type);
    decode(target_prefix, bl);
    decode(obj_roll_time, bl);
    decode(type, bl);
    record_type = static_cast<BucketLoggingRecordType>(type);
    decode(records_batch_size, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_logging)

/* S3 bucket log structure
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
*/
struct bucket_logging_record {
  std::string bucket_owner;
  std::string bucket;
  std::string time;             // The time at which the request was received
                                // at UTC time. The format, as follows: [%d/%b/%Y:%H:%M:%S %z]
  std::string remote_ip;        // The apparent IP address of the requester
  std::string requester;        // The canonical user ID of the requester, or a - for unauthenticated requests 
  std::string request_id;
  std::string operation;        // REST.HTTP_method.resource_type or 
                                // S3.action.resource_type for Lifecycle and logging
  std::string key;              // The key (object name) part of the request
  std::string request_uri;      // The Request-URI part of the HTTP request message
  int http_status;              // The numeric HTTP status code of the response
  std::string error_code;       // The S3 Error code, or - if no error occurred
  std::string bytes_sent;       // The number of response bytes sent, excluding HTTP protocol overhead, or - if zero
  uint64_t object_size;
  uint32_t total_time;          // milliseconds including network transmission time.
                                // from first byte received to last byte transmitted
  uint32_t turn_around_time;    // milliseconds exluding networks transmission time.
                                // from last byte received to first byte transmitted
  std::string referer;          // The value of the HTTP Referer header, if present, or - if not
  std::string user_agent;
  std::string version_id;       // The version ID in the request, or - if the operation doesn't take a versionId parameter
  std::string host_id;          // x-amz-id-2
  std::string sig_version;      // SigV2 or SigV4, that was used to authenticate the request or a - for unauthenticated requests
  std::string cipher_suite;     // SSL cipher that was negotiated for an HTTPS request or a - for HTTP
  std::string auth_type;        // The type of request authentication used: 
                                // AuthHeader, QueryString or a - for unauthenticated requests
  std::string host_header;      // The RGW endpoint
  std::string tls_version;      // TLS version negotiated by the client
                                // TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used
  std::string access_point_arn; // ARN of the access point of the request. 
                                // If the access point ARN is malformed or not used, the field will contain a -
  std::string acl_required;     // A string that indicates whether the request required an (ACL) for authorization. 
                                // If ACL is required, the string is Yes. If no ACLs were required, the string is -
  std::string etag;
};

inline std::string to_string(const bucket_logging_record& record) {
  std::string str_record = record.bucket;
  str_record.append(" ").append(record.time)
    .append(" ").append(record.operation)
    .append(" ").append(record.key)
    .append(" ").append(record.etag);
  // TODO: add all fields
  return str_record;
}

struct bucket_logging_short_record {
  std::string bucket; // TODO: use rgw_bucket
  std::string time; // TODO: use ceph time
  std::string operation;
  std::string key; // TODO: use: rgw_key
  std::optional<int> http_status; // TODO: we should have an option where we only log successsfull actions
  std::string etag;
};

inline std::string to_string(const bucket_logging_short_record& record) {
  std::string str_record = record.bucket;
  str_record.append(" ").append(record.time)
    .append(" ").append(record.operation)
    .append(" ").append(record.key)
    .append(" ").append(record.http_status ? std::to_string(*record.http_status) : "-")
    .append(" ").append(record.etag);
  return str_record;
}

constexpr unsigned MAX_BUCKET_LOGGING_BUFFER = 1000;

using bucket_logging_records = std::array<bucket_logging_record, MAX_BUCKET_LOGGING_BUFFER>;
using bucket_logging_short_records = std::array<bucket_logging_short_record, MAX_BUCKET_LOGGING_BUFFER>;

template <typename Records>
inline std::string to_string(const Records& records) {
  std::string str_records;
  for (const auto& record : records) {
    str_records.append(to_string(record)).append("\n");
  }
  return str_records;
}

