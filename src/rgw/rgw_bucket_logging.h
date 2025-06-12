// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <cstdint>
#include "rgw_sal_fwd.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/async/yield_context.h"
#include "rgw_s3_filter.h"

class XMLObj;
namespace ceph { class Formatter; }
class DoutPrefixProvider;
struct req_state;
struct RGWObjVersionTracker;
class RGWOp;

namespace rgw::bucketlogging {
/* S3 bucket logging configuration
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html
 * with ceph extensions
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
            <PartitionDateSource>DeliveryTime|EventTime</PartitionDateSource>
         </PartitionedPrefix>
         <SimplePrefix>
         </SimplePrefix>
      </TargetObjectKeyFormat>
      <TargetPrefix>string</TargetPrefix>
      <LoggingType>Standard|Journal</LoggingType>       <!-- Ceph extension -->
      <ObjectRollTime>integer</ObjectRollTime>          <!-- Ceph extension -->
      <RecordsBatchSize>integer</RecordsBatchSize>      <!-- Ceph extension -->
      <Filter>
        <S3Key>
          <FilterRule>
            <Name>suffix/prefix/regex</Name>
            <Value></Value>
          </FilterRule>
        </S3Key>
      </Filter>
   </LoggingEnabled>
</BucketLoggingStatus>
*/

enum class KeyFormat {Partitioned, Simple};
enum class LoggingType {Standard, Journal, Any};
enum class PartitionDateSource {DeliveryTime, EventTime};

struct configuration {
  bool operator==(const configuration& rhs) const {
    return enabled == rhs.enabled &&
           target_bucket == rhs.target_bucket &&
           obj_key_format == rhs.obj_key_format &&
           target_prefix == rhs.target_prefix &&
           obj_roll_time == rhs.obj_roll_time &&
           logging_type == rhs.logging_type &&
           records_batch_size == rhs.records_batch_size &&
           date_source == rhs.date_source &&
           key_filter == rhs.key_filter;
  }
  uint32_t default_obj_roll_time = 300;
  bool enabled = false;
  std::string target_bucket;
  KeyFormat obj_key_format = KeyFormat::Simple;
  // target object key formats:
  // Partitioned: [DestinationPrefix][SourceAccountId]/[SourceRegion]/[SourceBucket]/[YYYY]/[MM]/[DD]/[YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
  // Simple: [DestinationPrefix][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
  std::string target_prefix; // a prefix for all log object keys. 
                             // useful when multiple bucket log to the same target 
                             // or when the target bucket is used for other things than logs
  uint32_t obj_roll_time; // time in seconds to move object to bucket and start another object
  LoggingType logging_type = LoggingType::Standard;
  // in case of "Standard: logging type, all bucket operations are logged
  // in case of "Journal" logging type only the following operations are logged: PUT, COPY, MULTI/DELETE, Complete MPU
  uint32_t records_batch_size = 0; // how many records to batch in memory before writing to the object
                                   // if set to zero, records are written syncronously to the object.
                                   // if obj_roll_time is reached, the batch of records will be written to the object
                                   // regardless of the number of records
  PartitionDateSource date_source = PartitionDateSource::DeliveryTime;
  // EventTime: use only year, month, and day. The hour, minutes and seconds are set to 00 in the key
  // DeliveryTime: the time the log object was created
  rgw_s3_key_filter key_filter;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  void dump(Formatter *f) const; // json
  std::string to_json_str() const;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(target_bucket, bl);
    encode(static_cast<int>(obj_key_format), bl);
    encode(target_prefix, bl);
    encode(obj_roll_time, bl);
    encode(static_cast<int>(logging_type), bl);
    encode(records_batch_size, bl);
    encode(static_cast<int>(date_source), bl);
    if (logging_type == LoggingType::Journal) {
      encode(key_filter, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(target_bucket, bl);
    int type;
    decode(type, bl);
    obj_key_format = static_cast<KeyFormat>(type);
    decode(target_prefix, bl);
    decode(obj_roll_time, bl);
    decode(type, bl);
    logging_type = static_cast<LoggingType>(type);
    decode(records_batch_size, bl);
    decode(type, bl);
    date_source = static_cast<PartitionDateSource>(type);
    if (logging_type == LoggingType::Journal) {
      decode(key_filter, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(configuration)

static const std::string service_principal = "logging.s3.amazonaws.com";

using source_buckets = std::set<rgw_bucket>;

constexpr unsigned MAX_BUCKET_LOGGING_BUFFER = 1000;

using bucket_logging_records = std::array<std::string, MAX_BUCKET_LOGGING_BUFFER>;

template <typename Records>
inline std::string to_string(const Records& records) {
  std::string str_records;
  for (const auto& record : records) {
    str_records.append(to_string(record)).append("\n");
  }
  return str_records;
}

// log a bucket logging record according to the configuration
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
    bool log_source_bucket);

// commit the pending log objec to the log bucket
// and create a new pending log object
// if "must_commit" is "false" the function will return success even if the pending log object was not committed
// if "last_committed" is not null, it will be set to the name of the last committed object
int rollover_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& bucket,
    std::string& obj_name,
    const DoutPrefixProvider *dpp,
    const std::string& region,
    const std::unique_ptr<rgw::sal::Bucket>& source_bucket,
    optional_yield y,
    bool must_commit,
    RGWObjVersionTracker* objv_tracker,
    std::string* last_committed);

// commit the pending log object to the log bucket
// use this for cleanup, when new pending object is not needed
// and target bucket is known
// if "last_committed" is not null, it will be set to the name of the last committed object
int commit_logging_object(const configuration& conf,
    const std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    const DoutPrefixProvider *dpp,
    optional_yield y,
    std::string* last_committed);

// commit the pending log object to the log bucket
// use this for cleanup, when new pending object is not needed
// and target bucket shoud be loaded based on the configuration
// if "last_committed" is not null, it will be set to the name of the last committed object
int commit_logging_object(const configuration& conf,
    const DoutPrefixProvider *dpp,
    rgw::sal::Driver* driver,
    const std::string& tenant_name,
    optional_yield y,
    std::string* last_committed);

// return the oid of the object holding the name of the temporary logging object
// bucket - log bucket
// prefix - logging prefix from configuration. should be used when multiple buckets log into the same log bucket
std::string object_name_oid(const rgw::sal::Bucket* bucket, const std::string& prefix);

// log a bucket logging record according to type
// configuration is fetched from bucket attributes
// if no configuration exists, or if type does not match the function return zero (success)
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
    bool log_source_bucket);

// return (by ref) an rgw_bucket object with the bucket name and tenant name
// fails if the bucket name is not in the format: [tenant name:]<bucket name>
int get_bucket_id(const std::string& bucket_name, const std::string& tenant_name, rgw_bucket& bucket_id);

// update (add or remove) a source bucket from the list of source buckets in the target bucket
// use this function when the target bucket is already loaded
int update_bucket_logging_sources(const DoutPrefixProvider* dpp, std::unique_ptr<rgw::sal::Bucket>& bucket, 
    const rgw_bucket& src_bucket, bool add, optional_yield y);

// update (add or remove) a source bucket from the list of source buckets in the target bucket
// use this function when the target bucket is not known and needs to be loaded
int update_bucket_logging_sources(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver, const rgw_bucket& target_bucket_id, 
    const rgw_bucket& src_bucket_id, bool add, optional_yield y);

// when source bucket is deleted, all pending log objects should be comitted to the log bucket
// when the target bucket is deleted, all pending log objects should be deleted, as well as the object holding the pending log object name
int bucket_deletion_cleanup(const DoutPrefixProvider* dpp,
                                   sal::Driver* driver,
                                   sal::Bucket* bucket,
                                   optional_yield y);

// if bucket has bucket logging configuration associated with it then:
// if "remove_attr" is true, the bucket logging configuration should be removed from the bucket
// in addition:
// any pending log objects should be comitted to the log bucket
// and the log bucket should be updated to remove the bucket as a source
int source_bucket_cleanup(const DoutPrefixProvider* dpp,
                                   sal::Driver* driver,
                                   sal::Bucket* bucket,
                                   bool remove_attr,
                                   optional_yield y);

// verify that the target bucket has the correct policy to allow the source bucket to log to it
// note that this function adds entries to the request state environment
int verify_target_bucket_policy(const DoutPrefixProvider* dpp,
    rgw::sal::Bucket* target_bucket,
    const rgw::ARN& target_resource_arn,
    req_state* s);

// verify that target bucket does not have:
// - bucket logging
// - requester pays
// - encryption
int verify_target_bucket_attributes(const DoutPrefixProvider* dpp, rgw::sal::Bucket* target_bucket);

// given a source bucket this function is parsing the configuration object
// extracting the target bucket and load it
// the log bucket tenant is taken from configuration
// however, if not explicitly set there, it is taken from the tenant parameter
// both configuration and target bucket are returned by reference
int get_target_and_conf_from_source(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    rgw::sal::Bucket* src_bucket,
    const std::string& tenant,
    configuration& configuration,
    std::unique_ptr<rgw::sal::Bucket>& target_bucket,
    optional_yield y);

} // namespace rgw::bucketlogging

