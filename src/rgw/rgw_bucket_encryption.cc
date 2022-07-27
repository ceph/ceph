// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//
#include "rgw_bucket_encryption.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"

void ApplyServerSideEncryptionByDefault::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("KMSMasterKeyID", kmsMasterKeyID, obj, false);
  RGWXMLDecoder::decode_xml("SSEAlgorithm", sseAlgorithm, obj, false);
}

void ApplyServerSideEncryptionByDefault::dump_xml(Formatter *f) const {
  encode_xml("SSEAlgorithm", sseAlgorithm, f);
  if (kmsMasterKeyID != "") {
    encode_xml("KMSMasterKeyID", kmsMasterKeyID, f);
  }
}

void ServerSideEncryptionConfiguration::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, obj, false);
  RGWXMLDecoder::decode_xml("BucketKeyEnabled", bucketKeyEnabled, obj, false);
}

void ServerSideEncryptionConfiguration::dump_xml(Formatter *f) const {
  encode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, f);
  if (bucketKeyEnabled) {
    encode_xml("BucketKeyEnabled", true, f);
  }
}

void RGWBucketEncryptionConfig::decode_xml(XMLObj *obj) {
  rule_exist = RGWXMLDecoder::decode_xml("Rule", rule, obj);
}

void RGWBucketEncryptionConfig::dump_xml(Formatter *f) const {
  if (rule_exist) {
    encode_xml("Rule", rule, f);
  }
}

void RGWBucketEncryptionConfig::dump(Formatter *f) const {
  encode_json("rule_exist", has_rule(), f);
  if (has_rule()) {
    encode_json("sse_algorithm", sse_algorithm(), f);
    encode_json("kms_master_key_id", kms_master_key_id(), f);
    encode_json("bucket_key_enabled", bucket_key_enabled(), f);
  }
}
