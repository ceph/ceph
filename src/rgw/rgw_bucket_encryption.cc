// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//
#include "rgw_bucket_encryption.h"
#include "rgw_xml.h"

void ApplyServerSideEncryptionByDefault::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("KMSMasterKeyID", kmsMasterKeyID, obj, false);
  RGWXMLDecoder::decode_xml("SSEAlgorithm", sseAlgorithm, obj, false);
}

void ApplyServerSideEncryptionByDefault::dump_xml(Formatter *f) const {
  encode_xml("SSEAlgorithm", sseAlgorithm, f);
}

void ServerSideEncryptionConfiguration::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, obj, true);
  RGWXMLDecoder::decode_xml("BucketKeyEnabled", bucketKeyEnabled, obj, false);
}

void ServerSideEncryptionConfiguration::dump_xml(Formatter *f) const {
  encode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, f);
}

void RGWBucketEncryptionConfig::decode_xml(XMLObj *obj) {
  rule_exist = RGWXMLDecoder::decode_xml("Rule", rule, obj);
}

void RGWBucketEncryptionConfig::dump_xml(Formatter *f) const {
  encode_xml("Rule", rule, f);
}
