// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

//
#include "rgw_bucket_encryption.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"

#include <initializer_list>
#include <string_view>

// a misspelled or misplaced element would otherwise read as absent
static void check_known_children(XMLObj *obj, std::initializer_list<std::string_view> known) {
  auto iter = obj->find_first();
  for (std::string name; iter.get_name(name); iter.get_next()) {
    if (std::find(known.begin(), known.end(), name) == known.end()) {
      throw RGWXMLDecoder::err("unknown element " + name);
    }
  }
}

void ApplyServerSideEncryptionByDefault::decode_xml(XMLObj *obj) {
  check_known_children(obj, {"KMSMasterKeyID", "SSEAlgorithm"});
  RGWXMLDecoder::decode_xml("KMSMasterKeyID", kmsMasterKeyID, obj, false);
  RGWXMLDecoder::decode_xml("SSEAlgorithm", sseAlgorithm, obj, true);
}

void ApplyServerSideEncryptionByDefault::dump_xml(Formatter *f) const {
  encode_xml("SSEAlgorithm", sseAlgorithm, f);
  if (kmsMasterKeyID != "") {
    encode_xml("KMSMasterKeyID", kmsMasterKeyID, f);
  }
}

void ServerSideEncryptionConfiguration::decode_xml(XMLObj *obj) {
  check_known_children(obj, {"ApplyServerSideEncryptionByDefault", "BucketKeyEnabled",
                             "BlockedEncryptionTypes"});
  RGWXMLDecoder::decode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, obj, false);
  RGWXMLDecoder::decode_xml("BucketKeyEnabled", bucketKeyEnabled, obj, false);

  if (XMLObj *blocked = obj->find_first("BlockedEncryptionTypes"); blocked) {
    check_known_children(blocked, {"EncryptionType"});
    RGWXMLDecoder::decode_xml("EncryptionType", blockedEncryptionTypes, blocked);
    for (const auto& type : blockedEncryptionTypes) {
      if (type != "SSE-C" && type != "NONE") {
        throw RGWXMLDecoder::err("invalid EncryptionType: '" + type + "'");
      }
    }
  }
}

void ServerSideEncryptionConfiguration::dump_xml(Formatter *f) const {
  if (!sse_algorithm().empty()) {
    encode_xml("ApplyServerSideEncryptionByDefault", applyServerSideEncryptionByDefault, f);
  }
  if (!blockedEncryptionTypes.empty()) {
    f->open_object_section("BlockedEncryptionTypes");
    encode_xml("EncryptionType", blockedEncryptionTypes, f);
    f->close_section();
  }
  if (bucketKeyEnabled) {
    encode_xml("BucketKeyEnabled", true, f);
  }
}

void RGWBucketEncryptionConfig::decode_xml(XMLObj *obj) {
  check_known_children(obj, {"Rule"});
  rule_exist = RGWXMLDecoder::decode_xml("Rule", rule, obj, true);
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
    encode_json("blocked_encryption_types", rule.blocked_encryption_types(), f);
  }
}
