// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once
#include <algorithm>
#include <utility>
#include <include/types.h>
#include "include/encoding.h"

class XMLObj;
namespace ceph { class Formatter; }

class ApplyServerSideEncryptionByDefault
{
  std::string kmsMasterKeyID;
  std::string sseAlgorithm;

public:
  ApplyServerSideEncryptionByDefault() {};
  ApplyServerSideEncryptionByDefault(const std::string &algorithm,
     const std::string &key_id)
   : kmsMasterKeyID(key_id), sseAlgorithm(algorithm) {};

  const std::string& kms_master_key_id() const {
    return kmsMasterKeyID;
  }

  const std::string& sse_algorithm() const {
    return sseAlgorithm;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(kmsMasterKeyID, bl);
    encode(sseAlgorithm, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(kmsMasterKeyID, bl);
    decode(sseAlgorithm, bl);
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ApplyServerSideEncryptionByDefault)

class ServerSideEncryptionConfiguration
{
protected:
  ApplyServerSideEncryptionByDefault applyServerSideEncryptionByDefault;
  bool bucketKeyEnabled;
  std::vector<std::string> blockedEncryptionTypes;

public:
  ServerSideEncryptionConfiguration(): bucketKeyEnabled(false) {};
  ServerSideEncryptionConfiguration(const std::string &algorithm,
    const std::string &keyid="", bool enabled = false)
      : applyServerSideEncryptionByDefault(algorithm, keyid),
        bucketKeyEnabled(enabled) {}
  explicit ServerSideEncryptionConfiguration(
      std::vector<std::string> blocked_encryption_types)
      : bucketKeyEnabled(false),
        blockedEncryptionTypes(std::move(blocked_encryption_types)) {}

  const std::string& kms_master_key_id() const {
    return applyServerSideEncryptionByDefault.kms_master_key_id();
  }

  const std::string& sse_algorithm() const {
    return applyServerSideEncryptionByDefault.sse_algorithm();
  }

  bool bucket_key_enabled() const {
    return bucketKeyEnabled;
  }

  const std::vector<std::string>& blocked_encryption_types() const {
    return blockedEncryptionTypes;
  }

  bool sse_c_blocked() const {
    return std::find(blockedEncryptionTypes.begin(),
                     blockedEncryptionTypes.end(),
                     "SSE-C") != blockedEncryptionTypes.end();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(applyServerSideEncryptionByDefault, bl);
    encode(bucketKeyEnabled, bl);
    encode(blockedEncryptionTypes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(applyServerSideEncryptionByDefault, bl);
    decode(bucketKeyEnabled, bl);
    if (struct_v >= 2) {
      decode(blockedEncryptionTypes, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ServerSideEncryptionConfiguration)

class RGWBucketEncryptionConfig
{
protected:
  bool rule_exist;
  ServerSideEncryptionConfiguration rule;

public:
  RGWBucketEncryptionConfig(): rule_exist(false) {}
  RGWBucketEncryptionConfig(const std::string &algorithm,
    const std::string &keyid = "", bool enabled = false)
      : rule_exist(true), rule(algorithm, keyid, enabled) {}
  explicit RGWBucketEncryptionConfig(
      std::vector<std::string> blocked_encryption_types)
      : rule_exist(true), rule(std::move(blocked_encryption_types)) {}

  const std::string& kms_master_key_id() const {
    return rule.kms_master_key_id();
  }

  const std::string& sse_algorithm() const {
    return rule.sse_algorithm();
  }

  bool bucket_key_enabled() const {
    return rule.bucket_key_enabled();
  }

  bool sse_c_blocked() const {
    return rule.sse_c_blocked();
  }

  bool has_rule() const {
    return rule_exist;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(rule_exist, bl);
    if (rule_exist) {
      encode(rule, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(rule_exist, bl);
    if (rule_exist) {
      decode(rule, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  void dump(Formatter *f) const;
  static std::list<RGWBucketEncryptionConfig> generate_test_instances();
};
WRITE_CLASS_ENCODER(RGWBucketEncryptionConfig)
