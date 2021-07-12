// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once
#include <include/types.h>

class XMLObj;

class ApplyServerSideEncryptionByDefault
{
  string kmsMasterKeyID;
  string sseAlgorithm;

public:
  ApplyServerSideEncryptionByDefault(): kmsMasterKeyID(""), sseAlgorithm("") {};

  string kms_master_key_id() const {
    return kmsMasterKeyID;
  }

  string sse_algorithm() const {
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

public:
  ServerSideEncryptionConfiguration(): bucketKeyEnabled(false) {};

  string kms_master_key_id() const {
    return applyServerSideEncryptionByDefault.kms_master_key_id();
  }

  string sse_algorithm() const {
    return applyServerSideEncryptionByDefault.sse_algorithm();
  }

  bool bucket_key_enabled() const {
    return bucketKeyEnabled;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(applyServerSideEncryptionByDefault, bl);
    encode(bucketKeyEnabled, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(applyServerSideEncryptionByDefault, bl);
    decode(bucketKeyEnabled, bl);
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

  string kms_master_key_id() const {
    return rule.kms_master_key_id();
  }

  string sse_algorithm() const {
    return rule.sse_algorithm();
  }

  bool bucket_key_enabled() const {
    return rule.bucket_key_enabled();
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
};
WRITE_CLASS_ENCODER(RGWBucketEncryptionConfig)
