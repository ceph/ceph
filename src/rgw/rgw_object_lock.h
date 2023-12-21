// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include "common/ceph_time.h"
#include "common/iso_8601.h"
#include "rgw_xml.h"

class DefaultRetention
{
protected:
  std::string mode;
  int days;
  int years;

public:
  DefaultRetention(): days(0), years(0) {};

  int get_days() const {
    return days;
  }

  int get_years() const {
    return years;
  }

  std::string get_mode() const {
    return mode;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(mode, bl);
    encode(days, bl);
    encode(years, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(mode, bl);
    decode(days, bl);
    decode(years, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(DefaultRetention)

class ObjectLockRule
{
protected:
  DefaultRetention defaultRetention;
public:
  int get_days() const {
    return defaultRetention.get_days();
  }

  int get_years() const {
    return defaultRetention.get_years();
  }

  std::string get_mode() const {
    return defaultRetention.get_mode();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(defaultRetention, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(defaultRetention, bl);
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ObjectLockRule*>& o);
};
WRITE_CLASS_ENCODER(ObjectLockRule)

class RGWObjectLock
{
protected:
  bool enabled;
  bool rule_exist;
  ObjectLockRule rule;

public:
  RGWObjectLock():enabled(true), rule_exist(false) {}

  int get_days() const {
    return rule.get_days();
  }

  int get_years() const {
    return rule.get_years();
  }

  std::string get_mode() const {
    return rule.get_mode();
  }

  bool retention_period_valid() const {
    // DefaultRetention requires either Days or Years.
    // You can't specify both at the same time.
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTObjectLockConfiguration.html
    return (get_years() > 0) != (get_days() > 0);
  }

  bool has_rule() const {
    return rule_exist;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(rule_exist, bl);
    if (rule_exist) {
      encode(rule, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(rule_exist, bl);
    if (rule_exist) {
      decode(rule, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  ceph::real_time get_lock_until_date(const ceph::real_time& mtime) const;
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWObjectLock*>& o);
};
WRITE_CLASS_ENCODER(RGWObjectLock)

class RGWObjectRetention
{
protected:
  std::string mode;
  ceph::real_time retain_until_date;
public:
  RGWObjectRetention() {}
  RGWObjectRetention(std::string _mode, ceph::real_time _date): mode(_mode), retain_until_date(_date) {}

  void set_mode(std::string _mode) {
    mode = _mode;
  }

  std::string get_mode() const {
    return mode;
  }

  void set_retain_until_date(ceph::real_time _retain_until_date) {
    retain_until_date = _retain_until_date;
  }

  ceph::real_time get_retain_until_date() const {
    return retain_until_date;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(mode, bl);
    encode(retain_until_date, bl);
    ceph::round_trip_encode(retain_until_date, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(mode, bl);
    decode(retain_until_date, bl);
    if (struct_v >= 2) {
      ceph::round_trip_decode(retain_until_date, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWObjectRetention)

class RGWObjectLegalHold
{
protected:
  std::string status;
public:
  RGWObjectLegalHold() {}
  RGWObjectLegalHold(std::string _status): status(_status) {}
  void set_status(std::string _status) {
    status = _status;
  }

  std::string get_status() const {
    return status;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(status, bl);
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  bool is_enabled() const;
};
WRITE_CLASS_ENCODER(RGWObjectLegalHold)
