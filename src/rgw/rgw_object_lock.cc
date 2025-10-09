// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

//
#include "rgw_object_lock.h"

using namespace std;

void DefaultRetention::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("mode", mode, obj);
  JSONDecoder::decode_json("days", days, obj);
  JSONDecoder::decode_json("years", years, obj);
}

void DefaultRetention::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Mode", mode, obj, true);
  if (mode.compare("GOVERNANCE") != 0 && mode.compare("COMPLIANCE") != 0) {
    throw RGWXMLDecoder::err("bad Mode in lock rule");
  }
  bool days_exist = RGWXMLDecoder::decode_xml("Days", days, obj);
  bool years_exist = RGWXMLDecoder::decode_xml("Years", years, obj);
  if ((days_exist && years_exist) || (!days_exist && !years_exist)) {
    throw RGWXMLDecoder::err("either Days or Years must be specified, but not both");
  }
}

void DefaultRetention::dump(Formatter *f) const {
  encode_json("mode", mode, f);
  if (days > 0) {
    encode_json("days", days, f);
  } else {
    encode_json("years", years, f);
  }
}

void DefaultRetention::dump_xml(Formatter *f) const {
  encode_xml("Mode", mode, f);
  if (days > 0) {
    encode_xml("Days", days, f);
  } else {
    encode_xml("Years", years, f);
  }
}

void ObjectLockRule::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("defaultRetention", defaultRetention, obj);
}

void ObjectLockRule::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("DefaultRetention", defaultRetention, obj, true);
}

void ObjectLockRule::dump_xml(Formatter *f) const {
  encode_xml("DefaultRetention", defaultRetention, f);
}

void ObjectLockRule::dump(Formatter *f) const {
  encode_json("defaultRetention", defaultRetention, f);
}

std::list<ObjectLockRule> ObjectLockRule::generate_test_instances() {
  std::list<ObjectLockRule> o;
  o.emplace_back();
  return o;
}

void RGWObjectLock::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("enabled", enabled, obj);
  JSONDecoder::decode_json("rule_exist", rule_exist, obj);
  if (rule_exist) {
    JSONDecoder::decode_json("rule", rule, obj);
  }
}

void RGWObjectLock::decode_xml(XMLObj *obj) {
  string enabled_str;
  RGWXMLDecoder::decode_xml("ObjectLockEnabled", enabled_str, obj, true);
  if (enabled_str.compare("Enabled") != 0) {
    throw RGWXMLDecoder::err("invalid ObjectLockEnabled value");
  } else {
    enabled = true;
  }
  rule_exist = RGWXMLDecoder::decode_xml("Rule", rule, obj);
}

void RGWObjectLock::dump_xml(Formatter *f) const {
  if (enabled) {
    encode_xml("ObjectLockEnabled", "Enabled", f);
  }
  if (rule_exist) {
    encode_xml("Rule", rule, f);
  }
}

void RGWObjectLock::dump(Formatter *f) const {
  if (enabled) {
    encode_json("enabled", enabled, f);
  }
  encode_json("rule_exist", rule_exist, f);
  if (rule_exist) {
    encode_json("rule", rule, f);
  }
}

ceph::real_time RGWObjectLock::get_lock_until_date(const ceph::real_time& mtime) const {
  if (!rule_exist) {
    return ceph::real_time();
  }
  if (int days = get_days(); days > 0) {
    return mtime + std::chrono::days(days);
  }
  return mtime + std::chrono::years(get_years());
}

list<RGWObjectLock> RGWObjectLock::generate_test_instances() {
  list<RGWObjectLock> o;
  RGWObjectLock obj;
  obj.enabled = true;
  obj.rule_exist = true;
  o.push_back(std::move(obj));
  obj = RGWObjectLock{};
  obj.enabled = false;
  obj.rule_exist = false;
  o.push_back(std::move(obj));
  return o;
}

void RGWObjectRetention::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Mode", mode, obj, true);
  if (mode.compare("GOVERNANCE") != 0 && mode.compare("COMPLIANCE") != 0) {
    throw RGWXMLDecoder::err("bad Mode in retention");
  }
  string date_str;
  RGWXMLDecoder::decode_xml("RetainUntilDate", date_str, obj, true);
  boost::optional<ceph::real_time> date = ceph::from_iso_8601(date_str);
  if (boost::none == date) {
    throw RGWXMLDecoder::err("invalid RetainUntilDate value");
  }
  retain_until_date = *date;
}

void RGWObjectRetention::dump_xml(Formatter *f) const {
  encode_xml("Mode", mode, f);
  string date = ceph::to_iso_8601(retain_until_date);
  encode_xml("RetainUntilDate", date, f);
}

void RGWObjectLegalHold::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Status", status, obj, true);
  if (status.compare("ON") != 0 && status.compare("OFF") != 0) {
    throw RGWXMLDecoder::err("bad status in legal hold");
  }
}

void RGWObjectLegalHold::dump_xml(Formatter *f) const {
  encode_xml("Status", status, f);
}

bool RGWObjectLegalHold::is_enabled() const {
  return status.compare("ON") == 0;
}
