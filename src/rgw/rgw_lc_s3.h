#ifndef CEPH_RGW_LC_S3_H
#define CEPH_RGW_LC_S3_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "include/str_list.h"
#include "rgw_lc.h"
#include "rgw_xml.h"
#include "rgw_tag_s3.h"

class LCID_S3 : public XMLObj
{
public:
  LCID_S3() {}
  ~LCID_S3() override {}
  string& to_str() { return data; }
};

class LCPrefix_S3 : public XMLObj
{
public:
  LCPrefix_S3() {}
  ~LCPrefix_S3() override {}
  string& to_str() { return data; }
};

class LCFilter_S3 : public LCFilter, public XMLObj
{
 public:
  ~LCFilter_S3() override {}
  string& to_str() { return data; }
  void to_xml(ostream& out){
    out << "<Filter>";
    stringstream ss;
    if (has_prefix())
      out << "<Prefix>" << prefix << "</Prefix>";
    if (has_tags()){
      for (const auto&kv : obj_tags.get_tags()){
        ss << "<Tag>";
        ss << "<Key>" << kv.first << "</Key>";
        ss << "<Value>" << kv.second << "</Value>";
        ss << "</Tag>";
      }
    }

    if (has_multi_condition()) {
      out << "<And>" << ss.str() << "</And>";
    } else {
      out << ss.str();
    }

    out << "</Filter>";
  }
  void dump_xml(Formatter *f) const {
    f->open_object_section("Filter");
    if (has_multi_condition())
      f->open_object_section("And");
    if (!prefix.empty())
      encode_xml("Prefix", prefix, f);
    if (has_tags()){
      const auto& tagset_s3 = static_cast<const RGWObjTagSet_S3 &>(obj_tags);
      tagset_s3.dump_xml(f);
    }
    if (has_multi_condition())
      f->close_section(); // And;
    f->close_section(); // Filter
  }
  bool xml_end(const char *el) override;
};

class LCStatus_S3 : public XMLObj
{
public:
  LCStatus_S3() {}
  ~LCStatus_S3() override {}
  string& to_str() { return data; }
};

class LCDays_S3 : public XMLObj
{
public:
  LCDays_S3() {}
  ~LCDays_S3() override {}
  string& to_str() { return data; }
};

class LCDate_S3 : public XMLObj
{
public:
  LCDate_S3() {}
  ~LCDate_S3() override {}
  string& to_str() { return data; }
};

class LCDeleteMarker_S3 : public XMLObj
{
public:
  LCDeleteMarker_S3() {}
  ~LCDeleteMarker_S3() override {}
  string& to_str() { return data; }
};

class LCExpiration_S3 : public LCExpiration, public XMLObj
{
private:
  bool dm_expiration;
public:
  LCExpiration_S3(): dm_expiration(false) {}
  LCExpiration_S3(string _days, string _date, bool _dm_expiration) {
    days = _days;
    date = _date;
    dm_expiration = _dm_expiration;
  }
  ~LCExpiration_S3() override {}

  bool xml_end(const char *el) override;
  void to_xml(ostream& out) {
    out << "<Expiration>";
    if (dm_expiration) {
      out << "<ExpiredObjectDeleteMarker>" << "true" << "</ExpiredObjectDeleteMarker>";
    } else if (!days.empty()){
      out << "<Days>" << days << "</Days>";
    } else {
      out << "<Date>" << date << "</Date>";
    }
    out << "</Expiration>";
  }
  void dump_xml(Formatter *f) const {
    f->open_object_section("Expiration");
    if (dm_expiration) {
      encode_xml("ExpiredObjectDeleteMarker", "true", f);
    } else if (!days.empty()) {
      encode_xml("Days", days, f);
    } else {
      encode_xml("Date", date, f);
    }
    f->close_section(); // Expiration
  }

  void set_dm_expiration(bool _dm_expiration) {
    dm_expiration = _dm_expiration;
  }

  bool get_dm_expiration() {
    return dm_expiration;
  }
};

class LCNoncurExpiration_S3 : public LCExpiration, public XMLObj
{
public:
  LCNoncurExpiration_S3() {}
  ~LCNoncurExpiration_S3() override {}
  
  bool xml_end(const char *el) override;
  void to_xml(ostream& out) {
    out << "<NoncurrentVersionExpiration>" << "<NoncurrentDays>" << days << "</NoncurrentDays>"<< "</NoncurrentVersionExpiration>";
  }
  void dump_xml(Formatter *f) const {
    f->open_object_section("NoncurrentVersionExpiration");
    encode_xml("NoncurrentDays", days, f);
    f->close_section(); 
  }
};

class LCMPExpiration_S3 : public LCExpiration, public XMLObj
{
public:
  LCMPExpiration_S3() {}
  ~LCMPExpiration_S3() {}

  bool xml_end(const char *el) override;
  void to_xml(ostream& out) {
    out << "<AbortIncompleteMultipartUpload>" << "<DaysAfterInitiation>" << days << "</DaysAfterInitiation>" << "</AbortIncompleteMultipartUpload>";
  }
  void dump_xml(Formatter *f) const {
    f->open_object_section("AbortIncompleteMultipartUpload");
    encode_xml("DaysAfterInitiation", days, f);
    f->close_section();
  }
};

class LCStorageClass_S3 : public XMLObj
{
public:
  LCStorageClass_S3() {}
  ~LCStorageClass_S3() override {}
  string& to_str() { return data; }
};

class LCTransition_S3 : public LCTransition, public XMLObj
{
public:
  LCTransition_S3() {}
  ~LCTransition_S3() {}

  bool xml_end(const char *el) override;
  void to_xml(ostream& out) {
    out << "<Transition>";
    if (!days.empty()) {
      out << "<Days>" << days << "</Days>";
    } else {
      out << "<Date>" << date << "</Date>";
    }
    out << "<StorageClass>" << storage_class << "</StorageClass>" << "</Transition>";
  }

  void dump_xml(Formatter *f) const {
    f->open_object_section("Transition");
    if (!days.empty()) {
      encode_xml("Days", days, f);
    } else {
      encode_xml("Date", date, f);
    }
    encode_xml("StorageClass", storage_class, f);
    f->close_section();
  }
};

class LCNoncurTransition_S3 : public LCTransition, public XMLObj
{
public:
  LCNoncurTransition_S3() {}
  ~LCNoncurTransition_S3() {}

  bool xml_end(const char *el) override;
  void to_xml(ostream& out) {
    out << "<NoncurrentVersionTransition>" << "<NoncurrentDays>" << days << "</NoncurrentDays>"
        << "<StorageClass>" << storage_class << "</StorageClass>" << "</NoncurrentVersionTransition>";
  }

  void dump_xml(Formatter *f) const {
    f->open_object_section("NoncurrentVersionTransition");
    encode_xml("NoncurrentDays", days, f);
    encode_xml("StorageClass", storage_class, f);
    f->close_section();
  }
};


class LCRule_S3 : public LCRule, public XMLObj
{
private:
  CephContext *cct;
public:
  LCRule_S3(): cct(nullptr) {}
  explicit LCRule_S3(CephContext *_cct): cct(_cct) {}
  ~LCRule_S3() override {}

  void to_xml(ostream& out);
  bool xml_end(const char *el) override;
  bool xml_start(const char *el, const char **attr);
  void dump_xml(Formatter *f) const {
    f->open_object_section("Rule");
    encode_xml("ID", id, f);
    // In case of an empty filter and an empty Prefix, we defer to Prefix.
    if (!filter.empty()) {
      const LCFilter_S3& lc_filter = static_cast<const LCFilter_S3&>(filter);
      lc_filter.dump_xml(f);
    } else {
      encode_xml("Prefix", prefix, f);
    }
    encode_xml("Status", status, f);
    if (!expiration.empty() || dm_expiration) {
      LCExpiration_S3 expir(expiration.get_days_str(), expiration.get_date(), dm_expiration);
      expir.dump_xml(f);
    }
    if (!noncur_expiration.empty()) {
      const LCNoncurExpiration_S3& noncur_expir = static_cast<const LCNoncurExpiration_S3&>(noncur_expiration);
      noncur_expir.dump_xml(f);
    }
    if (!mp_expiration.empty()) {
      const LCMPExpiration_S3& mp_expir = static_cast<const LCMPExpiration_S3&>(mp_expiration);
      mp_expir.dump_xml(f);
    }
    if (!transitions.empty()) {
      for (auto &elem : transitions) {
        const LCTransition_S3& tran = static_cast<const LCTransition_S3&>(elem.second);
        tran.dump_xml(f);
      }
    }
    if (!noncur_transitions.empty()) {
      for (auto &elem : noncur_transitions) {
        const LCNoncurTransition_S3& noncur_tran = static_cast<const LCNoncurTransition_S3&>(elem.second);
        noncur_tran.dump_xml(f);
      }
    }
    f->close_section(); // Rule
  }

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }
};

class RGWLCXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el) override;
public:
  explicit RGWLCXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

class RGWLifecycleConfiguration_S3 : public RGWLifecycleConfiguration, public XMLObj
{
public:
  explicit RGWLifecycleConfiguration_S3(CephContext *_cct) : RGWLifecycleConfiguration(_cct) {}
  RGWLifecycleConfiguration_S3() : RGWLifecycleConfiguration(NULL) {}
  ~RGWLifecycleConfiguration_S3() override {}

  bool xml_end(const char *el) override;

  void to_xml(ostream& out) {
    out << "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    multimap<string, LCRule>::iterator iter;
    for (iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
      LCRule_S3& rule = static_cast<LCRule_S3&>(iter->second);
      rule.to_xml(out);
    }
    out << "</LifecycleConfiguration>";
  }
  int rebuild(RGWRados *store, RGWLifecycleConfiguration& dest);
  void dump_xml(Formatter *f) const;
};

bool check_date(const string& _date);
#endif
