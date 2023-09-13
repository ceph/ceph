// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "include/str_list.h"
#include "rgw_lc.h"
#include "rgw_xml.h"
#include "rgw_tag_s3.h"


class LCFilter_S3 : public LCFilter
{
public:
  void dump_xml(Formatter *f) const;
  void decode_xml(XMLObj *obj);
};

class LCExpiration_S3 : public LCExpiration
{
private:
  bool dm_expiration{false};
public:
  LCExpiration_S3() {}
  LCExpiration_S3(std::string _days, std::string _date, bool _dm_expiration) : LCExpiration(_days, _date), dm_expiration(_dm_expiration) {}

  void dump_xml(Formatter *f) const;
  void decode_xml(XMLObj *obj);

  void set_dm_expiration(bool _dm_expiration) {
    dm_expiration = _dm_expiration;
  }

  bool get_dm_expiration() {
    return dm_expiration;
  }
};

class LCNoncurExpiration_S3 : public LCExpiration
{
public:
  LCNoncurExpiration_S3() {}
  
  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class LCMPExpiration_S3 : public LCExpiration
{
public:
  LCMPExpiration_S3() {}

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class LCTransition_S3 : public LCTransition
{
public:
  LCTransition_S3() {}

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class LCNoncurTransition_S3 : public LCTransition
{
public:
  LCNoncurTransition_S3() {}
  ~LCNoncurTransition_S3() {}

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};


class LCRule_S3 : public LCRule
{
public:
  LCRule_S3() {}

  void dump_xml(Formatter *f) const;
  void decode_xml(XMLObj *obj);
};

class RGWLifecycleConfiguration_S3 : public RGWLifecycleConfiguration
{
public:
  explicit RGWLifecycleConfiguration_S3(CephContext *_cct) : RGWLifecycleConfiguration(_cct) {}
  RGWLifecycleConfiguration_S3() : RGWLifecycleConfiguration(nullptr) {}

  void decode_xml(XMLObj *obj);
  int rebuild(RGWLifecycleConfiguration& dest);
  void dump_xml(Formatter *f) const;
};
