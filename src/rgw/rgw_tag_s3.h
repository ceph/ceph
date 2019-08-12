// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_TAG_S3_H
#define RGW_TAG_S3_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>
#include <common/Formatter.h>
#include <expat.h>

#include "rgw_tag.h"
#include "rgw_xml.h"

class RGWObjTagEntry_S3
{
  std::string key;
  std::string val;
public:
  RGWObjTagEntry_S3() {}
  RGWObjTagEntry_S3(const std::string &k, const std::string &v):key(k),val(v) {};
  ~RGWObjTagEntry_S3() {}

  const std::string& get_key () const { return key; }
  const std::string& get_val () const { return val; }

  void dump_xml(Formatter *f) const;
  void decode_xml(XMLObj *obj);
};

class RGWObjTagSet_S3: public RGWObjTags
{
public:
  int rebuild(RGWObjTags& dest);

  void dump_xml(Formatter *f) const;
  void decode_xml(XMLObj *obj);
};

class RGWObjTagging_S3
{
  RGWObjTagSet_S3 tagset;
public:
  void decode_xml(XMLObj *obj);
  int rebuild(RGWObjTags& dest) {
    return tagset.rebuild(dest);
  }
};


#endif /* RGW_TAG_S3_H */
