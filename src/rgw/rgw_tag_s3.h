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

struct RGWObjTagKey_S3: public XMLObj
{
};

struct RGWObjTagValue_S3: public XMLObj
{
};

class RGWObjTagEntry_S3: public XMLObj
{
  std::string key;
  std::string val;
public:
  RGWObjTagEntry_S3() {}
  RGWObjTagEntry_S3(std::string k,std::string v):key(k),val(v) {};
  ~RGWObjTagEntry_S3() {}

  bool xml_end(const char*) override;
  const std::string& get_key () const { return key;}
  const std::string& get_val () const { return val;}
  //void to_xml(CephContext *cct, ostream& out) const;
};

class RGWObjTagSet_S3: public RGWObjTags, public XMLObj
{
public:
  bool xml_end(const char*) override;
  void dump_xml(Formatter *f) const;
  int rebuild(RGWObjTags& dest);
};

class RGWObjTagging_S3: public XMLObj
{
public:
  bool xml_end(const char*) override;
};

class RGWObjTagsXMLParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el);
public:
  RGWObjTagsXMLParser() {}
  ~RGWObjTagsXMLParser() {}
};

#endif /* RGW_TAG_S3_H */
