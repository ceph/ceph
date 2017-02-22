// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MULTI_H
#define CEPH_RGW_MULTI_H

#include <map>
#include "rgw_xml.h"

class RGWMultiCompleteUpload : public XMLObj
{
public:
  RGWMultiCompleteUpload() {}
  ~RGWMultiCompleteUpload() override {}
  bool xml_end(const char *el) override;

  std::map<int, string> parts;
};

class RGWMultiPart : public XMLObj
{
  string etag;
  int num;
public:
  RGWMultiPart() : num(0) {}
  ~RGWMultiPart() override {}
  bool xml_end(const char *el) override;

  string& get_etag() { return etag; }
  int get_num() { return num; }
};

class RGWMultiPartNumber : public XMLObj
{
public:
  RGWMultiPartNumber() {}
  ~RGWMultiPartNumber() override {}
};

class RGWMultiETag : public XMLObj
{
public:
  RGWMultiETag() {}
  ~RGWMultiETag() override {}
};

class RGWMultiXMLParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override;
public:
  RGWMultiXMLParser() {}
  ~RGWMultiXMLParser() override {}
};

#endif
