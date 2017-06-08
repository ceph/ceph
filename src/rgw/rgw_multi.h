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
  ~RGWMultiCompleteUpload() {}
  bool xml_end(const char *el);

  std::map<int, string> parts;
};

class RGWMultiPart : public XMLObj
{
  string etag;
  int num;
public:
  RGWMultiPart() : num(0) {}
  ~RGWMultiPart() {}
  bool xml_end(const char *el);

  string& get_etag() { return etag; }
  int get_num() { return num; }
};

class RGWMultiPartNumber : public XMLObj
{
public:
  RGWMultiPartNumber() {}
  ~RGWMultiPartNumber() {}
};

class RGWMultiETag : public XMLObj
{
public:
  RGWMultiETag() {}
  ~RGWMultiETag() {}
};

class RGWMultiXMLParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el);
public:
  RGWMultiXMLParser() {}
  ~RGWMultiXMLParser() {}
};

#endif
