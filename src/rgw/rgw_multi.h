#ifndef CEPH_RGW_MULTI_H
#define CEPH_RGW_MULTI_H

#include <map>
#include "rgw_xml.h"

class RGWMultiCompleteUpload : public XMLObj
{
  std::map<int, string> parts;
public:
  RGWMultiCompleteUpload() {}
  ~RGWMultiCompleteUpload() {}
  bool xml_end(const char *el);

  std::map<int, string>& get_parts() { return parts; }
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
};

#endif
