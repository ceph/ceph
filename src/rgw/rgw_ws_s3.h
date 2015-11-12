// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_WS_S3_H
#define CEPH_RGW_WS_S3_H

#include <map>
#include <string>
#include <iostream>
#include <expat.h>

#include <include/types.h>
#include <common/Formatter.h>
#include "rgw_xml.h"
#include "rgw_ws.h"

using namespace std;

class WSIdxDoc_S3 : public WSIdxDoc, public XMLObj
{
public:
  WSIdxDoc_S3() {}
  ~WSIdxDoc_S3() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    if (suffix.empty())
      return;
    out << "<IndexDocument>" << "<Suffix>" << suffix << "</Suffix>";
    out << "</IndexDocument>";
  }
};

class RGWWebsiteConfiguration_S3 : public RGWWebsiteConfiguration, public XMLObj
{
  public:
    RGWWebsiteConfiguration_S3() {}
    ~RGWWebsiteConfiguration_S3() {}

    bool xml_end(const char *el);
    void to_xml(ostream& out) {
      out << "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
      WSIdxDoc_S3& _idx_doc = static_cast<WSIdxDoc_S3 &>(idx_doc);
      _idx_doc.to_xml(out);
      out << "</WebsiteConfiguration>";
    }
};

class RGWWSXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  RGWWSXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

#endif /*CEPH_RGW_WS_S3_H*/

