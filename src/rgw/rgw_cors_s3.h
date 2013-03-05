#ifndef CEPH_RGW_CORS_S3_H
#define CEPH_RGW_CORS_S3_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

#include "rgw_xml.h"
#include "rgw_cors.h"

using namespace std;

class RGWCORSRule_S3 : public RGWCORSRule, public XMLObj
{
  public:
    RGWCORSRule_S3(){}
    ~RGWCORSRule_S3(){}
    
    bool xml_end(const char *el);
    void to_xml(ostream& out);
};

class RGWCORSConfiguration_S3 : public RGWCORSConfiguration, public XMLObj
{
  public:
    RGWCORSConfiguration_S3(){}
    ~RGWCORSConfiguration_S3(){}

    bool xml_end(const char *el);
    void to_xml(ostream& out);
};

class RGWCORSXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  RGWCORSXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};
#endif /*CEPH_RGW_CORS_S3_H*/
