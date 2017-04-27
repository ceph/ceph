#ifndef CEPH_RGW_BL_S3_H
#define CEPH_RGW_BL_S3_H

#include <expat.h>
#include <string.h>
#include "include/str_list.h"
#include <include/types.h>
#include "rgw_bl.h"
#include "rgw_xml.h"

class LoggingEnabled_S3 : public BLRule, public XMLObj
{
public:
    LoggingEnabled_S3() {}
    ~LoggingEnabled_S3() {}
                                             
    bool xml_end(const char* el);
};
class TargetBucket_S3 : public XMLObj
{
public:
    TargetBucket_S3() {}
    ~TargetBucket_S3() {}
    string& to_str() { return data;   }
};
class TargetPrefix_S3  : public XMLObj
{
public:
   TargetPrefix_S3() {}
   ~TargetPrefix_S3() {}
   string& to_str() { return data;   }
};
class TargetPerm_S3 : public XMLObj
{
public:
   TargetPerm_S3() {}  
   ~TargetPerm_S3() {}  
   string& to_str() { return data;   }
};
class RGWBucketloggingConfiguration_S3 : public RGWBucketloggingConfiguration, public XMLObj
{
public:
    RGWBucketloggingConfiguration_S3(CephContext *_cct) : RGWBucketloggingConfiguration(_cct) {}
    ~RGWBucketloggingConfiguration_S3() {}
    bool xml_end(const char* el);
    int rebuild(RGWRados *store, RGWBucketloggingConfiguration& dest);
};
class RGWBLXMLParser_S3 : public RGWXMLParser
{
    CephContext *cct;
    XMLObj *alloc_obj(const char* el);
public:
    RGWBLXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

#endif
