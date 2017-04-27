#include <string>
#include <iostream>
#include <list>
#include <vector>
#include <map>
#include "rgw_bl_s3.h"
#include <include/types.h>

bool RGWBucketloggingConfiguration_S3::xml_end(const char* el)                   
{
    XMLObjIter iter = find("LoggingEnabled");
    LoggingEnabled_S3 *rule = static_cast<LoggingEnabled_S3 *>(iter.get_next());
    add_rule(rule);
    return true;
}
bool LoggingEnabled_S3::xml_end(const char* el)
{
    TargetBucket_S3 *tbucket;
    TargetPrefix_S3 *tprefix;
    TargetPerm_S3 *tperm;
    
    targetbucket.clear();
    targetprefix.clear();
    perm.clear();
    
    tbucket = static_cast<TargetBucket_S3 *>(find_first("TargetBucket"));
    if(!tbucket)
        return false;
    targetbucket = tbucket->get_data();
    
    tprefix = static_cast<TargetPrefix_S3 *>(find_first("TargetPrefix"));
    if( !tprefix   )
        return false;
    targetprefix = tprefix->get_data();
    
    return true;
                                                                                  
}
int RGWBucketloggingConfiguration_S3::rebuild(RGWRados *store, RGWBucketloggingConfiguration& dest)
{
    //fixme: rebuild the config
    int ret =0;    
    dest = *this;
    return ret;
}
XMLObj* RGWBLXMLParser_S3::alloc_obj(const char* el)
{
    XMLObj *obj = NULL;
    if (strcmp(el, "BucketLoggingStatus") == 0) {
    obj = new RGWBucketloggingConfiguration_S3(cct);
    
    } else if (strcmp(el, "LoggingEnabled") == 0) {
    obj = new LoggingEnabled_S3();
    
    } else if (strcmp(el, "TargetBucket") == 0) {
    obj = new TargetBucket_S3();
    
    } 
    return obj; 
                  
}
