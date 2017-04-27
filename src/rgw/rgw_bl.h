#ifndef CEPH_RGW_BL_H
#define CEPH_RGW_BL_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>
#include "common/debug.h"
#include "include/types.h"
#include "include/atomic.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_multi.h"
#include "cls/rgw/cls_rgw_types.h"


using namespace std;

#define HASH_PRIME_BL 12289
#define MAX_ID_LEN 255

static string bl_oid_prefix = "bl";

class BLRule
{
protected:
    string status;
    string targetbucket;
    string targetprefix;
    string perm;
public:
    string& get_status() {
        return status;
    }
    string& get_prefix() {
        return targetprefix;
    }
    string& get_targetbucket() {
        return targetbucket;
    }
};
class RGWBucketloggingConfiguration
{
protected:
    CephContext *cct;
    map<string, string> blrule;
public:
    RGWBucketloggingConfiguration(CephContext *_cct) : cct(_cct) {}
    RGWBucketloggingConfiguration() : cct(NULL) {}
    virtual ~RGWBucketloggingConfiguration() {} 
    void add_rule(BLRule *rule);
    map<string, string>& get_rule() { return blrule; }

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(blrule, bl);
        ENCODE_FINISH(bl);
    }
};
#endif
