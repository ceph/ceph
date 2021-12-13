// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_zone.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"
#include "rgw_bucket.h"
#include "rgw_datalog.h"
#include "rgw_keystone.h"
#include "rgw_basic_types.h"
#include "rgw_op.h"
#include "rgw_data_sync.h"
#include "rgw_sync.h"
#include "rgw_orphan.h"
#include "rgw_bucket_sync.h"
#include "rgw_tools.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

void decode_json_obj(rgw_placement_rule& v, JSONObj *obj)
{
  string s;
  decode_json_obj(s, obj);
  v.from_str(s);
}

