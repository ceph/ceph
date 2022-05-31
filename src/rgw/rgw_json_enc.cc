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
#include "rgw_account.h"

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

void AccountQuota::dump(Formatter * const f) const
{
  f->open_object_section("AccountQuota");
  f->dump_unsigned("max_users", max_users);
  f->dump_unsigned("max_roles", max_roles);
  f->close_section();
}

void AccountQuota::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("max_users", max_users, obj);
  JSONDecoder::decode_json("max_roles", max_roles, obj);
}

void RGWAccountInfo::dump(Formatter * const f) const
{
  encode_json("id", id, f);
  encode_json("tenant", tenant, f);
  account_quota.dump(f);
}

void RGWAccountInfo::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("quota", account_quota, obj);
}