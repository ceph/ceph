// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <iostream>
#include <sstream>
#include <string>

#include "cls/user/cls_user_types.h"

#include "rgw_basic_types.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"

using std::string;
using std::stringstream;

void decode_json_obj(rgw_user& val, JSONObj *obj)
{
  val.from_str(obj->get_data());
}

void encode_json(const char *name, const rgw_user& val, Formatter *f)
{
  f->dump_string(name, val.to_str());
}

void encode_xml(const char *name, const rgw_user& val, Formatter *f)
{
  encode_xml(name, val.to_str(), f);
}

rgw_bucket::rgw_bucket(const rgw_user& u, const cls_user_bucket& b) :
    tenant(u.tenant),
    name(b.name),
    marker(b.marker),
    bucket_id(b.bucket_id),
    explicit_placement(b.explicit_placement.data_pool,
                       b.explicit_placement.data_extra_pool,
                       b.explicit_placement.index_pool)
{
}

void rgw_bucket::convert(cls_user_bucket *b) const
{
  b->name = name;
  b->marker = marker;
  b->bucket_id = bucket_id;
  b->explicit_placement.data_pool = explicit_placement.data_pool.to_str();
  b->explicit_placement.data_extra_pool = explicit_placement.data_extra_pool.to_str();
  b->explicit_placement.index_pool = explicit_placement.index_pool.to_str();
}

std::string rgw_bucket::get_key(char tenant_delim, char id_delim, size_t reserve) const
{
  const size_t max_len = tenant.size() + sizeof(tenant_delim) +
      name.size() + sizeof(id_delim) + bucket_id.size() + reserve;

  std::string key;
  key.reserve(max_len);
  if (!tenant.empty() && tenant_delim) {
    key.append(tenant);
    key.append(1, tenant_delim);
  }
  key.append(name);
  if (!bucket_id.empty() && id_delim) {
    key.append(1, id_delim);
    key.append(bucket_id);
  }
  return key;
}

std::string rgw_bucket_shard::get_key(char tenant_delim, char id_delim,
                                      char shard_delim, size_t reserve) const
{
  static constexpr size_t shard_len{12}; // ":4294967295\0"
  auto key = bucket.get_key(tenant_delim, id_delim, reserve + shard_len);
  if (shard_id >= 0 && shard_delim) {
    key.append(1, shard_delim);
    key.append(std::to_string(shard_id));
  }
  return key;
}

void encode(const rgw_bucket_shard& b, bufferlist& bl, uint64_t f)
{
  encode(b.bucket, bl, f);
  encode(b.shard_id, bl, f);
}

void decode(rgw_bucket_shard& b, bufferlist::const_iterator& bl)
{
  decode(b.bucket, bl);
  decode(b.shard_id, bl);
}

void encode_json_impl(const char *name, const rgw_zone_id& zid, Formatter *f)
{
  encode_json(name, zid.id, f);
}

void decode_json_obj(rgw_zone_id& zid, JSONObj *obj)
{
  decode_json_obj(zid.id, obj);
}

namespace rgw {
namespace auth {
ostream& operator <<(ostream& m, const Principal& p) {
  if (p.is_wildcard()) {
    return m << "*";
  }

  m << "arn:aws:iam:" << p.get_tenant() << ":";
  if (p.is_tenant()) {
    return m << "root";
  }
  return m << (p.is_user() ? "user/" : "role/") << p.get_id();
}
}
}
