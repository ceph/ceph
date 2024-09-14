// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <iostream>
#include <sstream>
#include <string>

#include "cls/user/cls_user_types.h"

#include "rgw_account.h"
#include "rgw_basic_types.h"
#include "rgw_bucket.h"
#include "rgw_xml.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "cls/user/cls_user_types.h"
#include "cls/rgw/cls_rgw_types.h"

using std::ostream;
using std::string;
using std::stringstream;

using namespace std;

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

void rgw_bucket::generate_test_instances(list<rgw_bucket*>& o)
{
  rgw_bucket *b = new rgw_bucket;
  init_bucket(b, "tenant", "name", "pool", ".index_pool", "marker", "123");
  o.push_back(b);
  o.push_back(new rgw_bucket);
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

void rgw_user::generate_test_instances(list<rgw_user*>& o)
{
  rgw_user *u = new rgw_user("tenant", "user");

  o.push_back(u);
  o.push_back(new rgw_user);
}

void rgw_data_placement_target::dump(Formatter *f) const
{
  encode_json("data_pool", data_pool, f);
  encode_json("data_extra_pool", data_extra_pool, f);
  encode_json("index_pool", index_pool, f);
}

void rgw_data_placement_target::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("data_pool", data_pool, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
  JSONDecoder::decode_json("index_pool", index_pool, obj);
}

void rgw_bucket::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("marker", marker, f);
  encode_json("bucket_id", bucket_id, f);
  encode_json("tenant", tenant, f);
  encode_json("explicit_placement", explicit_placement, f);
}

void rgw_bucket::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("bucket_id", bucket_id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("explicit_placement", explicit_placement, obj);
  if (explicit_placement.data_pool.empty()) {
    /* decoding old format */
    JSONDecoder::decode_json("pool", explicit_placement.data_pool, obj);
    JSONDecoder::decode_json("data_extra_pool", explicit_placement.data_extra_pool, obj);
    JSONDecoder::decode_json("index_pool", explicit_placement.index_pool, obj);
  }
}

namespace rgw {
namespace auth {
ostream& operator <<(ostream& m, const Principal& p) {
  if (p.is_wildcard()) {
    return m << "*";
  }

  m << "arn:aws:iam:" << p.get_account() << ":";
  if (p.is_account()) {
    return m << "root";
  }
  return m << (p.is_user() ? "user/" : "role/") << p.get_id();
}
}
}

// rgw_account_id
void encode_json_impl(const char* name, const rgw_account_id& id, Formatter* f)
{
  f->dump_string(name, id);
}

void decode_json_obj(rgw_account_id& id, JSONObj* obj)
{
  decode_json_obj(static_cast<std::string&>(id), obj);
}

// rgw_owner variant
rgw_owner parse_owner(const std::string& str)
{
  if (rgw::account::validate_id(str)) {
    return rgw_account_id{str};
  } else {
    return rgw_user{str};
  }
}

std::string to_string(const rgw_owner& o)
{
  struct visitor {
    std::string operator()(const rgw_account_id& a) { return a; }
    std::string operator()(const rgw_user& u) { return u.to_str(); }
  };
  return std::visit(visitor{}, o);
}

std::ostream& operator<<(std::ostream& out, const rgw_owner& o)
{
  struct visitor {
    std::ostream& out;
    std::ostream& operator()(const rgw_account_id& a) { return out << a; }
    std::ostream& operator()(const rgw_user& u) { return out << u; }
  };
  return std::visit(visitor{out}, o);
}

void encode_json_impl(const char *name, const rgw_owner& o, ceph::Formatter *f)
{
  encode_json(name, to_string(o), f);
}

void decode_json_obj(rgw_owner& o, JSONObj *obj)
{
  std::string str;
  decode_json_obj(str, obj);
  o = parse_owner(str);
}
