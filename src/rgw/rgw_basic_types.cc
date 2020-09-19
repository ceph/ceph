// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <iostream>
#include <sstream>
#include <string>

#include "cls/user/cls_user_types.h"

#include "rgw_basic_types.h"
#include "rgw_xml.h"
#include "common/ceph_json.h"
#include "cls/user/cls_user_types.h"
#include "cls/rgw/cls_rgw_types.h"

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
                                      char shard_delim) const
{
  static constexpr size_t shard_len{12}; // ":4294967295\0"
  auto key = bucket.get_key(tenant_delim, id_delim, shard_len);
  if (shard_id >= 0 && shard_delim) {
    key.append(1, shard_delim);
    key.append(std::to_string(shard_id));
  }
  return key;
}

void encode_json_impl(const char *name, const rgw_zone_id& zid, Formatter *f)
{
  encode_json(name, zid.id, f);
}

void decode_json_obj(rgw_zone_id& zid, JSONObj *obj)
{
  decode_json_obj(zid.id, obj);
}

rgw_obj_key::rgw_obj_key(const rgw_obj_index_key& k) {
  parse_index_key(k.name, &name, &ns);
  instance = k.instance;
}

bool rgw_obj_key::set(const rgw_obj_index_key& index_key) {
  if (!parse_raw_oid(index_key.name, this)) {
    return false;
  }
  instance = index_key.instance;
  return true;
}

void rgw_obj_key::get_index_key(rgw_obj_index_key *key) const {
  key->name = get_index_key_name();
  key->instance = instance;
}

void RGWObjManifest::obj_iterator::seek(uint64_t o)
{
  ofs = o;
  if (manifest->explicit_objs) {
    explicit_iter = manifest->objs.upper_bound(ofs);
    if (explicit_iter != manifest->objs.begin()) {
      --explicit_iter;
    }
    if (ofs < manifest->obj_size) {
      update_explicit_pos();
    } else {
      ofs = manifest->obj_size;
    }
    update_location();
    return;
  }
  if (o < manifest->get_head_size()) {
    rule_iter = manifest->rules.begin();
    stripe_ofs = 0;
    stripe_size = manifest->get_head_size();
    if (rule_iter != manifest->rules.end()) {
      cur_part_id = rule_iter->second.start_part_num;
      cur_override_prefix = rule_iter->second.override_prefix;
    }
    update_location();
    return;
  }

  rule_iter = manifest->rules.upper_bound(ofs);
  next_rule_iter = rule_iter;
  if (rule_iter != manifest->rules.begin()) {
    --rule_iter;
  }

  if (rule_iter == manifest->rules.end()) {
    update_location();
    return;
  }

  RGWObjManifestRule& rule = rule_iter->second;

  if (rule.part_size > 0) {
    cur_part_id = rule.start_part_num + (ofs - rule.start_ofs) / rule.part_size;
  } else {
    cur_part_id = rule.start_part_num;
  }
  part_ofs = rule.start_ofs + (cur_part_id - rule.start_part_num) * rule.part_size;

  if (rule.stripe_max_size > 0) {
    cur_stripe = (ofs - part_ofs) / rule.stripe_max_size;

    stripe_ofs = part_ofs + cur_stripe * rule.stripe_max_size;
    if (!cur_part_id && manifest->get_head_size() > 0) {
      cur_stripe++;
    }
  } else {
    cur_stripe = 0;
    stripe_ofs = part_ofs;
  }

  if (!rule.part_size) {
    stripe_size = rule.stripe_max_size;
    stripe_size = std::min(manifest->get_obj_size() - stripe_ofs, stripe_size);
  } else {
    uint64_t next = std::min(stripe_ofs + rule.stripe_max_size, part_ofs + rule.part_size);
    stripe_size = next - stripe_ofs;
  }

  cur_override_prefix = rule.override_prefix;

  update_location();
}

void RGWObjManifest::obj_iterator::update_location()
{
  if (manifest->explicit_objs) {
    if (manifest->empty()) {
      location = rgw_obj_select{};
    } else {
      location = explicit_iter->second.loc;
    }
    return;
  }

  if (ofs < manifest->get_head_size()) {
    location = manifest->get_obj();
    location.set_placement_rule(manifest->get_head_placement_rule());
    return;
  }

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, &cur_override_prefix, &location);
}

void RGWObjManifest::obj_iterator::update_explicit_pos()
{
  ofs = explicit_iter->first;
  stripe_ofs = ofs;

  map<uint64_t, RGWObjManifestPart>::iterator next_iter = explicit_iter;
  ++next_iter;
  if (next_iter != manifest->objs.end()) {
    stripe_size = next_iter->first - ofs;
  } else {
    stripe_size = manifest->obj_size - ofs;
  }
}

void rgw_pool::from_str(const string& s)
{
  size_t pos = rgw_unescape_str(s, 0, '\\', ':', &name);
  if (pos != string::npos) {
    pos = rgw_unescape_str(s, pos, '\\', ':', &ns);
    /* ignore return; if pos != string::npos it means that we had a colon
     * in the middle of ns that wasn't escaped, we're going to stop there
     */
  }
}

string rgw_pool::to_str() const
{
  string esc_name;
  rgw_escape_str(name, '\\', ':', &esc_name);
  if (ns.empty()) {
    return esc_name;
  }
  string esc_ns;
  rgw_escape_str(ns, '\\', ':', &esc_ns);
  return esc_name + ":" + esc_ns;
}

const static string shadow_ns = "shadow";
const static string RGW_OBJ_NS_MULTIPART = "multipart";

void RGWObjManifest::get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe, uint64_t ofs, string *override_prefix, rgw_obj_select *location)
{
  rgw_obj loc;

  string& oid = loc.key.name;
  string& ns = loc.key.ns;

  if (!override_prefix || override_prefix->empty()) {
    oid = prefix;
  } else {
    oid = *override_prefix;
  }

  if (!cur_part_id) {
    if (ofs < max_head_size) {
      location->set_placement_rule(head_placement_rule);
      *location = obj;
      return;
    } else {
      char buf[16];
      snprintf(buf, sizeof(buf), "%d", (int)cur_stripe);
      oid += buf;
      ns = shadow_ns;
    }
  } else {
    char buf[32];
    if (cur_stripe == 0) {
      snprintf(buf, sizeof(buf), ".%d", (int)cur_part_id);
      oid += buf;
      ns= RGW_OBJ_NS_MULTIPART;
    } else {
      snprintf(buf, sizeof(buf), ".%d_%d", (int)cur_part_id, (int)cur_stripe);
      oid += buf;
      ns = shadow_ns;
    }
  }

  if (!tail_placement.bucket.name.empty()) {
    loc.bucket = tail_placement.bucket;
  } else {
    loc.bucket = obj.bucket;
  }

  // Always overwrite instance with tail_instance
  // to get the right shadow object location
  loc.key.set_instance(tail_instance);

  location->set_placement_rule(tail_placement.placement_rule);
  *location = loc;
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
