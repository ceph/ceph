// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_obj_manifest.h"

#include "services/svc_zone.h"
#include "rgw_rados.h"
#include "rgw_bucket.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWObjManifest::generator::create_next(uint64_t ofs)
{
  if (ofs < last_ofs) /* only going forward */
    return -EINVAL;

  uint64_t max_head_size = manifest->get_max_head_size();

  if (ofs < max_head_size) {
    manifest->set_head_size(ofs);
  }

  if (ofs >= max_head_size) {
    manifest->set_head_size(max_head_size);
    cur_stripe = (ofs - max_head_size) / rule.stripe_max_size;
    cur_stripe_size = rule.stripe_max_size;

    if (cur_part_id == 0 && max_head_size > 0) {
      cur_stripe++;
    }
  }

  last_ofs = ofs;
  manifest->set_obj_size(ofs);

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, NULL, &cur_obj);

  return 0;
}

int RGWObjManifest::append(const DoutPrefixProvider *dpp, RGWObjManifest& m, const RGWZoneGroup& zonegroup,
                           const RGWZoneParams& zone_params)
{
  if (explicit_objs || m.explicit_objs) {
    return append_explicit(dpp, m, zonegroup, zone_params);
  }

  if (rules.empty()) {
    *this = m;
    return 0;
  }

  string override_prefix;

  if (prefix.empty()) {
    prefix = m.prefix;
  }

  if (prefix != m.prefix) {
    override_prefix = m.prefix;
  }

  map<uint64_t, RGWObjManifestRule>::iterator miter = m.rules.begin();
  if (miter == m.rules.end()) {
    return append_explicit(dpp, m, zonegroup, zone_params);
  }

  for (; miter != m.rules.end(); ++miter) {
    map<uint64_t, RGWObjManifestRule>::reverse_iterator last_rule = rules.rbegin();

    RGWObjManifestRule& rule = last_rule->second;

    if (rule.part_size == 0) {
      rule.part_size = obj_size - rule.start_ofs;
    }

    RGWObjManifestRule& next_rule = miter->second;
    if (!next_rule.part_size) {
      next_rule.part_size = m.obj_size - next_rule.start_ofs;
    }

    string rule_prefix = prefix;
    if (!rule.override_prefix.empty()) {
      rule_prefix = rule.override_prefix;
    }

    string next_rule_prefix = m.prefix;
    if (!next_rule.override_prefix.empty()) {
      next_rule_prefix = next_rule.override_prefix;
    }

    if (rule.part_size != next_rule.part_size ||
        rule.stripe_max_size != next_rule.stripe_max_size ||
        rule_prefix != next_rule_prefix) {
      if (next_rule_prefix != prefix) {
        append_rules(m, miter, &next_rule_prefix);
      } else {
        append_rules(m, miter, NULL);
      }
      break;
    }

    uint64_t expected_part_num = rule.start_part_num + 1;
    if (rule.part_size > 0) {
      expected_part_num = rule.start_part_num + (obj_size + next_rule.start_ofs - rule.start_ofs) / rule.part_size;
    }

    if (expected_part_num != next_rule.start_part_num) {
      append_rules(m, miter, NULL);
      break;
    }
  }

  set_obj_size(obj_size + m.obj_size);

  return 0;
}

void RGWObjManifest::append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& miter,
                                  string *override_prefix)
{
  for (; miter != m.rules.end(); ++miter) {
    RGWObjManifestRule rule = miter->second;
    rule.start_ofs += obj_size;
    if (override_prefix)
      rule.override_prefix = *override_prefix;
    rules[rule.start_ofs] = rule;
  }
}

void RGWObjManifest::convert_to_explicit(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params)
{
  if (explicit_objs) {
    return;
  }
  obj_iterator iter = obj_begin(dpp);

  while (iter != obj_end(dpp)) {
    RGWObjManifestPart& part = objs[iter.get_stripe_ofs()];
    const rgw_obj_select& os = iter.get_location();
    const rgw_raw_obj& raw_loc = os.get_raw_obj(zonegroup, zone_params);
    part.loc_ofs = 0;

    uint64_t ofs = iter.get_stripe_ofs();

    if (ofs == 0) {
      part.loc = obj;
    } else {
      RGWSI_Tier_RADOS::raw_obj_to_obj(tail_placement.bucket, raw_loc, &part.loc);
    }
    ++iter;
    uint64_t next_ofs = iter.get_stripe_ofs();

    part.size = next_ofs - ofs;
  }

  explicit_objs = true;
  rules.clear();
  prefix.clear();
}

int RGWObjManifest::append_explicit(const DoutPrefixProvider *dpp, RGWObjManifest& m, const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params)
{
  if (!explicit_objs) {
    convert_to_explicit(dpp, zonegroup, zone_params);
  }
  if (!m.explicit_objs) {
    m.convert_to_explicit(dpp, zonegroup, zone_params);
  }
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;

  return 0;
}

bool RGWObjManifest::get_rule(uint64_t ofs, RGWObjManifestRule *rule)
{
  if (rules.empty()) {
    return false;
  }

  map<uint64_t, RGWObjManifestRule>::iterator iter = rules.upper_bound(ofs);
  if (iter != rules.begin()) {
    --iter;
  }

  *rule = iter->second;

  return true;
}

auto RGWObjManifest::obj_find_part(const DoutPrefixProvider *dpp,
                                   int part_num) const
    -> obj_iterator
{
  const obj_iterator end = obj_end(dpp);
  if (end.get_cur_part_id() == 0) { // not mulitipart
    return end;
  }

  // linear search over parts/stripes
  for (obj_iterator i = obj_begin(dpp); i != end; ++i) {
    if (i.get_cur_part_id() == part_num) {
      return i;
    }
    if (i.get_cur_part_id() > part_num) {
      return end;
    }
  }
  return end;
}

int RGWObjManifest::generator::create_begin(CephContext *cct, RGWObjManifest *_m,
                                            const rgw_placement_rule& head_placement_rule,
                                            const rgw_placement_rule *tail_placement_rule,
                                            const rgw_bucket& _b, const rgw_obj& _obj)
{
  manifest = _m;

  if (!tail_placement_rule) {
    manifest->set_tail_placement(head_placement_rule, _b);
  } else {
    rgw_placement_rule new_tail_rule = *tail_placement_rule;
    new_tail_rule.inherit_from(head_placement_rule);
    manifest->set_tail_placement(new_tail_rule, _b);
  }

  manifest->set_head(head_placement_rule, _obj, 0);
  last_ofs = 0;

  if (manifest->get_prefix().empty()) {
    char buf[33];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);

    string oid_prefix = ".";
    oid_prefix.append(buf);
    oid_prefix.append("_");

    manifest->set_prefix(oid_prefix);
  }

  bool found = manifest->get_rule(0, &rule);
  if (!found) {
    derr << "ERROR: manifest->get_rule() could not find rule" << dendl;
    return -EIO;
  }

  uint64_t head_size = manifest->get_head_size();

  if (head_size > 0) {
    cur_stripe_size = head_size;
  } else {
    cur_stripe_size = rule.stripe_max_size;
  }
  
  cur_part_id = rule.start_part_num;

  manifest->get_implicit_location(cur_part_id, cur_stripe, 0, NULL, &cur_obj);

  // Normal object which not generated through copy operation 
  manifest->set_tail_instance(_obj.key.instance);

  return 0;
}

void RGWObjManifestPart::generate_test_instances(std::list<RGWObjManifestPart*>& o)
{
  o.push_back(new RGWObjManifestPart);

  RGWObjManifestPart *p = new RGWObjManifestPart;
  rgw_bucket b;
  init_bucket(&b, "tenant", "bucket", ".pool", ".index_pool", "marker_", "12");

  p->loc = rgw_obj(b, "object");
  p->loc_ofs = 512 * 1024;
  p->size = 128 * 1024;
  o.push_back(p);
}

void RGWObjManifest::generate_test_instances(std::list<RGWObjManifest*>& o)
{
  RGWObjManifest *m = new RGWObjManifest;
  map<uint64_t, RGWObjManifestPart> objs;
  uint64_t total_size = 0;
  for (int i = 0; i<10; i++) {
    RGWObjManifestPart p;
    rgw_bucket b;
    init_bucket(&b, "tenant", "bucket", ".pool", ".index_pool", "marker_", "12");
    p.loc = rgw_obj(b, "object");
    p.loc_ofs = 0;
    p.size = 512 * 1024;
    total_size += p.size;
    objs[total_size] = p;
  }
  m->set_explicit(total_size, objs);
  o.push_back(m);
  o.push_back(new RGWObjManifest);
}

void RGWObjManifestPart::dump(Formatter *f) const
{
  f->open_object_section("loc");
  loc.dump(f);
  f->close_section();
  f->dump_unsigned("loc_ofs", loc_ofs);
  f->dump_unsigned("size", size);
}

void RGWObjManifest::obj_iterator::dump(Formatter *f) const
{
  f->dump_unsigned("part_ofs", part_ofs);
  f->dump_unsigned("stripe_ofs", stripe_ofs);
  f->dump_unsigned("ofs", ofs);
  f->dump_unsigned("stripe_size", stripe_size);
  f->dump_int("cur_part_id", cur_part_id);
  f->dump_int("cur_stripe", cur_stripe);
  f->dump_string("cur_override_prefix", cur_override_prefix);
  f->dump_object("location", location);
}

void RGWObjManifest::dump(Formatter *f) const
{
  map<uint64_t, RGWObjManifestPart>::const_iterator iter = objs.begin();
  f->open_array_section("objs");
  for (; iter != objs.end(); ++iter) {
    f->dump_unsigned("ofs", iter->first);
    f->open_object_section("part");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("obj_size", obj_size);
  ::encode_json("explicit_objs", explicit_objs, f);
  ::encode_json("head_size", head_size, f);
  ::encode_json("max_head_size", max_head_size, f);
  ::encode_json("prefix", prefix, f);
  ::encode_json("rules", rules, f);
  ::encode_json("tail_instance", tail_instance, f);
  ::encode_json("tail_placement", tail_placement, f);
  ::encode_json("tier_type", tier_type, f);

  if (tier_type == "cloud-s3") {
    ::encode_json("tier_config", tier_config, f);
  }

  // nullptr being passed into iterators since there
  // is no cct and we aren't doing anything with these
  // iterators that would write do the log
  f->dump_object("begin_iter", obj_begin(nullptr));
  f->dump_object("end_iter", obj_end(nullptr));
}

void RGWObjManifestRule::dump(Formatter *f) const
{
  encode_json("start_part_num", start_part_num, f);
  encode_json("start_ofs", start_ofs, f);
  encode_json("part_size", part_size, f);
  encode_json("stripe_max_size", stripe_max_size, f);
  encode_json("override_prefix", override_prefix, f);
}

void RGWObjManifestRule::generate_test_instances(std::list<RGWObjManifestRule*>& o)
{
  RGWObjManifestRule *r = new RGWObjManifestRule;
  r->start_part_num = 0;
  r->start_ofs = 0;
  r->part_size = 512 * 1024;
  r->stripe_max_size = 512 * 1024 * 1024;
  r->override_prefix = "override_prefix";
  o.push_back(r);
  o.push_back(new RGWObjManifestRule);
}

void rgw_obj_select::dump(Formatter *f) const
{
  f->dump_string("placement_rule", placement_rule.to_str());
  f->dump_object("obj", obj);
  f->dump_object("raw_obj", raw_obj);
  f->dump_bool("is_raw", is_raw);
}

void RGWObjTier::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("tier_placement", tier_placement, f);
  encode_json("is_multipart_upload", is_multipart_upload, f);
}

void RGWObjTier::generate_test_instances(std::list<RGWObjTier*>& o)
{
  RGWObjTier *t = new RGWObjTier;
  t->name = "name";
  std::list<RGWZoneGroupPlacementTier *> tiers;
  RGWZoneGroupPlacementTier::generate_test_instances(tiers);
  for (auto iter = tiers.begin(); iter != tiers.end(); ++iter) {
    t->tier_placement = *(*iter);
  }
  t->is_multipart_upload = true;
  o.push_back(t);
  o.push_back(new RGWObjTier);
}

// returns true on success, false on failure
static bool rgw_get_obj_data_pool(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params,
                                  const rgw_placement_rule& head_placement_rule,
                                  const rgw_obj& obj, rgw_pool *pool)
{
  if (!zone_params.get_head_data_pool(head_placement_rule, obj, pool)) {
    RGWZonePlacementInfo placement;
    if (!zone_params.get_placement(zonegroup.default_placement.name, &placement)) {
      return false;
    }

    if (!obj.in_extra_data) {
      *pool = placement.get_data_pool(zonegroup.default_placement.storage_class);
    } else {
      *pool = placement.get_data_extra_pool();
    }
  }

  return true;
}

static bool rgw_obj_to_raw(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params,
                           const rgw_placement_rule& head_placement_rule,
                           const rgw_obj& obj, rgw_raw_obj *raw_obj)
{
  get_obj_bucket_and_oid_loc(obj, raw_obj->oid, raw_obj->loc);

  return rgw_get_obj_data_pool(zonegroup, zone_params, head_placement_rule, obj, &raw_obj->pool);
}

rgw_raw_obj rgw_obj_select::get_raw_obj(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params) const
{
  if (!is_raw) {
    rgw_raw_obj r;
    rgw_obj_to_raw(zonegroup, zone_params, placement_rule, obj, &r);
    return r;
  }
  return raw_obj;
}

// returns true on success, false on failure
bool RGWRados::get_obj_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool)
{
  return rgw_get_obj_data_pool(svc.zone->get_zonegroup(), svc.zone->get_zone_params(), placement_rule, obj, pool);
}

