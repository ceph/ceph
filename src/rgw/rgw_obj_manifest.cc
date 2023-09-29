// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_obj_manifest.h"

#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "rgw_rados.h" // RGW_OBJ_NS_SHADOW and RGW_OBJ_NS_MULTIPART
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

int RGWObjManifest::append(const DoutPrefixProvider *dpp, RGWObjManifest& m, rgw::sal::Zone* zone_svc)
{
  return append(dpp, m, zone_svc->get_zonegroup(), zone_svc->get_params());
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

void RGWObjManifest::obj_iterator::operator++()
{
  if (manifest->explicit_objs) {
    ++explicit_iter;

    if (explicit_iter == manifest->objs.end()) {
      ofs = manifest->obj_size;
      stripe_size = 0;
      return;
    }

    update_explicit_pos();

    update_location();
    return;
  }

  uint64_t obj_size = manifest->get_obj_size();
  uint64_t head_size = manifest->get_head_size();

  if (ofs == obj_size) {
    return;
  }

  if (manifest->rules.empty()) {
    return;
  }

  /* are we still pointing at the head? */
  if (ofs < head_size) {
    rule_iter = manifest->rules.begin();
    const RGWObjManifestRule *rule = &rule_iter->second;
    ofs = std::min(head_size, obj_size);
    stripe_ofs = ofs;
    cur_stripe = 1;
    stripe_size = std::min(obj_size - ofs, rule->stripe_max_size);
    if (rule->part_size > 0) {
      stripe_size = std::min(stripe_size, rule->part_size);
    }
    update_location();
    return;
  }

  const RGWObjManifestRule *rule = &rule_iter->second;

  stripe_ofs += rule->stripe_max_size;
  cur_stripe++;
  ldpp_dout(dpp, 20) << "RGWObjManifest::operator++(): rule->part_size=" << rule->part_size << " rules.size()=" << manifest->rules.size() << dendl;

  if (rule->part_size > 0) {
    /* multi part, multi stripes object */

    ldpp_dout(dpp, 20) << "RGWObjManifest::operator++(): stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;

    if (stripe_ofs >= part_ofs + rule->part_size) {
      /* moved to the next part */
      cur_stripe = 0;
      part_ofs += rule->part_size;
      stripe_ofs = part_ofs;

      bool last_rule = (next_rule_iter == manifest->rules.end());
      /* move to the next rule? */
      if (!last_rule && stripe_ofs >= next_rule_iter->second.start_ofs) {
        rule_iter = next_rule_iter;
        last_rule = (next_rule_iter == manifest->rules.end());
        if (!last_rule) {
          ++next_rule_iter;
        }
        cur_part_id = rule_iter->second.start_part_num;
      } else {
        cur_part_id++;
      }

      rule = &rule_iter->second;
    }

    stripe_size = std::min(rule->part_size - (stripe_ofs - part_ofs), rule->stripe_max_size);
  }

  cur_override_prefix = rule->override_prefix;

  ofs = stripe_ofs;
  if (ofs > obj_size) {
    ofs = obj_size;
    stripe_ofs = ofs;
    stripe_size = 0;
  }

  ldpp_dout(dpp, 20) << "RGWObjManifest::operator++(): result: ofs=" << ofs << " stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;
  update_location();
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

  const RGWObjManifestRule& rule = rule_iter->second;

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

  auto next_iter = explicit_iter;
  ++next_iter;
  if (next_iter != manifest->objs.end()) {
    stripe_size = next_iter->first - ofs;
  } else {
    stripe_size = manifest->obj_size - ofs;
  }
}

void RGWObjManifest::get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe,
                                           uint64_t ofs, string *override_prefix, rgw_obj_select *location) const
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
      ns = RGW_OBJ_NS_SHADOW;
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
      ns = RGW_OBJ_NS_SHADOW;
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
