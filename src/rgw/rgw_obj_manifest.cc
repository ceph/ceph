// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_obj_manifest.h"

#include "rgw_rados.h" // RGW_OBJ_NS_SHADOW and RGW_OBJ_NS_MULTIPART

using namespace std;

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

