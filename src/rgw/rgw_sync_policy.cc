

#include "rgw_common.h"
#include "rgw_sync_policy.h"

#define dout_subsys ceph_subsys_rgw


string rgw_sync_bucket_entity::bucket_key() const
{
  return rgw_sync_bucket_entities::bucket_key(bucket);
}

bool rgw_sync_pipe_filter_tag::from_str(const string& s)
{
  if (s.empty()) {
    return false;
  }

  auto pos = s.find('=');
  if (pos == string::npos) {
    key = s;
    return true;
  }

  key = s.substr(0, pos);
  if (pos < s.size() - 1) {
    value = s.substr(pos + 1);
  }

  return true;
}

bool rgw_sync_pipe_filter_tag::operator==(const string& s) const
{
  if (s.empty()) {
    return false;
  }

  auto pos = s.find('=');
  if (pos == string::npos) {
    return value.empty() && (s == key);
  }

  return s.compare(0, pos, s) == 0 &&
         s.compare(pos + 1, s.size() - pos - 1, value) == 0;
}

void rgw_sync_pipe_filter::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(prefix, bl);
  encode(tags, bl);
  ENCODE_FINISH(bl);
}

void rgw_sync_pipe_filter::decode(bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(prefix, bl);
  decode(tags, bl);
  DECODE_FINISH(bl);
}

void rgw_sync_pipe_filter::set_prefix(std::optional<std::string> opt_prefix,
                                      bool prefix_rm)
{
  if (opt_prefix) {
    prefix = *opt_prefix;    
  } else if (prefix_rm) {
    prefix.reset();
  }
}

void rgw_sync_pipe_filter::set_tags(std::list<std::string>& tags_add,
                                    std::list<std::string>& tags_rm)
{
  for (auto& t : tags_rm) {
    rgw_sync_pipe_filter_tag tag;
    if (tag.from_str(t)) {
      tags.erase(tag);
    }
  }

  for (auto& t : tags_add) {
    rgw_sync_pipe_filter_tag tag;
    if (tag.from_str(t)) {
      tags.insert(tag);
    }
  }
}

bool rgw_sync_pipe_filter::is_subset_of(const rgw_sync_pipe_filter& f) const
{
  if (f.prefix) {
    if (!prefix) {
      return false;
    }
    /* f.prefix exists, and this->prefix is either equal or bigger,
     * therefore this->prefix also set */

    if (!boost::starts_with(*prefix, *f.prefix)) {
      return false;
    }
  }

  /* prefix is subset, now check tags. All our tags should exist in f.tags */

  for (auto& t : tags) {
    if (f.tags.find(t) == f.tags.end()) {
      return false;
    }
  }

  return true;
}

bool rgw_sync_pipe_filter::check_tag(const string& s) const
{
  if (tags.empty()) { /* tag filter wasn't defined */
    return true;
  }

  auto iter = tags.find(rgw_sync_pipe_filter_tag(s));
  return (iter != tags.end());
}

bool rgw_sync_pipe_filter::check_tag(const string& k, const string& v) const
{
  if (tags.empty()) { /* tag filter wasn't defined */
    return true;
  }

  auto iter = tags.find(rgw_sync_pipe_filter_tag(k, v));
  return (iter != tags.end());
}

bool rgw_sync_pipe_filter::has_tags() const
{
  return !tags.empty();
}

bool rgw_sync_pipe_filter::check_tags(const std::vector<string>& _tags) const
{
  if (tags.empty()) {
    return true;
  }

  for (auto& t : _tags) {
    if (check_tag(t)) {
      return true;
    }
  }
  return false;
}

bool rgw_sync_pipe_filter::check_tags(const RGWObjTags::tag_map_t& _tags) const
{
  if (tags.empty()) {
    return true;
  }

  for (auto& item : _tags) {
    if (check_tag(item.first, item.second)) {
      return true;
    }
  }
  return false;
}

void rgw_sync_bucket_entity::apply_bucket(std::optional<rgw_bucket> b)
{
  if (!b) {
    return;
  }

  if (!bucket ||
      bucket->name.empty()) {
    bucket = b;
  }
}

void rgw_sync_bucket_entities::add_zones(const std::vector<rgw_zone_id>& new_zones) {
  for (auto& z : new_zones) {
    if (z == "*") {
      all_zones = true;
      zones.reset();
      return;
    }

    if (!zones) {
      zones.emplace();
    }

    zones->insert(z);

    all_zones = false;
  }
}

std::vector<rgw_sync_bucket_entity> rgw_sync_bucket_entities::expand() const
{
  std::vector<rgw_sync_bucket_entity> result;
  rgw_bucket b = get_bucket();
  if (all_zones) {
    rgw_sync_bucket_entity e;
    e.all_zones = true;
    e.bucket = b;
    result.push_back(e);
    return result;
  }

  if (!zones) {
    return result;
  }

  for (auto& z : *zones) {
    rgw_sync_bucket_entity e;
    e.all_zones = false;
    e.bucket = b;
    e.zone = z;
    result.push_back(e);
  }

  return result;
}

void rgw_sync_bucket_entities::remove_zones(const std::vector<rgw_zone_id>& rm_zones) {
  all_zones = false;

  if (!zones) {
    return;
  }

  for (auto& z : rm_zones) {
    zones->erase(z);
  }
}

static void set_bucket_field(std::optional<string> source, string *field) {
  if (!source) {
    return;
  }
  if (source == "*") {
    field->clear();
    return;
  }
  *field = *source;
}

void rgw_sync_bucket_entities::set_bucket(std::optional<string> tenant,
                std::optional<string> bucket_name,
                std::optional<string> bucket_id)
{
  if ((!bucket) && (tenant || bucket_name || bucket_id)) {
    bucket.emplace();
  }

  if (!bucket) {
    return;
  }

  set_bucket_field(tenant, &bucket->tenant);
  set_bucket_field(bucket_name, &bucket->name);
  set_bucket_field(bucket_id, &bucket->bucket_id);

  if (bucket->tenant.empty() &&
      bucket->name.empty() &&
      bucket->bucket_id.empty()) {
    bucket.reset();
  }
}

void rgw_sync_bucket_entities::remove_bucket(std::optional<string> tenant,
                                           std::optional<string> bucket_name,
                                           std::optional<string> bucket_id)
{
  if (!bucket) {
    return;
  }

  if (tenant) {
    bucket->tenant.clear();
  }
  if (bucket_name) {
    bucket->name.clear();
  }
  if (bucket_id) {
    bucket->bucket_id.clear();
  }

  if (bucket->tenant.empty() &&
      bucket->name.empty() &&
      bucket->bucket_id.empty()) {
    bucket.reset();
  }
}


string rgw_sync_bucket_entities::bucket_key(std::optional<rgw_bucket> b)
{
  if (!b) {
    return string("*");
  }

  rgw_bucket _b = *b;

  if (_b.name.empty()) {
    _b.name = "*";
  }

  return _b.get_key();
}

std::vector<rgw_sync_bucket_pipe> rgw_sync_bucket_pipes::expand() const
{
  std::vector<rgw_sync_bucket_pipe> result;

  auto sources = source.expand();
  auto dests = dest.expand();

  for (auto& s : sources) {
    for (auto& d : dests) {
      rgw_sync_bucket_pipe pipe;
      pipe.id = id;
      pipe.source = s;
      pipe.dest = d;
      pipe.params = params;
      result.push_back(pipe);
    }
  }

  return result;
}


void rgw_sync_bucket_pipes::get_potential_related_buckets(const rgw_bucket& bucket,
                                                          std::set<rgw_bucket> *sources,
                                                          std::set<rgw_bucket> *dests) const
{
  if (dest.match_bucket(bucket)) {
    auto expanded_sources = source.expand();

    for (auto& s : expanded_sources) {
      if (s.bucket && !s.bucket->name.empty()) {
        sources->insert(*s.bucket);
      }
    }
  }

  if (source.match_bucket(bucket)) {
    auto expanded_dests = dest.expand();

    for (auto& d : expanded_dests) {
      if (d.bucket && !d.bucket->name.empty()) {
        dests->insert(*d.bucket);
      }
    }
  }
}

bool rgw_sync_data_flow_group::find_or_create_symmetrical(const string& flow_id, rgw_sync_symmetric_group **flow_group)
{
  for (auto& group : symmetrical) {
    if (flow_id == group.id) {
      *flow_group = &group;
      return true;
    }
  }

  auto& group = symmetrical.emplace_back();
  *flow_group = &group;
  (*flow_group)->id = flow_id;
  return true;
}

void rgw_sync_data_flow_group::remove_symmetrical(const string& flow_id, std::optional<std::vector<rgw_zone_id> > zones)
{
  if (symmetrical.empty()) {
    return;
  }

  auto& groups = symmetrical;

  auto iter = groups.begin();

  for (; iter != groups.end(); ++iter) {
    if (iter->id == flow_id) {
      if (!zones) {
        groups.erase(iter);
        if (groups.empty()) {
          symmetrical.clear();
        }
        return;
      }
      break;
    }
  }

  if (iter == groups.end()) {
    return;
  }

  auto& flow_group = *iter;

  for (auto& z : *zones) {
    flow_group.zones.erase(z);
  }

  if (flow_group.zones.empty()) {
    groups.erase(iter);
  }
  if (groups.empty()) {
    symmetrical.clear();
  }
}

bool rgw_sync_data_flow_group::find_or_create_directional(const rgw_zone_id& source_zone, const rgw_zone_id& dest_zone, rgw_sync_directional_rule **flow_group)
{
  for (auto& rule : directional) {
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      *flow_group = &rule;
      return true;
    }
  }

  auto& rule = directional.emplace_back();
  *flow_group = &rule;

  rule.source_zone = source_zone;
  rule.dest_zone = dest_zone;

  return true;
}

void rgw_sync_data_flow_group::remove_directional(const rgw_zone_id& source_zone, const rgw_zone_id& dest_zone)
{
  if (directional.empty()) {
    return;
  }

  for (auto iter = directional.begin(); iter != directional.end(); ++iter) {
    auto& rule = *iter;
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      directional.erase(iter);
      return;
    }
  }
}

void rgw_sync_data_flow_group::init_default(const std::set<rgw_zone_id>& zones)
{
  symmetrical.clear();
  symmetrical.push_back(rgw_sync_symmetric_group("default", zones));
}

bool rgw_sync_policy_group::find_pipe(const string& pipe_id, bool create, rgw_sync_bucket_pipes **pipe)
{
  for (auto& p : pipes) {
    if (pipe_id == p.id) {
      *pipe = &p;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& p = pipes.emplace_back();
  *pipe = &p;
  p.id = pipe_id;

  return true;
}

void rgw_sync_policy_group::remove_pipe(const string& pipe_id)
{
  for (auto iter = pipes.begin(); iter != pipes.end(); ++iter) {
    if (pipe_id == iter->id) {
      pipes.erase(iter);
      return;
    }
  }
}

void rgw_sync_policy_group::get_potential_related_buckets(const rgw_bucket& bucket,
                                                          std::set<rgw_bucket> *sources,
                                                          std::set<rgw_bucket> *dests) const
{
  for (auto& pipe : pipes) {
    pipe.get_potential_related_buckets(bucket, sources, dests);
  }
}

void rgw_sync_policy_info::get_potential_related_buckets(const rgw_bucket& bucket,
                                                         std::set<rgw_bucket> *sources,
                                                         std::set<rgw_bucket> *dests) const
{
  for (auto& entry : groups) {
    auto& group = entry.second;
    group.get_potential_related_buckets(bucket, sources, dests);
  }
}
