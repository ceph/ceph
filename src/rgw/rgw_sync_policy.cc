

#include "rgw_common.h"
#include "rgw_sync_policy.h"

#define dout_subsys ceph_subsys_rgw


string rgw_sync_bucket_entity::bucket_key() const
{
  return rgw_sync_bucket_entities::bucket_key(bucket);
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

void rgw_sync_bucket_entities::add_zones(const std::vector<string>& new_zones) {
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
    return std::move(result);
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

void rgw_sync_bucket_entities::remove_zones(const std::vector<string>& rm_zones) {
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
      pipe.source = std::move(s);
      pipe.dest = std::move(d);
      result.push_back(pipe);
    }
  }

  return result;
}


bool rgw_sync_data_flow_group::find_symmetrical(const string& flow_id, bool create, rgw_sync_symmetric_group **flow_group)
{
  if (!symmetrical) {
    if (!create) {
      return false;
    }
    symmetrical.emplace();
  }

  for (auto& group : *symmetrical) {
    if (flow_id == group.id) {
      *flow_group = &group;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& group = symmetrical->emplace_back();
  *flow_group = &group;
  (*flow_group)->id = flow_id;
  return true;
}

void rgw_sync_data_flow_group::remove_symmetrical(const string& flow_id, std::optional<std::vector<string> > zones)
{
  if (!symmetrical) {
    return;
  }

  auto& groups = *symmetrical;

  auto iter = groups.begin();

  for (; iter != groups.end(); ++iter) {
    if (iter->id == flow_id) {
      if (!zones) {
        groups.erase(iter);
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
}

bool rgw_sync_data_flow_group::find_directional(const string& source_zone, const string& dest_zone, bool create, rgw_sync_directional_rule **flow_group)
{
  if (!directional) {
    if (!create) {
      return false;
    }
    directional.emplace();
  }

  for (auto& rule : *directional) {
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      *flow_group = &rule;
      return true;
    }
  }

  if (!create) {
    return false;
  }

  auto& rule = directional->emplace_back();
  *flow_group = &rule;

  rule.source_zone = source_zone;
  rule.dest_zone = dest_zone;

  return true;
}

void rgw_sync_data_flow_group::remove_directional(const string& source_zone, const string& dest_zone)
{
  if (!directional) {
    return;
  }

  for (auto iter = directional->begin(); iter != directional->end(); ++iter) {
    auto& rule = *iter;
    if (source_zone == rule.source_zone &&
        dest_zone == rule.dest_zone) {
      directional->erase(iter);
      return;
    }
  }
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
