// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mon/PoolCreateParams.h"

#include <limits>
#include <sstream>

#include "common/config.h"
#include "common/config_proxy.h"
#include "common/config_values.h"
#include "common/strtol.h"
#include "crush/CrushWrapper.h"
#include "include/str_map.h"
#include "mon/ConfigMap.h"
#include "osd/osd_types.h"

using std::string;
using ceph::common::cmd_getval;

namespace {

const char *CRUSH_PARAMS = "crush parameters (crush_root, zone_failure_domain, "
                           "osd_failure_domain, crush_device_class)";

std::optional<int64_t> map_value(const std::map<string, string>& profile,
                                 const string& key)
{
  auto i = profile.find(key);
  if (i == profile.end()) {
    return std::nullopt;
  }
  string err;
  const long long value = strict_strtoll(i->second, 10, &err);
  if (!err.empty()) {
    return std::nullopt;
  }
  return value;
}

} // anonymous namespace

bool PoolCreateParams::crush_params_given() const
{
  return is_given("root") || is_given("zone_failure_domain") ||
         is_given("osd_failure_domain") || is_given("class");
}

std::optional<int64_t> PoolCreateParams::k() const
{
  return map_value(profile, "k");
}

std::optional<int64_t> PoolCreateParams::m() const
{
  return map_value(profile, "m");
}

string PoolCreateParams::from_default(const string& param) const
{
  if (is_given(param)) {
    return "";
  }
  if ((param == "k" || param == "m") && !erasure_code_profile.empty()) {
    return " (erasure code profile '" + erasure_code_profile + "')";
  }
  auto i = pool_default_options().find(param);
  return i == pool_default_options().end() ? "" : " (" + i->second + ")";
}

PoolCreateParams load_pool_defaults(const ConfigProxy& conf)
{
  PoolCreateParams p;
  p.pool_type = conf.get_val<string>("osd_pool_default_type") == "erasure"
    ? pg_pool_t::TYPE_ERASURE : pg_pool_t::TYPE_REPLICATED;
  p.num_zones = conf.get_val<int64_t>("osd_pool_default_num_zones");
  p.replica = conf.get_osd_pool_default_replica();
  p.min_size = conf.get_val<uint64_t>("osd_pool_default_min_size");
  std::ostringstream err;
  get_json_str_map(conf.get_val<string>("osd_pool_default_erasure_code_profile"),
                   err, &p.profile);
  p.default_rule = conf.get_val<int64_t>("osd_pool_default_crush_rule");
  p.zone_failure_domain =
    conf.get_val<string>("osd_pool_default_zone_failure_domain");
  p.osd_failure_domain =
    conf.get_val<string>("osd_pool_default_osd_failure_domain");
  p.root = conf.get_val<string>("osd_pool_default_root");
  p.device_class = conf.get_val<string>("osd_pool_default_class");
  p.pg_num = conf.get_val<uint64_t>("osd_pool_default_pg_num");
  p.pgp_num = conf.get_val<uint64_t>("osd_pool_default_pgp_num");
  p.autoscale_mode = conf.get_val<string>("osd_pool_default_pg_autoscale_mode");
  p.bulk = conf.get_val<bool>("osd_pool_default_flag_bulk");
  p.crimson = conf.get_val<bool>("osd_pool_default_crimson");
  return p;
}

int use_profile(PoolCreateParams& p, const string& name,
                const std::map<string, string>* profile, std::ostream* ss)
{
  if (!name.empty()) {
    if (!profile) {
      *ss << "erasure code profile '" << name << "' does not exist";
      return -ENOENT;
    }
    p.erasure_code_profile = name;
    p.given.insert("erasure_code_profile");
  }
  if (profile) {
    p.profile = *profile;
  }
  return 0;
}

void apply_command_line(PoolCreateParams& p, const cmdmap_t& cmdmap)
{
  string str;
  int64_t n = 0;
  bool b = false;
  auto give = [&p](const char *param) { p.given.insert(param); };
  if (cmd_getval(cmdmap, "pool_type", str) && !str.empty()) {
    p.pool_type = str == "erasure" ? pg_pool_t::TYPE_ERASURE
                                   : pg_pool_t::TYPE_REPLICATED;
    give("pool_type");
  }
  if (cmd_getval(cmdmap, "num_zones", n)) {
    p.num_zones = n;
    give("num_zones");
  }
  if (cmd_getval(cmdmap, "rule", str) && !str.empty()) {
    p.rule = str;
    give("rule");
  }
  for (auto [param, value] :
         {std::pair{"root", &p.root},
          std::pair{"zone_failure_domain", &p.zone_failure_domain},
          std::pair{"osd_failure_domain", &p.osd_failure_domain},
          std::pair{"class", &p.device_class}}) {
    if (cmd_getval(cmdmap, param, str) && !str.empty()) {
      *value = str;
      give(param);
    }
  }
  if (cmd_getval(cmdmap, "replica", n) && n > 0) {
    p.replica = n;
    give("replica");
  }
  if (cmd_getval(cmdmap, "size", n) && n > 0) {
    p.size = n;
    give("size");
  }
  if (cmd_getval(cmdmap, "min_size", n)) {
    p.min_size = n;
    give("min_size");
  }
  for (const char *key : {"k", "m"}) {
    if (cmd_getval(cmdmap, key, n) && n > 0) {
      p.profile[key] = std::to_string(n);
      give(key);
    }
  }
  if (cmd_getval(cmdmap, "pg_num", n) && n > 0) {
    p.pg_num = n;
    give("pg_num");
  }
  if (cmd_getval(cmdmap, "pgp_num", n)) {
    p.pgp_num = n;
    give("pgp_num");
  }
  if (cmd_getval(cmdmap, "autoscale_mode", str) && !str.empty()) {
    p.autoscale_mode = str;
    give("autoscale_mode");
  }
  if (cmd_getval(cmdmap, "bulk", b)) {
    p.bulk = b;
    give("bulk");
  }
  if (cmd_getval(cmdmap, "crimson", b)) {
    p.crimson = b;
    give("crimson");
  }
}

int check_command_line(const PoolCreateParams& p, PoolCommand command,
                       std::ostream* ss)
{
  const bool create = command == PoolCommand::CREATE;
  const bool erasure = p.pool_type == pg_pool_t::TYPE_ERASURE;
  const bool replicated = p.pool_type == pg_pool_t::TYPE_REPLICATED;
  const bool profile_given = p.is_given("erasure_code_profile");

  // a profile defines k, m and the CRUSH parameters
  if (profile_given && p.ec_params_given()) {
    *ss << "cannot specify both erasure_code_profile and k/m parameters";
    return -EINVAL;
  }
  if (create && erasure && p.is_given("k") != p.is_given("m")) {
    *ss << "erasure_code_profile requires both k and m";
    return -EINVAL;
  }
  if (p.is_given("rule") && p.rule != "none" && p.crush_params_given()) {
    *ss << "cannot specify both crush rule and " << CRUSH_PARAMS;
    return -EINVAL;
  }
  if (profile_given && p.crush_params_given()) {
    *ss << "cannot specify both erasure_code_profile and " << CRUSH_PARAMS;
    return -EINVAL;
  }
  // Without k/m the pool uses the shared default profile, which cannot
  // record per-pool crush parameters.
  if (create && erasure && p.crush_params_given() && !p.ec_params_given()) {
    *ss << CRUSH_PARAMS << " require k and m";
    return -EINVAL;
  }
  if (create && replicated && p.ec_params_given()) {
    *ss << "cannot specify k/m parameters for replicated pools";
    return -EINVAL;
  }
  if (create && replicated && p.num_zones == 1 && p.crush_params_given()) {
    *ss << CRUSH_PARAMS << " require num_zones > 1 for a replicated pool";
    return -EINVAL;
  }
  if (p.is_given("size") && p.is_given("replica")) {
    *ss << "cannot specify both 'size' and 'replica' parameters; use 'replica' "
           "and 'num_zones' for new pools";
    return -EINVAL;
  }
  return 0;
}

string effective_zone_failure_domain(const PoolCreateParams& p)
{
  if (!p.is_given("zone_failure_domain") &&
      p.pool_type == pg_pool_t::TYPE_ERASURE && !p.erasure_code_profile.empty()) {
    if (auto i = p.profile.find("crush-zone-failure-domain");
        i != p.profile.end()) {
      return i->second;
    }
  }
  return p.zone_failure_domain;
}

int check_pool_params(const PoolCreateParams& p,
                      const PoolCreateCluster& cluster, std::ostream* ss)
{
  const bool erasure = p.pool_type == pg_pool_t::TYPE_ERASURE;
  const string zone_failure_domain = effective_zone_failure_domain(p);

  if (p.num_zones < 1 || p.num_zones > cluster.max_num_zones) {
    *ss << "num_zones must be from 1 to " << cluster.max_num_zones
        << p.from_default("num_zones");
    return -EINVAL;
  }
  if (!erasure) {
    if (p.is_given("size") && (p.size < 1 || p.size > 10)) {
      *ss << "pool size must be between 1 and 10";
      return -EINVAL;
    }
    if (p.is_given("size") && p.num_zones > 1) {
      *ss << "cannot specify 'size' with num_zones > 1; use 'replica' parameter "
             "instead" << p.from_default("num_zones");
      return -EINVAL;
    }
    if (!p.is_given("size") && (p.replica < 1 || p.replica > 10)) {
      *ss << "replica must be between 1 and 10" << p.from_default("replica");
      return -EINVAL;
    }
    if (p.is_given("min_size") && p.min_size &&
        (p.min_size < 1 || p.min_size > p.copies_per_zone())) {
      *ss << "pool min_size must be between 1 and replica, which is set to "
          << p.copies_per_zone();
      return -EINVAL;
    }
  } else {
    std::map<string, string> profile = p.profile;
    if (cluster.normalize_profile) {
      if (int r = cluster.normalize_profile(profile, ss); r < 0) {
        return r;
      }
    }
    // the LRC plugin checks its own k, m and l, and adds local parity chunks
    const bool lrc = profile.contains("plugin") && profile.at("plugin") == "lrc";
    const auto k = map_value(profile, "k");
    const auto m = map_value(profile, "m");
    if (!lrc) {
      if (!k || !m) {
        *ss << "the erasure code profile has no k or m" << p.from_default("k");
        return -EINVAL;
      }
      if (*k < 2) {
        *ss << "k=" << *k << " must be >= 2" << p.from_default("k");
        return -EINVAL;
      }
      if (*m < 1) {
        *ss << "m=" << *m << " must be >= 1" << p.from_default("m");
        return -EINVAL;
      }
      const int max_k_plus_m =
        std::numeric_limits<decltype(shard_id_t::id)>::max();
      if (*k + *m > max_k_plus_m) {
        *ss << "(k+m)=" << (*k + *m) << " must be <= " << max_k_plus_m
            << p.from_default("k");
        return -EINVAL;
      }
      if (p.num_zones * (*k + *m) > MAX_POOL_SIZE) {
        *ss << "a pool can have at most " << MAX_POOL_SIZE << " OSDs, but "
            << p.num_zones << " zones of k+m=" << (*k + *m) << " need "
            << p.num_zones * (*k + *m) << p.from_default("k");
        return -EINVAL;
      }
      if (p.is_given("min_size") && p.min_size &&
          (p.min_size < *k || p.min_size > *k + *m)) {
        *ss << "pool min_size must be between " << *k << " and " << (*k + *m)
            << " (k + m)";
        return -EINVAL;
      }
    }
  }
  if (p.pg_num < 1 || static_cast<uint64_t>(p.pg_num) > cluster.max_pool_pg_num) {
    *ss << "'pg_num' must be greater than 0 and less than or equal to "
        << cluster.max_pool_pg_num
        << " (you may adjust 'mon max pool pg num' for higher values)"
        << p.from_default("pg_num");
    return -ERANGE;
  }
  if (p.pgp_num > p.pg_num) {
    *ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
        << ", which in this case is " << p.pg_num << p.from_default("pgp_num");
    return -ERANGE;
  }
  if (p.crimson) {
    // crimson-osd needs static pg_num and pgp_num
    const char *suffix = " (--crimson specified or osd_pool_default_crimson set)";
    const string autoscale_mode =
      p.is_given("autoscale_mode") ? p.autoscale_mode : "off";
    if (autoscale_mode != "off") {
      *ss << "crimson-osd does not support changing pg_num or pgp_num, "
          << "pg_autoscale_mode must be set to 'off'" << suffix;
      return -EINVAL;
    }
    if (!cluster.allow_crimson) {
      *ss << "set-allow-crimson must be set to create a pool with the "
          << "crimson flag" << suffix;
      return -EINVAL;
    }
  }
  if (cluster.crush) {
    const CrushWrapper& crush = *cluster.crush;
    // the given CRUSH parameters, and those a generated stretch rule of a
    // replicated pool takes from the defaults
    const bool generated_replicated_rule = !erasure && p.num_zones > 1 &&
      (!p.is_given("rule") || p.rule == "none");
    auto used = [&](const char *param) {
      return p.is_given(param) || generated_replicated_rule;
    };
    for (auto [param, type] :
           {std::pair{"zone_failure_domain", zone_failure_domain},
            std::pair{"osd_failure_domain", p.osd_failure_domain}}) {
      const bool zone_of_stretch_pool =
        string(param) == "zone_failure_domain" && p.num_zones > 1;
      if ((used(param) || zone_of_stretch_pool) &&
          !crush.get_validated_type_id(type)) {
        *ss << "'" << type << "' is not a CRUSH bucket type"
            << p.from_default(param);
        return -EINVAL;
      }
    }
    if (used("root") && !crush.name_exists(p.root)) {
      *ss << "CRUSH root '" << p.root << "' does not exist"
          << p.from_default("root");
      return -ENOENT;
    }
    if (used("class") && !p.device_class.empty() &&
        !crush.class_exists(p.device_class)) {
      *ss << "device class '" << p.device_class << "' does not exist"
          << p.from_default("class");
      return -ENOENT;
    }
  }
  if (p.num_zones > 1 && cluster.validate_stretch) {
    if (int r = cluster.validate_stretch(zone_failure_domain, p.num_zones, ss);
        r < 0) {
      return r;
    }
  }
  return 0;
}

int check_pool_defaults(const PoolCreateParams& p,
                        const PoolCreateCluster& cluster, std::ostream* ss)
{
  std::set<int> types = {p.pool_type};
  if (p.ec_params_given() || p.is_given("erasure_code_profile")) {
    types.insert(pg_pool_t::TYPE_ERASURE);
  }
  if (p.is_given("replica") || p.is_given("size")) {
    types.insert(pg_pool_t::TYPE_REPLICATED);
  }
  for (int type : types) {
    PoolCreateParams pool = p;
    pool.pool_type = type;
    if (int r = check_pool_params(pool, cluster, ss); r < 0) {
      return r;
    }
  }
  return 0;
}

const std::map<string, string>& pool_default_options()
{
  static const std::map<string, string> options = {
    {"pool_type", "osd_pool_default_type"},
    {"num_zones", "osd_pool_default_num_zones"},
    {"rule", "osd_pool_default_crush_rule"},
    {"zone_failure_domain", "osd_pool_default_zone_failure_domain"},
    {"osd_failure_domain", "osd_pool_default_osd_failure_domain"},
    {"root", "osd_pool_default_root"},
    {"class", "osd_pool_default_class"},
    {"replica", "osd_pool_default_replica"},
    {"size", "osd_pool_default_replica"},
    {"min_size", "osd_pool_default_min_size"},
    {"erasure_code_profile", "osd_pool_default_erasure_code_profile"},
    {"k", "osd_pool_default_erasure_code_profile"},
    {"m", "osd_pool_default_erasure_code_profile"},
    {"pg_num", "osd_pool_default_pg_num"},
    {"pgp_num", "osd_pool_default_pgp_num"},
    {"autoscale_mode", "osd_pool_default_pg_autoscale_mode"},
    {"bulk", "osd_pool_default_flag_bulk"},
    {"crimson", "osd_pool_default_crimson"},
  };
  return options;
}

std::set<string> pool_default_option_names()
{
  std::set<string> names;
  for (const auto& [param, option] : pool_default_options()) {
    names.insert(option);
  }
  return names;
}

string profile_to_string(const std::map<string, string>& profile)
{
  std::ostringstream out;
  for (const auto& [key, value] : profile) {
    if (out.tellp() > 0)
      out << " ";
    out << key << "=" << value;
  }
  return out.str();
}

int check_size_one(bool allowed, bool sure, std::ostream* ss)
{
  if (!allowed) {
    *ss << "configuring pool size as 1 is disabled by default.";
    return -EPERM;
  }
  if (!sure) {
    *ss << "WARNING: setting pool size 1 could lead to data loss "
           "without recovery. If you are *ABSOLUTELY CERTAIN* that is what "
           "you want, pass the flag --yes-i-really-mean-it.";
    return -EPERM;
  }
  return 0;
}

string pool_default_value(const PoolCreateParams& p, const string& param)
{
  if (param == "pool_type") {
    return string(pg_pool_t::get_type_name(p.pool_type));
  } else if (param == "num_zones") {
    return std::to_string(p.num_zones);
  } else if (param == "rule") {
    return std::to_string(p.default_rule);
  } else if (param == "zone_failure_domain") {
    return p.zone_failure_domain;
  } else if (param == "osd_failure_domain") {
    return p.osd_failure_domain;
  } else if (param == "root") {
    return p.root;
  } else if (param == "class") {
    return p.device_class;
  } else if (param == "replica" || param == "size") {
    return std::to_string(p.copies_per_zone());
  } else if (param == "min_size") {
    return std::to_string(p.min_size);
  } else if (param == "erasure_code_profile" || param == "k" || param == "m") {
    return profile_to_string(p.profile);
  } else if (param == "pg_num") {
    return std::to_string(p.pg_num);
  } else if (param == "pgp_num") {
    return std::to_string(p.pgp_num);
  } else if (param == "autoscale_mode") {
    return p.autoscale_mode;
  } else if (param == "bulk") {
    return p.bulk ? "true" : "false";
  } else if (param == "crimson") {
    return p.crimson ? "true" : "false";
  }
  return "";
}

std::map<string, string> db_pool_default_overrides(const ConfigMap& config_map)
{
  const auto names = pool_default_option_names();
  std::map<string, string> overrides;
  auto scan = [&](const string& section, const Section& s, bool masked_only) {
    for (const auto& [name, option] : s.options) {
      if (!names.contains(name) || overrides.contains(name))
        continue;
      if (!option.mask.empty()) {
        overrides[name] = section + "/" + option.mask.to_str();
      } else if (!masked_only) {
        overrides[name] = section;
      }
    }
  };
  if (auto i = config_map.by_type.find("mon"); i != config_map.by_type.end()) {
    scan("mon", i->second, false);
  }
  for (const auto& [id, section] : config_map.by_id) {
    if (id.rfind("mon.", 0) == 0)
      scan(id, section, false);
  }
  scan("global", config_map.global, true);
  return overrides;
}

std::set<string> local_pool_default_overrides(const ConfigValues& values)
{
  std::set<string> overrides;
  for (const auto& name : pool_default_option_names()) {
    for (int level = CONF_FILE; level <= CONF_FINAL; ++level) {
      if (values.get_value(name, level).second) {
        overrides.insert(name);
        break;
      }
    }
  }
  return overrides;
}
