// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <string>

#include "common/cmdparse.h"
#include "common/config_fwd.h"

class ConfigValues;
class CrushWrapper;
struct ConfigMap;

// The parameters of a new pool, as ceph osd pool create and ceph osd pool
// default set build them:
//  1. load_pool_defaults(): the osd_pool_default_* options;
//  2. use_profile(): the erasure code profile, a named one or the default;
//  3. apply_command_line(): the parameters given on the command line;
//  4. check_pool_params(): the checks of the values that result.
struct PoolCreateParams {
  int pool_type = 0;  // pg_pool_t::TYPE_*
  int64_t num_zones = 1;
  int64_t replica = 3;  // copies in each zone of a replicated pool
  int64_t size = 0;     // the legacy --size: the copies of a single-zone pool
  int64_t min_size = 0; // 0: the default for the size
  std::string erasure_code_profile;           // a named profile, else empty
  std::map<std::string, std::string> profile; // the erasure code profile
  std::string rule;          // a named rule, else empty for a generated one
  int64_t default_rule = -1; // osd_pool_default_crush_rule
  std::string root;
  std::string zone_failure_domain;
  std::string osd_failure_domain;
  std::string device_class;
  int64_t pg_num = 0;
  int64_t pgp_num = 0;  // 0: pg_num
  std::string autoscale_mode;
  bool bulk = false;
  bool crimson = false;
  std::set<std::string> given;  // the parameters given on the command line

  bool is_given(const std::string& param) const {
    return given.contains(param);
  }
  bool crush_params_given() const;
  bool ec_params_given() const {
    return is_given("k") || is_given("m");
  }
  std::optional<int64_t> k() const;
  std::optional<int64_t> m() const;
  // The copies that a replicated pool keeps in each zone.
  int64_t copies_per_zone() const {
    return is_given("size") ? size : replica;
  }
  // For an error about param: the option that gave its value, unless the
  // command line did.
  std::string from_default(const std::string& param) const;
};

// 1. The defaults, as the osd_pool_default_* options hold them.
PoolCreateParams load_pool_defaults(const ConfigProxy& conf);

// 2. Use the erasure code profile name, or the default profile when name is
// empty. profile is the named profile, or the default profile to use instead
// of osd_pool_default_erasure_code_profile; nullptr if there is none. Fails
// if a named profile does not exist.
int use_profile(PoolCreateParams& p, const std::string& name,
                const std::map<std::string, std::string>* profile,
                std::ostream* ss);

// 3. The parameters given on the command line, over the defaults and the
// profile. --k and --m change the profile.
void apply_command_line(PoolCreateParams& p, const cmdmap_t& cmdmap);

// Which parameters a command line may give together. ceph osd pool create
// describes one pool; ceph osd pool default set describes the defaults of
// every pool.
enum class PoolCommand { CREATE, DEFAULT_SET };
int check_command_line(const PoolCreateParams& p, PoolCommand command,
                       std::ostream* ss);

// What check_pool_params() needs from the cluster.
struct PoolCreateCluster {
  const CrushWrapper *crush = nullptr;
  uint64_t max_pool_pg_num = 65536;
  bool allow_crimson = false;
  // Normalize an erasure code profile with its plugin, or fail with the
  // reason. Not set: the profile is used as it is.
  std::function<int(std::map<std::string, std::string>&, std::ostream*)>
    normalize_profile;
  // Whether pools can be stretched across num_zones buckets of the zone
  // failure domain, or the reason why not. Not set: they can.
  std::function<int(const std::string&, int64_t, std::ostream*)>
    validate_stretch;
};

// 4. The checks of the parameters that result, as a pool of p.pool_type
// would be created with them. Returns 0, or a negative errno with the reason
// in *ss.
int check_pool_params(const PoolCreateParams& p,
                      const PoolCreateCluster& cluster, std::ostream* ss);

// The checks of pool creation defaults p, as ceph osd pool default set makes
// them: those of a pool of the default type, and of each pool type whose own
// parameters p gives.
int check_pool_defaults(const PoolCreateParams& p,
                        const PoolCreateCluster& cluster, std::ostream* ss);

// The zone failure domain that a pool uses: the given one, a named erasure
// code profile's, else the default.
std::string effective_zone_failure_domain(const PoolCreateParams& p);

// The option that ceph osd pool default set writes for each parameter.
const std::map<std::string, std::string>& pool_default_options();

// The options that ceph osd pool default set writes.
std::set<std::string> pool_default_option_names();

// An erasure code profile in the form osd_pool_default_erasure_code_profile
// holds it.
std::string profile_to_string(const std::map<std::string, std::string>& profile);

// Whether a pool size of one may be configured: mon_allow_pool_size_one
// (allowed) must be set and the command must pass --yes-i-really-mean-it
// (sure).
int check_size_one(bool allowed, bool sure, std::ostream* ss);

// The value of param in p, in the form its option (pool_default_options())
// holds it: the rule as its id, and k and m as the whole profile.
std::string pool_default_value(const PoolCreateParams& p,
                               const std::string& param);

// The pool default options that the configuration database sets for
// monitors other than by an unmasked global value, with the section that
// sets them ("mon", "mon.a", or a masked "global/host:x").
std::map<std::string, std::string> db_pool_default_overrides(
  const ConfigMap& config_map);

// The pool default options that this daemon's configuration file, environment
// or command line sets, which the configuration database cannot override.
std::set<std::string> local_pool_default_overrides(const ConfigValues& values);
