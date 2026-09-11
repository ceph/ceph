// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>
#include <optional>
#include "include/ceph_assert.h"
#include "rgw_crypt_sanitize.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWEnv::init(CephContext *cct)
{
  conf.init(cct);
}

void RGWEnv::set(std::string name, std::string val)
{
  env_map[std::move(name)] = std::move(val);
}

void RGWEnv::set_raw_headers(boost::beast::http::fields headers)
{
  raw_headers = std::move(headers);
}

// offset of the first non-tchar byte (RFC 7230), or npos if none. spelled
// out rather than isalnum(), which is locale-dependent past ASCII
static size_t find_non_token(std::string_view s)
{
  static constexpr std::string_view tchar_punct{"!#$%&'*+-.^_`|~"};
  for (size_t i = 0; i < s.size(); ++i) {
    const unsigned char c = s[i];
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') ||
          tchar_punct.find(c) != std::string_view::npos)) {
      return i;
    }
  }
  return std::string_view::npos;
}

void RGWEnv::set_header(std::string_view name, std::string_view val)
{
  // reject non-token names: a caller can pass request-derived bytes here,
  // and the container stores elements as "name: value\r\n" verbatim. don't
  // log the name itself - it may contain CR/LF into a newline-delimited log
  if (name.empty()) {
    dout(10) << "rejecting header with empty name" << dendl;
    return;
  }
  if (const size_t bad = find_non_token(name); bad != std::string_view::npos) {
    dout(10) << "rejecting header with non-token name, length " << name.size()
             << ", first offending byte at offset " << bad << dendl;
    return;
  }
  // beast throws past its own field-size limits; drop instead of crashing a
  // caller with no handler for that
  if (name.size() > boost::beast::http::fields::max_name_size ||
      val.size() > boost::beast::http::fields::max_value_size) {
    dout(10) << "dropping oversized header, name=" << name.size()
             << " value=" << val.size() << " bytes" << dendl;
    return;
  }
  raw_headers.set(name, val);
}

void RGWEnv::remove_header(std::string_view name)
{
  raw_headers.erase(name);
}

std::optional<std::string>
RGWEnv::get_combined_header(std::string_view name) const
{
  const auto range = raw_headers.equal_range(name);
  if (range.first == range.second) {
    return std::nullopt;
  }

  std::string val;
  bool first = true;
  for (auto header = range.first; header != range.second; ++header) {
    if (!first) {
      val.push_back(',');
    }
    first = false;
    val.append(header->value().data(), header->value().size());
  }
  return val;
}

const char *rgw_conf_get(const map<string, string, ltstr_nocase>& conf_map, const char *name, const char *def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  return iter->second.c_str();
}

boost::optional<const std::string&> rgw_conf_get_optional(const map<string, string, ltstr_nocase>& conf_map, const std::string& name)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return boost::none;

  return boost::optional<const std::string&>(iter->second);
}

const char *RGWEnv::get(const char *name, const char *def_val) const
{
  return rgw_conf_get(env_map, name, def_val);
}

boost::optional<const std::string&>
RGWEnv::get_optional(const std::string& name) const
{
  return rgw_conf_get_optional(env_map, name);
}

int rgw_conf_get_int(const map<string, string, ltstr_nocase>& conf_map, const char *name, int def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoi(s);  
}

int RGWEnv::get_int(const char *name, int def_val) const
{
  return rgw_conf_get_int(env_map, name, def_val);
}

bool rgw_conf_get_bool(const map<string, string, ltstr_nocase>& conf_map, const char *name, bool def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return rgw_str_to_bool(s, def_val);
}

bool RGWEnv::get_bool(const char *name, bool def_val)
{
  return rgw_conf_get_bool(env_map, name, def_val);
}

size_t RGWEnv::get_size(const char *name, size_t def_val) const
{
  const auto iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  size_t sz;
  try{
    sz = stoull(iter->second);
  } catch(...){
    /* it is very unlikely that we'll ever encounter out_of_range, but let's
       return the default either way */
    sz = def_val;
  }

  return sz;
}

bool RGWEnv::exists(const char *name) const
{
  return env_map.find(name)!= env_map.end();
}

bool RGWEnv::exists_prefix(const char *prefix) const
{
  if (env_map.empty() || prefix == NULL)
    return false;

  const auto iter = env_map.lower_bound(prefix);
  if (iter == env_map.end())
    return false;

  return (strncmp(iter->first.c_str(), prefix, strlen(prefix)) == 0);
}

void RGWEnv::remove(const char *name)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  if (iter != env_map.end())
    env_map.erase(iter);
}

void RGWConf::init(CephContext *cct)
{
  enable_ops_log = cct->_conf->rgw_enable_ops_log;
  enable_usage_log = cct->_conf->rgw_enable_usage_log;

  defer_to_bucket_acls = 0;  // default
  if (cct->_conf->rgw_defer_to_bucket_acls == "recurse") {
    defer_to_bucket_acls = RGW_DEFER_TO_BUCKET_ACLS_RECURSE;
  } else if (cct->_conf->rgw_defer_to_bucket_acls == "full_control") {
    defer_to_bucket_acls = RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL;
  }
}
