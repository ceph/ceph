// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/variant.hpp>

//#include "include/uuid.h"
//#include "msg/msg_types.h"

struct Option {
  enum type_t {
    TYPE_INT,
    TYPE_STR,
    TYPE_FLOAT,
    TYPE_BOOL,
    TYPE_ADDR,
    TYPE_UUID,
  };

  const char *type_to_str(type_t t) {
    switch (t) {
    case TYPE_INT: return "int64_t";
    case TYPE_STR: return "std::string";
    case TYPE_FLOAT: return "double";
    case TYPE_BOOL: return "bool";
    case TYPE_ADDR: return "entity_addr_t";
    case TYPE_UUID: return "uuid_d";
    default: return "unknown";
    }
  }      

  enum level_t {
    LEVEL_BASIC,
    LEVEL_ADVANCED,
    LEVEL_DEV,
  };

  using value_t = boost::variant<
    std::string,
    int64_t,
    double,
    bool/*,
    entity_addr_t,
    uuid_d*/>;
  const std::string name;
  const type_t type;
  const level_t level;

  std::string desc;
  std::string long_desc;

  value_t value;
  value_t daemon_value;
  value_t nondaemon_value;

  std::list<std::string> tags;
  std::list<std::string> see_also;

  value_t min, max;
  std::list<std::string> enum_allowed;

  bool safe;

  Option(const char* name, type_t t, level_t l)
    : name(name), type(t), level(l), safe(false)
  {}

  // bool is an integer, but we don't think so. teach it the hard way.
  template<typename T>
  using is_not_integer = std::enable_if<!std::is_integral<T>::value ||
					std::is_same<T, bool>::value, int>;
  template<typename T>
  using is_integer = std::enable_if<std::is_integral<T>::value &&
				    !std::is_same<T, bool>::value, int>;
  template<typename T, typename is_not_integer<T>::type = 0>
  Option& set_value(value_t& v, const T& new_value) {
    v = new_value;
    return *this;
  }
  template<typename T, typename is_integer<T>::type = 0>
  Option& set_value(value_t& v, T new_value) {
    v = int64_t(new_value);
    return *this;
  }
  template<typename T>
  Option& set_default(const T& v) {
    return set_value(value, v);
  }
  template<typename T>
  Option& set_daemon_default(const T& v) {
    return set_value(daemon_value, v);
  }
  template<typename T>
  Option& set_nondaemon_default(const T& v) {
    return set_value(nondaemon_value, v);
  }
  Option& add_tag(const char* t) {
    tags.push_back(t);
    return *this;
  }
  Option& add_see_also(const char* t) {
    see_also.push_back(t);
    return *this;
  }
  Option& set_description(const char* new_desc) {
    desc = new_desc;
    return *this;
  }
  Option& set_long_description(const char* new_desc) {
    desc = new_desc;
    return *this;
  }
  template<typename T>
  Option& set_min_max(const T& mi, const T& ma) {
    min = mi;
    max = ma;
    return *this;
  }

  Option &set_safe() {
    safe = true;
    return *this;
  }

  /**
   * A crude indicator of whether the value may be
   * modified safely at runtime -- should be replaced
   * with proper locking!
   */
  bool is_safe() const
  {
    return type == TYPE_INT || type == TYPE_FLOAT;
  }
};

// TODO: reinstate corner case logic for these RBD settings
#if 0
#include "include/stringify.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>


class RbdDefaultPool : public Option
{
  int validate(md_config_t::option_rbd_default_pool_t *,
               std::string *value, std::string *error_message) {
    boost::regex pattern("^[^@/]+$");
    if (!boost::regex_match (*value, pattern)) {
      *value = "rbd";
      *error_message = "invalid RBD default pool, resetting to 'rbd'";
    }
    return 0;
  }
}

class RbdDefaultDataPool : public Option
{
  int validate(md_config_t::option_rbd_default_data_pool_t *,
               std::string *value, std::string *error_message) {
    boost::regex pattern("^[^@/]*$");
    if (!boost::regex_match (*value, pattern)) {
      *value = "";
      *error_message = "ignoring invalid RBD data pool";
    }
    return 0;
  }
}

class RbdDefaultFeatures : public Option
{
  int validate(md_config_t::option_rbd_default_features_t *,
               std::string *value, std::string *error_message) {
    static const std::map<std::string, uint64_t> FEATURE_MAP = {
      {RBD_FEATURE_NAME_LAYERING, RBD_FEATURE_LAYERING},
      {RBD_FEATURE_NAME_STRIPINGV2, RBD_FEATURE_STRIPINGV2},
      {RBD_FEATURE_NAME_EXCLUSIVE_LOCK, RBD_FEATURE_EXCLUSIVE_LOCK},
      {RBD_FEATURE_NAME_OBJECT_MAP, RBD_FEATURE_OBJECT_MAP},
      {RBD_FEATURE_NAME_FAST_DIFF, RBD_FEATURE_FAST_DIFF},
      {RBD_FEATURE_NAME_DEEP_FLATTEN, RBD_FEATURE_DEEP_FLATTEN},
      {RBD_FEATURE_NAME_JOURNALING, RBD_FEATURE_JOURNALING},
      {RBD_FEATURE_NAME_DATA_POOL, RBD_FEATURE_DATA_POOL},
    };
    static_assert((RBD_FEATURE_DATA_POOL << 1) > RBD_FEATURES_ALL,
                  "new RBD feature added");

    // convert user-friendly comma delimited feature name list to a bitmask
    // that is used by the librbd API
    uint64_t features = 0;
    error_message->clear();

    try {
      features = boost::lexical_cast<decltype(features)>(*value);

      uint64_t unsupported_features = (features & ~RBD_FEATURES_ALL);
      if (unsupported_features != 0ull) {
        features &= RBD_FEATURES_ALL;

        std::stringstream ss;
        ss << "ignoring unknown feature mask 0x"
           << std::hex << unsupported_features;
        *error_message = ss.str();
      }
    } catch (const boost::bad_lexical_cast& ) {
      int r = 0;
      std::vector<std::string> feature_names;
      boost::split(feature_names, *value, boost::is_any_of(","));
      for (auto feature_name: feature_names) {
        boost::trim(feature_name);
        auto feature_it = FEATURE_MAP.find(feature_name);
        if (feature_it != FEATURE_MAP.end()) {
          features += feature_it->second;
        } else {
          if (!error_message->empty()) {
            *error_message += ", ";
          }
          *error_message += "ignoring unknown feature " + feature_name;
          r = -EINVAL;
        }
      }

      if (features == 0 && r == -EINVAL) {
        features = RBD_FEATURES_DEFAULT;
      }
    }
    *value = stringify(features);
    return 0;
  }

}

#endif

extern const std::vector<Option> ceph_options;

