// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config_validators.h"
#include "include/stringify.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>

int validate(md_config_t::option_rbd_default_pool_t *,
             std::string *value, std::string *error_message) {
  boost::regex pattern("^[^@/]+$");
  if (!boost::regex_match (*value, pattern)) {
    *value = "rbd";
    *error_message = "invalid RBD default pool, resetting to 'rbd'";
  }
  return 0;
}

int validate(md_config_t::option_rbd_default_data_pool_t *,
             std::string *value, std::string *error_message) {
  boost::regex pattern("^[^@/]*$");
  if (!boost::regex_match (*value, pattern)) {
    *value = "";
    *error_message = "ignoring invalid RBD data pool";
  }
  return 0;
}

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

