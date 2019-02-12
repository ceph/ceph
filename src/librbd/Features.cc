// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "librbd/Features.h"
#include "include/rbd/features.h"

#include <map>
#include <vector>

static const std::map<std::string, uint64_t> RBD_FEATURE_MAP = {
  {RBD_FEATURE_NAME_LAYERING, RBD_FEATURE_LAYERING},
  {RBD_FEATURE_NAME_STRIPINGV2, RBD_FEATURE_STRIPINGV2},
  {RBD_FEATURE_NAME_EXCLUSIVE_LOCK, RBD_FEATURE_EXCLUSIVE_LOCK},
  {RBD_FEATURE_NAME_OBJECT_MAP, RBD_FEATURE_OBJECT_MAP},
  {RBD_FEATURE_NAME_FAST_DIFF, RBD_FEATURE_FAST_DIFF},
  {RBD_FEATURE_NAME_DEEP_FLATTEN, RBD_FEATURE_DEEP_FLATTEN},
  {RBD_FEATURE_NAME_JOURNALING, RBD_FEATURE_JOURNALING},
  {RBD_FEATURE_NAME_DATA_POOL, RBD_FEATURE_DATA_POOL},
  {RBD_FEATURE_NAME_OPERATIONS, RBD_FEATURE_OPERATIONS},
  {RBD_FEATURE_NAME_MIGRATING, RBD_FEATURE_MIGRATING},
};
static_assert((RBD_FEATURE_MIGRATING << 1) > RBD_FEATURES_ALL,
	      "new RBD feature added");


namespace librbd {

std::string rbd_features_to_string(uint64_t features,
				   std::ostream *err)
{
  std::string r;
  for (auto& i : RBD_FEATURE_MAP) {
    if (features & i.second) {
      if (r.empty()) {
	r += ",";
      }
      r += i.first;
      features &= ~i.second;
    }
  }
  if (err && features) {
    *err << "ignoring unknown feature mask 0x"
	 << std::hex << features << std::dec;
  }
  return r;
}

uint64_t rbd_features_from_string(const std::string& orig_value,
				  std::ostream *err)
{
  uint64_t features = 0;
  std::string value = orig_value;
  boost::trim(value);

  // empty string means default features
  if (!value.size()) {
    return RBD_FEATURES_DEFAULT;
  }

  try {
    // numeric?
    features = boost::lexical_cast<uint64_t>(value);

    // drop unrecognized bits
    uint64_t unsupported_features = (features & ~RBD_FEATURES_ALL);
    if (unsupported_features != 0ull) {
      features &= RBD_FEATURES_ALL;
      if (err) {
	*err << "ignoring unknown feature mask 0x"
             << std::hex << unsupported_features << std::dec;
      }
    }
    uint64_t internal_features = (features & RBD_FEATURES_INTERNAL);
    if (internal_features != 0ULL) {
      features &= ~RBD_FEATURES_INTERNAL;
      if (err) {
	*err << "ignoring internal feature mask 0x"
	     << std::hex << internal_features;
      }
    }
  } catch (boost::bad_lexical_cast&) {
    // feature name list?
    bool errors = false;
    std::vector<std::string> feature_names;
    boost::split(feature_names, value, boost::is_any_of(","));
    for (auto feature_name: feature_names) {
      boost::trim(feature_name);
      auto feature_it = RBD_FEATURE_MAP.find(feature_name);
      if (feature_it != RBD_FEATURE_MAP.end()) {
	features += feature_it->second;
      } else if (err) {
	if (errors) {
	  *err << ", ";
	} else {
	  errors = true;
	}
	*err << "ignoring unknown feature " << feature_name;
      }
    }
  }
  return features;
}

} // namespace librbd
