// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph distributed storage system
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_LRC_LAYERS_H
#define CEPH_ERASURE_CODE_LRC_LAYERS_H

/*! @file ErasureCodeLrcLayers.h
    @brief Shared helpers for the LRC sub-plugin profile key.

    These symbols are header-only so the mon, the OSD and the LRC plugin can
    all use them without linking the (dlopen'd) plugin.
 */

#include <cerrno>
#include <ostream>
#include <string>
#include "erasure-code/ErasureCodeInterface.h"

namespace ceph {

inline const std::string LRC_LAYER_PLUGIN_KEY = "layer-plugin";

inline const std::string LRC_LAYER_PLUGIN_DEFAULT = "isa";

inline bool is_lrc_profile(const ErasureCodeProfile &profile)
{
  auto plugin = profile.find("plugin");
  return plugin != profile.end() && plugin->second == "lrc";
}

inline bool lrc_profile_missing_layer_plugin(const ErasureCodeProfile &profile)
{
  return is_lrc_profile(profile) && profile.count(LRC_LAYER_PLUGIN_KEY) == 0;
}

inline bool is_valid_lrc_layer_plugin(const std::string &plugin)
{
  return plugin == "jerasure" || plugin == "isa";
}

// True if layer-plugin unset or not valid (e.g. a typo stored by a release that ignored the key)
inline bool lrc_profile_layer_plugin_unusable(const ErasureCodeProfile &profile)
{
  if (!is_lrc_profile(profile))
    return false;
  auto lp = profile.find(LRC_LAYER_PLUGIN_KEY);
  return lp == profile.end() || !is_valid_lrc_layer_plugin(lp->second);
}

// Describe why this profile is unusable
inline std::string lrc_layer_plugin_problem(const ErasureCodeProfile &profile)
{
  auto lp = profile.find(LRC_LAYER_PLUGIN_KEY);
  if (lp == profile.end())
    return "does not set '" + LRC_LAYER_PLUGIN_KEY + "'";
  return "sets an invalid '" + LRC_LAYER_PLUGIN_KEY + "' value '" +
    lp->second + "' (must be jerasure or isa)";
}

inline bool set_lrc_layer_plugin_default(ErasureCodeProfile &profile)
{
  if (!lrc_profile_missing_layer_plugin(profile))
    return false;
  profile[LRC_LAYER_PLUGIN_KEY] = LRC_LAYER_PLUGIN_DEFAULT;
  return true;
}

inline int resolve_lrc_layer_plugin(const ErasureCodeProfile *existing,
                                    ErasureCodeProfile &proposed,
                                    std::ostream *ss)
{
  auto lp = proposed.find(LRC_LAYER_PLUGIN_KEY);
  if (!is_lrc_profile(proposed)) {
    if (lp != proposed.end()) {
      *ss << "'" << LRC_LAYER_PLUGIN_KEY << "' only applies to the lrc "
          << "plugin";
      return -EINVAL;
    }
    return 0;
  }
  if (lp != proposed.end()) {
    if (!is_valid_lrc_layer_plugin(lp->second)) {
      *ss << "invalid " << LRC_LAYER_PLUGIN_KEY << " '" << lp->second
          << "': must be jerasure or isa";
      return -EINVAL;
    }
    return 0;
  }
  if (existing && is_lrc_profile(*existing)) {
    if (lrc_profile_layer_plugin_unusable(*existing)) {
      *ss << "the existing LRC profile " << lrc_layer_plugin_problem(*existing)
          << ", so the sub-plugin cannot be inferred; specify "
          << LRC_LAYER_PLUGIN_KEY << "=<jerasure|isa> explicitly, using the "
          << "sub-plugin the data was written with, with --force "
          << "--yes-i-really-mean-it. See "
          << "https://tracker.ceph.com/issues/81369.";
      return -EINVAL;
    }
    proposed[LRC_LAYER_PLUGIN_KEY] = existing->at(LRC_LAYER_PLUGIN_KEY);
    return 0;
  }
  proposed[LRC_LAYER_PLUGIN_KEY] = LRC_LAYER_PLUGIN_DEFAULT;
  return 0;
}

}

#endif
