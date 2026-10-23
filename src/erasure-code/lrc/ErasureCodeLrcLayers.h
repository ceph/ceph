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

#include <string>
#include "erasure-code/ErasureCodeInterface.h"

namespace ceph {

inline const std::string LRC_LAYER_PLUGIN_KEY = "layer-plugin";

inline const std::string LRC_LAYER_PLUGIN_DEFAULT = "isa";

inline bool lrc_profile_missing_layer_plugin(const ErasureCodeProfile &profile)
{
  auto plugin = profile.find("plugin");
  return plugin != profile.end() && plugin->second == "lrc" &&
         profile.count(LRC_LAYER_PLUGIN_KEY) == 0;
}

inline bool set_lrc_layer_plugin_default(ErasureCodeProfile &profile)
{
  if (!lrc_profile_missing_layer_plugin(profile))
    return false;
  profile[LRC_LAYER_PLUGIN_KEY] = LRC_LAYER_PLUGIN_DEFAULT;
  return true;
}

}

#endif
