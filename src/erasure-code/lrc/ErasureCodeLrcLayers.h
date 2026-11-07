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
    @brief Shared helper for pinning the LRC sub-plugin in a profile.

    These symbols are header-only so the mon, the OSD and the LRC plugin can
    all use them without linking the (dlopen'd) plugin.
 */

#include <string>
#include "erasure-code/ErasureCodeInterface.h"

namespace ceph {

// Profile key naming the plugin used for any LRC layer that does not pin its
// own plugin
inline const std::string LRC_LAYER_PLUGIN_KEY = "layer-plugin";

inline bool pin_lrc_layer_plugin(ErasureCodeProfile &profile, bool pre_tentacle)
{
  auto plugin = profile.find("plugin");
  if (plugin == profile.end() || plugin->second != "lrc")
    return false;
  if (profile.count(LRC_LAYER_PLUGIN_KEY))
    return false;
  profile[LRC_LAYER_PLUGIN_KEY] = pre_tentacle ? "jerasure" : "isa";
  return true;
}

}

#endif
