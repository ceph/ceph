/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


/**
 * @file   ErasureCodePluginIsa.cc
 *
 * @brief  Erasure Code Plug-in class wrapping the INTEL ISA-L library
 *
 * The factory plug-in class allows to call individual encoding techniques.
 * The INTEL ISA-L library provides two pre-defined encoding matrices (cauchy, reed_sol_van = default).
 */

// -----------------------------------------------------------------------------
#include "common/debug.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodePluginIsa.h"
#include "ErasureCodeIsaTableCache.h"
#include "ErasureCodeIsa.h"
// -----------------------------------------------------------------------------
ErasureCodeIsaTableCache ErasureCodePluginIsa::tcache; // singleton table cache

// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)
// -----------------------------------------------------------------------------

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginIsa: ";
}

// -----------------------------------------------------------------------------
int __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();

  return instance.add(plugin_name, new ErasureCodePluginIsa());
}


