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
 * The INTEL ISA-L library provides two pre-defined encoding matrices
 * (cauchy, reed_sol_van = default).
 */

// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "common/debug.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeIsaTableCache.h"
#include "ErasureCodeIsa.h"
// -----------------------------------------------------------------------------

class ErasureCodePluginIsa : public ErasureCodePlugin {
public:
  ErasureCodeIsaTableCache tcache;

  virtual int factory(const map<std::string, std::string> &parameters,
                      ErasureCodeInterfaceRef *erasure_code)
  {
    ErasureCodeIsa *interface;
    std::string t = "reed_sol_van";
    if (parameters.find("technique") != parameters.end())
      t = parameters.find("technique")->second;
    if ((t == "reed_sol_van")) {
      interface = new ErasureCodeIsaDefault(tcache,
                                            ErasureCodeIsaDefault::kVandermonde);
    } else {
      if ((t == "cauchy")) {
        interface = new ErasureCodeIsaDefault(tcache,
                                              ErasureCodeIsaDefault::kCauchy);
      } else {
        derr << "technique=" << t << " is not a valid coding technique. "
          << " Choose one of the following: "
          << "reed_sol_van,"
          << "cauchy" << dendl;
        return -ENOENT;
      }
    }

    interface->init(parameters);
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
  }
};

// -----------------------------------------------------------------------------

const char *__erasure_code_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __erasure_code_init(char *plugin_name, char *directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();

  return instance.add(plugin_name, new ErasureCodePluginIsa());
}
