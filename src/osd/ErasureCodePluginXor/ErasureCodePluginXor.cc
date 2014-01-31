// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 CERN - Switzerland 
 *
 * Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

// -----------------------------------------------------------------------------

#include "common/debug.h"
#include "ErasureCodeXor.h"
#include "osd/ErasureCodePlugin.h"

// -----------------------------------------------------------------------------

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

// -----------------------------------------------------------------------------

static ostream&
_prefix (std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginXor: ";
}

// -----------------------------------------------------------------------------

class ErasureCodePluginXor : public ErasureCodePlugin
{
public:

  virtual int
  factory (const map<std::string, std::string> &parameters,
           ErasureCodeInterfaceRef *erasure_code)
  {
    ErasureCodeXor *interface = new ErasureCodeXor();
    interface->init(parameters);
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
  }
};

// -----------------------------------------------------------------------------

int
__erasure_code_init (char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginXor());
}
