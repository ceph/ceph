// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_PLUGIN_CLAY_H
#define CEPH_ERASURE_CODE_PLUGIN_CLAY_H

#include "erasure-code/ErasureCodePlugin.h"

class ErasureCodePluginClay : public ceph::ErasureCodePlugin {
public:
  int factory(const std::string& directory,
	      ceph::ErasureCodeProfile &profile,
	      ceph::ErasureCodeInterfaceRef *erasure_code,
	      std::ostream *ss) override;
};

#endif
