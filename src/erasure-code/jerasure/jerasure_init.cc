// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "common/debug.h"
#include "jerasure_init.h"

extern "C" {
#include "galois.h"
}

#define dout_context g_ceph_context

extern "C" int jerasure_init(int count, int *words)
{
  for(int i = 0; i < count; i++) {
    int r = galois_init_default_field(words[i]);
    if (r) {
      derr << "failed to galois_init_default_field(" << words[i] << ")" << dendl;
      return -r;
    }
  }
  return 0;
}
