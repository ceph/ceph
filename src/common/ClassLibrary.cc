// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ClassVersion.h"
#include "common/debug.h"
#include "include/ClassLibrary.h"

#include "common/config.h"

ClassInfo *ClassVersionMap::get(ClassVersion& ver)
{
  ClassVersion v = ver;
  tClassVersionMap::iterator iter;

  if (ver.is_default()) {
    v.ver = default_ver;
  }
  dout(0) << "ClassVersionMap getting version " << v << " (requested " << ver << ")" << dendl;

  iter = m.find(v);

  if (iter != m.end())
    return &(iter->second);

  return NULL;
}

