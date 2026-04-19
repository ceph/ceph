// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef CEPH_COMMON_PLUGIN_H
#define CEPH_COMMON_PLUGIN_H

#include "include/common_fwd.h"

namespace ceph {

  class Plugin {
  public:
    void *library;
    CephContext *cct;

    explicit Plugin(CephContext *cct) : library(nullptr), cct(cct) {}
    virtual ~Plugin() {}
  };

}

#endif
