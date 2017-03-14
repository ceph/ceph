// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Greg Farnum/Red Hat <gfarnum@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * This service abstracts out the specific implementation providing information
 * needed by parts of the Monitor based around PGStats. This'll make for
 * an easier transition from the PGMonitor-based queries where we handle
 * PGStats directly, to where we are getting information passed in from
 * the Ceph Manager.
 *
 * This initial implementation cheats by wrapping a PGMap so we don't need
 * to reimplement everything in one go.
 */

#ifndef CEPH_PGSTATSERVICE_H
#define CEPH_PGSTATSERVICE_H

#include "mon/PGMap.h"

class PGStatService : public PGMap {
  PGMap& parent;
public:
  PGStatService() : PGMap(),
		    parent(*static_cast<PGMap*>(this)) {}
  PGStatService(const PGMap& o) : PGMap(o),
				  parent(*static_cast<PGMap*>(this)) {}
  PGStatService& operator=(const PGMap& o) {
    reset(o);
    return *this;
  }
  void reset(const PGMap& o) {
    parent = o;
  }
};


#endif
