// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/osd/MockPGBackendListener.h"
#include "test/osd/PGBackendTestFixture.h"

// get_tid() is defined here rather than inline in the header so that
// PGBackendTestFixture is fully defined at the point of the call.
// This mirrors PrimaryLogPG::get_tid() { return osd->get_tid(); } — the
// listener delegates to its owning fixture (the "OSD"), which holds the
// single process-wide TID counter shared by all callers.
ceph_tid_t MockPGBackendListener::get_tid()
{
  if (osd) {
    return osd->get_tid();
  }
  // Listener not wired to a fixture — return 0 so callers that rely on
  // unique TIDs will assert immediately rather than silently misbehave.
  return 0;
}
