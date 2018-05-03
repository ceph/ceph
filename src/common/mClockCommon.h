// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SK Telecom
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "dmclock/src/dmclock_recs.h"

// the following is done to unclobber _ASSERT_H so it returns to the
// way ceph likes it
#include "include/assert.h"


namespace ceph {

namespace dmc = crimson::dmclock;

inline void encode(const dmc::ReqParams &rp,
		   ::ceph::bufferlist& bl,
                   uint64_t features=0) {
  encode(rp.delta, bl);
  encode(rp.rho, bl);
}

inline void decode(dmc::ReqParams &rp,
		   ::ceph::bufferlist::iterator& p) {
  decode(rp.delta, p);
  decode(rp.rho, p);
}

inline void encode(const dmc::PhaseType &phase,
		   ::ceph::bufferlist& bl,
                   uint64_t features=0) {
  encode(static_cast<uint8_t>(phase), bl);
}

inline void decode(dmc::PhaseType &phase,
		   ::ceph::bufferlist::iterator& p) {
  uint8_t int_phase;
  decode((uint8_t&)int_phase, p);
  phase = static_cast<dmc::PhaseType>(int_phase);
}

} // namespace ceph
