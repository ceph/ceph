// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/lowres_clock.hh>

#include "common/ceph_time.h"

namespace std {

ostream& operator<<(ostream& out,
                    const seastar::lowres_system_clock::time_point& t);
ostream& operator<<(ostream& out,
                    const ceph::coarse_real_clock::time_point& t);

}
