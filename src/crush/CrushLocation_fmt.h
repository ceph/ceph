// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CRUSH_LOCATION_FMT_H
#define CEPH_CRUSH_LOCATION_FMT_H

#include "CrushLocation.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>

template <> struct fmt::formatter<ceph::crush::CrushLocation> : fmt::ostream_formatter {};
#endif

#endif
