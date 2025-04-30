// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef _CEPH_UUID_FMT_H
#define _CEPH_UUID_FMT_H

#include "uuid.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<uuid_d> : fmt::ostream_formatter {};
#endif

#endif
