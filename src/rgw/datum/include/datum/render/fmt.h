// Datum: External fmt integration for explicitly enrolled diagnostic values.
// A formatting adapter above Datum observation machinery.
//
// Please keep this layer independent of native logging contexts and application schema
// enrollment.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_RENDER_FMT_H
 #define CEPH_RGW_DATUM_RENDER_FMT_H

#include "datum/render/display.h"
#include <fmt/format.h>

namespace fmt {

template <::ceph::salad::datum::render::displayable ValueT>
struct formatter<ValueT, char> : ::ceph::salad::datum::render_detail::formatter<ValueT>
{};

} // fmt

#endif // CEPH_RGW_DATUM_RENDER_FMT_H
