// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

// Forward declarations for the ofi-rma library (src/ofi-rma), and
// ceph::ofi, the name Ceph code uses for it.
namespace ofi_rma { class Endpoint; class WindowPool; }
namespace ceph { namespace ofi = ::ofi_rma; }
