// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

// The libfabric RMA code lives in the ofi-rma library (src/ofi-rma), so
// that clients can lend windows without building Ceph. Ceph code includes
// this header and names the library ceph::ofi.
#include <ofi_rma/ofi_rma.hpp>

#include "common/ofi_rma_fwd.h"
#include "common/rdma_token.h"

// the tokens that ofi-rma writes are the ones that ceph::rdma takes: the
// longest, without its NUL, is not refused, and the tags agree
static_assert(ceph::rdma::RDMA_TOKEN_MAX_LEN >= OFI_RMA_TOKEN_MAX - 1);
static_assert(ceph::rdma::OFI_TOKEN_TAG == std::string_view(OFI_RMA_TOKEN_TAG));
