// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>

#include "include/buffer.h"

namespace crimson::os::seastore {

// Allocate a buffer suitable for device I/O: SPDK-DMA-capable hugepage memory
// when seastore is running over an SPDK transport, otherwise plain page-aligned
// memory. The SPDK-active check (seastore_spdk_transport_id) is resolved once.
//
// For call sites that have no device handle (the extent cache and the segment
// state tracker); device-backed paths should use Device::alloc_io_buffer(),
// which lets each device override the allocation. Keeping the SPDK dependency
// behind this declaration keeps spdk_dma_buffer.h (and <spdk/env.h>) out of the
// widely-included cached_extent.h and segment_manager/block.h headers.
ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_dma_or_page_aligned(
  size_t len);

}
