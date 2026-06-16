// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <new>

#include <spdk/env.h>

#include "include/buffer_raw.h"
#include "include/intarith.h"   // CEPH_PAGE_SIZE

namespace crimson::os::seastore {

/**
 * raw_spdk_dma
 *
 * A buffer::raw backed by SPDK's registered hugepage pool (spdk_dma_zmalloc /
 * spdk_dma_free), so the memory is DMA-safe for direct submission to an NVMe
 * queue pair with no bounce copy.
 *
 * This deliberately lives in the SPDK-guarded crimson tier rather than in
 * libcommon: src/common/buffer.cc is always compiled and must not depend on
 * SPDK. buffer::raw is public, so the subclass is free to live here.
 */
class raw_spdk_dma : public ceph::buffer::raw {
public:
  explicit raw_spdk_dma(unsigned len)
    : raw(len ? static_cast<char*>(spdk_dma_zmalloc(len, CEPH_PAGE_SIZE, nullptr)) : nullptr,
          len) {
    if (len && !data) {
      throw std::bad_alloc();
    }
  }
  ~raw_spdk_dma() override {
    spdk_dma_free(data);
  }
};

inline ceph::unique_leakable_ptr<ceph::buffer::raw> create_spdk_dma(unsigned len)
{
  return ceph::unique_leakable_ptr<ceph::buffer::raw>(new raw_spdk_dma(len));
}

}
