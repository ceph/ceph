// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/seastore_dma_alloc.h"

#include "include/buffer.h"
#ifdef HAVE_SPDK
#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/spdk_dma_buffer.h"
#endif

namespace crimson::os::seastore {

ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_dma_or_page_aligned(
  size_t len)
{
#ifdef HAVE_SPDK
  if (!crimson::common::local_conf().get_val<std::string>(
        "seastore_spdk_transport_id").empty()) {
    return create_spdk_dma(len);
  }
#endif
  return ceph::buffer::create_page_aligned(len);
}

}
