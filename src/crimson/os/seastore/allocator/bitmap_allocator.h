// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/allocator.h"
#include "crimson/os/seastore/allocator/fastbmap_allocator_impl.h"

namespace crimson::os::seastore {

class BitmapAllocator : public Allocator,
  public port::AllocatorLevel02<port::AllocatorLevel01Loose> {
public:
  BitmapAllocator(allocator_spec_t spec);
  ~BitmapAllocator() = default;

  void mark_free(paddr_t paddr, extent_len_t length) final;
  void mark_used(paddr_t paddr, extent_len_t length) final;

  pextent_vec_t allocate(size_t length) final;
  void release(paddr_t offset, extent_len_t length) final;

  uint64_t get_free() const final {
    return get_available();
  }
  const allocator_spec_t &get_spec() const final {
    return spec;
  }
private:
  allocator_spec_t spec;
  uint64_t hint;
};

}
