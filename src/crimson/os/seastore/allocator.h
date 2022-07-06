// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

struct allocator_spec_t {
  uint64_t capacity;
  uint32_t block_size;
  uint32_t min_alloc_size;
  uint32_t max_alloc_size;
  device_id_t device_id;
};
std::ostream& operator<<(std::ostream&, const allocator_spec_t&);

class Allocator {
public:
  virtual void mark_free(paddr_t paddr, extent_len_t length) = 0;

  virtual void mark_used(paddr_t paddr, extent_len_t length) = 0;

  struct pextent_t {
    paddr_t offset;
    extent_len_t length;
  };
  using pextent_vec_t = std::vector<pextent_t>;
  virtual pextent_vec_t allocate(size_t length) = 0;

  virtual void release(paddr_t offset, extent_len_t length) = 0;

  virtual uint64_t get_free() const = 0;
  virtual const allocator_spec_t &get_spec() const = 0;
};
using AllocatorRef = std::unique_ptr<Allocator>;

}
