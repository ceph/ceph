// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache/lru.h"
#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

void LRUCachePolicy::remove(CachedExtent &extent, bool need_to_promote) {
  assert(extent.is_clean() && !extent.is_placeholder());

  if (extent.primary_ref_list_hook.is_linked()) {
    lru.erase(lru.s_iterator_to(extent));
    assert(contents >= extent.get_length());
    contents -= extent.get_length();
    if (need_to_promote && cache->promotion_state) {
      cache->promotion_state->maybe_promote(extent);
    }
    intrusive_ptr_release(&extent);
  }
}

}
