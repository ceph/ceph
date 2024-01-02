#include "common/StackStringStream.h"

// for whatever reason, having these functions in a separate compilation unit
// prevent a double free exception in execuatbles that link to the common lib
// on a macOs >= 14

CachedStackStringStream::CachedStackStringStream()
{
  if (cache.destructed || cache.c.empty()) {
    osp = std::make_unique<sss>();
  } else {
    osp = std::move(cache.c.back());
    cache.c.pop_back();
    osp->reset();
  }
}

CachedStackStringStream::~CachedStackStringStream()
{
  if (!cache.destructed && cache.c.size() < max_elems) {
    cache.c.emplace_back(std::move(osp));
  }
}
