// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/pwl/DiscardRequest.h"
#include "librbd/cache/Utils.h"
#include "include/Context.h"

namespace librbd {
namespace cache {
namespace util {

template <typename I>
void discard_cache(I &image_ctx, Context *ctx) {
  cache::pwl::DiscardRequest<I> *req = cache::pwl::DiscardRequest<I>::create(
    image_ctx, ctx);
  req->send();
}

} // namespace util
} // namespace cache
} // namespace librbd

template void librbd::cache::util::discard_cache(
  librbd::ImageCtx &image_ctx, Context *ctx);
