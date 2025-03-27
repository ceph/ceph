// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ceph_ver.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/PluginRegistry.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/WriteLogImageDispatch.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/Utils.h"
#include "librbd/cache/pwl/DiscardRequest.h"
#include "librbd/cache/pwl/InitRequest.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/plugin/WriteLogImageCache.h"

extern "C" {

const char *__ceph_plugin_version() {
  return CEPH_GIT_NICE_VER;
}

int __ceph_plugin_init(CephContext *cct, const std::string& type,
                       const std::string& name) {
  auto plugin_registry = cct->get_plugin_registry();
  return plugin_registry->add(
    type, name, new librbd::plugin::WriteLogImageCache<librbd::ImageCtx>(cct));
}

} // extern "C"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::plugin::WriteLogImageCache: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace plugin {

template <typename I>
void WriteLogImageCache<I>::init(I* image_ctx, Api<I>& api,
                                 cache::ImageWritebackInterface& image_writeback,
                                 PluginHookPoints& hook_points_list,
                                 Context* on_finish) {
  bool pwl_enabled = librbd::cache::util::is_pwl_enabled(*image_ctx);
  if (!pwl_enabled || !image_ctx->data_ctx.is_valid()) {
    on_finish->complete(0);
    return;
  }

  auto cct = image_ctx->cct;
  ldout(cct, 5) << dendl;

  auto hook_points = std::make_unique<WriteLogImageCache::HookPoints>(
      image_ctx, image_writeback, api);
  hook_points_list.emplace_back(std::move(hook_points));
  
  on_finish->complete(0);
}

template <typename I>
WriteLogImageCache<I>::~WriteLogImageCache() {
}

template <typename I>
WriteLogImageCache<I>::HookPoints::HookPoints(
    I* image_ctx, cache::ImageWritebackInterface& image_writeback,
    plugin::Api<I>& plugin_api)
  : m_image_ctx(image_ctx), m_image_writeback(image_writeback),
    m_plugin_api(plugin_api)
{
}

template <typename I>
WriteLogImageCache<I>::HookPoints::~HookPoints() {
}

template <typename I>
void WriteLogImageCache<I>::HookPoints::acquired_exclusive_lock(
    Context* on_finish) {
  cache::pwl::InitRequest<I> *req = cache::pwl::InitRequest<I>::create(
    *m_image_ctx, m_image_writeback, m_plugin_api, on_finish);
  req->send();
}

template <typename I>
void WriteLogImageCache<I>::HookPoints::prerelease_exclusive_lock(
    Context* on_finish) {
  m_image_ctx->io_image_dispatcher->shut_down_dispatch(
    io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, on_finish);
}

template <typename I>
void WriteLogImageCache<I>::HookPoints::discard(
    Context* on_finish) {
  cache::pwl::DiscardRequest<I> *req = cache::pwl::DiscardRequest<I>::create(
    *m_image_ctx, m_plugin_api, on_finish);
  req->send();
}

} // namespace plugin
} // namespace librbd

template class librbd::plugin::WriteLogImageCache<librbd::ImageCtx>;
