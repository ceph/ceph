// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "common/Thread.h"
#include "include/compat.h"
#include "common/errno.h"
#include "rgw_asio_thread.h"
#include "rgw_common.h"
#include "rgw_kmip_client.h"

#include <atomic>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGWKMIPManager *rgw_kmip_manager;

int
RGWKMIPTransceiver::wait(const DoutPrefixProvider* dpp, optional_yield y)
{
  /* done is std::atomic<bool> so this load is safe without the lock.
   * The worker sets done under the lock before notify_all(), so the acquire
   * load here pairs with that release store. */
  if (done.load(std::memory_order_acquire))
    return ret;

  /* NOTE: optional_yield y is not yet used to yield the coroutine; the
   * current implementation blocks the calling thread.  A future improvement
   * should post the KMIP work to the asio executor and co_await the result
   * via rgw::run_coro(), removing this blocking wait entirely. */
  maybe_warn_about_blocking(dpp);

  std::unique_lock l{lock};
  cond.wait(l, [this] { return done.load(std::memory_order_relaxed); });

  if (ret) {
    lderr(cct) << "kmip process failed, " << ret << dendl;
  }
  return ret;
}

int
RGWKMIPTransceiver::send()
{
  int r = rgw_kmip_manager->add_request(this);
  if (r < 0) {
    lderr(cct) << "kmip send failed, " << r << dendl;
  }
  return r;
}

int
RGWKMIPTransceiver::process(const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = send();
  if (r < 0)
    return r;
  return wait(dpp, y);
}

RGWKMIPTransceiver::~RGWKMIPTransceiver()
{
  int i;
  if (out)
    free(out);
  out = nullptr;
  if (outlist->strings) {
    for (i = 0; i < outlist->string_count; ++i) {
      free(outlist->strings[i]);
    }
    free(outlist->strings);
    outlist->strings = 0;
  }
  if (outkey->data) {
    ::ceph::crypto::zeroize_for_security(outkey->data, outkey->keylen);
    free(outkey->data);
    outkey->data = 0;
  }
}

void
rgw_kmip_client_init(RGWKMIPManager &m)
{
  rgw_kmip_manager = &m;
  rgw_kmip_manager->start();
}

void
rgw_kmip_client_cleanup()
{
  rgw_kmip_manager->stop();
  delete rgw_kmip_manager;
}
