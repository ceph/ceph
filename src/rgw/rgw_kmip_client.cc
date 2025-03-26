// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
  if (done)
    return ret;

  // TODO: when given a coroutine yield context, suspend instead of blocking
  maybe_warn_about_blocking(dpp);

  std::unique_lock l{lock};
  if (!done)
    cond.wait(l);
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
