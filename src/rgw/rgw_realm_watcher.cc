// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"

#include "rgw_realm_watcher.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw realm watcher: ")


RGWRealmWatcher::RGWRealmWatcher(CephContext* cct, RGWRealm& realm)
  : cct(cct)
{
  // no default realm, nothing to watch
  if (realm.get_id().empty()) {
    ldout(cct, 4) << "No realm, disabling dynamic reconfiguration." << dendl;
    return;
  }

  // establish the watch on RGWRealm
  int r = watch_start(realm);
  if (r < 0) {
    lderr(cct) << "Failed to establish a watch on RGWRealm, "
        "disabling dynamic reconfiguration." << dendl;
    return;
  }
}

RGWRealmWatcher::~RGWRealmWatcher()
{
  watch_stop();
}

void RGWRealmWatcher::add_watcher(RGWRealmNotify type, Watcher& watcher)
{
  watchers.emplace(type, watcher);
}

void RGWRealmWatcher::handle_notify(uint64_t notify_id, uint64_t cookie,
                                    uint64_t notifier_id, bufferlist& bl)
{
  if (cookie != watch_handle)
    return;

  // send an empty notify ack
  bufferlist reply;
  pool_ctx.notify_ack(watch_oid, notify_id, cookie, reply);

  try {
    auto p = bl.cbegin();
    while (!p.end()) {
      RGWRealmNotify notify;
      decode(notify, p);
      auto watcher = watchers.find(notify);
      if (watcher == watchers.end()) {
        lderr(cct) << "Failed to find a watcher for notify type "
            << static_cast<int>(notify) << dendl;
        break;
      }
      watcher->second.handle_notify(notify, p);
    }
  } catch (const buffer::error &e) {
    lderr(cct) << "Failed to decode realm notifications." << dendl;
  }
}

void RGWRealmWatcher::handle_error(uint64_t cookie, int err)
{
  if (cookie != watch_handle)
    return;

  if (err == -ENOTCONN) {
    ldout(cct, 4) << "Disconnected watch on " << watch_oid << dendl;
    watch_restart();
  }
}

int RGWRealmWatcher::watch_start(RGWRealm& realm)
{
  // initialize a Rados client
  int r = rados.init_with_context(cct);
  if (r < 0) {
    lderr(cct) << "Rados client initialization failed with "
        << cpp_strerror(-r) << dendl;
    return r;
  }
  r = rados.connect();
  if (r < 0) {
    lderr(cct) << "Rados client connection failed with "
        << cpp_strerror(-r) << dendl;
    return r;
  }

  // open an IoCtx for the realm's pool
  rgw_pool pool(realm.get_pool(cct));
  r = rgw_init_ioctx(&rados, pool, pool_ctx);
  if (r < 0) {
    lderr(cct) << "Failed to open pool " << pool
        << " with " << cpp_strerror(-r) << dendl;
    rados.shutdown();
    return r;
  }

  // register a watch on the realm's control object
  auto oid = realm.get_control_oid();
  r = pool_ctx.watch2(oid, &watch_handle, this);
  if (r < 0) {
    lderr(cct) << "Failed to watch " << oid
        << " with " << cpp_strerror(-r) << dendl;
    pool_ctx.close();
    rados.shutdown();
    return r;
  }

  ldout(cct, 10) << "Watching " << oid << dendl;
  std::swap(watch_oid, oid);
  return 0;
}

int RGWRealmWatcher::watch_restart()
{
  ceph_assert(!watch_oid.empty());
  int r = pool_ctx.unwatch2(watch_handle);
  if (r < 0) {
    lderr(cct) << "Failed to unwatch on " << watch_oid
        << " with " << cpp_strerror(-r) << dendl;
  }
  r = pool_ctx.watch2(watch_oid, &watch_handle, this);
  if (r < 0) {
    lderr(cct) << "Failed to restart watch on " << watch_oid
        << " with " << cpp_strerror(-r) << dendl;
    pool_ctx.close();
    watch_oid.clear();
  }
  return r;
}

void RGWRealmWatcher::watch_stop()
{
  if (!watch_oid.empty()) {
    pool_ctx.unwatch2(watch_handle);
    pool_ctx.close();
    watch_oid.clear();
  }
}
