// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <system_error>

#include "common/errno.h"

#include "rgw_realm_watcher.h"
#include "rgw_rados.h"

#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_rest.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw realm watcher: ")


// safe callbacks from SafeTimer are unneccessary. reconfigure() can take a long
// time, so we don't want to hold the mutex and block handle_notify() for the
// duration
static constexpr bool USE_SAFE_TIMER_CALLBACKS = false;


RGWRealmWatcher::RGWRealmWatcher(CephContext *cct, RGWRados *&store,
                                 FrontendPauser *frontends)
  : cct(cct),
    store(store),
    frontends(frontends),
    timer(cct, mutex, USE_SAFE_TIMER_CALLBACKS),
    mutex("RGWRealmWatcher"),
    reconfigure_scheduled(false)
{
  // no default realm, nothing to watch
  if (store->realm.get_id().empty()) {
    ldout(cct, 4) << "No realm, disabling dynamic reconfiguration." << dendl;
    return;
  }

  // establish the watch on RGWRealm
  int r = watch_start();
  if (r < 0) {
    lderr(cct) << "Failed to establish a watch on RGWRealm, "
        "disabling dynamic reconfiguration." << dendl;
    return;
  }

  timer.init();
}

RGWRealmWatcher::~RGWRealmWatcher()
{
  watch_stop();
  timer.shutdown();
}


void RGWRealmWatcher::reconfigure()
{
  ldout(cct, 1) << "Pausing frontends for realm update..." << dendl;

  frontends->pause();

  // destroy the existing store
  RGWStoreManager::close_storage(store);
  store = nullptr;

  {
    // allow a new notify to reschedule us. it's important that we do this
    // before we start loading the new realm, or we could miss some updates
    Mutex::Locker lock(mutex);
    reconfigure_scheduled = false;
  }

  while (!store) {
    // recreate and initialize a new store
    store = RGWStoreManager::get_storage(cct,
                                         cct->_conf->rgw_enable_gc_threads,
                                         cct->_conf->rgw_enable_quota_threads,
                                         cct->_conf->rgw_run_sync_thread);

    RGWRados *store_cleanup = nullptr;
    {
      Mutex::Locker lock(mutex);

      // failure to recreate RGWRados is not a recoverable error, but we
      // don't want to assert or abort the entire cluster.  instead, just
      // sleep until we get another notification, and retry until we get
      // a working configuration
      if (store == nullptr) {
        lderr(cct) << "Failed to reinitialize RGWRados after a realm "
            "configuration update. Waiting for a new update." << dendl;

        // sleep until another event is scheduled
        while (!reconfigure_scheduled)
          cond.Wait(mutex);

        ldout(cct, 1) << "Woke up with a new configuration, retrying "
            "RGWRados initialization." << dendl;
      }

      if (reconfigure_scheduled) {
        // cancel the event; we'll handle it now
        reconfigure_scheduled = false;
        timer.cancel_all_events();

        // if we successfully created a store, clean it up outside of the lock,
        // then continue to loop and recreate another
        std::swap(store, store_cleanup);
      }
    }

    if (store_cleanup) {
      ldout(cct, 4) << "Got another notification, restarting RGWRados "
          "initialization." << dendl;

      RGWStoreManager::close_storage(store_cleanup);
    }
  }

  // finish initializing the new store
  rgw_rest_init(cct, store->get_zonegroup());
  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);
  rgw_log_usage_init(cct, store);

  ldout(cct, 1) << "Resuming frontends with new realm configuration." << dendl;

  frontends->resume(store);
}


class RGWRealmWatcher::C_Reconfigure : public Context {
  RGWRealmWatcher *watcher;
 public:
  C_Reconfigure(RGWRealmWatcher *watcher) : watcher(watcher) {}
  void finish(int r) { watcher->reconfigure(); }
};

void RGWRealmWatcher::handle_notify(uint64_t notify_id, uint64_t cookie,
                                    uint64_t notifier_id, bufferlist& bl)
{
  if (cookie != watch_handle)
    return;

  // send an empty notify ack
  bufferlist reply_bl;
  pool_ctx.notify_ack(watch_oid, notify_id, cookie, reply_bl);

  Mutex::Locker lock(mutex);
  if (reconfigure_scheduled) {
    ldout(cct, 4) << "Notification on " << watch_oid << ", reconfiguration "
        "already scheduled" << dendl;
    return;
  }

  reconfigure_scheduled = true;
  cond.SignalOne(); // wake reconfigure() if it blocked on a bad configuration

  // schedule reconfigure() with a delay so we can batch up changes
  auto delay = cct->_conf->rgw_realm_reconfigure_delay;
  timer.add_event_after(delay, new C_Reconfigure(this));

  ldout(cct, 4) << "Notification on " << watch_oid << ", reconfiguration "
      "scheduled in " << delay << 's' << dendl;
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


int RGWRealmWatcher::watch_start()
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
  auto& realm = store->realm;
  auto pool = realm.get_pool_name(cct);
  r = rados.ioctx_create(pool.c_str(), pool_ctx);
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
  assert(!watch_oid.empty());
  int r = pool_ctx.watch2(watch_oid, &watch_handle, this);
  if (r < 0)
    lderr(cct) << "Failed to restart watch on " << watch_oid
        << " with " << cpp_strerror(-r) << dendl;
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
