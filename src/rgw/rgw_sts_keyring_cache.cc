// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_sts_keyring_cache.h"

#include <condition_variable>
#include <mutex>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/compat.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace STS {

KeyringCache::KeyringCache(CephContext* cct, rgw::sal::Driver* driver,
                           std::chrono::milliseconds interval)
    : cct(cct), driver(driver), interval(interval) {}

KeyringCache::~KeyringCache() { stop(); }

int KeyringCache::fetch(const std::string& key, ceph::bufferlist* bl)
{
  return driver->get_config_key_val(key, bl);
}

void KeyringCache::refresh_keyring()
{
  bufferlist bl;
  int ret = fetch(std::string{rgw::sts::STS_KEYRING_CONFIG_KEY}, &bl);
  if (ret == -ENOENT) {
    // the keyring is gone; drop it so we stop sealing and verifying with it
    if (snapshot.load()) {
      ldout(cct, 1) << "STS keyring " << rgw::sts::STS_KEYRING_CONFIG_KEY
          << " was removed; revoking the cached keyring" << dendl;
    }
    snapshot.store(nullptr);
    keyring_invalid = false;
    return;
  }
  if (ret < 0) {
    // mon error, keep the last keyring
    ldout(cct, 5) << "WARNING: failed to refresh STS keyring: "
        << cpp_strerror(-ret) << dendl;
    return;
  }
  std::string config = bl.to_str();
  bl.zero();
  auto keyring = std::make_shared<rgw::sts::StsKeyring>();
  std::string err;
  ret = rgw::sts::StsKeyring::parse(config, *keyring, err);
  ceph_memzero_s(config.data(), config.size(), config.size());
  if (ret < 0) {
    if (!keyring_invalid) {
      ldout(cct, 0) << "ERROR: STS keyring is invalid, keeping the previous"
          " keyring: " << err << dendl;
      keyring_invalid = true;
    }
    return;
  }
  keyring_invalid = false;
  snapshot.store(KeyringSnapshot{std::move(keyring)});
}

void KeyringCache::refresh_legacy()
{
  bufferlist bl;
  int ret = fetch(std::string{rgw::sts::STS_LEGACY_KEY_CONFIG_KEY}, &bl);
  if (ret == -ENOENT) {
    if (legacy_snapshot.load()) {
      ldout(cct, 1) << "legacy STS key " << rgw::sts::STS_LEGACY_KEY_CONFIG_KEY
          << " was removed; revoking the cached key" << dendl;
    }
    legacy_snapshot.store(nullptr);
    legacy_conflict = false;
    return;
  }
  if (ret < 0) {
    // mon error, keep the last key
    ldout(cct, 5) << "WARNING: failed to refresh the legacy STS key: "
        << cpp_strerror(-ret) << dendl;
    return;
  }
  std::string key = bl.to_str();
  bl.zero();
  rgw::sts::trim_legacy_key(key);
  if (key.empty()) {
    // an empty value revokes the key just like removing it
    legacy_snapshot.store(nullptr);
    legacy_conflict = false;
    return;
  }
  const std::string conf_key = cct->_conf.get_val<std::string>("rgw_sts_key");
  if (!conf_key.empty() && conf_key != key) {
    if (!legacy_conflict) {
      ldout(cct, 0) << "WARNING: rgw_sts_key is set and differs from the"
          " stored legacy STS key; this daemon seals and verifies legacy"
          " tokens with rgw_sts_key" << dendl;
      legacy_conflict = true;
    }
  } else {
    legacy_conflict = false;
  }
  std::shared_ptr<std::string> next{
      new std::string(std::move(key)),
      [](std::string* p) {
        ceph_memzero_s(p->data(), p->size(), p->size());
        delete p;
      }};
  legacy_snapshot.store(LegacyKeySnapshot{std::move(next)});
}

void KeyringCache::refresh()
{
  refresh_keyring();
  refresh_legacy();
}

void KeyringCache::run(std::stop_token stop)
{
  ceph_pthread_setname("sts-keyring");
  std::mutex mutex;
  std::condition_variable cond;
  std::stop_callback on_stop(stop, [&cond]() { cond.notify_all(); });
  while (!stop.stop_requested()) {
    // refresh first so the keys load without blocking daemon startup
    refresh();
    std::unique_lock lock(mutex);
    if (cond.wait_for(lock, interval,
                      [&stop] { return stop.stop_requested(); })) {
      break;
    }
  }
}

void KeyringCache::start()
{
  if (!thread.joinable()) {
    thread = std::jthread([this](std::stop_token stop) { run(std::move(stop)); });
  }
}

void KeyringCache::stop()
{
  if (thread.joinable()) {
    thread.request_stop();
    thread.join();
  }
}

void KeyringCache::pause()
{
  stop();
}

void KeyringCache::resume(rgw::sal::Driver* new_driver)
{
  driver = new_driver;
  start();
}

} // namespace STS
