// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "include/buffer_fwd.h"
#include "rgw_realm_reloader.h"
#include "rgw_sts_keyring.h"

class CephContext;
namespace rgw::sal { class Driver; }

namespace STS {

using KeyringSnapshot = std::shared_ptr<const rgw::sts::StsKeyring>;
using LegacyKeySnapshot = std::shared_ptr<const std::string>;

/*
 * Caches the token-sealing keyring and the stored legacy key from the mon
 * config-key store. A background thread refreshes the snapshots every interval
 * so requests just read them and never talk to the mon; the blocking reads
 * stay off the shared executor.
 */
class KeyringCache : public RGWRealmReloader::Pauser {
  CephContext* const cct;
  rgw::sal::Driver* driver;
  const std::chrono::milliseconds interval;

  std::atomic<KeyringSnapshot> snapshot;
  std::atomic<LegacyKeySnapshot> legacy_snapshot;
  std::jthread thread;
  // set while the stored keyring won't parse, to log the problem only once
  bool keyring_invalid = false;
  // set while rgw_sts_key overrides a different stored key, to log only once
  bool legacy_conflict = false;

  /*
   * re-read a key and swap its snapshot. a missing key clears it; a mon
   * error leaves the last one in place
   */
  void refresh_keyring();
  void refresh_legacy();
  void run(std::stop_token stop);

 protected:
  // reads one config-key value from the mon; unit tests override this
  virtual int fetch(const std::string& key, ceph::bufferlist* bl);

  // one pass over both keys; the thread repeats it every interval
  void refresh();

 public:
  KeyringCache(CephContext* cct, rgw::sal::Driver* driver,
               std::chrono::milliseconds interval);
  ~KeyringCache() override;
  KeyringCache(const KeyringCache&) = delete;
  KeyringCache& operator=(const KeyringCache&) = delete;

  // refresh the keyring on a background thread until stopped
  void start();
  void stop();

  // realm reload stops the thread while the old driver is torn down, then
  // rebinds to its replacement
  void pause() override;
  void resume(rgw::sal::Driver* new_driver) override;

  // the current keyring, or nullptr when none is loaded
  KeyringSnapshot get() const { return snapshot.load(); }

  // the stored legacy key, or nullptr when none is loaded
  LegacyKeySnapshot get_legacy() const { return legacy_snapshot.load(); }
};

} // namespace STS
