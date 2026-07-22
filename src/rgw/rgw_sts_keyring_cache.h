// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "rgw_realm_reloader.h"
#include "rgw_sts_keyring.h"

class CephContext;
namespace rgw::sal { class Driver; }

namespace STS {

using KeyringSnapshot = std::shared_ptr<const rgw::sts::StsKeyring>;

/*
 * Caches the token-sealing keyring from the mon config-key store. A background
 * thread refreshes the snapshot every interval so requests just read it and
 * never talk to the mon; the blocking read stays off the shared executor.
 */
class KeyringCache : public RGWRealmReloader::Pauser {
  CephContext* const cct;
  rgw::sal::Driver* driver;
  const std::chrono::milliseconds interval;

  std::atomic<KeyringSnapshot> snapshot;
  std::jthread thread;
  // set while the stored keyring won't parse, to log the problem only once
  bool keyring_invalid = false;

  /*
   * re-read the keyring and swap the snapshot. a missing keyring clears it; a
   * mon error leaves the last one in place
   */
  void refresh();
  void run(std::stop_token stop);

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
};

} // namespace STS
