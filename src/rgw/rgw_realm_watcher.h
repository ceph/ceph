// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include "include/buffer.h"
#include "include/encoding.h"

enum class RGWRealmNotify {
  Reload,
  ZonesNeedPeriod,
};
WRITE_RAW_ENCODER(RGWRealmNotify);

/**
 * RGWRealmWatcher establishes a watch on the current RGWRealm's control object,
 * and forwards notifications to registered observers.
 */
class RGWRealmWatcher {
 public:
  /**
   * Watcher is an interface that allows the RGWRealmWatcher to pass
   * notifications on to other interested objects.
   */
  class Watcher {
   public:
    virtual ~Watcher() = default;

    virtual void handle_notify(RGWRealmNotify type,
                               bufferlist::const_iterator& p) = 0;
  };

  virtual ~RGWRealmWatcher();

  /// register a watcher for the given notification type
  void add_watcher(RGWRealmNotify type, Watcher& watcher);

 protected:
  std::map<RGWRealmNotify, Watcher&> watchers;
};
