// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <coroutine>
#include <memory>

#include "rgw_sync_asio.h"
#include "rgw_sync_trace.h"
#include "sync_fairness_asio.h"

namespace rgw::sync::meta {
struct Env {
  const DoutPrefixProvider* const dpp;
  sal::RadosStore* const store;

  // Initialized later as part of RemoteLog setup
  std::unique_ptr<ErrorLogger> error_logger;
  std::unique_ptr<sync_fairness::BidManagerAsio> bid_manager;

  Env(const DoutPrefixProvider* dpp, sal::RadosStore* store)
    : dpp(dpp), store(store) {}

  CephContext* const cct{store->ctx()};

  RGWSyncTraceManager* const sync_tracer = store->getRados()->get_sync_tracer();

  std::string shard_obj_name(int shard_id);
  std::string_view status_oid();
};
}
