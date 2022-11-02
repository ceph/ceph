// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>

class ActiveRateLimiter;
class OpsLogSink;
class RGWREST;

namespace rgw::auth {
  class StrategyRegistry;
}
namespace rgw::lua {
  class Background;
}
namespace rgw::sal {
  class Store;
  class LuaManager;
}

struct RGWProcessEnv {
  rgw::sal::Driver* driver = nullptr;
  RGWREST *rest = nullptr;
  OpsLogSink *olog = nullptr;
  std::unique_ptr<rgw::auth::StrategyRegistry> auth_registry;
  ActiveRateLimiter* ratelimiting = nullptr;
  rgw::lua::Background* lua_background = nullptr;
  std::unique_ptr<rgw::sal::LuaManager> lua_manager;
};
