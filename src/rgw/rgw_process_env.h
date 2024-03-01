// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>

class ActiveRateLimiter;
class OpsLogSink;
class RGWREST;

namespace rgw {
  class SiteConfig;
}
namespace rgw::auth {
  class StrategyRegistry;
}
namespace rgw::lua {
  class Background;
}
namespace rgw::sal {
  class ConfigStore;
  class Driver;
  class LuaManager;
}

#ifdef WITH_ARROW_FLIGHT
namespace rgw::flight {
  class FlightServer;
  class FlightStore;
}
#endif

struct RGWLuaProcessEnv {
  rgw::lua::Background* background = nullptr;
  std::unique_ptr<rgw::sal::LuaManager> manager;
};

struct RGWProcessEnv {
  RGWLuaProcessEnv lua;
  rgw::sal::ConfigStore* cfgstore = nullptr;
  rgw::sal::Driver* driver = nullptr;
  rgw::SiteConfig* site = nullptr;
  RGWREST *rest = nullptr;
  OpsLogSink *olog = nullptr;
  std::unique_ptr<rgw::auth::StrategyRegistry> auth_registry;
  ActiveRateLimiter* ratelimiting = nullptr;

#ifdef WITH_ARROW_FLIGHT
  // managed by rgw:flight::FlightFrontend in rgw_flight_frontend.cc
  rgw::flight::FlightServer* flight_server = nullptr;
  rgw::flight::FlightStore* flight_store = nullptr;
#endif
};

