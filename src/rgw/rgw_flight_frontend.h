// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "common/Thread.h"
#include "rgw_frontend.h"
#include "rgw_op.h"


namespace rgw::flight {

  using FlightKey = uint32_t;

  class FlightServer;

  class FlightFrontend : public RGWFrontend {

    static constexpr std::string_view server_thread_name =
      "Arrow Flight Server thread";

    boost::intrusive_ptr<ceph::common::CephContext>& cct;
    const DoutPrefix dp;
    FlightServer* flight_server; // pointer so header file doesn't need to pull in too much
    std::thread flight_thread;
    RGWFrontendConfig* config;
    int port;

  public:

    // port <= 0 -> let server decide; typically 8077
    FlightFrontend(boost::intrusive_ptr<ceph::common::CephContext>& cct,
		   RGWFrontendConfig* config,
		   rgw::sal::Store* store,
		   int port = -1);
    ~FlightFrontend() override;
    int init() override;
    int run() override;
    void stop() override;
    void join() override;

    void pause_for_new_config() override;
    void unpause_with_new_config(rgw::sal::Store* store,
				 rgw_auth_registry_ptr_t auth_registry) override;

  }; // class FlightFrontend

  class FlightGetObj_Filter : public RGWGetObj_Filter {

    FlightKey key;

  public:

    FlightGetObj_Filter(const FlightKey& _key, RGWGetObj_Filter* next) :
      RGWGetObj_Filter(next),
      key(_key)
    {
      // empty
    }

    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
#if 0
    // this would allow the range to be modified if necessary;
    int fixup_range(off_t& ofs, off_t& end) override;
#endif
  };

} // namespace rgw::flight
