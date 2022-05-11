// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "arrow/type.h"
#include "arrow/flight/server.h"

#include "rgw_flight_frontend.h"
#include "rgw_flight.h"

#define dout_subsys ceph_subsys_rgw_flight

namespace rgw::flight {

  FlightFrontend::FlightFrontend(boost::intrusive_ptr<ceph::common::CephContext>& _cct,
				 RGWFrontendConfig* _config,
				 rgw::sal::Store* _store,
				 int _port) :
    cct(_cct),
    dp(cct.get(), dout_subsys, "rgw arrow_flight: "),
    config(_config),
    port(_port)
  {
    FlightStore* flight_store = new MemoryFlightStore();
    flight_server = new FlightServer(_cct, _store, flight_store);
  }

  FlightFrontend::~FlightFrontend() {
    delete flight_server;
  }

  int FlightFrontend::init() {
    if (port <= 0) {
      port = FlightServer::default_port;
    }
    const std::string url =
      std::string("grpc+tcp://localhost:") + std::to_string(port);
    flt::Location location;
    arw::Status s = flt::Location::Parse(url, &location);
    if (!s.ok()) {
      return -EINVAL;
    }

    flt::FlightServerOptions options(location);
    options.verify_client = false;
    s = flight_server->Init(options);
    if (!s.ok()) {
      return -EINVAL;
    }

    dout(20) << "STATUS: " << __func__ <<
      ": FlightServer inited; will use port " << port << dendl;
    return 0;
  }

  int FlightFrontend::run() {
    try {
      flight_thread = make_named_thread(server_thread_name,
					&FlightServer::Serve,
					flight_server);
      set_flight_server(flight_server);

      dout(20) << "INFO: " << __func__ <<
	": FlightServer thread started, id=" << flight_thread.get_id() <<
	", joinable=" << flight_thread.joinable() << dendl;
      return 0;
    } catch (std::system_error& e) {
      derr << "ERROR: " << __func__ <<
	": FlightServer thread failed to start" << dendl;
      return -ENOSPC;
    }
  }

  void FlightFrontend::stop() {
    set_flight_server(nullptr);
    flight_server->Shutdown();
    flight_server->Wait();
    dout(20) << "INFO: " << __func__ << ": FlightServer shut down" << dendl;
  }

  void FlightFrontend::join() {
    flight_thread.join();
    dout(20) << "INFO: " << __func__ << ": FlightServer thread joined" << dendl;
  }

  void FlightFrontend::pause_for_new_config() {
    // ignore since config changes won't alter flight_server
  }

  void FlightFrontend::unpause_with_new_config(rgw::sal::Store* store,
			       rgw_auth_registry_ptr_t auth_registry) {
    // ignore since config changes won't alter flight_server
  }

  int FlightGetObj_Filter::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
    // do work here
    dout(0) << "ERIC: " << __func__ << ": flight handling data from offset " << bl_ofs << " of size " << bl_len << dendl;

    // chain upwards
    return RGWGetObj_Filter::handle_data(bl, bl_ofs, bl_len);
  }

} // namespace rgw::flight
