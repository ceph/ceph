// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <iostream>
#include <mutex>
#include <map>

#include "arrow/type.h"
#include "arrow/flight/server.h"

#include "common/dout.h"
#include "rgw_op.h"

#include "rgw_flight.h"
#include "rgw_flight_frontend.h"


#define dout_subsys ceph_subsys_rgw_flight

#define INFO_F(dp)   ldpp_dout(&dp, 20) << "INFO: " << __func__ << ": "
#define STATUS_F(dp) ldpp_dout(&dp, 10) << "STATUS: " << __func__ << ": "
#define WARN_F(dp)   ldpp_dout(&dp,  0) << "WARNING: " << __func__ << ": "
#define ERROR_F(dp)  ldpp_dout(&dp,  0) << "ERROR: " << __func__ << ": "

#define INFO   INFO_F(dp)
#define STATUS STATUS_F(dp)
#define WARN   WARN_F(dp)
#define ERROR  ERROR_F(dp)


namespace rgw::flight {

  std::atomic<FlightKey> next_flight_key = 0;

  FlightData::FlightData(const req_state* state) :
    key(next_flight_key++),
    expires(coarse_real_clock::now() + lifespan)
  {
#if 0
    bucket = new rgw::sal::Bucket(*state->bucket);
#endif
  }

  FlightStore::~FlightStore() {
    // empty
  }

  MemoryFlightStore::~MemoryFlightStore() {
    // empty
  }

  FlightKey MemoryFlightStore::add_flight(FlightData&& flight) {
    FlightKey key = flight.key;

    auto p = map.insert( {key, flight} );
    ceph_assertf(p.second,
		 "unable to add FlightData to MemoryFlightStore"); // temporary until error handling

    return key;
  }
  // int MemoryFlightStore::add_flight(const FlightKey& key) { return 0; }
  int MemoryFlightStore::get_flight(const FlightKey& key) { return 0; }
  int MemoryFlightStore::remove_flight(const FlightKey& key) { return 0; }
  int MemoryFlightStore::expire_flights() { return 0; }

  FlightServer::FlightServer(boost::intrusive_ptr<ceph::common::CephContext>& _cct,
			     rgw::sal::Store* _store,
			     FlightStore* _flight_store) :
    cct(_cct),
    dp(cct.get(), dout_subsys, "rgw arrow_flight: "),
    store(_store),
    flight_store(_flight_store)
  {
    INFO << "FlightServer constructed" << dendl;
  }

  FlightServer::~FlightServer()
  {
    INFO << "FlightServer destructed" << dendl;
  }


class RGWFlightListing : public flt::FlightListing {
public:

  RGWFlightListing() {
#if 0
    const int64_t total_records = 2;
    const int64_t total_bytes = 2048;
    const std::vector<flt::FlightEndpoint> endpoints;
    const auto descriptor = flt::FlightDescriptor::Command("fake-cmd");
    arw::FieldVector fields;
    const arw::Schema schema(fields, nullptr);
    auto info1 = flt::FlightInfo::Make(schema, descriptor, endpoints, total_records, total_bytes);
#endif
  }

  arw::Status Next(std::unique_ptr<flt::FlightInfo>* info) {
    *info = nullptr;
    return arw::Status::OK();
  }
};


  arw::Status FlightServer::ListFlights(const flt::ServerCallContext& context,
					const flt::Criteria* criteria,
					std::unique_ptr<flt::FlightListing>* listings) {
    *listings = std::make_unique<RGWFlightListing>();
    return arw::Status::OK();
  }

  static FlightServer* fs;

  void set_flight_server(FlightServer* _server) {
    fs = _server;
  }

  FlightServer* get_flight_server() {
    return fs;
  }

  FlightStore* get_flight_store() {
    return fs ? fs->get_flight_store() : nullptr;
  }

  FlightKey propose_flight(const req_state* request) {
    FlightKey key = get_flight_store()->add_flight(FlightData(request));
    return key;
  }

} // namespace rgw::flight
