// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <mutex>
#include <atomic>

#include "include/common_fwd.h"
#include "common/ceph_context.h"
#include "common/Thread.h"
#include "common/ceph_time.h"
#include "rgw_frontend.h"
#include "arrow/type.h"
#include "arrow/flight/server.h"

#include "rgw_flight_frontend.h"

namespace arw = arrow;
namespace flt = arrow::flight;

struct req_state;

namespace rgw::flight {

  static const coarse_real_clock::duration lifespan = std::chrono::hours(1);

  struct FlightData {
    FlightKey key;
    coarse_real_clock::time_point expires;

    rgw::sal::Bucket* bucket;
#if 0
    rgw::sal::Object object;
    rgw::sal::User user;
#endif


    FlightData(const req_state* state);
  };

  // stores flights that have been created and helps expire them
  class FlightStore {
  public:
    virtual ~FlightStore();
    virtual FlightKey add_flight(FlightData&& flight) = 0;
    virtual int get_flight(const FlightKey& key) = 0;
    virtual int remove_flight(const FlightKey& key) = 0;
    virtual int expire_flights() = 0;
  };

  class MemoryFlightStore : public FlightStore {
    std::map<FlightKey, FlightData> map;

  public:

    virtual ~MemoryFlightStore();
    FlightKey add_flight(FlightData&& flight) override;
    int get_flight(const FlightKey& key) override;
    int remove_flight(const FlightKey& key) override;
    int expire_flights() override;
  };

  class FlightServer : public flt::FlightServerBase {

    using Data1 = std::vector<std::shared_ptr<arw::RecordBatch>>;

    boost::intrusive_ptr<ceph::common::CephContext> cct;
    const DoutPrefix dp;
    rgw::sal::Store* store;
    FlightStore* flight_store;

    std::map<std::string, Data1> data;

  public:

    static constexpr int default_port = 8077;

    FlightServer(boost::intrusive_ptr<ceph::common::CephContext>& _cct,
		 rgw::sal::Store* _store,
		 FlightStore* _flight_store);
    ~FlightServer() override;

    FlightStore* get_flight_store() {
      return flight_store;
    }

    arw::Status ListFlights(const flt::ServerCallContext& context,
			    const flt::Criteria* criteria,
			    std::unique_ptr<flt::FlightListing>* listings) override;
  }; // class FlightServer

  // GLOBAL

  void set_flight_server(FlightServer* _server);
  FlightServer* get_flight_server();
  FlightStore* get_flight_store();
  FlightKey propose_flight(const req_state* request);

} // namespace rgw::flight
