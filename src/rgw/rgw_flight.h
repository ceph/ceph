// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

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


#define INFO_F(dp)   ldpp_dout(&dp, 20) << "INFO: " << __func__ << ": "
#define STATUS_F(dp) ldpp_dout(&dp, 10) << "STATUS: " << __func__ << ": "
#define WARN_F(dp)   ldpp_dout(&dp,  0) << "WARNING: " << __func__ << ": "
#define ERROR_F(dp)  ldpp_dout(&dp,  0) << "ERROR: " << __func__ << ": "

#define INFO   INFO_F(dp)
#define STATUS STATUS_F(dp)
#define WARN   WARN_F(dp)
#define ERROR  ERROR_F(dp)


namespace arw = arrow;
namespace flt = arrow::flight;


struct req_state;

namespace rgw::flight {

static const coarse_real_clock::duration lifespan = std::chrono::hours(1);

struct FlightData {
  FlightKey key;
  // coarse_real_clock::time_point expires;
  std::string uri;
  std::string tenant_name;
  std::string bucket_name;
  rgw_obj_key object_key;
  // NB: what about object's namespace and instance?
  uint64_t num_records;
  uint64_t obj_size;
  std::shared_ptr<arw::Schema> schema;
  std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;

  rgw_user user_id; // TODO: this should be removed when we do
  // proper flight authentication

  FlightData(const std::string& _uri,
	     const std::string& _tenant_name,
	     const std::string& _bucket_name,
	     const rgw_obj_key& _object_key,
	     uint64_t _num_records,
	     uint64_t _obj_size,
	     std::shared_ptr<arw::Schema>& _schema,
	     std::shared_ptr<const arw::KeyValueMetadata>& _kv_metadata,
	     rgw_user _user_id);
};

// stores flights that have been created and helps expire them
class FlightStore {

protected:

  const DoutPrefix& dp;

public:

  FlightStore(const DoutPrefix& dp);
  virtual ~FlightStore();
  virtual FlightKey add_flight(FlightData&& flight) = 0;

  // TODO consider returning const shared pointers to FlightData in
  // the following two functions
  virtual arw::Result<FlightData> get_flight(const FlightKey& key) const = 0;
  virtual std::optional<FlightData> after_key(const FlightKey& key) const = 0;

  virtual int remove_flight(const FlightKey& key) = 0;
  virtual int expire_flights() = 0;
};

class MemoryFlightStore : public FlightStore {
  std::map<FlightKey, FlightData> map;
  mutable std::mutex mtx; // for map

public:

  MemoryFlightStore(const DoutPrefix& dp);
  virtual ~MemoryFlightStore();
  FlightKey add_flight(FlightData&& flight) override;
  arw::Result<FlightData> get_flight(const FlightKey& key) const override;
  std::optional<FlightData> after_key(const FlightKey& key) const override;
  int remove_flight(const FlightKey& key) override;
  int expire_flights() override;
};

class FlightServer : public flt::FlightServerBase {

  using Data1 = std::vector<std::shared_ptr<arw::RecordBatch>>;

  RGWProcessEnv& env;
  rgw::sal::Driver* driver;
  const DoutPrefix& dp;
  FlightStore* flight_store;

  std::map<std::string, Data1> data;
  arw::Status serve_return_value;

public:

  static constexpr int default_port = 8077;

  FlightServer(RGWProcessEnv& env,
	       FlightStore* flight_store,
	       const DoutPrefix& dp);
  ~FlightServer() override;

  // provides a version of Serve that has no return value, to avoid
  // warnings when launching in a thread
  void ServeAlt() {
    serve_return_value = Serve();
  }

  FlightStore* get_flight_store() {
    return flight_store;
  }

  arw::Status ListFlights(const flt::ServerCallContext& context,
			  const flt::Criteria* criteria,
			  std::unique_ptr<flt::FlightListing>* listings) override;

  arw::Status GetFlightInfo(const flt::ServerCallContext &context,
			    const flt::FlightDescriptor &request,
			    std::unique_ptr<flt::FlightInfo> *info) override;

  arw::Status GetSchema(const flt::ServerCallContext &context,
			const flt::FlightDescriptor &request,
			std::unique_ptr<flt::SchemaResult> *schema) override;

  arw::Status DoGet(const flt::ServerCallContext &context,
		    const flt::Ticket &request,
		    std::unique_ptr<flt::FlightDataStream> *stream) override;
}; // class FlightServer

class OwningStringView : public std::string_view {

  uint8_t* buffer;
  int64_t capacity;
  int64_t consumed;

  OwningStringView(uint8_t* _buffer, int64_t _size) :
    std::string_view((const char*) _buffer, _size),
    buffer(_buffer),
    capacity(_size),
    consumed(_size)
    { }

  OwningStringView(OwningStringView&& from, int64_t new_size) :
    buffer(nullptr),
    capacity(from.capacity),
    consumed(new_size)
    {
      // should be impossible due to static function check
      ceph_assertf(consumed <= capacity, "new size cannot exceed capacity");

      std::swap(buffer, from.buffer);
      from.capacity = 0;
      from.consumed = 0;
    }

public:

  OwningStringView(OwningStringView&&) = default;
  OwningStringView& operator=(OwningStringView&&) = default;

  uint8_t* writeable_data() {
    return buffer;
  }

  ~OwningStringView() {
    if (buffer) {
      delete[] buffer;
    }
  }

  static arw::Result<OwningStringView> make(int64_t size) {
    uint8_t* buffer = new uint8_t[size];
    if (!buffer) {
      return arw::Status::OutOfMemory("could not allocated buffer of size %" PRId64, size);
    }
    return OwningStringView(buffer, size);
  }

  static arw::Result<OwningStringView> shrink(OwningStringView&& from,
					      int64_t new_size) {
    if (new_size > from.capacity) {
      return arw::Status::Invalid("new size cannot exceed capacity");
    } else {
      return OwningStringView(std::move(from), new_size);
    }
  }

};

// GLOBAL

flt::Ticket FlightKeyToTicket(const FlightKey& key);
arw::Status TicketToFlightKey(const flt::Ticket& t, FlightKey& key);

} // namespace rgw::flight
