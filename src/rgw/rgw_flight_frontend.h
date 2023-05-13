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

#include "include/common_fwd.h"
#include "common/Thread.h"
#include "rgw_frontend.h"
#include "rgw_op.h"

#include "arrow/status.h"


namespace rgw::flight {

using FlightKey = uint32_t;
extern const FlightKey null_flight_key;

class FlightServer;

class FlightFrontend : public RGWFrontend {

  static constexpr std::string_view server_thread_name =
    "Arrow Flight Server thread";

  RGWProcessEnv& env;
  std::thread flight_thread;
  RGWFrontendConfig* config;
  int port;

  const DoutPrefix dp;

public:

  // port <= 0 means let server decide; typically 8077
  FlightFrontend(RGWProcessEnv& env,
		 RGWFrontendConfig* config,
		 int port = -1);
  ~FlightFrontend() override;
  int init() override;
  int run() override;
  void stop() override;
  void join() override;

  void pause_for_new_config() override;
  void unpause_with_new_config() override;
}; // class FlightFrontend

class FlightGetObj_Filter : public RGWGetObj_Filter {

  const RGWProcessEnv& penv;
  const DoutPrefix dp;
  FlightKey key;
  uint64_t current_offset;
  uint64_t expected_size;
  std::string uri;
  std::string tenant_name;
  std::string bucket_name;
  rgw_obj_key object_key;
  std::string temp_file_name;
  std::ofstream temp_file;
  arrow::Status schema_status;
  rgw_user user_id; // TODO: this should be removed when we do
  // proper flight authentication

public:

  FlightGetObj_Filter(const req_state* request, RGWGetObj_Filter* next);
  ~FlightGetObj_Filter();

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
#if 0
  // this would allow the range to be modified if necessary;
  int fixup_range(off_t& ofs, off_t& end) override;
#endif
};

} // namespace rgw::flight
