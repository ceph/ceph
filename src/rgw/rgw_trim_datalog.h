// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

class RGWCoroutine;
class RGWRados;
class RGWHTTPManager;
class utime_t;
namespace rgw { namespace sal {
  class RGWRadosStore;
} }

// DataLogTrimCR factory function
extern RGWCoroutine* create_data_log_trim_cr(rgw::sal::RGWRadosStore *store,
                                             RGWHTTPManager *http,
                                             int num_shards, utime_t interval);

// factory function for datalog trim via radosgw-admin
RGWCoroutine* create_admin_data_log_trim_cr(rgw::sal::RGWRadosStore *store,
                                            RGWHTTPManager *http,
                                            int num_shards,
                                            std::vector<std::string>& markers);
