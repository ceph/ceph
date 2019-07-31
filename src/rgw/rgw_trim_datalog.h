// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class RGWCoroutine;
class RGWRados;
class RGWHTTPManager;
class utime_t;

// DataLogTrimCR factory function
extern RGWCoroutine* create_data_log_trim_cr(RGWRados *store,
                                             RGWHTTPManager *http,
                                             int num_shards, utime_t interval);

// factory function for datalog trim via radosgw-admin
RGWCoroutine* create_admin_data_log_trim_cr(RGWRados *store,
                                            RGWHTTPManager *http,
                                            int num_shards,
                                            std::vector<std::string>& markers);
