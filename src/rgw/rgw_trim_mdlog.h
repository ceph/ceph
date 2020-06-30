// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

class RGWCoroutine;
class DoutPrefixProvider;
class RGWRados;
class RGWHTTPManager;
class utime_t;
namespace rgw { namespace sal {
  class RGWRadosStore;
} }

// MetaLogTrimCR factory function
RGWCoroutine* create_meta_log_trim_cr(const DoutPrefixProvider *dpp,
                                      rgw::sal::RGWRadosStore *store,
                                      RGWHTTPManager *http,
                                      int num_shards, utime_t interval);

// factory function for mdlog trim via radosgw-admin
RGWCoroutine* create_admin_meta_log_trim_cr(const DoutPrefixProvider *dpp,
                                            rgw::sal::RGWRadosStore *store,
                                            RGWHTTPManager *http,
                                            int num_shards);
