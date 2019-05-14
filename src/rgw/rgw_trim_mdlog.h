// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class RGWCoroutine;
class DoutPrefixProvider;
class RGWRados;
class RGWHTTPManager;
class utime_t;

// MetaLogTrimCR factory function
RGWCoroutine* create_meta_log_trim_cr(const DoutPrefixProvider *dpp,
                                      RGWRados *store,
                                      RGWHTTPManager *http,
                                      int num_shards, utime_t interval);

// factory function for mdlog trim via radosgw-admin
RGWCoroutine* create_admin_meta_log_trim_cr(const DoutPrefixProvider *dpp,
                                            RGWRados *store,
                                            RGWHTTPManager *http,
                                            int num_shards);
