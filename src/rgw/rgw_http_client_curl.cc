// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_http_client_curl.h"
#include <mutex>
#include <curl/curl.h>

#include "rgw_common.h"

namespace rgw {
namespace curl {

std::once_flag curl_init_flag;

void setup_curl([[maybe_unused]] boost::optional<const fe_map_t&> m) {
  std::call_once(curl_init_flag, curl_global_init, CURL_GLOBAL_ALL);
  rgw_setup_saved_curl_handles();
}

void cleanup_curl() {
  rgw_release_all_curl_handles();
  curl_global_cleanup();
}

} /* namespace curl */
} /* namespace rgw */
