// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_zone_utils.h"
#include "svc_zone.h"

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "rgw_zone.h"

using namespace std;

int RGWSI_ZoneUtils::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  init_unique_trans_id_deps();

  return 0;
}

string RGWSI_ZoneUtils::gen_host_id() {
  /* uint64_t needs 16, two '-' separators and a trailing null */
  return fmt::format("{}-{}-{}", rados->get_instance_id(),
		     zone_svc->get_zone().name,
		     zone_svc->get_zonegroup().get_name());
}

string RGWSI_ZoneUtils::unique_id(uint64_t unique_num)
{
  return fmt::format("{}.{}.{}",
		     zone_svc->get_zone_params().get_id(),
		     rados->get_instance_id(),
		     unique_num);
}

void RGWSI_ZoneUtils::init_unique_trans_id_deps() {
  url_encode(fmt::format("-{}-{}",
			 rados->get_instance_id(),
			 zone_svc->get_zone().name),
	     trans_id_suffix);
}

/* In order to preserve compatibility with Swift API, transaction ID
 * should contain at least 32 characters satisfying following spec:
 *  - first 21 chars must be in range [0-9a-f]. Swift uses this
 *    space for storing fragment of UUID obtained through a call to
 *    uuid4() function of Python's uuid module;
 *  - char no. 22 must be a hyphen;
 *  - at least 10 next characters constitute hex-formatted timestamp
 *    padded with zeroes if necessary. All bytes must be in [0-9a-f]
 *    range;
 *  - last, optional part of transaction ID is any url-encoded string
 *    without restriction on length. */
string RGWSI_ZoneUtils::unique_trans_id(const uint64_t unique_num) {
  char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
  time_t timestamp = time(NULL);

  snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
           (unsigned long long)unique_num,
           (unsigned long long)timestamp);

  return string(buf) + trans_id_suffix;
}

