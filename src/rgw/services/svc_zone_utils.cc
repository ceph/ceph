#include "svc_zone_utils.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

int RGWSI_ZoneUtils::do_start()
{
  init_unique_trans_id_deps();

  return 0;
}

string RGWSI_ZoneUtils::gen_host_id() {
  /* uint64_t needs 16, two '-' separators and a trailing null */
  const string& zone_name = zone_svc->get_zone().name;
  const string& zonegroup_name = zone_svc->get_zonegroup().get_name();
  char charbuf[16 + zone_name.size() + zonegroup_name.size() + 2 + 1];
  snprintf(charbuf, sizeof(charbuf), "%llx-%s-%s", (unsigned long long)rados_svc->instance_id(), zone_name.c_str(), zonegroup_name.c_str());
  return string(charbuf);
}

string RGWSI_ZoneUtils::unique_id(uint64_t unique_num)
{
  char buf[32];
  snprintf(buf, sizeof(buf), ".%llu.%llu", (unsigned long long)rados_svc->instance_id(), (unsigned long long)unique_num);
  string s = zone_svc->get_zone_params().get_id() + buf;
  return s;
}

void RGWSI_ZoneUtils::init_unique_trans_id_deps() {
  char buf[16 + 2 + 1]; /* uint64_t needs 16, 2 hyphens add further 2 */

  snprintf(buf, sizeof(buf), "-%llx-", (unsigned long long)rados_svc->instance_id());
  url_encode(string(buf) + zone_svc->get_zone().name, trans_id_suffix);
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

