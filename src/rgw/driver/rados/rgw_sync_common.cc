#include "rgw_sync_common.h"

#include "common/ceph_json.h"

namespace rgw::sync {
void error_info::dump(ceph::Formatter *f) const {
  encode_json("source_zone", source_zone, f);
  encode_json("error_code", error_code, f);
  encode_json("message", message, f);
}
}
