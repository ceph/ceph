#include "rgw_sync_common.h"

#include "common/ceph_json.h"

namespace rgw::sync {
void error_info::dump(ceph::Formatter *f) const {
  encode_json("source_zone", source_zone, f);
  encode_json("error_code", error_code, f);
  encode_json("message", message, f);
}

ErrorLoggerBase::ErrorLoggerBase(sal::RadosStore* store,
                                 std:: string_view oid_prefix,
                                 int num_shards)
    : store(store), num_shards(num_shards) {
  for (int i = 0; i < num_shards; i++) {
    oids.push_back(get_shard_oid(oid_prefix, i));
  }
}

std::string ErrorLoggerBase::get_shard_oid(std::string_view oid_prefix,
                                           int shard_id) {
  return fmt::format("{}.{}", oid_prefix, shard_id);
}
}
