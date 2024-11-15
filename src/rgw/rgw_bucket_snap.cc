#include "rgw_bucket_snap.h"
#include "common/ceph_json.h"


void rgw_bucket_snap::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("name", id, f);
  encode_json("description", id, f);
  encode_json("creation_time", id, f);
}
