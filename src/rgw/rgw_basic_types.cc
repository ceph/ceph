#include "rgw_basic_types.h"
#include "common/ceph_json.h"

void decode_json_obj(rgw_user& val, JSONObj *obj)
{
  string s = obj->get_data();
  val.from_str(s);
}

void encode_json(const char *name, const rgw_user& val, Formatter *f)
{
  string s = val.to_str();
  f->dump_string(name, s);
}
