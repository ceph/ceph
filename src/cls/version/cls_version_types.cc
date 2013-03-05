
#include "cls/version/cls_version_types.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"


void obj_version::dump(Formatter *f) const
{
  f->dump_int("ver", ver);
  f->dump_string("tag", tag);
}

void obj_version::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("ver", ver, obj);
  JSONDecoder::decode_json("tag", tag, obj);
}

