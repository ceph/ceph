
#include "cls/version/cls_version_types.h"
#include "common/Formatter.h"


void obj_version::dump(Formatter *f) const
{
  f->dump_int("ver", ver);
  f->dump_string("tag", tag);
}

