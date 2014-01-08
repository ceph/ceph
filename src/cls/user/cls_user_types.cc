
#include "cls/user/cls_user_types.h"
#include "common/Formatter.h"


void cls_user_header::dump(Formatter *f) const
{
  f->dump_int("total_entries", total_entries);
  f->dump_int("total_bytes", total_bytes);
  f->dump_int("total_bytes_rounded", total_bytes_rounded);
}

