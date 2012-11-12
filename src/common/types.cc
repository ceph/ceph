// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "common/Formatter.h"

void dump(const ceph_file_layout& l, Formatter *f)
{
  f->dump_unsigned("stripe_unit", l.fl_stripe_unit);
  f->dump_unsigned("stripe_count", l.fl_stripe_count);
  f->dump_unsigned("object_size", l.fl_object_size);
  if (l.fl_cas_hash)
    f->dump_unsigned("cas_hash", l.fl_cas_hash);
  if (l.fl_object_stripe_unit)
    f->dump_unsigned("object_stripe_unit", l.fl_object_stripe_unit);
  if (l.fl_pg_pool)
    f->dump_unsigned("pg_pool", l.fl_pg_pool);
}

void dump(const ceph_dir_layout& dl, Formatter *f)
{
  f->dump_unsigned("dir_hash", dl.dl_dir_hash);
}
