// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "common/Formatter.h"

void default_file_layout::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(layout, bl);
  ENCODE_FINISH(bl);
}

void default_file_layout::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(layout, bl);
  DECODE_FINISH(bl);
}

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

void default_file_layout::dump(Formatter *f) const
{
  ::dump(layout, f);
}

void default_file_layout::generate_test_instances(list<default_file_layout*>& ls)
{
  ls.push_back(new default_file_layout);
  ls.push_back(new default_file_layout);
  ls.back()->layout.fl_stripe_unit = 1024;
  ls.back()->layout.fl_stripe_count = 2;
  ls.back()->layout.fl_object_size = 2048;
  ls.back()->layout.fl_cas_hash = 3;
  ls.back()->layout.fl_object_stripe_unit = 8;
  ls.back()->layout.fl_pg_pool = 9;
}
