// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "common/Formatter.h"

/*
 * file_layout_policy_t
 */

void file_layout_policy_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(layout, bl);
  ENCODE_FINISH(bl);
}

void file_layout_policy_t::decode(bufferlist::iterator& bl)
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

void file_layout_policy_t::dump(Formatter *f) const
{
  ::dump(layout, f);
}

void file_layout_policy_t::generate_test_instances(list<file_layout_policy_t*>& ls)
{
  ls.push_back(new file_layout_policy_t);
  ls.push_back(new file_layout_policy_t);
  ls.back()->layout.fl_stripe_unit = 1024;
  ls.back()->layout.fl_stripe_count = 2;
  ls.back()->layout.fl_object_size = 2048;
  ls.back()->layout.fl_cas_hash = 3;
  ls.back()->layout.fl_object_stripe_unit = 8;
  ls.back()->layout.fl_pg_pool = 9;
}


/*
 * frag_info_t
 */

void frag_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(mtime, bl);
  ::encode(nfiles, bl);
  ::encode(nsubdirs, bl);
  ENCODE_FINISH(bl);
}

void frag_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(mtime, bl);
  ::decode(nfiles, bl);
  ::decode(nsubdirs, bl);
  DECODE_FINISH(bl);
}

void frag_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("num_files", nfiles);
  f->dump_unsigned("num_subdirs", nsubdirs);
}

void frag_info_t::generate_test_instances(list<frag_info_t*>& ls)
{
  ls.push_back(new frag_info_t);
  ls.push_back(new frag_info_t);
  ls.back()->version = 1;
  ls.back()->mtime = utime_t(2, 3);
  ls.back()->nfiles = 4;
  ls.back()->nsubdirs = 5;
}

ostream& operator<<(ostream &out, const frag_info_t &f)
{
  if (f == frag_info_t())
    return out << "f()";
  out << "f(v" << f.version;
  if (f.mtime != utime_t())
    out << " m" << f.mtime;
  if (f.nfiles || f.nsubdirs)
    out << " " << f.size() << "=" << f.nfiles << "+" << f.nsubdirs;
  out << ")";
  return out;
}


