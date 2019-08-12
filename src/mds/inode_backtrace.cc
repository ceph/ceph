// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "inode_backtrace.h"

#include "common/Formatter.h"

/* inode_backpointer_t */

void inode_backpointer_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dirino, bl);
  encode(dname, bl);
  encode(version, bl);
  ENCODE_FINISH(bl);
}

void inode_backpointer_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dirino, bl);
  decode(dname, bl);
  decode(version, bl);
  DECODE_FINISH(bl);
}

void inode_backpointer_t::decode_old(bufferlist::const_iterator& bl)
{
  using ceph::decode;
  decode(dirino, bl);
  decode(dname, bl);
  decode(version, bl);
}

void inode_backpointer_t::dump(Formatter *f) const
{
  f->dump_unsigned("dirino", dirino);
  f->dump_string("dname", dname);
  f->dump_unsigned("version", version);
}

void inode_backpointer_t::generate_test_instances(std::list<inode_backpointer_t*>& ls)
{
  ls.push_back(new inode_backpointer_t);
  ls.push_back(new inode_backpointer_t);
  ls.back()->dirino = 1;
  ls.back()->dname = "foo";
  ls.back()->version = 123;
}


/*
 * inode_backtrace_t
 */

void inode_backtrace_t::encode(bufferlist& bl) const
{
  ENCODE_START(5, 4, bl);
  encode(ino, bl);
  encode(ancestors, bl);
  encode(pool, bl);
  encode(old_pools, bl);
  ENCODE_FINISH(bl);
}

void inode_backtrace_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  if (struct_v < 3)
    return;  // sorry, the old data was crap
  decode(ino, bl);
  if (struct_v >= 4) {
    decode(ancestors, bl);
  } else {
    __u32 n;
    decode(n, bl);
    while (n--) {
      ancestors.push_back(inode_backpointer_t());
      ancestors.back().decode_old(bl);
    }
  }
  if (struct_v >= 5) {
    decode(pool, bl);
    decode(old_pools, bl);
  }
  DECODE_FINISH(bl);
}

void inode_backtrace_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->open_array_section("ancestors");
  for (vector<inode_backpointer_t>::const_iterator p = ancestors.begin(); p != ancestors.end(); ++p) {
    f->open_object_section("backpointer");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_int("pool", pool);
  f->open_array_section("old_pools");
  for (set<int64_t>::iterator p = old_pools.begin(); p != old_pools.end(); ++p) {
    f->dump_int("old_pool", *p);
  }
  f->close_section();
}

void inode_backtrace_t::generate_test_instances(std::list<inode_backtrace_t*>& ls)
{
  ls.push_back(new inode_backtrace_t);
  ls.push_back(new inode_backtrace_t);
  ls.back()->ino = 1;
  ls.back()->ancestors.push_back(inode_backpointer_t());
  ls.back()->ancestors.back().dirino = 123;
  ls.back()->ancestors.back().dname = "bar";
  ls.back()->ancestors.back().version = 456;
  ls.back()->pool = 0;
  ls.back()->old_pools.insert(10);
  ls.back()->old_pools.insert(7);
}

int inode_backtrace_t::compare(const inode_backtrace_t& other,
                               bool *equivalent, bool *divergent) const
{
  int min_size = std::min(ancestors.size(),other.ancestors.size());
  *equivalent = true;
  *divergent = false;
  if (min_size == 0)
    return 0;
  int comparator = 0;
  if (ancestors[0].version > other.ancestors[0].version)
    comparator = 1;
  else if (ancestors[0].version < other.ancestors[0].version)
    comparator = -1;
  if (ancestors[0].dirino != other.ancestors[0].dirino ||
      ancestors[0].dname != other.ancestors[0].dname)
    *divergent = true;
  for (int i = 1; i < min_size; ++i) {
    if (*divergent) {
      /**
       * we already know the dentries and versions are
       * incompatible; no point checking farther
       */
      break;
    }
    if (ancestors[i].dirino != other.ancestors[i].dirino ||
        ancestors[i].dname != other.ancestors[i].dname) {
      *equivalent = false;
      return comparator;
    } else if (ancestors[i].version > other.ancestors[i].version) {
      if (comparator < 0)
        *divergent = true;
      comparator = 1;
    } else if (ancestors[i].version < other.ancestors[i].version) {
      if (comparator > 0)
        *divergent = true;
      comparator = -1;
    }
  }
  if (*divergent)
    *equivalent = false;
  return comparator;
}
