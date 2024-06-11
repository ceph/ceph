// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "inode_backtrace.h"

#include "common/Formatter.h"

/* inode_backpointer_t */

void inode_backpointer_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dirino, bl);
  encode(dname, bl);
  encode(version, bl);
  ENCODE_FINISH(bl);
}

void inode_backpointer_t::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dirino, bl);
  decode(dname, bl);
  decode(version, bl);
  DECODE_FINISH(bl);
}

void inode_backpointer_t::decode_old(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  decode(dirino, bl);
  decode(dname, bl);
  decode(version, bl);
}

void inode_backpointer_t::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("dirino", dirino);
  f->dump_string("dname", dname);
  f->dump_unsigned("version", version);
  if (is_auth > 0)
    f->dump_string("auth", "yes");
  else if (is_auth == 0)
    f->dump_string("auth", "no");
  else
    f->dump_string("auth", "unknown");
}

void inode_backpointer_t::generate_test_instances(std::list<inode_backpointer_t*>& ls)
{
  ls.push_back(new inode_backpointer_t);
  ls.push_back(new inode_backpointer_t);
  ls.back()->dirino = 1;
  ls.back()->dname = "foo";
  ls.back()->version = 123;
  ls.back()->is_auth = 1;
}


/*
 * inode_backtrace_t
 */

void inode_backtrace_t::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(5, 4, bl);
  encode(ino, bl);
  encode(ancestors, bl);
  encode(pool, bl);
  encode(old_pools, bl);
  ENCODE_FINISH(bl);
}

void inode_backtrace_t::decode(ceph::buffer::list::const_iterator& bl)
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

void inode_backtrace_t::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->open_array_section("ancestors");
  for (auto p = ancestors.begin(); p != ancestors.end(); ++p) {
    f->open_object_section("backpointer");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_int("pool", pool);
  f->open_array_section("old_pools");
  for (auto p = old_pools.begin(); p != old_pools.end(); ++p) {
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
  ls.back()->ancestors.back().is_auth = 1;
  ls.back()->pool = 0;
  ls.back()->old_pools.push_back(10);
  ls.back()->old_pools.push_back(7);
}

/*
 * The consequence of the scrub will not acquire nest locks which would update
 * the iversion for the in-memory inode prior to comparing to on-disk values.
 *
 * Only compare the versions when the inode is auth locally
 */
int inode_backtrace_t::compare(const inode_backtrace_t& other,
                               bool *divergent) const
{
  int min_size = std::min(ancestors.size(),other.ancestors.size());
  *divergent = false;
  if (min_size == 0)
    return 0;
  int comparator = 0;
  int i;

  // find the first auth inode
  for (i = 0; i < min_size && !(*divergent) && !comparator; ++i) {
    if (ancestors[i].dirino != other.ancestors[i].dirino ||
	ancestors[i].dname != other.ancestors[i].dname) {
      *divergent = true;
      continue;
    }

    if (ancestors[i].is_auth <= 0 && other.ancestors[i].is_auth <= 0)
      continue;
    if (ancestors[i].version > other.ancestors[i].version) {
      comparator = 1;
      continue;
    } else if (ancestors[i].version < other.ancestors[i].version) {
      comparator = -1;
      continue;
    }
  }
  // compare the rest auth inodes
  for (; i < min_size && !(*divergent); ++i) {
    if (ancestors[i].dirino != other.ancestors[i].dirino ||
        ancestors[i].dname != other.ancestors[i].dname) {
      *divergent = true;
      return comparator;
    }

    if (ancestors[i].is_auth <= 0 && other.ancestors[i].is_auth <= 0)
      continue;
    if (ancestors[i].version > other.ancestors[i].version) {
      if (comparator < 0)
        *divergent = true;
      comparator = 1;
    } else if (ancestors[i].version < other.ancestors[i].version) {
      if (comparator > 0)
        *divergent = true;
      comparator = -1;
    }
  }
  return comparator;
}
