// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004- Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <string_view>

#include "snap.h"

#include "common/Formatter.h"

/*
 * SnapInfo
 */

void SnapInfo::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(snapid, bl);
  encode(ino, bl);
  encode(stamp, bl);
  encode(name, bl);
  ENCODE_FINISH(bl);
}

void SnapInfo::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(snapid, bl);
  decode(ino, bl);
  decode(stamp, bl);
  decode(name, bl);
  DECODE_FINISH(bl);
}

void SnapInfo::dump(Formatter *f) const
{
  f->dump_unsigned("snapid", snapid);
  f->dump_unsigned("ino", ino);
  f->dump_stream("stamp") << stamp;
  f->dump_string("name", name);
}

void SnapInfo::generate_test_instances(std::list<SnapInfo*>& ls)
{
  ls.push_back(new SnapInfo);
  ls.push_back(new SnapInfo);
  ls.back()->snapid = 1;
  ls.back()->ino = 2;
  ls.back()->stamp = utime_t(3, 4);
  ls.back()->name = "foo";
}

ostream& operator<<(ostream& out, const SnapInfo &sn)
{
  return out << "snap(" << sn.snapid
	     << " " << sn.ino
	     << " '" << sn.name
	     << "' " << sn.stamp << ")";
}

std::string_view SnapInfo::get_long_name() const
{
  if (long_name.empty() ||
      long_name.compare(1, name.size(), name) ||
      long_name.find_last_of("_") != name.size() + 1) {
    char nm[80];
    snprintf(nm, sizeof(nm), "_%s_%llu", name.c_str(), (unsigned long long)ino);
    long_name = nm;
  }
  return long_name;
}

/*
 * snaplink_t
 */

void snaplink_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(ino, bl);
  encode(first, bl);
  ENCODE_FINISH(bl);
}

void snaplink_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(ino, bl);
  decode(first, bl);
  DECODE_FINISH(bl);
}

void snaplink_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("first", first);
}

void snaplink_t::generate_test_instances(std::list<snaplink_t*>& ls)
{
  ls.push_back(new snaplink_t);
  ls.push_back(new snaplink_t);
  ls.back()->ino = 2;
  ls.back()->first = 123;
}

ostream& operator<<(ostream& out, const snaplink_t &l)
{
  return out << l.ino << "@" << l.first;
}

/*
 * sr_t
 */

void sr_t::encode(bufferlist& bl) const
{
  ENCODE_START(6, 4, bl);
  encode(seq, bl);
  encode(created, bl);
  encode(last_created, bl);
  encode(last_destroyed, bl);
  encode(current_parent_since, bl);
  encode(snaps, bl);
  encode(past_parents, bl);
  encode(past_parent_snaps, bl);
  encode(flags, bl);
  ENCODE_FINISH(bl);
}

void sr_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, p);
  if (struct_v == 2) {
    __u8 struct_v;
    decode(struct_v, p);  // yes, really: extra byte for v2 encoding only, see 6ee52e7d.
  }
  decode(seq, p);
  decode(created, p);
  decode(last_created, p);
  decode(last_destroyed, p);
  decode(current_parent_since, p);
  decode(snaps, p);
  decode(past_parents, p);
  if (struct_v >= 5)
    decode(past_parent_snaps, p);
  if (struct_v >= 6)
    decode(flags, p);
  else
    flags = 0;
  DECODE_FINISH(p);
}

void sr_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_created", last_created);
  f->dump_unsigned("last_destroyed", last_destroyed);
  f->dump_unsigned("current_parent_since", current_parent_since);

  f->open_array_section("snaps");
  for (map<snapid_t,SnapInfo>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
    f->open_object_section("snapinfo");
    f->dump_unsigned("last", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("past_parents");
  for (map<snapid_t,snaplink_t>::const_iterator p = past_parents.begin(); p != past_parents.end(); ++p) {
    f->open_object_section("past_parent");
    f->dump_unsigned("last", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("past_parent_snaps");
  for (auto p = past_parent_snaps.begin(); p != past_parent_snaps.end(); ++p) {
    f->open_object_section("snapinfo");
    f->dump_unsigned("snapid", *p);
    f->close_section();
  }
  f->close_section();
}

void sr_t::generate_test_instances(std::list<sr_t*>& ls)
{
  ls.push_back(new sr_t);
  ls.push_back(new sr_t);
  ls.back()->seq = 1;
  ls.back()->created = 2;
  ls.back()->last_created = 3;
  ls.back()->last_destroyed = 4;
  ls.back()->current_parent_since = 5;
  ls.back()->snaps[123].snapid = 7;
  ls.back()->snaps[123].ino = 8;
  ls.back()->snaps[123].stamp = utime_t(9, 10);
  ls.back()->snaps[123].name = "name1";
  ls.back()->past_parents[12].ino = 12;
  ls.back()->past_parents[12].first = 3;

  ls.back()->past_parent_snaps.insert(5);
  ls.back()->past_parent_snaps.insert(6);
}

