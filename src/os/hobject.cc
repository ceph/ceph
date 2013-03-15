// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "hobject.h"
#include "common/Formatter.h"

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

set<string> hobject_t::get_prefixes(
  uint32_t bits,
  uint32_t mask,
  int64_t pool)
{
  uint32_t len = bits;
  while (len % 4 /* nibbles */) len++;

  set<uint32_t> from;
  if (bits < 32)
    from.insert(mask & ~((uint32_t)(~0) << bits));
  else if (bits == 32)
    from.insert(mask);
  else
    assert(0);


  set<uint32_t> to;
  for (uint32_t i = bits; i < len; ++i) {
    for (set<uint32_t>::iterator j = from.begin();
	 j != from.end();
	 ++j) {
      to.insert(*j | (1 << i));
      to.insert(*j);
    }
    to.swap(from);
    to.clear();
  }

  char buf[20];
  char *t = buf;
  uint64_t poolid(pool);
  t += snprintf(t, sizeof(buf), "%.*llX", 16, (long long unsigned)poolid);
  *(t++) = '.';
  string poolstr(buf, t - buf);
  set<string> ret;
  for (set<uint32_t>::iterator i = from.begin();
       i != from.end();
       ++i) {
    uint32_t revhash(hobject_t::_reverse_nibbles(*i));
    snprintf(buf, sizeof(buf), "%.*X", (int)(sizeof(revhash))*2, revhash);
    ret.insert(poolstr + string(buf, len/4));
  }
  return ret;
}

string hobject_t::to_str() const
{
  string out;

  char snap_with_hash[1000];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);

  uint64_t poolid(pool);
  t += snprintf(t, end - t, "%.*llX", 16, (long long unsigned)poolid);

  uint32_t revhash(get_filestore_key_u32());
  t += snprintf(t, end - t, ".%.*X", 8, revhash);

  if (snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, ".head");
  else if (snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, ".snapdir");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)snap);

  out += string(snap_with_hash);

  out.push_back('.');
  append_escaped(oid.name, &out);
  out.push_back('.');
  append_escaped(get_key(), &out);
  out.push_back('.');
  append_escaped(nspace, &out);

  return out;
}

void hobject_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(key, bl);
  ::encode(oid, bl);
  ::encode(snap, bl);
  ::encode(hash, bl);
  ::encode(max, bl);
  ::encode(nspace, bl);
  ::encode(pool, bl);
  ENCODE_FINISH(bl);
}

void hobject_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  if (struct_v >= 1)
    ::decode(key, bl);
  ::decode(oid, bl);
  ::decode(snap, bl);
  ::decode(hash, bl);
  if (struct_v >= 2)
    ::decode(max, bl);
  else
    max = false;
  if (struct_v >= 4) {
    ::decode(nspace, bl);
    ::decode(pool, bl);
  }
  DECODE_FINISH(bl);
}

void hobject_t::decode(json_spirit::Value& v)
{
  using namespace json_spirit;
  Object& o = v.get_obj();
  for (Object::size_type i=0; i<o.size(); i++) {
    Pair& p = o[i];
    if (p.name_ == "oid")
      oid.name = p.value_.get_str();
    else if (p.name_ == "key")
      key = p.value_.get_str();
    else if (p.name_ == "snapid")
      snap = p.value_.get_uint64();
    else if (p.name_ == "hash")
      hash = p.value_.get_int();
    else if (p.name_ == "max")
      max = p.value_.get_int();
  }
}

void hobject_t::dump(Formatter *f) const
{
  f->dump_string("oid", oid.name);
  f->dump_string("key", key);
  f->dump_int("snapid", snap);
  f->dump_int("hash", hash);
  f->dump_int("max", (int)max);
}

void hobject_t::generate_test_instances(list<hobject_t*>& o)
{
  o.push_back(new hobject_t);
  o.push_back(new hobject_t);
  o.back()->max = true;
  o.push_back(new hobject_t(object_t("oname"), string(), 1, 234, -1));
  o.push_back(new hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP, 67, 0));
  o.push_back(new hobject_t(object_t("oname3"), string("oname3"), CEPH_SNAPDIR, 910, 1));
}

ostream& operator<<(ostream& out, const hobject_t& o)
{
  if (o.is_max())
    return out << "MAX";
  out << std::hex << o.hash << std::dec;
  if (o.get_key().length())
    out << "." << o.get_key();
  out << "/" << o.oid << "/" << o.snap;
  out << "/" << o.nspace << "/" << o.pool;
  return out;
}
