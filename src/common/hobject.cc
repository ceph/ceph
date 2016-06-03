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

  uint32_t revhash(get_nibblewise_key_u32());
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
    // for compat with hammer, which did not handle the transition
    // from pool -1 -> pool INT64_MIN for MIN properly.  this object
    // name looks a bit like a pgmeta object for the meta collection,
    // but those do not ever exist (and is_pgmeta() pool >= 0).
    if (pool == -1 &&
	snap == 0 &&
	hash == 0 &&
	!max &&
	oid.name.empty()) {
      pool = INT64_MIN;
      assert(is_min());
    }

    // for compatibility with some earlier verisons which might encoded
    // a non-canonical max object
    if (max) {
      *this = hobject_t::get_max();
    }
  }
  DECODE_FINISH(bl);
  build_hash_cache();
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
    else if (p.name_ == "pool")
      pool = p.value_.get_int();
    else if (p.name_ == "namespace")
      nspace = p.value_.get_str();
  }
  build_hash_cache();
}

void hobject_t::dump(Formatter *f) const
{
  f->dump_string("oid", oid.name);
  f->dump_string("key", key);
  f->dump_int("snapid", snap);
  f->dump_int("hash", hash);
  f->dump_int("max", (int)max);
  f->dump_int("pool", pool);
  f->dump_string("namespace", nspace);
}

void hobject_t::generate_test_instances(list<hobject_t*>& o)
{
  o.push_back(new hobject_t);
  o.push_back(new hobject_t);
  o.back()->max = true;
  o.push_back(new hobject_t(object_t("oname"), string(), 1, 234, -1, ""));
  o.push_back(new hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP,
	67, 0, "n1"));
  o.push_back(new hobject_t(object_t("oname3"), string("oname3"),
	CEPH_SNAPDIR, 910, 1, "n2"));
}

static void append_out_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%' || *i == ':' || *i == '/' || *i < 32 || *i >= 127) {
      out->push_back('%');
      char buf[3];
      snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)*i);
      out->append(buf);
    } else {
      out->push_back(*i);
    }
  }
}

static const char *decode_out_escaped(const char *in, string *out)
{
  while (*in && *in != ':') {
    if (*in == '%') {
      ++in;
      char buf[3];
      buf[0] = *in;
      ++in;
      buf[1] = *in;
      buf[2] = 0;
      int v = strtol(buf, NULL, 16);
      out->push_back(v);
    } else {
      out->push_back(*in);
    }
    ++in;
  }
  return in;
}

ostream& operator<<(ostream& out, const hobject_t& o)
{
  if (o == hobject_t())
    return out << "MIN";
  if (o.is_max())
    return out << "MAX";
  out << o.pool << ':';
  out << std::hex;
  out.width(8);
  out.fill('0');
  out << o.get_bitwise_key_u32(); // << '~' << o.get_hash();
  out.width(0);
  out.fill(' ');
  out << std::dec;
  out << ':';
  string v;
  append_out_escaped(o.nspace, &v);
  v.push_back(':');
  append_out_escaped(o.get_key(), &v);
  v.push_back(':');
  append_out_escaped(o.oid.name, &v);
  out << v << ':' << o.snap;
  return out;
}

bool hobject_t::parse(const string &s)
{
  if (s == "MIN") {
    *this = hobject_t();
    return true;
  }
  if (s == "MAX") {
    *this = hobject_t::get_max();
    return true;
  }

  const char *start = s.c_str();
  long long po;
  unsigned h;
  int r = sscanf(start, "%lld:%x:", &po, &h);
  if (r != 2)
    return false;
  for (; *start && *start != ':'; ++start) ;
  for (++start; *start && isxdigit(*start); ++start) ;
  if (*start != ':')
    return false;

  string ns, k, name;
  const char *p = decode_out_escaped(start + 1, &ns);
  if (*p != ':')
    return false;
  p = decode_out_escaped(p + 1, &k);
  if (*p != ':')
    return false;
  p = decode_out_escaped(p + 1, &name);
  if (*p != ':')
    return false;
  start = p + 1;

  unsigned long long sn;
  if (strncmp(start, "head", 4) == 0) {
    sn = CEPH_NOSNAP;
    start += 4;
    if (*start != 0)
      return false;
  } else {
    r = sscanf(start, "%llx", &sn);
    if (r != 1)
      return false;
    for (++start; *start && isxdigit(*start); ++start) ;
    if (*start)
      return false;
  }

  max = false;
  pool = po;
  set_hash(_reverse_bits(h));
  nspace = ns;
  oid.name = name;
  set_key(k);
  snap = sn;
  return true;
}

int cmp_nibblewise(const hobject_t& l, const hobject_t& r)
{
  if (l.max < r.max)
    return -1;
  if (l.max > r.max)
    return 1;
  if (l.pool < r.pool)
    return -1;
  if (l.pool > r.pool)
    return 1;
  if (l.get_nibblewise_key() < r.get_nibblewise_key())
    return -1;
  if (l.get_nibblewise_key() > r.get_nibblewise_key())
    return 1;
  if (l.nspace < r.nspace)
    return -1;
  if (l.nspace > r.nspace)
    return 1;
  if (l.get_effective_key() < r.get_effective_key())
    return -1;
  if (l.get_effective_key() > r.get_effective_key())
    return 1;
  if (l.oid < r.oid)
    return -1;
  if (l.oid > r.oid)
    return 1;
  if (l.snap < r.snap)
    return -1;
  if (l.snap > r.snap)
    return 1;
  return 0;
}

int cmp_bitwise(const hobject_t& l, const hobject_t& r)
{
  if (l.max < r.max)
    return -1;
  if (l.max > r.max)
    return 1;
  if (l.pool < r.pool)
    return -1;
  if (l.pool > r.pool)
    return 1;
  if (l.get_bitwise_key() < r.get_bitwise_key())
    return -1;
  if (l.get_bitwise_key() > r.get_bitwise_key())
    return 1;
  if (l.nspace < r.nspace)
    return -1;
  if (l.nspace > r.nspace)
    return 1;
  if (l.get_effective_key() < r.get_effective_key())
    return -1;
  if (l.get_effective_key() > r.get_effective_key())
    return 1;
  if (l.oid < r.oid)
    return -1;
  if (l.oid > r.oid)
    return 1;
  if (l.snap < r.snap)
    return -1;
  if (l.snap > r.snap)
    return 1;
  return 0;
}



// This is compatible with decode for hobject_t prior to
// version 5.
void ghobject_t::encode(bufferlist& bl) const
{
  // when changing this, remember to update encoded_size() too.
  ENCODE_START(6, 3, bl);
  ::encode(hobj.key, bl);
  ::encode(hobj.oid, bl);
  ::encode(hobj.snap, bl);
  ::encode(hobj.hash, bl);
  ::encode(hobj.max, bl);
  ::encode(hobj.nspace, bl);
  ::encode(hobj.pool, bl);
  ::encode(generation, bl);
  ::encode(shard_id, bl);
  ::encode(max, bl);
  ENCODE_FINISH(bl);
}

size_t ghobject_t::encoded_size() const
{
  // this is not in order of encoding or appearance, but rather
  // in order of known constants first, so it can be (mostly) computed
  // at compile time.
  //  - encoding header + 3 string lengths
  size_t r = sizeof(ceph_le32) + 2 * sizeof(__u8) + 3 * sizeof(__u32);

  // hobj.snap
  r += sizeof(uint64_t);

  // hobj.hash
  r += sizeof(uint32_t);

  // hobj.max
  r += sizeof(bool);

  // hobj.pool
  r += sizeof(uint64_t);

  // hobj.generation
  r += sizeof(uint64_t);

  // hobj.shard_id
  r += sizeof(int8_t);

  // max
  r += sizeof(bool);

  // hobj.key
  r += hobj.key.size();

  // hobj.oid
  r += hobj.oid.name.size();

  // hobj.nspace
  r += hobj.nspace.size();

  return r;
}

void ghobject_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
  if (struct_v >= 1)
    ::decode(hobj.key, bl);
  ::decode(hobj.oid, bl);
  ::decode(hobj.snap, bl);
  ::decode(hobj.hash, bl);
  if (struct_v >= 2)
    ::decode(hobj.max, bl);
  else
    hobj.max = false;
  if (struct_v >= 4) {
    ::decode(hobj.nspace, bl);
    ::decode(hobj.pool, bl);
    // for compat with hammer, which did not handle the transition from
    // pool -1 -> pool INT64_MIN for MIN properly (see hobject_t::decode()).
    if (hobj.pool == -1 &&
	hobj.snap == 0 &&
	hobj.hash == 0 &&
	!hobj.max &&
	hobj.oid.name.empty()) {
      hobj.pool = INT64_MIN;
      assert(hobj.is_min());
    }
  }
  if (struct_v >= 5) {
    ::decode(generation, bl);
    ::decode(shard_id, bl);
  } else {
    generation = ghobject_t::NO_GEN;
    shard_id = shard_id_t::NO_SHARD;
  }
  if (struct_v >= 6) {
    ::decode(max, bl);
  } else {
    max = false;
  }
  DECODE_FINISH(bl);
  hobj.build_hash_cache();
}

void ghobject_t::decode(json_spirit::Value& v)
{
  hobj.decode(v);
  using namespace json_spirit;
  Object& o = v.get_obj();
  for (Object::size_type i=0; i<o.size(); i++) {
    Pair& p = o[i];
    if (p.name_ == "generation")
      generation = p.value_.get_uint64();
    else if (p.name_ == "shard_id")
      shard_id.id = p.value_.get_int();
    else if (p.name_ == "max")
      max = p.value_.get_int();
  }
}

void ghobject_t::dump(Formatter *f) const
{
  hobj.dump(f);
  if (generation != NO_GEN)
    f->dump_int("generation", generation);
  if (shard_id != shard_id_t::NO_SHARD)
    f->dump_int("shard_id", shard_id);
  f->dump_int("max", (int)max);
}

void ghobject_t::generate_test_instances(list<ghobject_t*>& o)
{
  o.push_back(new ghobject_t);
  o.push_back(new ghobject_t);
  o.back()->hobj.max = true;
  o.push_back(new ghobject_t(hobject_t(object_t("oname"), string(), 1, 234, -1, "")));

  o.push_back(new ghobject_t(hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP,
        67, 0, "n1"), 1, shard_id_t(0)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP,
        67, 0, "n1"), 1, shard_id_t(1)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP,
        67, 0, "n1"), 1, shard_id_t(2)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname3"), string("oname3"),
        CEPH_SNAPDIR, 910, 1, "n2"), 1, shard_id_t(0)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname3"), string("oname3"),
        CEPH_SNAPDIR, 910, 1, "n2"), 2, shard_id_t(0)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname3"), string("oname3"),
        CEPH_SNAPDIR, 910, 1, "n2"), 3, shard_id_t(0)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname3"), string("oname3"),
        CEPH_SNAPDIR, 910, 1, "n2"), 3, shard_id_t(1)));
  o.push_back(new ghobject_t(hobject_t(object_t("oname3"), string("oname3"),
        CEPH_SNAPDIR, 910, 1, "n2"), 3, shard_id_t(2)));
}

ostream& operator<<(ostream& out, const ghobject_t& o)
{
  if (o == ghobject_t())
    return out << "GHMIN";
  if (o.is_max())
    return out << "GHMAX";
  if (o.shard_id != shard_id_t::NO_SHARD)
    out << std::hex << o.shard_id << std::dec;
  out << '#' << o.hobj << '#';
  if (o.generation != ghobject_t::NO_GEN)
    out << std::hex << (unsigned long long)(o.generation) << std::dec;
  return out;
}

bool ghobject_t::parse(const string& s)
{
  if (s == "GHMIN") {
    *this = ghobject_t();
    return true;
  }
  if (s == "GHMAX") {
    *this = ghobject_t::get_max();
    return true;
  }

  // look for shard# prefix
  const char *start = s.c_str();
  const char *p;
  int sh = shard_id_t::NO_SHARD;
  for (p = start; *p && isxdigit(*p); ++p) ;
  if (!*p && *p != '#')
    return false;
  if (p > start) {
    int r = sscanf(s.c_str(), "%x", &sh);
    if (r < 1)
      return false;
    start = p + 1;
  } else {
    ++start;
  }

  // look for #generation suffix
  long long unsigned g = NO_GEN;
  const char *last = start + strlen(start) - 1;
  p = last;
  while (isxdigit(*p))
    p--;
  if (*p != '#')
    return false;
  if (p < last) {
    sscanf(p + 1, "%llx", &g);
  }

  string inner(start, p - start);
  hobject_t h;
  if (!h.parse(inner)) {
    return false;
  }

  shard_id = shard_id_t(sh);
  hobj = h;
  generation = g;
  max = false;
  return true;
}

int cmp_nibblewise(const ghobject_t& l, const ghobject_t& r)
{
  if (l.max < r.max)
    return -1;
  if (l.max > r.max)
    return 1;
  if (l.shard_id < r.shard_id)
    return -1;
  if (l.shard_id > r.shard_id)
    return 1;
  int ret = cmp_nibblewise(l.hobj, r.hobj);
  if (ret != 0)
    return ret;
  if (l.generation < r.generation)
    return -1;
  if (l.generation > r.generation)
    return 1;
  return 0;
}

int cmp_bitwise(const ghobject_t& l, const ghobject_t& r)
{
  if (l.max < r.max)
    return -1;
  if (l.max > r.max)
    return 1;
  if (l.shard_id < r.shard_id)
    return -1;
  if (l.shard_id > r.shard_id)
    return 1;
  int ret = cmp_bitwise(l.hobj, r.hobj);
  if (ret != 0)
    return ret;
  if (l.generation < r.generation)
    return -1;
  if (l.generation > r.generation)
    return 1;
  return 0;
}
