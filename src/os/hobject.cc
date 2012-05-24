
#include "include/types.h"
#include "hobject.h"
#include "common/Formatter.h"

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
