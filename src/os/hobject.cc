
#include "include/types.h"
#include "hobject.h"
#include "common/Formatter.h"

void hobject_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(key, bl);
  ::encode(oid, bl);
  ::encode(snap, bl);
  ::encode(hash, bl);
  ::encode(max, bl);
  ENCODE_FINISH(bl);
}

void hobject_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 1)
    ::decode(key, bl);
  ::decode(oid, bl);
  ::decode(snap, bl);
  ::decode(hash, bl);
  if (struct_v >= 2)
    ::decode(max, bl);
  else
    max = false;
  DECODE_FINISH(bl);
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
  o.push_back(new hobject_t(object_t("oname"), string(), 1, 234));
  o.push_back(new hobject_t(object_t("oname2"), string("okey"), CEPH_NOSNAP, 67));
  o.push_back(new hobject_t(object_t("oname3"), string("oname3"), CEPH_SNAPDIR, 910));
}

ostream& operator<<(ostream& out, const hobject_t& o)
{
  if (o.is_max())
    return out << "MAX";
  out << std::hex << o.hash << std::dec;
  if (o.get_key().length())
    out << "." << o.get_key();
  out << "/" << o.oid << "/" << o.snap;
  return out;
}
