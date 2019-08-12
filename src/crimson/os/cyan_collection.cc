#include "cyan_collection.h"

#include "cyan_object.h"

namespace ceph::os
{

Collection::Collection(const coll_t& c)
  : cid{c}
{}

Collection::~Collection() = default;

Collection::ObjectRef Collection::create_object() const
{
  return new ceph::os::Object{};
}

Collection::ObjectRef Collection::get_object(ghobject_t oid)
{
  auto o = object_hash.find(oid);
  if (o == object_hash.end())
    return ObjectRef();
  return o->second;
}

Collection::ObjectRef Collection::get_or_create_object(ghobject_t oid)
{
  auto result = object_hash.emplace(oid, ObjectRef{});
  if (result.second)
    object_map[oid] = result.first->second = create_object();
  return result.first->second;
}

uint64_t Collection::used_bytes() const
{
  uint64_t result = 0;
  for (auto& obj : object_map) {
    result += obj.second->get_size();
  }
  return result;
}

void Collection::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(xattr, bl);
  encode(use_page_set, bl);
  uint32_t s = object_map.size();
  encode(s, bl);
  for (auto& [oid, obj] : object_map) {
    encode(oid, bl);
    obj->encode(bl);
  }
  ENCODE_FINISH(bl);
}

void Collection::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(xattr, p);
  decode(use_page_set, p);
  uint32_t s;
  decode(s, p);
  while (s--) {
    ghobject_t k;
    decode(k, p);
    auto o = create_object();
    o->decode(p);
    object_map.insert(make_pair(k, o));
    object_hash.insert(make_pair(k, o));
  }
  DECODE_FINISH(p);
}

}
