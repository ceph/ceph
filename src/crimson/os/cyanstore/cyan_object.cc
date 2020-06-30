#include "cyan_object.h"
#include "include/encoding.h"

namespace crimson::os {

size_t Object::get_size() const {
  return data.length();
}

ceph::bufferlist Object::read(uint64_t offset, uint64_t len)
{
  bufferlist ret;
  ret.substr_of(data, offset, len);
  return ret;
}

int Object::write(uint64_t offset, const bufferlist &src)
{
  unsigned len = src.length();
  // before
  bufferlist newdata;
  if (get_size() >= offset) {
    newdata.substr_of(data, 0, offset);
  } else {
    if (get_size()) {
      newdata.substr_of(data, 0, get_size());
    }
    newdata.append_zero(offset - get_size());
  }

  newdata.append(src);

  // after
  if (get_size() > offset + len) {
    bufferlist tail;
    tail.substr_of(data, offset + len, get_size() - (offset + len));
    newdata.append(tail);
  }

  data = std::move(newdata);
  return 0;
}

int Object::clone(Object *src, uint64_t srcoff, uint64_t len,
                  uint64_t dstoff)
{
  bufferlist bl;
  if (srcoff == dstoff && len == src->get_size()) {
    data = src->data;
    return 0;
  }
  bl.substr_of(src->data, srcoff, len);
  return write(dstoff, bl);

}

int Object::truncate(uint64_t size)
{
  if (get_size() > size) {
    bufferlist bl;
    bl.substr_of(data, 0, size);
    data = std::move(bl);
  } else if (get_size() == size) {
    // do nothing
  } else {
    data.append_zero(size - get_size());
  }
  return 0;
}

void Object::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(data, bl);
  encode(xattr, bl);
  encode(omap_header, bl);
  encode(omap, bl);
  ENCODE_FINISH(bl);
}

void Object::decode(bufferlist::const_iterator& p) {
  DECODE_START(1, p);
  decode(data, p);
  decode(xattr, p);
  decode(omap_header, p);
  decode(omap, p);
  DECODE_FINISH(p);
}

}
