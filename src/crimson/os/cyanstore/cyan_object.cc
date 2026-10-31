#include "cyan_object.h"
#include <algorithm>

#include "include/encoding.h"

namespace crimson::os {

size_t Object::get_size() const {
  return size;
}

ceph::bufferptr& Object::get_writable_page(uint64_t idx)
{
  auto [p, inserted] = pages.try_emplace(idx);
  if (inserted) {
    p->second = ceph::bufferptr(ceph::buffer::create_page_aligned(DATA_PAGE_SIZE));
    p->second.zero(false);
  } else if (p->second.raw_nref() > 1) {
    // still referenced by a read result or a clone: copy before writing
    ceph::bufferptr copy(ceph::buffer::create_page_aligned(DATA_PAGE_SIZE));
    copy.copy_in(0, DATA_PAGE_SIZE, p->second.c_str(), false);
    p->second = std::move(copy);
  }
  return p->second;
}

void Object::zero_tail(uint64_t from)
{
  if (auto off = from % DATA_PAGE_SIZE; off && pages.contains(from / DATA_PAGE_SIZE)) {
    get_writable_page(from / DATA_PAGE_SIZE).zero(off, DATA_PAGE_SIZE - off, false);
  }
}

ceph::bufferlist Object::read(uint64_t offset, uint64_t len) const
{
  bufferlist ret;
  for (uint64_t pos = offset, end = offset + len; pos < end;) {
    uint64_t off = pos % DATA_PAGE_SIZE;
    uint64_t n = std::min(DATA_PAGE_SIZE - off, end - pos);
    if (auto p = pages.find(pos / DATA_PAGE_SIZE); p != pages.end()) {
      ret.append(ceph::bufferptr(p->second, off, n));
    } else {
      ret.append_zero(n);
    }
    pos += n;
  }
  return ret;
}

int Object::write(uint64_t offset, const bufferlist &src)
{
  auto it = src.cbegin();
  const uint64_t end = offset + src.length();
  for (uint64_t pos = offset; pos < end;) {
    uint64_t off = pos % DATA_PAGE_SIZE;
    uint64_t n = std::min(DATA_PAGE_SIZE - off, end - pos);
    it.copy(n, get_writable_page(pos / DATA_PAGE_SIZE).c_str() + off);
    pos += n;
  }
  size = std::max(size, end);
  return 0;
}

int Object::clone(Object *src, uint64_t srcoff, uint64_t len,
                  uint64_t dstoff)
{
  if (srcoff == dstoff && len == src->get_size()) {
    // pages are shared and copied on the next write to either object
    pages = src->pages;
    size = src->size;
    return 0;
  }
  return write(dstoff, src->read(srcoff, len));
}

int Object::truncate(uint64_t new_size)
{
  if (new_size < size) {
    pages.erase(pages.lower_bound((new_size + DATA_PAGE_SIZE - 1) / DATA_PAGE_SIZE),
                pages.end());
    zero_tail(new_size);
  }
  size = new_size;
  return 0;
}

void Object::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  bufferlist data = read(0, size);
  encode(data, bl);
  encode(xattr, bl);
  encode(omap_header, bl);
  encode(omap, bl);
  ENCODE_FINISH(bl);
}

void Object::decode(bufferlist::const_iterator& p) {
  DECODE_START(1, p);
  bufferlist data;
  decode(data, p);
  pages.clear();
  size = 0;
  write(0, data);
  decode(xattr, p);
  decode(omap_header, p);
  decode(omap, p);
  DECODE_FINISH(p);
}

}
