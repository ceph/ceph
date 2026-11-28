// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <string>

#include "gtest/gtest.h"

#include "crimson/os/cyanstore/cyan_object.h"
#include "include/buffer.h"

using ceph::bufferlist;
using crimson::os::Object;

namespace {

bufferlist filled(size_t len, char c)
{
  bufferlist bl;
  bl.append(std::string(len, c));
  return bl;
}

// the crc32c of a buffer list's data, computed afresh: in a new buffer,
// without any cached crc
uint32_t fresh_crc32c(const bufferlist& bl)
{
  bufferlist copy;
  copy.append(bl.to_str());
  return copy.crc32c(-1);
}

// read the whole object, and check that the crc32c of the read result (as
// the OSD checks it against the object's data digest) matches its data.
// The read result is released on return.
void check_read_crc(const Object& o, const bufferlist& expected)
{
  bufferlist r = o.read(0, o.get_size());
  ASSERT_TRUE(r.contents_equal(expected));
  EXPECT_EQ(r.crc32c(-1), fresh_crc32c(r));
  EXPECT_EQ(r.crc32c(-1), expected.crc32c(-1));
}

} // namespace

// a read result's crc32c() is cached in the object's page buffer; an
// overwrite in place must not leave it there (tracker 83662)
TEST(cyan_object, overwrite_after_read)
{
  for (size_t size : {size_t{4096}, size_t{Object::DATA_PAGE_SIZE},
                      size_t{Object::DATA_PAGE_SIZE + 4096}}) {
    SCOPED_TRACE(size);
    Object o;
    o.write(0, filled(size, 'A'));
    check_read_crc(o, filled(size, 'A'));
    // the page is not referenced by a read result anymore: written in place
    o.write(0, filled(size, 'B'));
    check_read_crc(o, filled(size, 'B'));
  }
}

// a source made of several buffers, written across a page boundary
TEST(cyan_object, overwrite_from_fragmented_source)
{
  const size_t size = 2 * Object::DATA_PAGE_SIZE;
  Object o;
  o.write(0, filled(size, 'A'));
  check_read_crc(o, filled(size, 'A'));

  bufferlist src;
  for (char c : {'x', 'y', 'z'}) {
    src.append(filled(3000, c));
  }
  const uint64_t offset = Object::DATA_PAGE_SIZE - 4000;
  o.write(offset, src);

  bufferlist expected;
  expected.append(filled(offset, 'A'));
  expected.append(src);
  expected.append(filled(size - offset - src.length(), 'A'));
  check_read_crc(o, expected);
}

// truncating zeroes the tail of the last page in place
TEST(cyan_object, truncate_after_read)
{
  Object o;
  o.write(0, filled(8192, 'A'));
  check_read_crc(o, filled(8192, 'A'));
  o.truncate(4096);
  o.truncate(8192);
  bufferlist expected = filled(4096, 'A');
  expected.append_zero(4096);
  check_read_crc(o, expected);
}

// a page still referenced by a read result is copied before a write: the
// read result keeps the data it read
TEST(cyan_object, read_result_is_not_overwritten)
{
  Object o;
  o.write(0, filled(4096, 'A'));
  bufferlist held = o.read(0, 4096);
  EXPECT_EQ(held.crc32c(-1), fresh_crc32c(held));
  o.write(0, filled(4096, 'B'));
  EXPECT_TRUE(held.contents_equal(filled(4096, 'A')));
  EXPECT_EQ(held.crc32c(-1), fresh_crc32c(held));
  check_read_crc(o, filled(4096, 'B'));
}
