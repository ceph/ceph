// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/object_data_handler.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

class TestOnode : public Onode {
  onode_layout_t layout;
  bool dirty = false;

public:
  const onode_layout_t &get_layout() const final {
    return layout;
  }
  onode_layout_t &get_mutable_layout(Transaction &t) final {
    dirty = true;
    return layout;
  }
  bool is_dirty() const { return dirty; }
  ~TestOnode() final = default;
};

struct object_data_handler_test_t:
  public seastar_test_suite_t,
  TMTestState {
  OnodeRef onode;

  bufferptr known_contents;
  extent_len_t size = 0;

  object_data_handler_test_t() {}

  void write(Transaction &t, objaddr_t offset, extent_len_t len, char fill) {
    ceph_assert(offset + len <= known_contents.length());
    size = std::max<extent_len_t>(size, offset + len);
    memset(
      known_contents.c_str() + offset,
      fill,
      len);
    bufferlist bl;
    bl.append(
      bufferptr(
	known_contents,
	offset,
	len));
    return ObjectDataHandler().write(
      ObjectDataHandler::context_t{
	itm,
	t,
	*onode,
      },
      offset,
      bl).unsafe_get0();
  }
  void write(objaddr_t offset, extent_len_t len, char fill) {
    auto t = tm->create_transaction();
    write(*t, offset, len, fill);
    return submit_transaction(std::move(t));
  }

  void truncate(Transaction &t, objaddr_t offset) {
    if (size > offset) {
      memset(
	known_contents.c_str() + offset,
	0,
	size - offset);
      ObjectDataHandler().truncate(
	ObjectDataHandler::context_t{
	  itm,
	  t,
	  *onode
	},
	offset).unsafe_get0();
    }
    size = offset;
  }
  void truncate(objaddr_t offset) {
    auto t = tm->create_transaction();
    truncate(*t, offset);
    return submit_transaction(std::move(t));
  }

  void read(Transaction &t, objaddr_t offset, extent_len_t len) {
    bufferlist bl = ObjectDataHandler().read(
      ObjectDataHandler::context_t{
	itm,
	t,
	*onode
      },
      offset,
      len).unsafe_get0();
    bufferlist known;
    known.append(
      bufferptr(
	known_contents,
	offset,
	len));
    EXPECT_EQ(bl.length(), known.length());
    EXPECT_EQ(bl, known);
  }
  void read(objaddr_t offset, extent_len_t len) {
    auto t = tm->create_transaction();
    read(*t, offset, len);
  }
  void read_near(objaddr_t offset, extent_len_t len, extent_len_t fuzz) {
    auto fuzzes = std::vector<int32_t>{-1 * (int32_t)fuzz, 0, (int32_t)fuzz};
    for (auto left_fuzz : fuzzes) {
      for (auto right_fuzz : fuzzes) {
	read(offset + left_fuzz, len - left_fuzz + right_fuzz);
      }
    }
  }

  seastar::future<> set_up_fut() final {
    onode = new TestOnode{};
    known_contents = buffer::create(4<<20 /* 4MB */);
    memset(known_contents.c_str(), 0, known_contents.length());
    size = 0;
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    onode.reset();
    size = 0;
    return tm_teardown();
  }
};

TEST_F(object_data_handler_test_t, single_write)
{
  run_async([this] {
    write(1<<20, 8<<10, 'c');

    read_near(1<<20, 8<<10, 1);
    read_near(1<<20, 8<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, multi_write)
{
  run_async([this] {
    write((1<<20) - (4<<10), 4<<10, 'a');
    write(1<<20, 4<<10, 'b');
    write((1<<20) + (4<<10), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20)-(4<<10), 12<<10, 1);
    read_near((1<<20)-(4<<10), 12<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, write_hole)
{
  run_async([this] {
    write((1<<20) - (4<<10), 4<<10, 'a');
    // hole at 1<<20
    write((1<<20) + (4<<10), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20)-(4<<10), 12<<10, 1);
    read_near((1<<20)-(4<<10), 12<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, overwrite_single)
{
  run_async([this] {
    write((1<<20), 4<<10, 'a');
    write((1<<20), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, overwrite_double)
{
  run_async([this] {
    write((1<<20), 4<<10, 'a');
    write((1<<20)+(4<<10), 4<<10, 'c');
    write((1<<20), 8<<10, 'b');

    read_near(1<<20, 8<<10, 1);
    read_near(1<<20, 8<<10, 512);

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20) + (4<<10), 4<<10, 1);
    read_near((1<<20) + (4<<10), 4<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, overwrite_partial)
{
  run_async([this] {
    write((1<<20), 12<<10, 'a');
    read_near(1<<20, 12<<10, 1);

    write((1<<20)+(8<<10), 4<<10, 'b');
    read_near(1<<20, 12<<10, 1);

    write((1<<20)+(4<<10), 4<<10, 'c');
    read_near(1<<20, 12<<10, 1);

    write((1<<20), 4<<10, 'd');

    read_near(1<<20, 12<<10, 1);
    read_near(1<<20, 12<<10, 512);

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20) + (4<<10), 4<<10, 1);
    read_near((1<<20) + (4<<10), 4<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, unaligned_write)
{
  run_async([this] {
    objaddr_t base = 1<<20;
    write(base, (4<<10)+(1<<10), 'a');
    read_near(base-(4<<10), 12<<10, 512);

    base = (1<<20) + (64<<10);
    write(base+(1<<10), (4<<10)+(1<<10), 'b');
    read_near(base-(4<<10), 12<<10, 512);

    base = (1<<20) + (128<<10);
    write(base-(1<<10), (4<<10)+(2<<20), 'c');
    read_near(base-(4<<10), 12<<10, 512);
  });
}

TEST_F(object_data_handler_test_t, unaligned_overwrite)
{
  run_async([this] {
    objaddr_t base = 1<<20;
    write(base, (128<<10) + (16<<10), 'x');

    write(base, (4<<10)+(1<<10), 'a');
    read_near(base-(4<<10), 12<<10, 2<<10);

    base = (1<<20) + (64<<10);
    write(base+(1<<10), (4<<10)+(1<<10), 'b');
    read_near(base-(4<<10), 12<<10, 2<<10);

    base = (1<<20) + (128<<10);
    write(base-(1<<10), (4<<10)+(2<<20), 'c');
    read_near(base-(4<<10), 12<<10, 2<<10);

    read(base, (128<<10) + (16<<10));
  });
}

TEST_F(object_data_handler_test_t, truncate)
{
  run_async([this] {
    objaddr_t base = 1<<20;
    write(base, 8<<10, 'a');
    write(base+(8<<10), 8<<10, 'b');
    write(base+(16<<10), 8<<10, 'c');

    truncate(base + (32<<10));
    read(base, 64<<10);

    truncate(base + (24<<10));
    read(base, 64<<10);

    truncate(base + (12<<10));
    read(base, 64<<10);

    truncate(base - (12<<10));
    read(base, 64<<10);
  });
}
