// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/object_data_handler.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

#define MAX_OBJECT_SIZE (16<<20)
#define DEFAULT_OBJECT_DATA_RESERVATION (16<<20)
#define DEFAULT_OBJECT_METADATA_RESERVATION (16<<20)

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

class TestOnode final : public Onode {
  onode_layout_t layout;
  bool dirty = false;

public:
  TestOnode(uint32_t ddr, uint32_t dmr) : Onode(ddr, dmr) {}
  const onode_layout_t &get_layout() const final {
    return layout;
  }
  template <typename Func>
  void with_mutable_layout(Transaction &t, Func&& f) {
    f(layout);
  }
  bool is_alive() const {
    return true;
  }
  bool is_dirty() const { return dirty; }
  laddr_t get_hint() const final {return L_ADDR_MIN; }
  ~TestOnode() final = default;

  void update_onode_size(Transaction &t, uint32_t size) final {
    with_mutable_layout(t, [size](onode_layout_t &mlayout) {
      mlayout.size = size;
    });
  }

  void update_omap_root(Transaction &t, omap_root_t &oroot) final {
    with_mutable_layout(t, [&oroot](onode_layout_t &mlayout) {
      mlayout.omap_root.update(oroot);
    });
  }

  void update_xattr_root(Transaction &t, omap_root_t &xroot) final {
    with_mutable_layout(t, [&xroot](onode_layout_t &mlayout) {
      mlayout.xattr_root.update(xroot);
    });
  }

  void update_object_data(Transaction &t, object_data_t &odata) final {
    with_mutable_layout(t, [&odata](onode_layout_t &mlayout) {
      mlayout.object_data.update(odata);
    });
  }

  void update_object_info(Transaction &t, ceph::bufferlist &oi_bl) final {
    with_mutable_layout(t, [&oi_bl](onode_layout_t &mlayout) {
      maybe_inline_memcpy(
	&mlayout.oi[0],
	oi_bl.c_str(),
	oi_bl.length(),
	onode_layout_t::MAX_OI_LENGTH);
      mlayout.oi_size = oi_bl.length();
    });
  }

  void clear_object_info(Transaction &t) final {
    with_mutable_layout(t, [](onode_layout_t &mlayout) {
      memset(&mlayout.oi[0], 0, mlayout.oi_size);
      mlayout.oi_size = 0;
    });
  }

  void update_snapset(Transaction &t, ceph::bufferlist &ss_bl) final {
    with_mutable_layout(t, [&ss_bl](onode_layout_t &mlayout) {
      maybe_inline_memcpy(
	&mlayout.ss[0],
	ss_bl.c_str(),
	ss_bl.length(),
	onode_layout_t::MAX_OI_LENGTH);
      mlayout.ss_size = ss_bl.length();
    });
  }

  void clear_snapset(Transaction &t) final {
    with_mutable_layout(t, [](onode_layout_t &mlayout) {
      memset(&mlayout.ss[0], 0, mlayout.ss_size);
      mlayout.ss_size = 0;
    });
  }

};

struct object_data_handler_test_t:
  public seastar_test_suite_t,
  TMTestState {
  OnodeRef onode;

  bufferptr known_contents;
  extent_len_t size = 0;
  std::random_device rd;
  std::mt19937 gen;

  object_data_handler_test_t() : gen(rd()) {}

  void write(Transaction &t, objaddr_t offset, extent_len_t len, char fill) {
    ceph_assert(offset + len <= known_contents.length());
    size = std::max<extent_len_t>(size, offset + len);
    Option::size_t olen = crimson::common::local_conf().get_val<Option::size_t>(
      "seastore_data_delta_based_overwrite");
    ceph_assert(olen == 0 || len <= olen);
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
    with_trans_intr(t, [&](auto &t) {
      return seastar::do_with(
	std::move(bl),
	ObjectDataHandler(MAX_OBJECT_SIZE),
	[=, this, &t](auto &bl, auto &objhandler) {
	  return objhandler.write(
	    ObjectDataHandler::context_t{
	      *tm,
	      t,
	      *onode,
	    },
	    offset,
	    bl);
	});
    }).unsafe_get0();
  }
  void write(objaddr_t offset, extent_len_t len, char fill) {
    auto t = create_mutate_transaction();
    write(*t, offset, len, fill);
    return submit_transaction(std::move(t));
  }

  void truncate(Transaction &t, objaddr_t offset) {
    if (size > offset) {
      memset(
	known_contents.c_str() + offset,
	0,
	size - offset);
      with_trans_intr(t, [&](auto &t) {
      return seastar::do_with(
	ObjectDataHandler(MAX_OBJECT_SIZE),
	[=, this, &t](auto &objhandler) {
	  return objhandler.truncate(
	    ObjectDataHandler::context_t{
	      *tm,
	      t,
	      *onode
	    },
	    offset);
	});
      }).unsafe_get0();
    }
    size = offset;
  }
  void truncate(objaddr_t offset) {
    auto t = create_mutate_transaction();
    truncate(*t, offset);
    return submit_transaction(std::move(t));
  }

  void read(Transaction &t, objaddr_t offset, extent_len_t len) {
    bufferlist bl = with_trans_intr(t, [&](auto &t) {
      return ObjectDataHandler(MAX_OBJECT_SIZE).read(
        ObjectDataHandler::context_t{
          *tm,
          t,
          *onode
        },
        offset,
        len);
    }).unsafe_get0();
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
    auto t = create_read_transaction();
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
  std::list<LBAMappingRef> get_mappings(
    Transaction &t,
    objaddr_t offset,
    extent_len_t length) {
    auto ret = with_trans_intr(t, [&](auto &t) {
      return tm->get_pins(t, offset, length);
    }).unsafe_get0();
    return ret;
  }
  std::list<LBAMappingRef> get_mappings(objaddr_t offset, extent_len_t length) {
    auto t = create_mutate_transaction();
    auto ret = with_trans_intr(*t, [&](auto &t) {
      return tm->get_pins(t, offset, length);
    }).unsafe_get0();
    return ret;
  }

  using remap_entry = TransactionManager::remap_entry;
  LBAMappingRef remap_pin(
    Transaction &t,
    LBAMappingRef &&opin,
    extent_len_t new_offset,
    extent_len_t new_len) {
    auto pin = with_trans_intr(t, [&](auto& trans) {
      return tm->remap_pin<ObjectDataBlock>(
        trans, std::move(opin), std::array{
          remap_entry(new_offset, new_len)}
      ).si_then([](auto ret) {
        return std::move(ret[0]);
      });
    }).handle_error(crimson::ct_error::eagain::handle([] {
      LBAMappingRef t = nullptr;
      return t;
    }), crimson::ct_error::pass_further_all{}).unsafe_get0();
    EXPECT_TRUE(pin);
    return pin;
  }

  ObjectDataBlockRef get_extent(
    Transaction &t,
    laddr_t addr,
    extent_len_t len) {
    auto ext = with_trans_intr(t, [&](auto& trans) {
	return tm->read_extent<ObjectDataBlock>(trans, addr, len);
	}).unsafe_get0();
    EXPECT_EQ(addr, ext->get_laddr());
    return ext;
  }

  seastar::future<> set_up_fut() final {
    onode = new TestOnode(
      DEFAULT_OBJECT_DATA_RESERVATION,
      DEFAULT_OBJECT_METADATA_RESERVATION);
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

  void enable_delta_based_overwrite() {
    crimson::common::local_conf().set_val("seastore_data_delta_based_overwrite",
      "16777216").get();
  }
  void disable_delta_based_overwrite() {
    crimson::common::local_conf().set_val("seastore_data_delta_based_overwrite", "0").get();
  }

  void disable_max_extent_size() {
    epm->set_max_extent_size(16777216);
    crimson::common::local_conf().set_val(
      "seastore_max_data_allocation_size", "16777216").get();
  }
  void enable_max_extent_size() {
    epm->set_max_extent_size(8192);
    crimson::common::local_conf().set_val(
      "seastore_max_data_allocation_size", "8192").get();
  }

  laddr_t get_random_laddr(size_t block_size, laddr_t limit) {
    return block_size *
      std::uniform_int_distribution<>(0, (limit / block_size) - 1)(gen);
  }

  void test_multi_write() {
    write((1<<20) - (4<<10), 4<<10, 'a');
    write(1<<20, 4<<10, 'b');
    write((1<<20) + (4<<10), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20)-(4<<10), 12<<10, 1);
    read_near((1<<20)-(4<<10), 12<<10, 512);
  }

  void test_write_hole() {
    write((1<<20) - (4<<10), 4<<10, 'a');
    // hole at 1<<20
    write((1<<20) + (4<<10), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20)-(4<<10), 12<<10, 1);
    read_near((1<<20)-(4<<10), 12<<10, 512);
  }

  void test_overwrite_single() {
    write((1<<20), 4<<10, 'a');
    write((1<<20), 4<<10, 'c');

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);
  }

  void test_overwrite_double() {
    write((1<<20), 4<<10, 'a');
    write((1<<20)+(4<<10), 4<<10, 'c');
    write((1<<20), 8<<10, 'b');

    read_near(1<<20, 8<<10, 1);
    read_near(1<<20, 8<<10, 512);

    read_near(1<<20, 4<<10, 1);
    read_near(1<<20, 4<<10, 512);

    read_near((1<<20) + (4<<10), 4<<10, 1);
    read_near((1<<20) + (4<<10), 4<<10, 512);
  }

  void test_overwrite_partial() {
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
  }

  void test_unaligned_write() {
    objaddr_t base = 1<<20;
    write(base, (4<<10)+(1<<10), 'a');
    read_near(base-(4<<10), 12<<10, 512);

    base = (1<<20) + (64<<10);
    write(base+(1<<10), (4<<10)+(1<<10), 'b');
    read_near(base-(4<<10), 12<<10, 512);

    base = (1<<20) + (128<<10);
    write(base-(1<<10), (4<<10)+(2<<20), 'c');
    read_near(base-(4<<10), 12<<10, 512);
  }

  void test_unaligned_overwrite() {
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
  }

  void test_truncate() {
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
  }

  void write_same() {
    write(0, 8<<10, 'x');
    write(0, 8<<10, 'a');

    auto pins = get_mappings(0, 8<<10);
    EXPECT_EQ(pins.size(), 1);

    read(0, 8<<10);
  }

  void write_right() {
    write(0, 128<<10, 'x');
    write(64<<10, 60<<10, 'a');
  }

  void write_left() {
    write(0, 128<<10, 'x');
    write(4<<10, 60<<10, 'a');
  }

  void write_right_left() {
    write(0, 128<<10, 'x');
    write(48<<10, 32<<10, 'a');
  }

  void multiple_write() {
    write(0, 128<<10, 'x');

    auto t = create_mutate_transaction();
    // normal split
    write(*t, 120<<10, 4<<10, 'a');
    // not aligned right
    write(*t, 4<<10, 5<<10, 'b');
    // split right extent of last split result
    write(*t, 32<<10, 4<<10, 'c');
    // non aligned overwrite
    write(*t, 13<<10, 4<<10, 'd');

    write(*t, 64<<10, 32<<10, 'e');
    // not split right
    write(*t, 60<<10, 8<<10, 'f');

    submit_transaction(std::move(t));
  }
};

TEST_P(object_data_handler_test_t, single_write)
{
  run_async([this] {
    write(1<<20, 8<<10, 'c');

    read_near(1<<20, 8<<10, 1);
    read_near(1<<20, 8<<10, 512);
  });
}

TEST_P(object_data_handler_test_t, multi_write)
{
  run_async([this] {
    test_multi_write();
  });
}

TEST_P(object_data_handler_test_t, delta_over_multi_write)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_multi_write();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, write_hole)
{
  run_async([this] {
    test_write_hole();
  });
}

TEST_P(object_data_handler_test_t, delta_over_write_hole)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_write_hole();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, overwrite_single)
{
  run_async([this] {
    test_overwrite_single();
  });
}

TEST_P(object_data_handler_test_t, delta_over_overwrite_single)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_overwrite_single();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, overwrite_double)
{
  run_async([this] {
    test_overwrite_double();
  });
}

TEST_P(object_data_handler_test_t, delta_over_overwrite_double)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_overwrite_double();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, overwrite_partial)
{
  run_async([this] {
    test_overwrite_partial();
  });
}

TEST_P(object_data_handler_test_t, delta_over_overwrite_partial)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_overwrite_partial();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, unaligned_write)
{
  run_async([this] {
    test_unaligned_write();
  });
}

TEST_P(object_data_handler_test_t, delta_over_unaligned_write)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_unaligned_write();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, unaligned_overwrite)
{
  run_async([this] {
    test_unaligned_overwrite();
  });
}

TEST_P(object_data_handler_test_t, delta_over_unaligned_overwrite)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_unaligned_overwrite();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, truncate)
{
  run_async([this] {
    test_truncate();
  });
}

TEST_P(object_data_handler_test_t, delta_over_truncate)
{
  run_async([this] {
    enable_delta_based_overwrite();
    test_truncate();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, no_remap) {
  run_async([this] {
    write_same();
  });
}

TEST_P(object_data_handler_test_t, no_overwrite) {
  run_async([this] {
    enable_delta_based_overwrite();
    write_same();
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, remap_left) {
  run_async([this] {
    disable_max_extent_size();
    write_right();

    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 2);

    size_t res[2] = {0, 64<<10};
    auto base = pins.front()->get_key();
    int i = 0;
    for (auto &pin : pins) {
      EXPECT_EQ(pin->get_key() - base, res[i]);
      i++;
    }
    read(0, 128<<10);
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, overwrite_right) {
  run_async([this] {
    disable_max_extent_size();
    enable_delta_based_overwrite();
    write_right();

    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 1);
    read(0, 128<<10);
    disable_delta_based_overwrite();
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, remap_right) {
  run_async([this] {
    disable_max_extent_size();
    write_left();

    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 2);

    size_t res[2] = {0, 64<<10};
    auto base = pins.front()->get_key();
    int i = 0;
    for (auto &pin : pins) {
      EXPECT_EQ(pin->get_key() - base, res[i]);
      i++;
    }
    read(0, 128<<10);
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, overwrite_left) {
  run_async([this] {
    disable_max_extent_size();
    enable_delta_based_overwrite();
    write_left();
    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 1);
    read(0, 128<<10);
    disable_delta_based_overwrite();
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, remap_right_left) {
  run_async([this] {
    disable_max_extent_size();
    write_right_left();

    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 3);

    size_t res[3] = {0, 48<<10, 80<<10};
    auto base = pins.front()->get_key();
    int i = 0;
    for (auto &pin : pins) {
      EXPECT_EQ(pin->get_key() - base, res[i]);
      i++;
    }
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, overwrite_right_left) {
  run_async([this] {
    disable_max_extent_size();
    enable_delta_based_overwrite();
    write_right_left();
    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 1);
    read(0, 128<<10);
    disable_delta_based_overwrite();
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, multiple_remap) {
  run_async([this] {
    disable_max_extent_size();
    multiple_write();
    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 3);

    size_t res[3] = {0, 120<<10, 124<<10};
    auto base = pins.front()->get_key();
    int i = 0;
    for (auto &pin : pins) {
      EXPECT_EQ(pin->get_key() - base, res[i]);
      i++;
    }
    read(0, 128<<10);
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, multiple_overwrite) {
  run_async([this] {
    disable_max_extent_size();
    enable_delta_based_overwrite();
    multiple_write();
    auto pins = get_mappings(0, 128<<10);
    EXPECT_EQ(pins.size(), 1);
    read(0, 128<<10);
    disable_delta_based_overwrite();
    enable_max_extent_size();
  });
}

TEST_P(object_data_handler_test_t, random_overwrite) {
  constexpr size_t TOTAL = 4<<20;
  constexpr size_t BSIZE = 4<<10;
  constexpr size_t BLOCKS = TOTAL / BSIZE;
  run_async([this] {
    enable_delta_based_overwrite();
    size_t wsize = std::uniform_int_distribution<>(10, BSIZE - 1)(gen);
    uint8_t div[3] = {1, 2, 4};
    uint8_t block_num = div[std::uniform_int_distribution<>(0, 2)(gen)];
    for (unsigned i = 0; i < BLOCKS / block_num; ++i) {
      auto t = create_mutate_transaction();
      write(i * (BSIZE * block_num), BSIZE * block_num, 'a');
    }

    for (unsigned i = 0; i < 4; ++i) {
      for (unsigned j = 0; j < 100; ++j) {
	auto t = create_mutate_transaction();
	for (unsigned k = 0; k < 2; ++k) {
	  write(*t, get_random_laddr(BSIZE, TOTAL), wsize,
	    (char)((j*k) % std::numeric_limits<char>::max()));
	}
	submit_transaction(std::move(t));
      }
      restart();
      epm->check_usage();
      logger().info("random_writes: {} done replaying/checking", i);
    }
    read(0, 4<<20);
    disable_delta_based_overwrite();
  });
}

TEST_P(object_data_handler_test_t, overwrite_then_read_within_transaction) {
  run_async([this] {
    disable_max_extent_size();
    enable_delta_based_overwrite();
    auto t = create_mutate_transaction();
    auto base = 4096 * 4;
    auto len = 4096 * 6;
    write(*t, base, len, 'a');
    submit_transaction(std::move(t));

    t = create_mutate_transaction();
    { 
      auto pins = get_mappings(*t, base, len);
      assert(pins.size() == 1);
      auto pin1 = remap_pin(*t, std::move(pins.front()), 4096, 8192);
      auto ext = get_extent(*t, base + 4096, 4096 * 2);
      ASSERT_TRUE(ext->is_exist_clean());
      write(*t, base + 4096, 4096, 'y');
      ASSERT_TRUE(ext->is_exist_mutation_pending());
      write(*t, base + 8092, 4096, 'z');
    }
    submit_transaction(std::move(t));
    read(base + 4096, 4096);
    read(base + 4096, 8192);
    restart();
    epm->check_usage();
    read(base + 4096, 8192);

    t = create_mutate_transaction();
    base = 0;
    len = 4096 * 3;
    write(*t, base, len, 'a');
    submit_transaction(std::move(t));

    t = create_mutate_transaction();
    write(*t, base + 4096, 4096, 'b');
    read(*t, base + 1024, 4096 + 1024);
    write(*t, base + 8192, 4096, 'c');
    read(*t, base + 2048, 8192);
    write(*t, base, 4096, 'd');
    write(*t, base + 4096, 4096, 'x');
    submit_transaction(std::move(t));
    read(base + 1024, 8192 - 1024);
    read(base, 4096 * 3);
    restart();
    epm->check_usage();
    read(base, 4096 * 3);

    auto t1 = create_mutate_transaction();
    write(*t1, base + 4096, 4096, 'e');
    read(*t1, base + 4096, 4096);
    auto t2 = create_read_transaction();
    bufferlist committed = with_trans_intr(*t2, [&](auto &t) {
      return ObjectDataHandler(MAX_OBJECT_SIZE).read(
        ObjectDataHandler::context_t{
          *tm,
          t,
          *onode
        },
        base + 4096,
        4096);
    }).unsafe_get0();
    bufferlist pending;
    pending.append(
      bufferptr(
	known_contents,
	base + 4096,
	4096));
    EXPECT_EQ(committed.length(), pending.length());
    EXPECT_NE(committed, pending);
    disable_delta_based_overwrite();
    enable_max_extent_size();
  });
}

INSTANTIATE_TEST_SUITE_P(
  object_data_handler_test,
  object_data_handler_test_t,
  ::testing::Combine(
    ::testing::Values (
      "segmented",
      "circularbounded"
    ),
    ::testing::Values(
      integrity_check_t::FULL_CHECK,
      integrity_check_t::NONFULL_CHECK)
  )
);
