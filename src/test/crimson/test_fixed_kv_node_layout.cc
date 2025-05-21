// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <iostream>

#include "gtest/gtest.h"

#include "crimson/common/fixed_kv_node_layout.h"

using namespace crimson;
using namespace crimson::common;

struct test_val_t {
  uint32_t t1 = 0;
  int32_t t2 = 0;

  bool operator==(const test_val_t &rhs) const {
    return rhs.t1 == t1 && rhs.t2 == t2;
  }
  bool operator!=(const test_val_t &rhs) const {
    return !(*this == rhs);
  }
};

struct test_val_le_t {
  ceph_le32 t1{0};
  ceph_les32 t2{0};

  test_val_le_t() = default;
  test_val_le_t(const test_val_le_t &) = default;
  test_val_le_t(const test_val_t &nv)
    : t1(nv.t1), t2(nv.t2) {}

  operator test_val_t() const {
    return test_val_t{t1, t2};
  }
};

struct test_meta_t {
  uint32_t t1 = 0;
  uint32_t t2 = 0;

  bool operator==(const test_meta_t &rhs) const {
    return rhs.t1 == t1 && rhs.t2 == t2;
  }
  bool operator!=(const test_meta_t &rhs) const {
    return !(*this == rhs);
  }

  std::pair<test_meta_t, test_meta_t> split_into(uint32_t pivot) const {
    return std::make_pair(
      test_meta_t{t1, pivot},
      test_meta_t{pivot, t2});
  }

  static test_meta_t merge_from(const test_meta_t &lhs, const test_meta_t &rhs) {
    return test_meta_t{lhs.t1, rhs.t2};
  }

  static std::pair<test_meta_t, test_meta_t>
  rebalance(const test_meta_t &lhs, const test_meta_t &rhs, uint32_t pivot) {
    return std::make_pair(
      test_meta_t{lhs.t1, pivot},
      test_meta_t{pivot, rhs.t2});
  }
};

struct test_meta_le_t {
  ceph_le32 t1{0};
  ceph_le32 t2{0};

  test_meta_le_t() = default;
  test_meta_le_t(const test_meta_le_t &) = default;
  test_meta_le_t(const test_meta_t &nv)
    : t1(nv.t1), t2(nv.t2) {}

  operator test_meta_t() const {
    return test_meta_t{t1, t2};
  }
};

constexpr size_t CAPACITY = 339;

struct TestNode : FixedKVNodeLayout<
  CAPACITY,
  test_meta_t, test_meta_le_t,
  uint32_t, ceph_le32,
  test_val_t, test_val_le_t> {
  char buf[4096];
  TestNode() : FixedKVNodeLayout() {
    set_layout_buf(buf);
    memset(buf, 0, sizeof(buf));
    set_meta({0, std::numeric_limits<uint32_t>::max()});
  }
  TestNode(const TestNode &rhs)
    : FixedKVNodeLayout() {
    set_layout_buf(buf);
    ::memcpy(buf, rhs.buf, sizeof(buf));
  }

  TestNode &operator=(const TestNode &rhs) {
    memcpy(buf, rhs.buf, sizeof(buf));
    return *this;
  }
};

TEST(FixedKVNodeTest, basic) {
  auto node = TestNode();
  ASSERT_EQ(node.get_size(), 0);

  auto val = test_val_t{ 1, 1 };
  node.journal_insert(node.begin(), 1, val, nullptr);
  ASSERT_EQ(node.get_size(), 1);

  auto iter = node.begin();
  ASSERT_EQ(iter.get_key(), 1);
  ASSERT_EQ(val, iter.get_val());

  ASSERT_EQ(std::numeric_limits<uint32_t>::max(), iter.get_next_key_or_max());
}

TEST(FixedKVNodeTest, at_capacity) {
  auto node = TestNode();
  ASSERT_EQ(CAPACITY, node.get_capacity());

  ASSERT_EQ(node.get_size(), 0);

  unsigned short num = 0;
  auto iter = node.begin();
  while (num < CAPACITY) {
    node.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }
  ASSERT_EQ(node.get_size(), CAPACITY);

  num = 0;
  for (auto &i : node) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < (CAPACITY - 1)) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
}

TEST(FixedKVNodeTest, split) {
  auto node = TestNode();

  ASSERT_EQ(node.get_size(), 0);

  unsigned short num = 0;
  auto iter = node.begin();
  while (num < CAPACITY) {
    node.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }
  ASSERT_EQ(node.get_size(), CAPACITY);

  auto split_left = TestNode();
  auto split_right = TestNode();
  node.split_into(split_left, split_right);

  ASSERT_EQ(split_left.get_size() + split_right.get_size(), CAPACITY);
  ASSERT_EQ(split_left.get_meta().t1, split_left.begin()->get_key());
  ASSERT_EQ(split_left.get_meta().t2, split_right.get_meta().t1);
  ASSERT_EQ(split_right.get_meta().t2, std::numeric_limits<uint32_t>::max());

  num = 0;
  for (auto &i : split_left) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < split_left.get_size() - 1) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
  for (auto &i : split_right) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < CAPACITY - 1) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
  ASSERT_EQ(num, CAPACITY);
}

TEST(FixedKVNodeTest, merge) {
  auto node = TestNode();
  auto node2 = TestNode();

  ASSERT_EQ(node.get_size(), 0);
  ASSERT_EQ(node2.get_size(), 0);

  unsigned short num = 0;
  auto iter = node.begin();
  while (num < CAPACITY/2) {
    node.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }
  node.set_meta({0, num});
  node2.set_meta({num, std::numeric_limits<uint32_t>::max()});
  iter = node2.begin();
  while (num < (2 * (CAPACITY / 2))) {
    node2.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }

  ASSERT_EQ(node.get_size(), CAPACITY / 2);
  ASSERT_EQ(node2.get_size(), CAPACITY / 2);

  auto total = node.get_size() + node2.get_size();

  auto node_merged = TestNode();
  node_merged.merge_from(node, node2);

  ASSERT_EQ(
    node_merged.get_meta(),
    (test_meta_t{0, std::numeric_limits<uint32_t>::max()}));

  ASSERT_EQ(node_merged.get_size(), total);
  num = 0;
  for (auto &i : node_merged) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < node_merged.get_size() - 1) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
  ASSERT_EQ(num, total);
}

void run_balance_test(unsigned left, unsigned right)
{
  auto node = TestNode();
  auto node2 = TestNode();

  ASSERT_EQ(node.get_size(), 0);
  ASSERT_EQ(node2.get_size(), 0);

  unsigned short num = 0;
  auto iter = node.begin();
  while (num < left) {
    node.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }
  node.set_meta({0, num});
  node2.set_meta({num, std::numeric_limits<uint32_t>::max()});
  iter = node2.begin();
  while (num < (left + right)) {
    node2.journal_insert(iter, num, test_val_t{num, num}, nullptr);
    ++num;
    ++iter;
  }

  ASSERT_EQ(node.get_size(), left);
  ASSERT_EQ(node2.get_size(), right);

  auto total = node.get_size() + node2.get_size();

  auto node_balanced = TestNode();
  auto node_balanced2 = TestNode();
  auto pivot_idx = TestNode::get_balance_pivot_idx(node, node2);
  auto pivot = TestNode::balance_into_new_nodes(
    node,
    node2,
    pivot_idx,
    node_balanced,
    node_balanced2);

  ASSERT_EQ(total, node_balanced.get_size() + node_balanced2.get_size());

  unsigned left_size, right_size;
  if (total % 2) {
      left_size = total/2;
      right_size = (total/2) + 1;
  } else {
    left_size = right_size = total/2;
  }
  ASSERT_EQ(pivot, left_size);
  ASSERT_EQ(left_size, node_balanced.get_size());
  ASSERT_EQ(right_size, node_balanced2.get_size());

  ASSERT_EQ(
    node_balanced.get_meta(),
    (test_meta_t{0, left_size}));
  ASSERT_EQ(
    node_balanced2.get_meta(),
    (test_meta_t{left_size, std::numeric_limits<uint32_t>::max()}));

  num = 0;
  for (auto &i: node_balanced) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < node_balanced.get_size() - 1) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
  for (auto &i: node_balanced2) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < total - 1) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
}

TEST(FixedKVNodeTest, balanced) {
  run_balance_test(CAPACITY / 2, CAPACITY);
  run_balance_test(CAPACITY, CAPACITY / 2);
  run_balance_test(CAPACITY - 1, CAPACITY / 2);
  run_balance_test(CAPACITY / 2, CAPACITY - 1);
  run_balance_test(CAPACITY / 2, CAPACITY / 2 + 2);
}

void run_replay_test(
  std::vector<std::function<void(TestNode&, TestNode::delta_buffer_t&)>> &&f
) {
  TestNode node;
  for (unsigned i = 0; i < f.size(); ++i) {
    TestNode::delta_buffer_t buf;
    TestNode replayed = node;
    f[i](node, buf);
    buf.replay(replayed);
    ASSERT_EQ(node.get_size(), replayed.get_size());
    ASSERT_EQ(node, replayed);
  }
}

TEST(FixedKVNodeTest, replay) {
  run_replay_test({
      [](auto &n, auto &b) {
	n.journal_insert(n.lower_bound(1), 1, test_val_t{1, 1}, &b);
	ASSERT_EQ(1, n.get_size());
      },
      [](auto &n, auto &b) {
	n.journal_insert(n.lower_bound(3), 3, test_val_t{1, 2}, &b);
	ASSERT_EQ(2, n.get_size());
      },
      [](auto &n, auto &b) {
	n.journal_remove(n.find(3), &b);
	ASSERT_EQ(1, n.get_size());
      },
      [](auto &n, auto &b) {
	n.journal_insert(n.lower_bound(2), 2, test_val_t{5, 1}, &b);
	ASSERT_EQ(2, n.get_size());
      }
  });

}
