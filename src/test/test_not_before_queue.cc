// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "common/not_before_queue.h"
#include "gtest/gtest.h"

// Just to have a default constructor that sets it to 0
struct test_time_t {
  unsigned time = 0;

  operator unsigned() const { return time; }
  test_time_t() = default;
  test_time_t(unsigned t) : time(t) {}
  test_time_t &operator=(unsigned t) {
    time = t;
    return *this;
  }
};

struct tv_t {
  unsigned not_before = 0;
  unsigned ordering_value = 0;
  unsigned removal_class = 0;

  tv_t() = default;
  tv_t(const tv_t &) = default;
  tv_t(unsigned not_before, unsigned ov, unsigned rc)
    : not_before(not_before), ordering_value(ov), removal_class(rc) {}

  auto to_tuple() const {
    return std::make_tuple(not_before, ordering_value, removal_class);
  }
  bool operator==(const tv_t &rhs) const {
    return to_tuple() == rhs.to_tuple();
  }
};

std::ostream &operator<<(std::ostream &lhs, const tv_t &val) {
  return lhs << val.to_tuple();
}

const unsigned &project_not_before(const tv_t &v) {
  return v.not_before;
}

const unsigned &project_removal_class(const tv_t &v) {
  return v.removal_class;
}

bool operator<(const tv_t &lhs, const tv_t &rhs) {
  return lhs.ordering_value < rhs.ordering_value;
}

class NotBeforeTest : public testing::Test {
protected:
  not_before_queue_t<tv_t, test_time_t> queue;

  void dump() {
    std::cout << "Dumping queue: " << std::endl;
    queue.for_each([](auto v, bool eligible) {
      std::cout << "    item: " << v << ", eligible: " << eligible << std::endl;
    });
  }
};

TEST_F(NotBeforeTest, Basic) {
  tv_t e0{0, 0, 0};
  tv_t e1{0, 1, 0};

  queue.enqueue(e0);
  queue.enqueue(e1);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e1));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}

TEST_F(NotBeforeTest, NotBefore) {
  tv_t e0{0, 0, 0};
  tv_t e1{1, 1, 0};
  tv_t e2{1, 2, 0};
  tv_t e3{1, 3, 0};
  tv_t e4{0, 4, 0};
  tv_t e5{0, 5, 0};

  queue.enqueue(e5);
  queue.enqueue(e1);
  queue.enqueue(e3);
  queue.enqueue(e0);
  queue.enqueue(e2);
  queue.enqueue(e4);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e4));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e5));
  ASSERT_EQ(queue.dequeue(), std::nullopt);

  queue.advance_time(1);

  EXPECT_EQ(queue.dequeue(), std::make_optional(e1));
  EXPECT_EQ(queue.dequeue(), std::make_optional(e2));
  EXPECT_EQ(queue.dequeue(), std::make_optional(e3));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}

TEST_F(NotBeforeTest, RemoveByClass) {
  tv_t e0{0, 0, 1};
  tv_t e1{1, 1, 0};
  tv_t e2{1, 2, 1};
  tv_t e3{1, 3, 1};
  tv_t e4{0, 4, 1};
  tv_t e5{0, 5, 0};

  queue.enqueue(e5);
  queue.enqueue(e1);
  queue.enqueue(e3);
  queue.enqueue(e0);
  queue.enqueue(e2);
  queue.enqueue(e4);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));

  queue.remove_by_class(1u);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e5));
  ASSERT_EQ(queue.dequeue(), std::nullopt);

  queue.advance_time(1u);

  EXPECT_EQ(queue.dequeue(), std::make_optional(e1));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}



