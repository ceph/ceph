#include "gtest/gtest.h"
#include "gtest/gtest-matchers.h"

#include "include/auto_shared_ptr.h"

using namespace std;

struct A {};
struct B: public enable_shared_from_this<B> {};
struct C: public B {};

template <class T>
struct validate {
  // the template will conditionally complie operations
  // that are only supported for classes that inherit from enable_shared_from_this.
  template <class Y>
    requires convertible_to<T*, const enable_shared_from_this<Y>*>
  bool test_shared_from_this(enable_shared_from_this<Y>* up, enable_shared_from_this<Y>* bp)
  {
    EXPECT_THROW(auto_shared_ptr<T> unbound1(up), bad_weak_ptr);
    EXPECT_THROW(auto_shared_ptr<T> unbound2 = up, bad_weak_ptr);
    EXPECT_THROW(auto_shared_ptr<T> unbound3 { up }, bad_weak_ptr);
    EXPECT_THROW(auto_shared_ptr<T> unbound4 = { up }, bad_weak_ptr);

    auto shared = bp->shared_from_this();
    int use_count = shared.use_count();

    auto_shared_ptr<T> boundp1(bp);
    EXPECT_EQ(++use_count, boundp1.use_count());
    auto_shared_ptr<T> boundp2 = bp;
    EXPECT_EQ(++use_count, boundp2.use_count());
    auto_shared_ptr<T> boundp3 { bp };
    EXPECT_EQ(++use_count, boundp3.use_count());
    auto_shared_ptr<T> boundp4 = { bp };
    EXPECT_EQ(++use_count, boundp4.use_count());

    auto_shared_ptr<T> asp;
    EXPECT_THROW(asp = up, bad_weak_ptr);
    asp = bp;
    EXPECT_EQ(++use_count, asp.use_count());
    return true;
  }
  bool test_shared_from_this(...) { return false; }

  validate() {
    auto unbound = new T();
    auto bound = shared_ptr<T>(new T());

    auto_shared_ptr<T> zero1(0);
    auto_shared_ptr<T> zero2 = 0;
    auto_shared_ptr<T> zero3 { 0 };
    auto_shared_ptr<T> zero4 = { 0 };
    auto_shared_ptr<T> null1(nullptr);
    auto_shared_ptr<T> null2 = nullptr;
    auto_shared_ptr<T> null3 { nullptr };
    auto_shared_ptr<T> null4 = { nullptr };
    auto_shared_ptr<T> bound1(bound);
    auto_shared_ptr<T> bound2 = bound;
    auto_shared_ptr<T> bound3 { bound };
    auto_shared_ptr<T> bound4 = { bound };

    auto_shared_ptr<T> asp;

    asp = 0;
    asp = nullptr;
    asp = zero1;
    asp = bound;

    tested_shared_from_this = test_shared_from_this(unbound, bound.get());
  }

  bool tested_shared_from_this;
};

TEST(AutoSharedPtr, Behavior) {
  EXPECT_FALSE(validate<A>().tested_shared_from_this);
  EXPECT_TRUE(validate<B>().tested_shared_from_this);
  EXPECT_TRUE(validate<C>().tested_shared_from_this);
}
