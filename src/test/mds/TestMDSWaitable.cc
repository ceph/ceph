
#include "mds/MDSWaitable.h"
#include "gtest/gtest.h"
#include <random>
#include <vector>
#include <string>

TEST(MDSWaitable, ConstexprWaitTags) {

  // the next line fails due to mask being larger than supported
  // constexpr auto a = WaitTag::any(WaitTag::FULL_MASK << 1);


  // constexpr auto a = WaitTag(1, WaitTag::FULL_MASK);
  // constexpr auto b = WaitTag(2, WaitTag::FULL_MASK);
  // the below two lines fail because tag ids are different
  //constexpr auto c = a | b;
  //constexpr auto c = a & ~b;
}

TEST(MDSWaitable, WaitTags)
{
  std::random_device rd;
  std::mt19937_64 gen(rd());

  uint64_t raw1 = gen();

  auto a = WaitTag::from_raw(raw1);

  EXPECT_EQ((raw1 >> WaitTag::MASK_BITS), a.id);
  EXPECT_EQ((raw1 & WaitTag::FULL_MASK), a.mask);
  EXPECT_EQ(raw1, a.raw);

  uint64_t mask2 = gen() & WaitTag::FULL_MASK;
  uint64_t raw2 = (raw1 & ~WaitTag::FULL_MASK) | mask2;

  auto b = a | mask2;
  EXPECT_EQ(raw1 | raw2, b.raw);

  auto c = a & mask2;
  EXPECT_EQ(raw1 & raw2, c.raw);

  auto d = b & c;
  EXPECT_EQ(((raw1 | raw2) & raw1 & raw2) & WaitTag::FULL_MASK, d.mask);
}

struct TagContext : public Context {
      TagContext(WaitTag const tag)
          : tag(tag)
      {
      }
      void finish(int r) override
      {
      }

      void complete(int r) override {
        Context::complete(r);
      }
      const WaitTag tag;
};

static void add_waiter(MDSWaitable<TagContext>& w, WaitTag tag) {
  w.add_waiter(tag, new TagContext(tag));
}

TEST(MDSWaitable, AddTake)
{
  WaitTag any0 = WaitTag::any().bit_mask(0);
  WaitTag any1 = WaitTag::any().bit_mask(1);
  WaitTag any2 = WaitTag::any().bit_mask(2);
  WaitTag one0 = WaitTag(1).bit_mask(0);
  WaitTag one1 = WaitTag(1).bit_mask(1);
  WaitTag two0 = WaitTag(2).bit_mask(0);
  WaitTag two1 = WaitTag(2).bit_mask(1);
  WaitTag tre1 = WaitTag(3).bit_mask(1);

  MDSWaitable<TagContext> setup;

  EXPECT_TRUE(setup.waiting_empty());

  add_waiter(setup, any0);
  add_waiter(setup, any1);
  add_waiter(setup, one0);
  add_waiter(setup, one1);
  add_waiter(setup, two0);
  add_waiter(setup, two1);
  add_waiter(setup, one1 | any0);
  add_waiter(setup, one1 | one0);
  add_waiter(setup, two1 | any0);
  add_waiter(setup, two1 | two0);

  EXPECT_FALSE(setup.waiting_empty());

  EXPECT_TRUE(setup.has_waiter_for(any0));
  EXPECT_TRUE(setup.has_waiter_for(any1));
  EXPECT_TRUE(setup.has_waiter_for(one0));
  EXPECT_TRUE(setup.has_waiter_for(one1));
  EXPECT_TRUE(setup.has_waiter_for(one1 | any0));
  EXPECT_TRUE(setup.has_waiter_for(one1 | one0));
  EXPECT_TRUE(setup.has_waiter_for(tre1)); // because of any1
  EXPECT_FALSE(setup.has_waiter_for(any2));

  {
    auto w = setup;
    std::vector<TagContext*> cc;
    w.take_waiting(one0, std::back_inserter(cc));
    EXPECT_EQ(4, cc.size());
    EXPECT_FALSE(w.has_waiter_for(one0));
  }

  {
    auto w = setup;
    std::vector<TagContext*> cc;
    w.take_waiting(two1, std::back_inserter(cc));
    EXPECT_EQ(4, cc.size());
    EXPECT_FALSE(w.has_waiter_for(two1));
  }

  {
    auto w = setup;
    std::vector<TagContext*> cc;
    w.take_waiting(any0, std::back_inserter(cc));
    EXPECT_EQ(7, cc.size());
    EXPECT_FALSE(w.has_waiter_for(two0));
    EXPECT_FALSE(w.has_waiter_for(one0));
  }
}
