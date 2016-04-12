#include <algorithm>
#include <iterator>
#include <vector>
#include "include/xlist.h"

#include "gtest/gtest.h"


struct Item {
  xlist<Item*>::item xitem;
  int val;

  explicit Item(int v) :
    xitem(this),
    val(v)
  {}
};

class XlistTest : public testing::Test
{
protected:
  typedef xlist<Item*> ItemList;
  typedef std::vector<Item*> Items;
  typedef std::vector<ItemList::item*> Refs;
  Items items;
  // for filling up an ItemList
  Refs refs;

  virtual void SetUp() {
    for (int i = 0; i < 13; i++) {
      items.push_back(new Item(i));
      refs.push_back(&items.back()->xitem);
    }
  }
  virtual void TearDown() {
    for (Items::iterator i = items.begin(); i != items.end(); ++i) {
      delete *i;
    }
    items.clear();
  }
};

TEST_F(XlistTest, capability) {
  ItemList list;
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(list.size(), 0);

  std::copy(refs.begin(), refs.end(), std::back_inserter(list));
  ASSERT_EQ((size_t)list.size(), refs.size());

  list.clear();
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(list.size(), 0);
}

TEST_F(XlistTest, traverse) {
  ItemList list;
  std::copy(refs.begin(), refs.end(), std::back_inserter(list));

  // advance until iterator::end()
  size_t index = 0;
  for (ItemList::iterator i = list.begin(); !i.end(); ++i) {
    ASSERT_EQ(*i, items[index]);
    index++;
  }
  // advance until i == v.end()
  index = 0;
  for (ItemList::iterator i = list.begin(); i != list.end(); ++i) {
    ASSERT_EQ(*i, items[index]);
    index++;
  }
  list.clear();
}

TEST_F(XlistTest, move_around) {
  Item item1(42), item2(17);
  ItemList list;

  // only a single element in the list
  list.push_back(&item1.xitem);
  ASSERT_EQ(&item1, list.front());
  ASSERT_EQ(&item1, list.back());

  list.push_back(&item2.xitem);
  ASSERT_EQ(&item1, list.front());
  ASSERT_EQ(&item2, list.back());

  // move item2 to the front
  list.push_front(&item2.xitem);
  ASSERT_EQ(&item2, list.front());
  ASSERT_EQ(&item1, list.back());

  // and move it back
  list.push_back(&item2.xitem);
  ASSERT_EQ(&item1, list.front());
  ASSERT_EQ(&item2, list.back());

  list.clear();
}

TEST_F(XlistTest, item_queries) {
  Item item(42);
  ItemList list;
  list.push_back(&item.xitem);

  ASSERT_TRUE(item.xitem.is_on_list());
  ASSERT_EQ(&list, item.xitem.get_list());

  ASSERT_TRUE(item.xitem.remove_myself());
  ASSERT_FALSE(item.xitem.is_on_list());
  ASSERT_TRUE(item.xitem.get_list() == NULL);
}

// Local Variables:
// compile-command: "cd .. ;
//   make unittest_xlist &&
//   ./unittest_xlist"
// End:
