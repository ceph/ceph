// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "os/memstore/PageSet.h"

template <typename T>
bool is_aligned(T* ptr) {
  const auto align_mask = alignof(T) - 1;
  return (reinterpret_cast<uintptr_t>(ptr) & align_mask) == 0;
}

TEST(PageSet, AllocAligned)
{
  PageSet pages(1);
  PageSet::page_vector range;

  pages.alloc_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(1u, range[1]->offset);
  ASSERT_EQ(2u, range[2]->offset);
  ASSERT_EQ(3u, range[3]->offset);

  // verify that the Page pointers are properly aligned
  ASSERT_TRUE(is_aligned(range[0].get()));
  ASSERT_TRUE(is_aligned(range[1].get()));
  ASSERT_TRUE(is_aligned(range[2].get()));
  ASSERT_TRUE(is_aligned(range[3].get()));
}

TEST(PageSet, AllocUnaligned)
{
  PageSet pages(2);
  PageSet::page_vector range;

  // front of first page
  pages.alloc_range(0, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page
  pages.alloc_range(1, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page and front of second
  pages.alloc_range(1, 2, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page and all of second
  pages.alloc_range(1, 3, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page, all of second, and front of third
  pages.alloc_range(1, 4, range);
  ASSERT_EQ(3u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(4u, range[2]->offset);
}

TEST(PageSet, GetAligned)
{
  // allocate 4 pages
  PageSet pages(1);
  PageSet::page_vector range;
  pages.alloc_range(0, 4, range);
  range.clear();

  // get first page
  pages.get_range(0, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // get second and third pages
  pages.get_range(1, 2, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // get all four pages
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(1u, range[1]->offset);
  ASSERT_EQ(2u, range[2]->offset);
  ASSERT_EQ(3u, range[3]->offset);
  range.clear();
}

TEST(PageSet, GetUnaligned)
{
  // allocate 3 pages
  PageSet pages(2);
  PageSet::page_vector range;
  pages.alloc_range(0, 6, range);
  range.clear();

  // front of first page
  pages.get_range(0, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page
  pages.get_range(1, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page and front of second
  pages.get_range(1, 2, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page and all of second
  pages.get_range(1, 3, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page, all of second, and front of third
  pages.get_range(1, 4, range);
  ASSERT_EQ(3u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(4u, range[2]->offset);
  range.clear();

  // back of third page with nothing beyond
  pages.get_range(5, 999, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(4u, range[0]->offset);
  range.clear();
}

TEST(PageSet, GetHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  PageSet pages(1);
  PageSet::page_vector range;
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range);
  range.clear();

  // nothing at offset 0, page at offset 1
  pages.get_range(0, 2, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  range.clear();

  // nothing at offset 0, pages at offset 1 and 2, nothing at offset 3
  pages.get_range(0, 4, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // page at offset 2, nothing at offset 3 or 4
  pages.get_range(2, 3, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(2u, range[0]->offset);
  range.clear();

  // get the full range
  pages.get_range(0, 999, range);
  ASSERT_EQ(4u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(5u, range[2]->offset);
  ASSERT_EQ(7u, range[3]->offset);
  range.clear();
}

TEST(PageSet, FreeAligned)
{
  // allocate 4 pages
  PageSet pages(1);
  PageSet::page_vector range;
  pages.alloc_range(0, 4, range);
  range.clear();

  // get the full range
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free after offset 4 has no effect
  pages.free_pages_after(4);
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 4
  pages.free_pages_after(3);
  pages.get_range(0, 4, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free pages 2 and 3
  pages.free_pages_after(1);
  pages.get_range(0, 4, range);
  ASSERT_EQ(1u, range.size());
  range.clear();
}

TEST(PageSet, FreeUnaligned)
{
  // allocate 4 pages
  PageSet pages(2);
  PageSet::page_vector range;
  pages.alloc_range(0, 8, range);
  range.clear();

  // get the full range
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free after offset 7 has no effect
  pages.free_pages_after(7);
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 4
  pages.free_pages_after(5);
  pages.get_range(0, 8, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free pages 2 and 3
  pages.free_pages_after(1);
  pages.get_range(0, 8, range);
  ASSERT_EQ(1u, range.size());
  range.clear();
}

TEST(PageSet, FreeHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  PageSet pages(1);
  PageSet::page_vector range;
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range);
  range.clear();

  // get the full range
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 7
  pages.free_pages_after(6);
  pages.get_range(0, 8, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free page 5
  pages.free_pages_after(3);
  pages.get_range(0, 8, range);
  ASSERT_EQ(2u, range.size());
  range.clear();

  // free pages 1 and 2
  pages.free_pages_after(0);
  pages.get_range(0, 8, range);
  ASSERT_EQ(0u, range.size());
}
