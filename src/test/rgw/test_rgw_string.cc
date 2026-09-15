// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_string.h"
#include "rgw_tar.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string_view>

#include "include/buffer.h"

const std::string abc{"abc"};
const char *def{"def"}; // const char*
char ghi_arr[] = {'g', 'h', 'i', '\0'};
char *ghi{ghi_arr}; // char*
constexpr std::string_view jkl{"jkl", 3};
#define mno "mno" // string literal (char[4])
char pqr[] = {'p', 'q', 'r', '\0'};

TEST(string_size, types)
{
  ASSERT_EQ(3u, string_size(abc));
  ASSERT_EQ(3u, string_size(def));
  ASSERT_EQ(3u, string_size(ghi));
  ASSERT_EQ(3u, string_size(jkl));
  ASSERT_EQ(3u, string_size(mno));
  ASSERT_EQ(3u, string_size(pqr));

  constexpr auto compile_time_string_view_size = string_size(jkl);
  ASSERT_EQ(3u, compile_time_string_view_size);
  constexpr auto compile_time_string_literal_size = string_size(mno);
  ASSERT_EQ(3u, compile_time_string_literal_size);

  char arr[] = {'a', 'b', 'c'}; // not null-terminated
  ASSERT_THROW(string_size(arr), std::invalid_argument);
}

TEST(string_cat_reserve, types)
{
  ASSERT_EQ("abcdefghijklmnopqr",
            string_cat_reserve(abc, def, ghi, jkl, mno, pqr));
}

TEST(string_cat_reserve, count)
{
  ASSERT_EQ("", string_cat_reserve());
  ASSERT_EQ("abc", string_cat_reserve(abc));
  ASSERT_EQ("abcdef", string_cat_reserve(abc, def));
}

TEST(string_join_reserve, types)
{
  ASSERT_EQ("abc, def, ghi, jkl, mno, pqr",
            string_join_reserve(", ", abc, def, ghi, jkl, mno, pqr));
}

TEST(string_join_reserve, count)
{
  ASSERT_EQ("", string_join_reserve(", "));
  ASSERT_EQ("abc", string_join_reserve(", ", abc));
  ASSERT_EQ("abc, def", string_join_reserve(", ", abc, def));
}

TEST(string_join_reserve, delim)
{
  ASSERT_EQ("abcdef", string_join_reserve("", abc, def));
  ASSERT_EQ("abc def", string_join_reserve(' ', abc, def));
  ASSERT_EQ("abc\ndef", string_join_reserve('\n', abc, def));
  ASSERT_EQ("abcfoodef", string_join_reserve(std::string{"foo"}, abc, def));
}

namespace {

constexpr size_t filename_offset = 0;
constexpr size_t filesize_offset = 124;
constexpr size_t filetype_offset = 156;

using TarBlock = std::array<char, rgw::tar::TAR_BLOCK_SIZE>;

TarBlock make_header(const std::string_view name,
                     const std::string_view octal_size,
                     const rgw::tar::FileType filetype)
{
  TarBlock block {};
  ceph_assert(name.size() < 100);
  ceph_assert(octal_size.size() < 12);

  std::memcpy(block.data() + filename_offset, name.data(), name.size());
  std::memcpy(block.data() + filesize_offset, octal_size.data(), octal_size.size());
  block[filetype_offset] = static_cast<char>(filetype);

  return block;
}

ceph::bufferlist make_bufferlist(const TarBlock& block)
{
  ceph::bufferlist bl;
  bl.append(block.data(), block.size());
  return bl;
}

} // namespace

TEST(RGWTar, InterpretNormalFileHeader)
{
  auto block = make_header("bucket/object.txt", "00000000123",
                           rgw::tar::FileType::NORMAL_FILE);
  auto bl = make_bufferlist(block);
  const auto status = rgw::tar::StatusIndicator::create();

  const auto [next_status, header] = rgw::tar::interpret_block(status, bl);

  EXPECT_FALSE(next_status.empty());
  EXPECT_FALSE(next_status.eof());
  ASSERT_TRUE(header);
  EXPECT_EQ(rgw::tar::FileType::NORMAL_FILE, header->get_filetype());
  EXPECT_EQ("bucket/object.txt", header->get_filename());
  EXPECT_EQ(83u, header->get_filesize());
}

TEST(RGWTar, InterpretDirectoryHeader)
{
  auto block = make_header("bucket/path", "00000000000",
                           rgw::tar::FileType::DIRECTORY);
  auto bl = make_bufferlist(block);
  const auto status = rgw::tar::StatusIndicator::create();

  const auto [next_status, header] = rgw::tar::interpret_block(status, bl);

  EXPECT_FALSE(next_status.empty());
  EXPECT_FALSE(next_status.eof());
  ASSERT_TRUE(header);
  EXPECT_EQ(rgw::tar::FileType::DIRECTORY, header->get_filetype());
  EXPECT_EQ("bucket/path", header->get_filename());
  EXPECT_EQ(0u, header->get_filesize());
}

TEST(RGWTar, RequiresTwoEmptyBlocksForEof)
{
  const TarBlock block {};
  auto first = make_bufferlist(block);
  auto second = make_bufferlist(block);
  const auto status = rgw::tar::StatusIndicator::create();

  const auto [first_status, first_header] = rgw::tar::interpret_block(status, first);
  EXPECT_TRUE(first_status.empty());
  EXPECT_FALSE(first_status.eof());
  EXPECT_FALSE(first_header);

  const auto [second_status, second_header] = rgw::tar::interpret_block(first_status, second);
  EXPECT_TRUE(second_status.empty());
  EXPECT_TRUE(second_status.eof());
  EXPECT_FALSE(second_header);
}
