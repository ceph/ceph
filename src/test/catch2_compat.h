/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_TEST_CATCH2_COMPAT_H
#define CEPH_TEST_CATCH2_COMPAT_H

#include <catch2/catch_session.hpp>

#include <cstdio>
#include <fstream>
#include <istream>
#include <iterator>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace ceph::test {

namespace detail {

constexpr bool contains(const std::string_view line,
                        const std::string_view text) noexcept
{
  return std::string_view::npos != line.find(text);
}

constexpr std::string_view skipped_junit_child_end(const std::string_view line) noexcept
{
  if (contains(line, "<properties")) {
    return "</properties>";
  }

  if (contains(line, "<system-out")) {
    return "</system-out>";
  }

  if (contains(line, "<system-err")) {
    return "</system-err>";
  }

  return {};
}

constexpr bool skipped_junit_child_line(const std::string_view line) noexcept
{
  return contains(line, "<property ") ||
         contains(line, "</properties>") ||
         contains(line, "</system-out>") ||
         contains(line, "</system-err>");
}

inline void sanitize_catch2_junit_for_gtest2subunit(std::istream& in,
                                                   std::ostream& out)
{
  std::string_view skipped_child_end;
  std::string line;

  while (std::getline(in, line)) {
    const std::string_view view { line };

    if (!skipped_child_end.empty()) {
      if (contains(view, skipped_child_end)) {
        skipped_child_end = {};
      }

      continue;
    }

    if (skipped_junit_child_line(view)) {
      continue;
    }

    const auto child_end = skipped_junit_child_end(view);

    if (!child_end.empty()) {
      if (!contains(view, "/>") &&
          !contains(view, child_end)) {
        skipped_child_end = child_end;
      }

      continue;
    }

    out << line << '\n';
  }
}

inline bool sanitize_catch2_junit_for_gtest2subunit(const std::string& input_path,
                                                   const std::string& output_path)
{
  std::ifstream in { input_path };
  std::ofstream out { output_path, std::ios::trunc };

  if (!in || !out) {
    return false;
  }

  sanitize_catch2_junit_for_gtest2subunit(in, out);

  return out.good();
}

} // namespace detail

class catch2_args final
{
 public:
  catch2_args(const int argc, char *argv[])
  {
    args.reserve(static_cast<std::size_t>(argc) + 3);
    args.emplace_back(argv[0]);

    translate_args(argc, argv);

    argv_out.reserve(std::size(args));

    for (auto& arg : args) {
      argv_out.push_back(arg.data());
    }
  }

  int argc() const
  {
    return static_cast<int>(std::size(argv_out));
  }

  char **argv()
  {
    return argv_out.data();
  }

  bool write_gtest2subunit_xml() const
  {
    if (!gtest_xml_output_path) {
      return true;
    }

    if (!detail::sanitize_catch2_junit_for_gtest2subunit(
          catch2_xml_output_path, *gtest_xml_output_path)) {
      std::fprintf(stderr,
                   "failed to write gtest-compatible Catch2 XML: %s\n",
                   gtest_xml_output_path->c_str());
      return false;
    }

    std::remove(catch2_xml_output_path.c_str());

    return true;
  }

 private:
  static constexpr std::string_view gtest_xml_output = "--gtest_output=xml:";

  void translate_args(const int argc, char *argv[])
  {
    for (int i = 1; i < argc; ++i) {
      const std::string_view arg{ argv[i] };

      if (arg.starts_with(gtest_xml_output)) {
        gtest_xml_output_path = arg.substr(std::size(gtest_xml_output));
        catch2_xml_output_path = *gtest_xml_output_path + ".catch2.xml";

        args.emplace_back("-r");
        args.emplace_back("JUnit");
        args.emplace_back("-o");
        args.emplace_back(catch2_xml_output_path);
        continue;
      }

      args.emplace_back(arg);
    }
  }

  std::vector<std::string> args;
  std::vector<char *> argv_out;
  std::optional<std::string> gtest_xml_output_path;
  std::string catch2_xml_output_path;
};

inline int run_catch2(const int argc, char *argv[])
{
  catch2_args args(argc, argv);
  const auto result = Catch::Session().run(args.argc(), args.argv());

  if (!args.write_gtest2subunit_xml()) {
    return 1;
  }

  return result;
}

} // namespace ceph::test

#endif
