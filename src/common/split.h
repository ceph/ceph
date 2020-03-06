// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <string_view>

namespace ceph {

// a forward iterator over the parts of a split string
class spliterator {
  std::string_view str; // full string
  std::string_view delims; // delimiters

  using size_type = std::string_view::size_type;
  size_type pos = 0; // start position of current part
  std::string_view part; // view of current part

  // return the next part after the given position
  std::string_view next(size_type end) {
    pos = str.find_first_not_of(delims, end);
    if (pos == str.npos) {
      return {};
    }
    return str.substr(pos, str.find_first_of(delims, pos) - pos);
  }
 public:
  // types required by std::iterator_traits
  using difference_type = int;
  using value_type = std::string_view;
  using pointer = const value_type*;
  using reference = const value_type&;
  using iterator_category = std::forward_iterator_tag;

  spliterator() = default;

  spliterator(std::string_view str, std::string_view delims)
    : str(str), delims(delims), pos(0), part(next(0))
  {}

  spliterator& operator++() {
    part = next(pos + part.size());
    return *this;
  }
  spliterator operator++(int) {
    spliterator tmp = *this;
    part = next(pos + part.size());
    return tmp;
  }

  reference operator*() const { return part; }
  pointer operator->() const { return &part; }

  friend bool operator==(const spliterator& lhs, const spliterator& rhs) {
    return lhs.part.data() == rhs.part.data()
        && lhs.part.size() == rhs.part.size();
  }
  friend bool operator!=(const spliterator& lhs, const spliterator& rhs) {
    return lhs.part.data() != rhs.part.data()
        || lhs.part.size() != rhs.part.size();
  }
};

// represents an immutable range of split string parts
//
// ranged-for loop example:
//
//   for (std::string_view s : split(input)) {
//     ...
//
// container initialization example:
//
//   auto parts = split(input);
//
//   std::vector<std::string> strings;
//   strings.assign(parts.begin(), parts.end());
//
class split {
  std::string_view str; // full string
  std::string_view delims; // delimiters
 public:
  split(std::string_view str, std::string_view delims = ";,= \t\n")
    : str(str), delims(delims) {}

  using iterator = spliterator;
  using const_iterator = spliterator;

  iterator begin() const { return {str, delims}; }
  const_iterator cbegin() const { return {str, delims}; }

  iterator end() const { return {}; }
  const_iterator cend() const { return {}; }
};

} // namespace ceph
