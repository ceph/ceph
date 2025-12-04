// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_COMMON_INP_OUT_ADAPTERS_H
#define CEPH_COMMON_INP_OUT_ADAPTERS_H

#include <string>
#include <map>
#include <set>
#include "include/byteorder.h"
#include "include/buffer.h"

class InputStrSet {
public:
  virtual ~InputStrSet() {}
  virtual std::string_view get_current() = 0;
  virtual bool begin() = 0;
  virtual bool next() = 0;
};

class OutputStr2BListMap {
public:
  virtual ~OutputStr2BListMap() {}
  virtual bool emplace(std::string_view s, const ceph::buffer::list& bl) = 0;
};

class InputStrSetStlImpl : public InputStrSet {
  const std::set<std::string>* my_set = nullptr;
  std::set<std::string>::const_iterator it;

public:
  InputStrSetStlImpl(const std::set<std::string>* _set) : my_set(_set) {
    ceph_assert(my_set);
  }
  std::string_view get_current() override {
    return *it;
  }
  bool begin() override {
    it = my_set->cbegin();
    return it != my_set->cend();
  }
  bool next() override {
    ++it;
    return it != my_set->cend();
  }
};

class InputStrSetEncodedImpl : public InputStrSet {
  ceph::buffer::list::const_iterator bp0;
  ceph::buffer::list::const_iterator bp;
  std::string current;
  uint32_t size = 0;
  uint32_t pos = 0;
  bool error = false;
public:
  InputStrSetEncodedImpl(ceph::buffer::list::const_iterator _bp) : bp0(_bp) {}
  bool had_decode_error() const {
    return error;
  }
  std::string_view get_current() override {
    return current;
  }
  bool begin() override {
    bp = bp0;
    error = false;
    pos = 0;
    size = 0;
    current.clear();
    try {
      decode(size, bp);
      if (size > 0) {
        decode(current, bp);
        pos++;
      }
    }
    catch (ceph::buffer::error& e) {
      error = true;
    }
    return !error && size > 0;
  }
  bool next() override {
    bool decoded = false;
    if (!error && pos < size) {
      current.clear();
      try {
        decode(current, bp);
        decoded = true;
        pos++;
      }
      catch (ceph::buffer::error& e) {
        error = true;
      }
    }
    return decoded;
  }
};

class OutputStr2BListMapStlImpl : public OutputStr2BListMap {
  std::map<std::string, ceph::buffer::list>* my_map = nullptr;
public:
  OutputStr2BListMapStlImpl(std::map<std::string, ceph::buffer::list>* _map) : my_map(_map) {
    ceph_assert(my_map);
  }
  bool emplace(std::string_view s, const ceph::buffer::list& bl) override {
    my_map->emplace(s, bl);
    return true;
  }
};

struct OutputStr2BListMapEncodedImpl : public OutputStr2BListMap {
  bufferlist* out;
  ceph_le32 entry_count;
  ceph::buffer::list::contiguous_filler count_filler;
  OutputStr2BListMapEncodedImpl(bufferlist* _out) : out(_out), entry_count(0) {
    ceph_assert(out);
    count_filler = out->append_hole(sizeof(entry_count));
  }
  bool emplace(std::string_view s, const ceph::buffer::list& bl) override {
    encode(s, *out);
    encode(bl, *out);
    entry_count = entry_count + 1;
    return true;
  }
  void finalize() {
    count_filler.copy_in(sizeof(entry_count), (char*)&entry_count);
  }
};

#endif
