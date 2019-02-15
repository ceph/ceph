// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

#include <string_view>

class CInode;

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 */
class ScrubHeader {
public:
  ScrubHeader(std::string_view tag_, bool is_tag_internal_, bool force_,
              bool recursive_, bool repair_, Formatter *f_)
    : tag(tag_), is_tag_internal(is_tag_internal_), force(force_),
      recursive(recursive_), repair(repair_), formatter(f_), origin(nullptr)
  {
    ceph_assert(formatter != nullptr);
  }

  // Set after construction because it won't be known until we've
  // started resolving path and locking
  void set_origin(CInode *origin_) { origin = origin_; }

  bool get_recursive() const { return recursive; }
  bool get_repair() const { return repair; }
  bool get_force() const { return force; }
  bool is_internal_tag() const { return is_tag_internal; }
  CInode *get_origin() const { return origin; }
  std::string_view get_tag() const { return tag; }
  Formatter &get_formatter() const { return *formatter; }

  bool get_repaired() const { return repaired; }
  void set_repaired() { repaired = true; }

protected:
  const std::string tag;
  bool is_tag_internal;
  const bool force;
  const bool recursive;
  const bool repair;
  Formatter * const formatter;
  CInode *origin;

  bool repaired = false;  // May be set during scrub if repairs happened
};

typedef std::shared_ptr<ScrubHeader> ScrubHeaderRef;
typedef std::shared_ptr<const ScrubHeader> ScrubHeaderRefConst;

#endif // SCRUB_HEADER_H_

