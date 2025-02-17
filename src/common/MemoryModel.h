// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MEMORYMODEL_H
#define CEPH_MEMORYMODEL_H

#include <fstream>
#include <string>
#include <string_view>
#include "include/common_fwd.h"
#include "include/compat.h"
#include "include/expected.hpp"


class MemoryModel {
public:
  struct mem_snap_t {
    long peak{0};
    long size{0};
    long hwm{0};
    long rss{0};
    long data{0};
    long lib{0};
    long heap{0};

    long get_total() const { return size; }
    long get_rss() const { return rss; }
    long get_heap() const { return heap; }
  };

private:
  static inline constexpr const char* proc_stat_fn = PROCPREFIX "/proc/self/status";
  static inline constexpr const char* proc_maps_fn = PROCPREFIX "/proc/self/maps";

  std::ifstream proc_status{proc_stat_fn};
  std::ifstream proc_maps{proc_maps_fn};

  /**
   * @brief Get the mapped heap size
   *
   * Read /proc/self/maps to get the heap size.
   * \retval the mapped heap size, or an error message if the file had not been opened
   *    when the object was constructed.
   */
  tl::expected<int64_t, std::string> get_mapped_heap();

  /**
   * @brief Compare a line against an expected data label
   *
   * If the line starts with the expected label, extract the value and store it in v.
   * \retval true if the line starts with the expected label
   */
  bool cmp_against(const std::string& ln, std::string_view param, long& v) const;

public:
  /**
   * @brief extract memory usage information from /proc/self/status &
   *        /proc/self/maps
   *
   * Read /proc/self/status and /proc/self/maps to get memory usage information.
   * \retval a structure containing the memory usage information, or an error
   *    message if /proc/status had not been opened when the object was
   *    constructed.
   *    Note that no error is returned if only /proc/maps is not open (the heap
   *    size will be reported as 0).
   */
  tl::expected<mem_snap_t, std::string> full_sample();
};

#endif
