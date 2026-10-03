/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Dongdong Tao <dongdong.tao@canonical.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * @file test_radostrace_struct_compat.cc
 *
 * This test verifies that data structures used by radostrace BPF tracing
 * still have the expected field names and hierarchy. The radostrace tool
 * depends on specific struct layouts and field names that it discovers
 * via DWARF debug info at runtime.
 *
 * If any of these tests fail, it means someone has renamed or restructured
 * a field that radostrace depends on. The radostrace.cc file's rados_probes
 * definition must be updated to match the new field names.
 *
 */

#include "gtest/gtest.h"

// Ceph headers that define the structures radostrace depends on
#include "osdc/Objecter.h"
#include "osd/osd_types.h"
#include "include/object.h"
#include "include/rados.h"
#include "include/buffer.h"

#include <type_traits>
#include <vector>
#include <string>

using namespace std;

/**
 * Test that Objecter::Op has the expected fields.
 *
 * radostrace.cc rados_probes uses:
 *   {"op", "tid"}
 *   {"op", "target", ...}
 *   {"op", "ops", ...}
 */
TEST(RadostraceStructCompat, ObjecterOpFields)
{
  // Verify Op::tid exists and is the expected type
  static_assert(std::is_same_v<decltype(Objecter::Op::tid), ceph_tid_t>,
                "Objecter::Op::tid type changed - update radostrace.cc");

  // Verify Op::target exists and is op_target_t
  static_assert(std::is_same_v<decltype(Objecter::Op::target), Objecter::op_target_t>,
                "Objecter::Op::target type changed - update radostrace.cc");

  // Verify Op::ops exists (osdc_opvec which is boost::container::small_vector)
  // We use a pointer-to-member to verify it exists without checking exact type
  // (the type is a complex boost template)
  [[maybe_unused]] auto ops_check = &Objecter::Op::ops;
}

/**
 * Test that Objecter::op_target_t has the expected fields.
 *
 * radostrace.cc rados_probes uses:
 *   {"op", "target", "osd"}
 *   {"op", "target", "base_oid", ...}
 *   {"op", "target", "flags"}
 *   {"op", "target", "actual_pgid", ...}
 *   {"op", "target", "acting", ...}
 */
TEST(RadostraceStructCompat, OpTargetFields)
{
  // Verify op_target_t::osd exists and is int
  static_assert(std::is_same_v<decltype(Objecter::op_target_t::osd), int>,
                "op_target_t::osd type changed - update radostrace.cc");

  // Verify op_target_t::base_oid exists and is object_t
  static_assert(std::is_same_v<decltype(Objecter::op_target_t::base_oid), object_t>,
                "op_target_t::base_oid type changed - update radostrace.cc");

  // Verify op_target_t::flags exists and is int
  static_assert(std::is_same_v<decltype(Objecter::op_target_t::flags), int>,
                "op_target_t::flags type changed - update radostrace.cc");

  // Verify op_target_t::actual_pgid exists and is spg_t
  static_assert(std::is_same_v<decltype(Objecter::op_target_t::actual_pgid), spg_t>,
                "op_target_t::actual_pgid type changed - update radostrace.cc");

  // Verify op_target_t::acting exists and is std::vector<int>
  static_assert(std::is_same_v<decltype(Objecter::op_target_t::acting), std::vector<int>>,
                "op_target_t::acting type changed - update radostrace.cc");
}

/**
 * Test that object_t has the expected fields.
 *
 * radostrace.cc rados_probes uses:
 *   {"op", "target", "base_oid", "name", ...}
 */
TEST(RadostraceStructCompat, ObjectTFields)
{
  // Verify object_t::name exists and is std::string
  static_assert(std::is_same_v<decltype(object_t::name), std::string>,
                "object_t::name type changed - update radostrace.cc");
}

/**
 * Test that spg_t and pg_t have the expected fields.
 *
 * radostrace.cc rados_probes uses:
 *   {"op", "target", "actual_pgid", "pgid", "m_pool"}
 *   {"op", "target", "actual_pgid", "pgid", "m_seed"}
 */
TEST(RadostraceStructCompat, PgFields)
{
  // Verify spg_t::pgid exists and is pg_t
  static_assert(std::is_same_v<decltype(spg_t::pgid), pg_t>,
                "spg_t::pgid type changed - update radostrace.cc");

  // Verify pg_t::m_pool exists and is uint64_t
  static_assert(std::is_same_v<decltype(pg_t::m_pool), uint64_t>,
                "pg_t::m_pool type changed - update radostrace.cc");

  // Verify pg_t::m_seed exists and is uint32_t
  static_assert(std::is_same_v<decltype(pg_t::m_seed), uint32_t>,
                "pg_t::m_seed type changed - update radostrace.cc");
}

/**
 * Test that ceph_osd_op has the expected fields.
 *
 * radostrace.cc set_ceph_offsets() uses:
 *   offsetof(struct ceph_osd_op, extent.offset)
 *   offsetof(struct ceph_osd_op, extent.length)
 *   offsetof(struct ceph_osd_op, cls.class_len)
 *   offsetof(struct ceph_osd_op, cls.method_len)
 */
TEST(RadostraceStructCompat, CephOsdOpFields)
{
  // These offsetof calls will fail to compile if the fields don't exist
  [[maybe_unused]] size_t extent_offset = offsetof(struct ceph_osd_op, extent.offset);
  [[maybe_unused]] size_t extent_length = offsetof(struct ceph_osd_op, extent.length);
  [[maybe_unused]] size_t cls_class_len = offsetof(struct ceph_osd_op, cls.class_len);
  [[maybe_unused]] size_t cls_method_len = offsetof(struct ceph_osd_op, cls.method_len);

  // Note: ceph_osd_op size is already verified by static_assert in rados.h
}
