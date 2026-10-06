// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TYPES_OP_H
#define CEPH_OSD_TYPES_OP_H

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "osd/osd_types_core.h"


struct OSDOp {
  ceph_osd_op op;

  ceph::buffer::list indata, outdata;
  errorcode32_t rval;

  OSDOp() {
    // FIPS zeroization audit 20191115: this memset clean for security
    memset(&op, 0, sizeof(ceph_osd_op));
  }

  OSDOp(const int op_code) {
    // FIPS zeroization audit 20191115: this memset clean for security
    memset(&op, 0, sizeof(ceph_osd_op));
    op.op = op_code;
  }

  /**
   * split a ceph::buffer::list into constituent indata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  template<typename V>
  static void split_osd_op_vector_in_data(V& ops,
					  ceph::buffer::list& in) {
    ceph::buffer::list::iterator datap = in.begin();
    for (unsigned i = 0; i < ops.size(); i++) {
      if (ops[i].op.payload_len) {
	datap.copy(ops[i].op.payload_len, ops[i].indata);
      }
    }
  }

  /**
   * merge indata members of a vector of OSDOp into a single ceph::buffer::list
   *
   * Notably this also encodes certain other OSDOp data into the data
   * buffer, including the sobject_t soid.
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  template<typename V>
  static void merge_osd_op_vector_in_data(V& ops, ceph::buffer::list& out) {
    for (unsigned i = 0; i < ops.size(); i++) {
      if (ops[i].indata.length()) {
	ops[i].op.payload_len = ops[i].indata.length();
	out.append(ops[i].indata);
      }
    }
  }

  /**
   * split a ceph::buffer::list into constituent outdata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_out_data(std::vector<OSDOp>& ops, ceph::buffer::list& in);

  /**
   * merge outdata members of a vector of OSDOps into a single ceph::buffer::list
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(std::vector<OSDOp>& ops, ceph::buffer::list& out);

  /**
   * Clear data as much as possible, leave minimal data for historical op dump
   *
   * @param ops [in] vector of OSDOps
   */
  template<typename V>
  static void clear_data(V& ops) {
    for (unsigned i = 0; i < ops.size(); i++) {
      OSDOp& op = ops[i];
      op.outdata.clear();
      if (ceph_osd_op_type_attr(op.op.op) &&
	  op.op.xattr.name_len &&
	  op.indata.length() >= op.op.xattr.name_len) {
	ceph::buffer::list bl;
	bl.push_back(ceph::buffer::ptr_node::create(op.op.xattr.name_len));
	bl.begin().copy_in(op.op.xattr.name_len, op.indata);
	op.indata = std::move(bl);
      } else if (ceph_osd_op_type_exec(op.op.op) &&
		 op.op.cls.class_len &&
		 op.indata.length() >
	         (op.op.cls.class_len + op.op.cls.method_len)) {
	__u8 len = op.op.cls.class_len + op.op.cls.method_len;
	ceph::buffer::list bl;
	bl.push_back(ceph::buffer::ptr_node::create(len));
	bl.begin().copy_in(len, op.indata);
	op.indata = std::move(bl);
      } else {
	op.indata.clear();
      }
    }
  }
};
std::ostream& operator<<(std::ostream& out, const OSDOp& op);
template <> struct fmt::formatter<OSDOp> : fmt::ostream_formatter {};

struct pg_log_op_return_item_t {
  int32_t rval;
  ceph::buffer::list bl;
  void encode(ceph::buffer::list& p) const {
    using ceph::encode;
    encode(rval, p);
    encode(bl, p);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    decode(rval, p);
    decode(bl, p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_int("rval", rval);
    f->dump_unsigned("bl_length", bl.length());
  }
  static std::list<pg_log_op_return_item_t> generate_test_instances() {
    std::list<pg_log_op_return_item_t> o;
    o.emplace_back();
    o.back().rval = 0;
    o.emplace_back();
    o.back().rval = 1;
    o.back().bl.append("asdf");
    return o;
  }
  friend bool operator==(const pg_log_op_return_item_t& lhs,
			 const pg_log_op_return_item_t& rhs) {
    return lhs.rval == rhs.rval &&
      lhs.bl.contents_equal(rhs.bl);
  }
  friend bool operator!=(const pg_log_op_return_item_t& lhs,
			 const pg_log_op_return_item_t& rhs) {
    return !(lhs == rhs);
  }
  friend std::ostream& operator<<(std::ostream& out, const pg_log_op_return_item_t& i) {
    return out << "r=" << i.rval << "+" << i.bl.length() << "b";
  }
};
WRITE_CLASS_ENCODER(pg_log_op_return_item_t)
namespace fmt {
template <>
struct formatter<pg_log_op_return_item_t> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const pg_log_op_return_item_t& litm, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "r={}+{}b", litm.rval, litm.bl.length());
  }
};
} // namespace fmt

#endif // CEPH_OSD_TYPES_OP_H
