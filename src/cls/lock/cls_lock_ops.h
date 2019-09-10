// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOCK_OPS_H
#define CEPH_CLS_LOCK_OPS_H

#include <cstdint>
#include <list>
#include <string>
#include <type_traits>
#include <vector>

#include <boost/container/flat_map.hpp>

#include "include/encoding.h"
#include "include/types.h"

#include "common/ceph_time.h"
#include "common/Formatter.h"

#include "cls/lock/cls_lock_types.h"

struct cls_lock_lock_op
{
  std::string name;
  ClsLockType type = LOCK_NONE;
  std::string cookie;
  std::string tag;
  std::string description;
  ceph::timespan duration;
  std::uint8_t flags = 0;

  cls_lock_lock_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    auto t = static_cast<std::uint8_t>(type);
    encode(t, bl);
    encode(cookie, bl);
    encode(tag, bl);
    encode(description, bl);
    encode(duration, bl);
    encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    std::uint8_t t;
    decode(t, bl);
    type = static_cast<ClsLockType>(t);
    decode(cookie, bl);
    decode(tag, bl);
    decode(description, bl);
    decode(duration, bl);
    decode(flags, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_lock_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_lock_op)

struct cls_lock_unlock_op
{
  std::string name;
  std::string cookie;

  cls_lock_unlock_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    decode(cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_unlock_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_unlock_op)

struct cls_lock_break_op
{
  std::string name;
  entity_name_t locker;
  std::string cookie;

  cls_lock_break_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(locker, bl);
    encode(cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    decode(locker, bl);
    decode(cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_break_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_break_op)

struct cls_lock_get_info_op
{
  std::string name;

  cls_lock_get_info_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_get_info_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_get_info_op)

struct cls_lock_get_info_reply
{
  boost::container::flat_map<rados::cls::lock::locker_id_t,
			     rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type = LOCK_NONE;
  std::string tag;

  cls_lock_get_info_reply() = default;

  void encode(ceph::buffer::list& bl, std::uint64_t features) const {
    ENCODE_START(1, 1, bl);
    encode(lockers, bl, features);
    auto t = static_cast<std::uint8_t>(lock_type);
    encode(t, bl);
    encode(tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(lockers, bl);
    std::uint8_t t;
    decode(t, bl);
    lock_type = static_cast<ClsLockType>(t);
    decode(tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_get_info_reply*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(cls_lock_get_info_reply)

struct cls_lock_list_locks_reply
{
  std::vector<std::string> locks;

  cls_lock_list_locks_reply() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(locks, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(locks, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_list_locks_reply*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_list_locks_reply)

struct cls_lock_assert_op
{
  std::string name;
  ClsLockType type = LOCK_NONE;
  std::string cookie;
  std::string tag;

  cls_lock_assert_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    auto t = static_cast<std::uint8_t>(type);
    encode(t, bl);
    encode(cookie, bl);
    encode(tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    std::uint8_t t;
    decode(t, bl);
    type = static_cast<ClsLockType>(t);
    decode(cookie, bl);
    decode(tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_assert_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_assert_op)

struct cls_lock_set_cookie_op
{
  std::string name;
  ClsLockType type = LOCK_NONE;
  std::string cookie;
  std::string tag;
  std::string new_cookie;

  cls_lock_set_cookie_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    auto t = static_cast<std::uint8_t>(type);
    encode(t, bl);
    encode(cookie, bl);
    encode(tag, bl);
    encode(new_cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(name, bl);
    std::uint8_t t;
    decode(t, bl);
    type = static_cast<ClsLockType>(t);
    decode(cookie, bl);
    decode(tag, bl);
    decode(new_cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<cls_lock_set_cookie_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_set_cookie_op)

#endif
