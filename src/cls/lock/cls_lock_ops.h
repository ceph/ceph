#ifndef CEPH_CLS_LOCK_OPS_H
#define CEPH_CLS_LOCK_OPS_H

#include "include/types.h"
#include "include/utime.h"
#include "cls/lock/cls_lock_types.h"

struct cls_lock_lock_op
{
  string name;
  ClsLockType type;
  string cookie;
  string tag;
  string description;
  utime_t duration;
  uint8_t flags;

  cls_lock_lock_op() : type(LOCK_NONE), flags(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    uint8_t t = (uint8_t)type;
    ::encode(t, bl);
    ::encode(cookie, bl);
    ::encode(tag, bl);
    ::encode(description, bl);
    ::encode(duration, bl);
    ::encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    uint8_t t;
    ::decode(t, bl);
    type = (ClsLockType)t;
    ::decode(cookie, bl);
    ::decode(tag, bl);
    ::decode(description, bl);
    ::decode(duration, bl);
    ::decode(flags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_lock_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_lock_op)

struct cls_lock_unlock_op
{
  string name;
  string cookie;

  cls_lock_unlock_op() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    ::decode(cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_unlock_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_unlock_op)

struct cls_lock_break_op
{
  string name;
  entity_name_t locker;
  string cookie;

  cls_lock_break_op() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(locker, bl);
    ::encode(cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    ::decode(locker, bl);
    ::decode(cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_break_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_break_op)

struct cls_lock_get_info_op
{
  string name;

  cls_lock_get_info_op() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_get_info_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_get_info_op)

struct cls_lock_get_info_reply
{
  map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  string tag;

  cls_lock_get_info_reply() : lock_type(LOCK_NONE) {}

  void encode(bufferlist &bl, uint64_t features) const {
    ENCODE_START(1, 1, bl);
    ::encode(lockers, bl, features);
    uint8_t t = (uint8_t)lock_type;
    ::encode(t, bl);
    ::encode(tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(lockers, bl);
    uint8_t t;
    ::decode(t, bl);
    lock_type = (ClsLockType)t; 
    ::decode(tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_get_info_reply*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(cls_lock_get_info_reply)

struct cls_lock_list_locks_reply
{
  list<string> locks;

  cls_lock_list_locks_reply() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(locks, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(locks, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_list_locks_reply*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_list_locks_reply)

struct cls_lock_assert_op
{
  string name;
  ClsLockType type;
  string cookie;
  string tag;

  cls_lock_assert_op() : type(LOCK_NONE) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    uint8_t t = (uint8_t)type;
    ::encode(t, bl);
    ::encode(cookie, bl);
    ::encode(tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    uint8_t t;
    ::decode(t, bl);
    type = (ClsLockType)t;
    ::decode(cookie, bl);
    ::decode(tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_assert_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_assert_op)

struct cls_lock_set_cookie_op
{
  string name;
  ClsLockType type;
  string cookie;
  string tag;
  string new_cookie;

  cls_lock_set_cookie_op() : type(LOCK_NONE) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    uint8_t t = (uint8_t)type;
    ::encode(t, bl);
    ::encode(cookie, bl);
    ::encode(tag, bl);
    ::encode(new_cookie, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(name, bl);
    uint8_t t;
    ::decode(t, bl);
    type = (ClsLockType)t;
    ::decode(cookie, bl);
    ::decode(tag, bl);
    ::decode(new_cookie, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_lock_set_cookie_op*>& o);
};
WRITE_CLASS_ENCODER(cls_lock_set_cookie_op)

#endif
