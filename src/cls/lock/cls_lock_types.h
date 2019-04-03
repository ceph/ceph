// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOCK_TYPES_H
#define CEPH_CLS_LOCK_TYPES_H

#include "include/encoding.h"
#include "include/types.h"
#include "include/utime.h"
#include "msg/msg_types.h"

/* lock flags */
#define LOCK_FLAG_MAY_RENEW 0x1    /* idempotent lock acquire */
#define LOCK_FLAG_MUST_RENEW 0x2   /* lock must already be acquired */

enum ClsLockType {
  LOCK_NONE                = 0,
  LOCK_EXCLUSIVE           = 1,
  LOCK_SHARED              = 2,
  LOCK_EXCLUSIVE_EPHEMERAL = 3, /* lock object is removed @ unlock */
};

inline const char *cls_lock_type_str(ClsLockType type)
{
    switch (type) {
      case LOCK_NONE:
	return "none";
      case LOCK_EXCLUSIVE:
	return "exclusive";
      case LOCK_SHARED:
	return "shared";
      case LOCK_EXCLUSIVE_EPHEMERAL:
	return "exclusive-ephemeral";
      default:
	return "<unknown>";
    }
}

inline bool cls_lock_is_exclusive(ClsLockType type) {
  return LOCK_EXCLUSIVE == type || LOCK_EXCLUSIVE_EPHEMERAL == type;
}

inline bool cls_lock_is_ephemeral(ClsLockType type) {
  return LOCK_EXCLUSIVE_EPHEMERAL == type;
}

inline bool cls_lock_is_valid(ClsLockType type) {
  return LOCK_SHARED == type ||
    LOCK_EXCLUSIVE == type ||
    LOCK_EXCLUSIVE_EPHEMERAL == type;
}

namespace rados {
  namespace cls {
    namespace lock {

      /*
       * locker_id_t: the locker id, needs to be unique in a single lock
       */
      struct locker_id_t {
        entity_name_t locker;   // locker's client name
	std::string cookie;          // locker's cookie.

        locker_id_t() {}
        locker_id_t(entity_name_t& _n, const std::string& _c) : locker(_n), cookie(_c) {}

        void encode(ceph::buffer::list &bl) const {
          ENCODE_START(1, 1, bl);
          encode(locker, bl);
          encode(cookie, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
          decode(locker, bl);
          decode(cookie, bl);
          DECODE_FINISH(bl);
        }

        bool operator<(const locker_id_t& rhs) const {
          if (locker == rhs.locker)
            return cookie.compare(rhs.cookie) < 0;
          if (locker < rhs.locker)
            return true;
          return false;
        }
        void dump(Formatter *f) const;
        static void generate_test_instances(list<locker_id_t*>& o);
      };
      WRITE_CLASS_ENCODER(locker_id_t)

      struct locker_info_t
      {
        utime_t expiration;  // expiration: non-zero means epoch of locker expiration
        entity_addr_t addr;  // addr: locker address
	std::string description;  // description: locker description, may be empty

        locker_info_t() {}
        locker_info_t(const utime_t& _e, const entity_addr_t& _a,
                      const std::string& _d) :  expiration(_e), addr(_a), description(_d) {}

        void encode(ceph::buffer::list &bl, uint64_t features) const {
          ENCODE_START(1, 1, bl);
          encode(expiration, bl);
          encode(addr, bl, features);
          encode(description, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
          decode(expiration, bl);
          decode(addr, bl);
          decode(description, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        static void generate_test_instances(list<locker_info_t *>& o);
      };
      WRITE_CLASS_ENCODER_FEATURES(locker_info_t)

      struct lock_info_t {
        map<locker_id_t, locker_info_t> lockers; // map of lockers
        ClsLockType lock_type;                   // lock type (exclusive / shared)
	std::string tag;                              // tag: operations on lock can only succeed with this tag
                                                 //      as long as set of non expired lockers
                                                 //      is bigger than 0.

        void encode(ceph::buffer::list &bl, uint64_t features) const {
          ENCODE_START(1, 1, bl);
          encode(lockers, bl, features);
          uint8_t t = (uint8_t)lock_type;
          encode(t, bl);
          encode(tag, bl);
          ENCODE_FINISH(bl);
        }
        void decode(ceph::buffer::list::const_iterator &bl) {
          DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
          decode(lockers, bl);
          uint8_t t;
          decode(t, bl);
          lock_type = (ClsLockType)t;
          decode(tag, bl);
          DECODE_FINISH(bl);
        }
        lock_info_t() : lock_type(LOCK_NONE) {}
        void dump(Formatter *f) const;
        static void generate_test_instances(list<lock_info_t *>& o);
      };
      WRITE_CLASS_ENCODER_FEATURES(lock_info_t);
    }
  }
}

#endif
