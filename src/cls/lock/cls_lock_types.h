#ifndef CEPH_CLS_LOCK_TYPES_H
#define CEPH_CLS_LOCK_TYPES_H

#include "include/encoding.h"
#include "include/types.h"
#include "include/utime.h"
#include "msg/msg_types.h"

/* lock flags */
#define LOCK_FLAG_RENEW 0x1        /* idempotent lock acquire */

enum ClsLockType {
  LOCK_NONE      = 0,
  LOCK_EXCLUSIVE = 1,
  LOCK_SHARED    = 2,
};

static inline const char *cls_lock_type_str(ClsLockType type)
{
    switch (type) {
      case LOCK_NONE:
	return "none";
      case LOCK_EXCLUSIVE:
	return "exclusive";
      case LOCK_SHARED:
	return "shared";
      default:
	return "<unknown>";
    }
}

namespace rados {
  namespace cls {
    namespace lock {

      /*
       * locker_id_t: the locker id, needs to be unique in a single lock
       */
      struct locker_id_t {
        entity_name_t locker;   // locker's client name
        string cookie;          // locker's cookie.

        locker_id_t() {}
        locker_id_t(entity_name_t& _n, const string& _c) : locker(_n), cookie(_c) {}

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          ::encode(locker, bl);
          ::encode(cookie, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::iterator &bl) {
          DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
          ::decode(locker, bl);
          ::decode(cookie, bl);
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
      WRITE_CLASS_ENCODER(rados::cls::lock::locker_id_t)

      struct locker_info_t
      {
        utime_t expiration;  // expiration: non-zero means epoch of locker expiration
        entity_addr_t addr;  // addr: locker address
        string description;  // description: locker description, may be empty

        locker_info_t() {}
        locker_info_t(const utime_t& _e, const entity_addr_t& _a,
                      const string& _d) :  expiration(_e), addr(_a), description(_d) {}

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          ::encode(expiration, bl);
          ::encode(addr, bl);
          ::encode(description, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::iterator &bl) {
          DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
          ::decode(expiration, bl);
          ::decode(addr, bl);
          ::decode(description, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        static void generate_test_instances(list<locker_info_t *>& o);
      };
      WRITE_CLASS_ENCODER(rados::cls::lock::locker_info_t)
    }
  }
}

#endif
