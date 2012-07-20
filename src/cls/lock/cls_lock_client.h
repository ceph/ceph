#ifndef CEPH_CLS_LOCK_CLIENT_H
#define CEPH_CLS_LOCK_CLIENT_H


#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls/lock/cls_lock_types.h"


namespace rados {
  namespace cls {
    namespace lock {

      extern void lock(ObjectWriteOperation& rados_op,
                       std::string& name, ClsLockType type,
                       std::string& cookie, std::string& tag,
                       std::string description, utime_t& duration, uint8_t flags);

      extern int lock(IoCtx& ioctx,
                      std::string& oid,
                      std::string& name, ClsLockType type,
                      std::string& cookie, std::string& tag,
                      std::string description, utime_t& duration, uint8_t flags);

      extern void unlock(ObjectWriteOperation& rados_op,
                         std::string& name, std::string& cookie);

      extern int unlock(IoCtx& ioctx, std::string& oid,
                        std::string& name, std::string& cookie);

      extern void break_lock(ObjectWriteOperation& op,
                             std::string& name, std::string& cookie,
                             entity_name_t& locker);

      extern int break_lock(IoCtx& ioctx, std::string& oid,
                            std::string& name, std::string& cookie,
                            entity_name_t& locker);

      extern int list_locks(librados::IoCtx& ioctx, std::string& oid, list<std::string> *locks);
      extern int get_lock_info(librados::IoCtx& ioctx, std::string& oid, std::string& lock,
                               map<cls_lock_id_t, cls_lock_locker_info_t> *lockers,
                               ClsLockType *lock_type,
                               std::string *tag);

      class Lock {
        std::string name;
        std::string cookie;
        std::string tag;
        std::string description;
        utime_t duration;
        uint8_t flags;

      public:

        Lock(const std::string& _n) : name(_n), flags(0) {}

        void set_cookie(const std::string& c) { cookie = c; }
        void set_tag(const std::string& t) { tag = t; }
        void set_description(const std::string& desc) { description = desc; }
        void set_duration(const utime_t& e) { duration = e; }
        void set_renew(bool renew) {
          if (renew) {
            flags |= LOCK_FLAG_RENEW;
          } else {
            flags &= ~LOCK_FLAG_RENEW;
          }
        }

        /* ObjectWriteOperation */
        void lock_exclusive(ObjectWriteOperation& ioctx);
        void lock_shared(ObjectWriteOperation& ioctx);
        void unlock(ObjectWriteOperation& ioctx);
        void break_lock(ObjectWriteOperation& ioctx, entity_name_t& locker);

        /* IoCtx*/
        int lock_exclusive(IoCtx& ioctx, std::string& oid);
        int lock_shared(IoCtx& ioctx, std::string& oid);
        int unlock(IoCtx& ioctx, std::string& oid);
        int break_lock(IoCtx& ioctx, std::string& oid, entity_name_t& locker);
      };

    } // namespace lock
  }  // namespace cls
} // namespace rados

#endif

