// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOCK_CLIENT_H
#define CEPH_CLS_LOCK_CLIENT_H


#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls/lock/cls_lock_types.h"


namespace rados {
  namespace cls {
    namespace lock {
      extern void lock(librados::ObjectWriteOperation *rados_op,
		       const std::string& name, ClsLockType type,
		       const std::string& cookie, const std::string& tag,
		       const std::string& description, const utime_t& duration,
		       uint8_t flags);

      extern int lock(librados::IoCtx *ioctx,
		      const std::string& oid,
		      const std::string& name, ClsLockType type,
		      const std::string& cookie, const std::string& tag,
		      const std::string& description, const utime_t& duration,
		      uint8_t flags);

      extern void unlock(librados::ObjectWriteOperation *rados_op,
			 const std::string& name, const std::string& cookie);

      extern int unlock(librados::IoCtx *ioctx, const std::string& oid,
			const std::string& name, const std::string& cookie);

      extern void break_lock(librados::ObjectWriteOperation *op,
			     const std::string& name, const std::string& cookie,
			     const entity_name_t& locker);

      extern int break_lock(librados::IoCtx *ioctx, const std::string& oid,
			    const std::string& name, const std::string& cookie,
			    const entity_name_t& locker);

      extern int list_locks(librados::IoCtx *ioctx, const std::string& oid,
			    list<std::string> *locks);
      extern void get_lock_info_start(librados::ObjectReadOperation *rados_op,
				      const std::string& name);
      extern int get_lock_info_finish(ceph::bufferlist::iterator *out,
				      map<locker_id_t, locker_info_t> *lockers,
				      ClsLockType *type, std::string *tag);

      extern int get_lock_info(librados::IoCtx *ioctx, const std::string& oid,
			       const std::string& name,
			       map<locker_id_t, locker_info_t> *lockers,
			       ClsLockType *type, std::string *tag);

      extern void assert_locked(librados::ObjectOperation *rados_op,
                                const std::string& name, ClsLockType type,
                                const std::string& cookie,
                                const std::string& tag);

      extern void set_cookie(librados::ObjectWriteOperation *rados_op,
                             const std::string& name, ClsLockType type,
                             const std::string& cookie, const std::string& tag,
                             const std::string& new_cookie);

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

        void assert_locked_exclusive(librados::ObjectOperation *rados_op);
        void assert_locked_shared(librados::ObjectOperation *rados_op);

	/* ObjectWriteOperation */
	void lock_exclusive(librados::ObjectWriteOperation *ioctx);
	void lock_shared(librados::ObjectWriteOperation *ioctx);
	void unlock(librados::ObjectWriteOperation *ioctx);
	void break_lock(librados::ObjectWriteOperation *ioctx, const entity_name_t& locker);

	/* IoCtx */
	int lock_exclusive(librados::IoCtx *ioctx, const std::string& oid);
	int lock_shared(librados::IoCtx *ioctx, const std::string& oid);
	int unlock(librados::IoCtx *ioctx, const std::string& oid);
	int break_lock(librados::IoCtx *ioctx, const std::string& oid,
		       const entity_name_t& locker);
      };

    } // namespace lock
  }  // namespace cls
} // namespace rados

#endif
