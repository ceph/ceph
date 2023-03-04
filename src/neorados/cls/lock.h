// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#pragma once

/// \file neodrados/cls/lock.h
///
/// \brief NeoRADOS interface to object locking
///
/// The `lock` object class provides advisory, optionally timed,
/// locking of RADOS objects.

#include <boost/system/detail/errc.hpp>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>

#include "include/buffer.h"

#include "include/neorados/RADOS.hpp"

#include "common/ceph_time.h"
#include "common/random_string.h"

#include "cls/lock/cls_lock_types.h"
#include "cls/lock/cls_lock_ops.h"

#include "common.h"

namespace neorados::cls::lock {
/// \name Flags
///
/// \note If may_renew and must_renew are both set, the lock operation
/// will fail with boost::system::errc::invalid_argument
///
/// \note If neither may_renew nor no_renew are set and the lock
/// exists, then the lock operation will fail with
/// boost::system::errc::file_exists
/// @{

/// This flag allows a lock operation to renew an existing lock,
/// resetting its expiration to the current time (according to the
/// OSD) plus the supplied duration.
inline constexpr std::byte may_renew{LOCK_FLAG_MAY_RENEW};

/// If this flag is set and the lock does not exist, the lock
/// operation will fail with boost::system::errc::no_such_file_or_directory.
/// On renew, the expiration will be current time (according to the
/// OSD. But you're running ntp, right?) plus the supplied duration.
inline constexpr std::byte must_renew{LOCK_FLAG_MUST_RENEW};
/// @}

/// \brief Type of lock to acquire or assert
///
/// \note Redeclaring rather than using an alias to downcase
/// the members, add documentation, and to allow `using enum`.
enum class type : std::underlying_type_t<ClsLockType> {
  /// All other clients are denied the lock
  exclusive = int(ClsLockType::EXCLUSIVE),
  /// Other shared lockers are allowed, but not exclusive or ephemeral
  shared = int(ClsLockType::SHARED),
  /// Identical to exclusive, except that the object is removed on unlock
  ephemeral = int(ClsLockType::EXCLUSIVE_EPHEMERAL),
};
inline bool operator ==(const type& lhs, const ClsLockType& rhs) {
  return int(lhs) == int(rhs);
}
using rados::cls::lock::locker_id_t;
using rados::cls::lock::locker_info_t;


/// \brief Acquire a lock on an object
///
/// Append a call to a write operation to acquire a lock.
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
/// \param description Human-readable text describing the lock
/// \param duration Lifetime of the lock (0 for unbounded)
/// \param flags Flags governing the lock operation, \see Flags
inline void lock(WriteOp& wop, std::string name, type t,
		 std::string cookie, std::string tag,
		 std::string description, ceph::timespan duration,
		 std::byte flags) {
  cls_lock_lock_op op;
  op.name = std::move(name);
  op.type = ClsLockType(t);
  op.cookie = std::move(cookie);
  op.tag = std::move(tag);
  op.description = std::move(description);
  op.duration = duration;
  op.flags = std::to_integer<uint8_t>(flags);
  bufferlist in;
  encode(op, in);

  wop.exec("lock", "lock", in);
}

/// \brief Acquire a lock on an object
///
/// Append a call to a write operation to acquire a lock.
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
/// \param description Human-readable text describing the lock
/// \param duration Lifetime of the lock (0 for unbounded)
/// \param flags Flags governing the lock operation, \see Flags
///
/// \return The op, for chaining
inline WriteOp&& lock(WriteOp&& wop, std::string name, type t,
		      std::string cookie, std::string tag,
		      std::string description, ceph::timespan duration,
		      std::byte flags) {
  lock(wop, std::move(name), t, std::move(cookie), std::move(tag),
       std::move(description), duration, flags);
  return std::move(wop);
}

/// \brief Release a lock on an object
///
/// Append a call to a write operation to release a lock
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
inline void unlock(WriteOp& wop, std::string name, std::string cookie) {
  cls_lock_unlock_op op;
  op.name = std::move(name);
  op.cookie = cookie;
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "unlock", in);
}

/// \brief Release a lock on an object
///
/// Append a call to a write operation to release a lock
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
///
/// \return The op, for chaining
inline WriteOp&& unlock(WriteOp&& wop, std::string name, std::string cookie) {
  unlock(wop, std::move(name), std::move(cookie));
  return std::move(wop);
}

/// \brief Break a lock on an object
///
/// Append a call to a write operation to break a lock
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param locker Identifier of the RADOS client holding the lock,
///               \see info
inline void break_lock(WriteOp& wop, std::string name, std::string cookie,
		       entity_name_t locker) {
  cls_lock_break_op op;
  op.name = std::move(name);
  op.cookie = std::move(cookie);
  op.locker = std::move(locker);
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "break_lock", in);
}

/// \brief Break a lock on an object
///
/// Append a call to a write operation to break a lock
///
/// \param wop Write operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param locker Identifier of the RADOS client holding the lock,
///               \see info
///
/// \return The op, for chaining
inline WriteOp&& break_lock(WriteOp&& wop, std::string name, std::string cookie,
			    entity_name_t locker) {
  break_lock(wop, std::move(name), std::move(cookie), std::move(locker));
  return std::move(wop);
}

/// \brief Assert that a lock is held on an object
///
/// Append a call to a write operation requiring that the lock be held
/// for the oepration to complete.
///
/// \param wop Operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
inline void assert_locked(Op& rop, std::string name, type t,
			  std::string cookie, std::string tag) {
  cls_lock_assert_op op;
  op.name = std::move(name);
  op.type = ClsLockType(t);
  op.cookie = std::move(cookie);
  op.tag = std::move(tag);
  bufferlist in;
  encode(op, in);
  rop.exec("lock", "assert_locked", in);
}

/// \brief Assert that a lock is held on an object
///
/// Append a call to a write operation requiring that the lock be held
/// for the oepration to complete.
///
/// \param wop Operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
///
/// \return The op, for chaining
inline ReadOp&& assert_locked(ReadOp&& rop, std::string name, type t,
			      std::string cookie, std::string tag) {
  assert_locked(rop, std::move(name), t, std::move(cookie), std::string(tag));
  return std::move(rop);
}

/// \brief Assert that a lock is held on an object
///
/// Append a call to a write operation requiring that the lock be held
/// for the oepration to complete.
///
/// \param wop Operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
///
/// \return The op, for chaining
inline WriteOp&& assert_locked(WriteOp&& rop, std::string name, type t,
			      std::string cookie, std::string tag) {
  assert_locked(rop, std::move(name), t, std::move(cookie), std::string(tag));
  return std::move(rop);
}

/// \brief Change the cookie associated with a lock
///
/// Append a call to a write operation to change the cookie associated
/// with a lock. (So, for example, a racing break_lock or unlock won't
/// remove it.)
///
/// \param wop Operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
/// \param new_cookie The new cookie to use
inline void set_cookie(WriteOp& wop, std::string name, type t,
		       std::string cookie, std::string tag,
		       std::string new_cookie) {
  cls_lock_set_cookie_op op;
  op.name = name;
  op.type = ClsLockType(t);
  op.cookie = cookie;
  op.tag = tag;
  op.new_cookie = new_cookie;
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "set_cookie", in);
}

/// \brief Change the cookie associated with a lock
///
/// Append a call to a write operation to change the cookie associated
/// with a lock. (So, for example, a racing break_lock or unlock won't
/// remove it.)
///
/// \param wop Operation to modify
/// \param name Lock name, multiple locks (differentiated by name) may
///             exist on the same object. May not be empty.
/// \param type Lock type (exclusive, shared, ephemeral)
/// \param cookie Identifies a specific lock by a specific holder,
///               needed to unlock/break a lock.
/// \param tag Essentially an 'epoch'. If locks exist on an object, a
///            new lock, even if it would otherwise be concordant with
///            the others, will be rejected if the tag doesn't match.
/// \param new cookie The new cookie to use
///
/// \return The op, for chaining
inline WriteOp&& set_cookie(WriteOp&& wop, std::string name, type t,
			    std::string cookie, std::string tag,
			    std::string new_cookie) {
  set_cookie(wop, std::move(name), t, std::move(cookie), std::move(tag),
	     std::move(new_cookie));
  return std::move(wop);
}

/// \brief List current locks on the object
///
/// Execute an asynchronous operation that lists outstanding locks.
///
/// \param r RADOS handle
/// \param o Object to query
/// \param ioc IOContext determining the object location
/// \param token Boost.Asio CompletionToken
///
/// \return The lock list in a way appropriate to the completion
/// token. See Boost.Asio documentation.
template<boost::asio::completion_token_for<
	   void(boost::system::error_code,
                std::vector<std::string>)> CompletionToken>
auto list(RADOS& r, Object o, IOContext ioc,
	  CompletionToken&& token)
{
  using namespace std::literals;
  return exec<cls_lock_list_locks_reply>(
    r, std::move(o), std::move(ioc),
    "lock"s, "list_locks"s, nullptr,
    [](cls_lock_list_locks_reply&& ret) {
      return std::move(ret.locks);
    }, std::forward<CompletionToken>(token));
}

/// \brief Get info for a named lock
///
/// Execute an asynchronous operation that returns all information on
/// the named lock
///
/// \param r RADOS handle
/// \param o Object to query
/// \param ioc IOContext determining the object location
/// \param name Lock name
/// \param token Boost.Asio CompletionToken
///
/// \return A map of lockers, type, and tag in way appropriate to the completion
/// token. See Boost.Asio documentation.
template<boost::asio::completion_token_for<
	   void(boost::system::error_code,
  std::map<locker_id_t, locker_info_t>,
  type, std::string)> CompletionToken>
auto info(RADOS& r, Object o, IOContext ioc,
	  std::string name,
	  CompletionToken&& token)
{
  using namespace std::literals;
  cls_lock_get_info_op op;
  op.name = std::move(name);
  return exec<cls_lock_get_info_reply>(
    r, std::move(o), std::move(ioc),
    "lock"s, "get_info"s, op,
    [](cls_lock_get_info_reply&& ret) {
      return std::make_tuple(std::move(ret.lockers), ret.lock_type,
			     std::move(ret.tag));
    }, std::forward<CompletionToken>(token));
}

/// \brief A class representing a single lock
class Lock {
  RADOS& r;
  const Object obj;
  const IOContext ioc;
  const std::string name;
  std::string cookie;
  std::string tag;
  std::string description;
  ceph::timespan duration{0};
public:

  /// \brief Constructor
  ///
  /// \param r RADOS handle
  /// \param obj Object name
  /// \param ioc Object locator
  /// \param name Lock name
  Lock(neorados::RADOS& r, Object obj, IOContext ioc,
       std::string name)
    : r(r), obj(std::move(obj)), ioc(std::move(ioc)),
      name(std::move(name)) {}

  Lock(const Lock&) = default;
  Lock(Lock&&) = default;

  Lock& operator =(const Lock&) = delete;
  Lock& operator =(Lock&&) = delete;

  ~Lock() = default;

  /// \brief Set the cookie (for use in further calls)
  ///
  /// \return The lock, for chaining purposes
  Lock&& with_cookie(std::string c) {
    cookie = std::move(c);
    return std::move(*this);
  }

  /// \brief Set the cookie to a random value
  ///
  /// \return The lock, for chaining purposes
  Lock&& with_random_cookie() {
    cookie = random_cookie();
    return std::move(*this);
  }

  /// \brief Set the lock's tag
  ///
  /// \return The lock, for chaining purposes
  Lock&& with_tag(std::string t) {
    tag = std::move(t);
    return std::move(*this);
  }

  /// \brief Set the lock's description
  ///
  /// \return The lock, for chaining purposes
  Lock&& with_description(std::string d) {
    description = std::move(d);
    return std::move(*this);
  }

  /// \brief Set the lock's duration
  ///
  /// \return The lock, for chaining purposes
  Lock&& with_duration(ceph::timespan d) {
    duration = d;
    return std::move(*this);
  }

  /// \brief Assert that a lock is held on an object
  ///
  /// Append a call to a write operation requiring that the lock be held
  /// for the oepration to complete.
  ///
  /// \param op The operation to modify
  /// \param type The lock type to assert
  void assert_locked(Op& op, type t) {
    return lock::assert_locked(op, name, t, cookie, tag);
  }

  /// \brief Assert that a lock is held on an object
  ///
  /// Append a call to a write operation requiring that the lock be held
  /// for the oepration to complete.
  ///
  /// \param op The operation to modify
  /// \param type The lock type to assert
  ///
  /// \return The op, for chaining
  ReadOp&& assert_locked(ReadOp&& op, type t) {
    assert_locked(op, t);
    return std::move(op);
  }

  /// \brief Assert that a lock is held on an object
  ///
  /// Append a call to a write operation requiring that the lock be held
  /// for the oepration to complete.
  ///
  /// \param op The operation to modify
  /// \param type The lock type to assert
  ///
  /// \return The op, for chaining
  WriteOp&& assert_locked(WriteOp&& op, type t) {
    assert_locked(op, t);
    return std::move(op);
  }

  /// \brief Take the lock
  ///
  /// Execute an asynchronous operation that takes the lock
  /// represented by this object.
  ///
  /// \param token Boost.Asio completion token
  /// \param flags Flags for the operation, \see Flags
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token. See Boost.Asio documentation.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto lock(type t, CompletionToken&& token, std::byte flags = std::byte{0}) {
    return r.execute(obj, ioc,
		     lock::lock(WriteOp{}, name, t, cookie, tag, description,
				duration, flags),
		     std::forward<CompletionToken>(token));
  }

  /// \brief Release the lock
  ///
  /// Execute an asynchronous operation that releases the lock
  /// represented by this object.
  ///
  /// \param token Boost.Asio completion token
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token. See Boost.Asio documentation.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto unlock(CompletionToken&& token) {
    return r.execute(obj, ioc,
		     lock::unlock(WriteOp{}, name, cookie),
		     std::forward<CompletionToken>(token));
  }

  /// \brief Break another client's lock
  ///
  /// Execute an asynchronous operation that breaks the lock
  /// of another client.
  ///
  /// \param other_cookie The other client lock's cookie
  /// \param locker Identifier of the RADOS client holding the lock,
  ///               \see info
  /// \param token Boost.Asio completion token
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token. See Boost.Asio documentation.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto break_lock(std::string other_cookie, entity_name_t locker,
		  CompletionToken&& token) {
    return r.execute(obj, ioc,
		     lock::break_lock(WriteOp{}, name, std::move(other_cookie),
				      std::move(locker)),
		     std::forward<CompletionToken>(token));
  }

  /// \brief Get info for a named lock
  ///
  /// Execute an asynchronous operation that returns all information on
  /// the named lock
  ///
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return The lock list in a way appropriate to the completion
  /// token. See Boost.Asio documentation.
  template <boost::asio::completion_token_for<void(
      boost::system::error_code, std::map<locker_id_t, locker_info_t>, type,
      std::string)> CompletionToken>
  auto info(CompletionToken&& token) {
    return lock::info(r, obj, ioc, name,
		      std::forward<CompletionToken>(token));
  }

  /// \brief Change the lock's cookie
  ///
  /// Execute an asynchronous operation that sets the lock's cookie to
  /// a new value.
  ///
  /// \param t Must match type of existing lock
  /// \param token Boost.Asio CompletionToken
  /// \param new_cookie The new cookie
  ///
  /// \return Errors as appropriate to the completion token. See Boost.Asio
  /// documentation.
  template<typename CompletionToken>
  auto set_cookie(type t, std::string new_cookie,
		  CompletionToken &&token) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
    namespace asio = boost::asio;
    using boost::system::error_code;
    using boost::system::system_error;
    using boost::system::errc::no_message_available;

  return asio::async_initiate<CompletionToken,
			      void(error_code)>
    (asio::experimental::co_composed<void(error_code)>
     ([](auto state, RADOS& r, Object oid, IOContext ioc,
	 std::string name, type t, std::string& cookie,
	 std::string tag, std::string new_cookie) -> void {
       try {
	 co_await r.execute(oid, ioc,
			    lock::set_cookie(WriteOp{}, std::move(name), t,
					     cookie, std::move(tag), new_cookie),
			    asio::deferred);
	 cookie.swap(new_cookie);
       } catch (const system_error& e) {
	 co_return e.code();
       }
       co_return error_code{};
     }, r.get_executor()),
     token, std::ref(r), obj, ioc, name, t, std::ref(cookie), tag,
     std::move(new_cookie));
#pragma GCC diagnostic pop
  }

  /// \brief Return a randomly cookie
  ///
  /// \return A randomly generated cookie
  std::string random_cookie() {
    static constexpr std::size_t COOKIE_LEN = 16;
    std::string s(COOKIE_LEN, '\0');
    gen_rand_alphanumeric(r.cct(), s.data(), COOKIE_LEN);
    return s;
  }
};
} // namespace neorados::cls::lock
