// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/neorados/RADOS.hpp"

#include "common/ceph_context.h"

#include "neorados/cls/version.h"

namespace rgw::neorados {
using namespace ::neorados;
/// `VersionTracker`
/// ================
///
/// What and why is this?
/// ---------------------
///
/// This is a wrapper around `cls_version` functionality. If two RGWs
/// (or two non-synchronized threads in the same RGW) are accessing
/// the same object, they may race and overwrite each other's work.
///
/// This class solves this issue by tracking and recording an object's
/// version in the extended attributes. Operations are failed with
/// `sys::errc::operation_canceled` if the version is not what we expect.
///
/// How to Use It
/// -------------
///
/// When preparing a read operation, call `prepare_read`.
/// For a write, call `prepare_write` when preparing the
/// operation, and `apply_write` after it succeeds.
///
/// Adhere to the following guidelines:
///
/// - Each `VersionTracker` should be used with only one object.
///
/// - If you receive `sys::errc::operation_canceled`, throw away
///   whatever you were doing based on the content of the versioned
///   object, re-read, and restart as appropriate.
///
/// - If one code path uses `VersionTracker`, then they all should. In
///   a situation where a writer should unconditionally overwrite an
///   object, call `new_write_version` on a default constructed
///   `VersionTracker`.
///
/// - If we have a version from a previous read, we will check against
///   it and fail the read if it doesn't match. Thus, if we want to
///   re-read a new version of the object, call `clear()` on the
///   `VersionTracker`.
///
/// - This type is not thread-safe. Every thread must have its own
///   instance.
struct VersionTracker {
  obj_version read_version; //< The version read from an object. If
			    //  set, this value is used to check the
			    //  stored version.
  obj_version write_version; //< Set the object to this version on
			     //  write, if set.

  /// Pointer to the read version.
  obj_version* version_for_read() {
    return &read_version;
  }

  /// If we have a write version, return a pointer to it. Otherwise
  /// return null. This is used in `prepare_write` to treat the
  /// `write_version` as effectively an `option` type.
  obj_version* version_for_write() {
    if (write_version.ver == 0)
      return nullptr;

    return &write_version;
  }

  /// If read_version is non-empty, return a pointer to it, otherwise
  /// null. This is used internally by `prepare_op_for_read` and
  /// `prepare_op_for_write` to treat the `read_version` as
  /// effectively an `option` type.
  obj_version* version_for_check() {
    if (read_version.ver == 0)
      return nullptr;

    return &read_version;
  }

  /// This function is to be called on any read operation. If we have
  /// a non-empty `read_version`, assert on the OSD that the object
  /// has the same version. Also reads the version into `read_version`.
  void prepare_read(ReadOp& op);

  /// This function is to be called on any write operation. If we have
  /// a non-empty read operation, assert on the OSD that the object
  /// has the same version. If we have a non-empty `write_version`,
  /// set the object to it. Otherwise increment the version on the OSD.
  void prepare_write(WriteOp& op);

  /// This function is to be called after the completion of any write
  /// operation on which `prepare_op_for_write` was called. If we did
  /// not set the write version explicitly, it increments
  /// `read_version`. If we did, it sets `read_version` to
  /// `write_version`. In either case, it clears `write_version`.
  ///
  /// RADOS write operations, at least those not using the relatively
  /// new RETURNVEC flag, cannot return more information than an error
  /// code. Thus, write operations can't simply fill in the read
  /// version the way read operations can, so prepare_op_for_write`
  /// instructs the OSD to increment the object as stored in RADOS and
  /// `apply_write` increments our `read_version` in RAM.
  void apply_write();

  /// Clear `read_version` and `write_version`, making the instance
  /// identical to a default-constructed instance.
  void clear() {
    read_version = obj_version();
    write_version = obj_version();
  }

  /// Set `write_version` to a new, unique version.
  ///
  /// An `obj_version` contains an opaque, random tag and a
  /// sequence. If the tags of two `obj_version`s don't match, the
  /// versions are unordered and unequal. This function creates a
  /// version with a new tag, ensuring that any other process
  /// operating on the object will receive `ECANCELED` and will know
  /// to re-read the object and restart whatever it was doing.
  void new_write_version(CephContext* cct);
};
}
