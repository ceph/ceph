// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_REPLAY_ACTION_TYPES_H
#define CEPH_RBD_REPLAY_ACTION_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <iosfwd>
#include <list>
#include <string>
#include <vector>
#include <boost/variant/variant.hpp>

namespace ceph { class Formatter; }

namespace rbd_replay {
namespace action {

typedef uint64_t imagectx_id_t;
typedef uint64_t thread_id_t;

/// Even IDs are normal actions, odd IDs are completions.
typedef uint32_t action_id_t;

static const std::string BANNER("rbd-replay-trace");

/**
 * Dependencies link actions to earlier actions or completions.
 * If an action has a dependency \c d then it waits until \c d.time_delta
 * nanoseconds after the action or completion with ID \c d.id has fired.
 */
struct Dependency {
  /// ID of the action or completion to wait for.
  action_id_t id;

  /// Nanoseconds of delay to wait until after the action or completion fires.
  uint64_t time_delta;

  /**
   * @param id ID of the action or completion to wait for.
   * @param time_delta Nanoseconds of delay to wait after the action or
   *                   completion fires.
   */
  Dependency() : id(0), time_delta(0) {
  }
  Dependency(action_id_t id, uint64_t time_delta)
    : id(id), time_delta(time_delta) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<Dependency *> &o);
};

WRITE_CLASS_ENCODER(Dependency);

typedef std::vector<Dependency> Dependencies;

enum ActionType {
  ACTION_TYPE_START_THREAD    = 0,
  ACTION_TYPE_STOP_THREAD     = 1,
  ACTION_TYPE_READ            = 2,
  ACTION_TYPE_WRITE           = 3,
  ACTION_TYPE_AIO_READ        = 4,
  ACTION_TYPE_AIO_WRITE       = 5,
  ACTION_TYPE_OPEN_IMAGE      = 6,
  ACTION_TYPE_CLOSE_IMAGE     = 7,
  ACTION_TYPE_AIO_OPEN_IMAGE  = 8,
  ACTION_TYPE_AIO_CLOSE_IMAGE = 9,
};

struct ActionBase {
  action_id_t id;
  thread_id_t thread_id;
  Dependencies dependencies;

  ActionBase() : id(0), thread_id(0) {
  }
  ActionBase(action_id_t id, thread_id_t thread_id,
             const Dependencies &dependencies)
    : id(id), thread_id(thread_id), dependencies(dependencies) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

struct StartThreadAction : public ActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_START_THREAD;

  StartThreadAction() {
  }
  StartThreadAction(action_id_t id, thread_id_t thread_id,
                    const Dependencies &dependencies)
    : ActionBase(id, thread_id, dependencies) {
  }
};

struct StopThreadAction : public ActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_STOP_THREAD;

  StopThreadAction() {
  }
  StopThreadAction(action_id_t id, thread_id_t thread_id,
                   const Dependencies &dependencies)
    : ActionBase(id, thread_id, dependencies) {
  }
};

struct ImageActionBase : public ActionBase {
  imagectx_id_t imagectx_id;

  ImageActionBase() : imagectx_id(0) {
  }
  ImageActionBase(action_id_t id, thread_id_t thread_id,
                  const Dependencies &dependencies, imagectx_id_t imagectx_id)
    : ActionBase(id, thread_id, dependencies), imagectx_id(imagectx_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

struct IoActionBase : public ImageActionBase {
  uint64_t offset;
  uint64_t length;

  IoActionBase() : offset(0), length(0) {
  }
  IoActionBase(action_id_t id, thread_id_t thread_id,
               const Dependencies &dependencies, imagectx_id_t imagectx_id,
               uint64_t offset, uint64_t length)
    : ImageActionBase(id, thread_id, dependencies, imagectx_id),
      offset(offset), length(length) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

struct ReadAction : public IoActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_READ;

  ReadAction() {
  }
  ReadAction(action_id_t id, thread_id_t thread_id,
             const Dependencies &dependencies, imagectx_id_t imagectx_id,
             uint64_t offset, uint64_t length)
    : IoActionBase(id, thread_id, dependencies, imagectx_id, offset, length) {
  }
};

struct WriteAction : public IoActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_WRITE;

  WriteAction() {
  }
  WriteAction(action_id_t id, thread_id_t thread_id,
              const Dependencies &dependencies, imagectx_id_t imagectx_id,
              uint64_t offset, uint64_t length)
    : IoActionBase(id, thread_id, dependencies, imagectx_id, offset, length) {
  }
};

struct AioReadAction : public IoActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_AIO_READ;

  AioReadAction() {
  }
  AioReadAction(action_id_t id, thread_id_t thread_id,
                const Dependencies &dependencies, imagectx_id_t imagectx_id,
                uint64_t offset, uint64_t length)
    : IoActionBase(id, thread_id, dependencies, imagectx_id, offset, length) {
  }
};

struct AioWriteAction : public IoActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_AIO_WRITE;

  AioWriteAction() {
  }
  AioWriteAction(action_id_t id, thread_id_t thread_id,
                 const Dependencies &dependencies, imagectx_id_t imagectx_id,
                 uint64_t offset, uint64_t length)
    : IoActionBase(id, thread_id, dependencies, imagectx_id, offset, length) {
  }
};

struct OpenImageAction : public ImageActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_OPEN_IMAGE;

  std::string name;
  std::string snap_name;
  bool read_only;

  OpenImageAction() : read_only(false) {
  }
  OpenImageAction(action_id_t id, thread_id_t thread_id,
                  const Dependencies &dependencies, imagectx_id_t imagectx_id,
                  const std::string &name, const std::string &snap_name,
                  bool read_only)
    : ImageActionBase(id, thread_id, dependencies, imagectx_id),
      name(name), snap_name(snap_name), read_only(read_only) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

struct CloseImageAction : public ImageActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_CLOSE_IMAGE;

  CloseImageAction() {
  }
  CloseImageAction(action_id_t id, thread_id_t thread_id,
                   const Dependencies &dependencies, imagectx_id_t imagectx_id)
    : ImageActionBase(id, thread_id, dependencies, imagectx_id) {
  }
};

struct AioOpenImageAction : public ImageActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_AIO_OPEN_IMAGE;

  std::string name;
  std::string snap_name;
  bool read_only;

  AioOpenImageAction() : read_only(false) {
  }
  AioOpenImageAction(action_id_t id, thread_id_t thread_id,
		     const Dependencies &dependencies, imagectx_id_t imagectx_id,
		     const std::string &name, const std::string &snap_name,
		     bool read_only)
    : ImageActionBase(id, thread_id, dependencies, imagectx_id),
      name(name), snap_name(snap_name), read_only(read_only) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

struct AioCloseImageAction : public ImageActionBase {
  static const ActionType ACTION_TYPE = ACTION_TYPE_AIO_CLOSE_IMAGE;

  AioCloseImageAction() {
  }
  AioCloseImageAction(action_id_t id, thread_id_t thread_id,
		      const Dependencies &dependencies, imagectx_id_t imagectx_id)
    : ImageActionBase(id, thread_id, dependencies, imagectx_id) {
  }
};

struct UnknownAction {
  static const ActionType ACTION_TYPE = static_cast<ActionType>(-1);

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &it);
  void dump(Formatter *f) const;
};

typedef boost::variant<StartThreadAction,
                       StopThreadAction,
                       ReadAction,
                       WriteAction,
                       AioReadAction,
                       AioWriteAction,
                       OpenImageAction,
                       CloseImageAction,
                       AioOpenImageAction,
                       AioCloseImageAction,
                       UnknownAction> Action;

class ActionEntry {
public:
  Action action;

  ActionEntry() : action(UnknownAction()) {
  }
  ActionEntry(const Action &action) : action(action) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void decode_unversioned(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ActionEntry *> &o);

private:
  void decode(__u8 version, bufferlist::iterator &it);
};

WRITE_CLASS_ENCODER(ActionEntry);

} // namespace action
} // namespace rbd_replay

std::ostream &operator<<(std::ostream &out,
                         const rbd_replay::action::ActionType &type);

using rbd_replay::action::decode;
using rbd_replay::action::encode;

#endif // CEPH_RBD_REPLAY_ACTION_TYPES_H
