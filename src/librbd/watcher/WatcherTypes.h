// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_TYPES_H
#define CEPH_LIBRBD_WATCHER_TYPES_H

#include "include/buffer_fwd.h"
#include "include/encoding.h"

namespace ceph {
class Formatter;
}

namespace librbd {
namespace watcher {

class TaskCode {
public:
  TaskCode(int code) : code(code) {}

  inline bool operator<(const TaskCode& rhs) const {
    return code < rhs.code;
  }
private:
  int code;
};

class Task {
public:
  Task(TaskCode task_code) : m_task_code(task_code) {}

  inline bool operator<(const Task& rhs) const {
    return m_task_code < rhs.m_task_code;
  }
private:
  TaskCode m_task_code;
};

struct ResponseMessage {
  ResponseMessage() : result(0) {}
  ResponseMessage(int result_) : result(result_) {}

  int result;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ResponseMessage *> &o);
};

} // namespace watcher
} // namespace librbd

WRITE_CLASS_ENCODER(librbd::watcher::ResponseMessage);

#endif // CEPH_LIBRBD_WATCHER_TYPES_H
