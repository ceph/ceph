// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_TYPES_H
#define CEPH_LIBRBD_WATCHER_TYPES_H

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


} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_TYPES_H
