// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_GROUP_MUTATION_LOCK_H
#define CEPH_LIBRBD_GROUP_MUTATION_LOCK_H

#include "include/rados/librados_fwd.hpp"
#include <functional>
#include <string>

struct Context;

namespace librbd {
namespace group {

// Hold the group header lock until the action completes its callback. Replay
// also checks the current role under this lock before changing the group.
void execute_mutation(librados::IoCtx& io_ctx, const std::string& group_id,
                      bool non_primary_only, bool wait_for_lock,
                      std::function<void(int, Context*)> action,
                      Context* on_finish);

} // namespace group
} // namespace librbd

#endif // CEPH_LIBRBD_GROUP_MUTATION_LOCK_H
