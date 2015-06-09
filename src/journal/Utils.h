// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_UTILS_H
#define CEPH_JOURNAL_UTILS_H

#include "include/int_types.h"
#include <string>

namespace journal {
namespace utils {

std::string get_object_name(const std::string &prefix, uint64_t number);

std::string unique_lock_name(const std::string &name, void *address);

} // namespace utils
} // namespace journal

#endif // CEPH_JOURNAL_UTILS_H
