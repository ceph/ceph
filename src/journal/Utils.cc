// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Utils.h"
#include "include/Context.h"
#include "include/stringify.h"

namespace journal {
namespace utils {

std::string get_object_name(const std::string &prefix, uint64_t number) {
  return prefix + stringify(number);
}

std::string unique_lock_name(const std::string &name, void *address) {
  return name + " (" + stringify(address) + ")";
}

void rados_ctx_callback(rados_completion_t c, void *arg) {
  Context *comp = reinterpret_cast<Context *>(arg);
  comp->complete(rados_aio_get_return_value(c));
}

} // namespace utils
} // namespace journal
