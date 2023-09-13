// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/file-types.hh>

#include "include/buffer_fwd.h"

namespace crimson {
  seastar::future<> write_file(ceph::buffer::list&& bl,
                               seastar::sstring fn,
                               seastar::file_permissions= // 0644
                                 (seastar::file_permissions::user_read |
                                  seastar::file_permissions::user_write |
                                  seastar::file_permissions::group_read |
                                  seastar::file_permissions::others_read));
  seastar::future<seastar::temporary_buffer<char>>
  read_file(const seastar::sstring fn);
}
