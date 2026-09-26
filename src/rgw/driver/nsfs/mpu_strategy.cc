// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "mpu_strategy.h"

#include <cerrno>
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/stat.h>
#include <unistd.h>

#include <charconv>

#include <fmt/format.h>

#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#include "fs_strategy.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* The spellings, in one place.  They were three constants and two
 * open-coded concatenations. */
static const std::string RGW_MP_STAGING_PREFIX = ".multipart_";
static const std::string RGW_MP_PART_PREFIX = "part-";
static const std::string RGW_MP_META_NAME = ".meta";
static const std::string RGW_MP_ASSEMBLED_NAME = ".assembled";

std::string RGWMPUStrategy::staging_dir_name(const std::string& upload_id) const
{
  return RGW_MP_STAGING_PREFIX + upload_id;
}

bool RGWMPUStrategy::names_staging_dir(std::string_view name) const
{
  return name.starts_with(RGW_MP_STAGING_PREFIX);
}

std::string RGWMPUStrategy::part_name(uint32_t part_num) const
{
  return RGW_MP_PART_PREFIX + fmt::format("{:0>5}", part_num);
}

bool RGWMPUStrategy::is_part_name(std::string_view name) const
{
  return name.starts_with(RGW_MP_PART_PREFIX);
}

std::optional<uint32_t> RGWMPUStrategy::part_number(std::string_view name) const
{
  if (! is_part_name(name)) {
    return std::nullopt;
  }
  const std::string_view digits{name.substr(RGW_MP_PART_PREFIX.length())};
  if (digits.empty()) {
    return std::nullopt;
  }

  /* from_chars rather than stoul:  it parses into the target type, so a
   * value above 2^32-1 is reported as out of range instead of being
   * truncated by a narrowing cast;  it needs no try/catch;  and
   * requiring ptr == end rejects "00001x", which stoul would accept.
   * This reads a name found on disk, so anything may be there. */
  uint32_t part_num = 0;
  const char* const end = digits.data() + digits.size();
  const auto [ptr, ec] = std::from_chars(digits.data(), end, part_num);
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return part_num;
}

std::string RGWMPUStrategy::head_name() const
{
  return part_name(0);
}

std::string RGWMPUStrategy::meta_name() const
{
  return RGW_MP_META_NAME;
}

std::string RGWMPUStrategy::assembled_name() const
{
  return RGW_MP_ASSEMBLED_NAME;
}

const ReservedNames& RGWMPUStrategy::reserved_names() const
{
  static const ReservedNames names{
    .exact = { RGW_MP_META_NAME, RGW_MP_ASSEMBLED_NAME },
    .prefixes = {},
    .staging_prefixes = { RGW_MP_STAGING_PREFIX },
  };
  return names;
}

int RGWMPUStrategy::assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
			     int dir_fd, int num_parts,
			     const std::string& output_name) const
{
  if (! fs) {
    return -EINVAL;
  }

  int out_fd = openat(dir_fd, output_name.c_str(),
		      O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  if (out_fd < 0) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not create assembly file "
		      << output_name << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }
  auto close_out = make_scope_guard([out_fd] { ::close(out_fd); });

  off_t out_offset = 0;
  for (int i = 1; i <= num_parts; ++i) {
    const std::string pname = part_name(i);
    int part_fd = openat(dir_fd, pname.c_str(), O_RDONLY);
    if (part_fd < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open part " << pname
			<< ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }
    auto close_part = make_scope_guard([part_fd] { ::close(part_fd); });

    struct statx stx;
    int ret = statx(part_fd, "", AT_EMPTY_PATH, STATX_SIZE, &stx);
    if (ret < 0) {
      ret = errno;
      return -ret;
    }

    ret = fs->copy_range(dpp, part_fd, 0, out_fd, out_offset, stx.stx_size);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not copy part " << pname
			<< ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    out_offset += stx.stx_size;
  }

  return 0;
}

}}} // namespace rgw::sal::nsfs
