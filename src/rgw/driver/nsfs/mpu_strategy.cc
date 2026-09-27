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
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* The spellings, in one place.  They were three constants and two
 * open-coded concatenations. */
static const std::string RGW_MP_STAGING_PREFIX = ".multipart_";
static const std::string RGW_MP_PART_PREFIX = "part-";
static const std::string RGW_MP_META_NAME = ".meta";
static const std::string RGW_MP_ASSEMBLED_NAME = ".assembled";
/* the data file the strided layout writes its parts into, named for
 * the stride it runs at.
 *
 * Keyed by the stride so that establishing it is one atomic act.  A
 * multipart upload is concurrent by nature, so the stride has to be
 * published without a window in which it is half-known.  Creating this
 * file publishes both that a stride exists and what it is:  the linkat
 * either succeeds, committing the value in the name, or fails EEXIST
 * because the value is already there.
 *
 * A fixed name cannot do that -- it says a stride exists and leaves
 * the value to a second record, and two places holding one fact is
 * what let the part record and the multipart cache disagree.
 *
 * NooBaa's spelling, because it is NooBaa's scheme.  We arrived at
 * keying by size independently, for the atomicity;  differing in a dot
 * and a hyphen would leave this driver carrying two size-keyed name
 * schemes once it also reads theirs, distinguishable only by looking
 * twice.  What still differs is what the key means -- theirs is the
 * size of the parts in that file, ours is the upload's stride, so a
 * part which does not fit takes its own file where theirs would open
 * a second size file. */
static const std::string RGW_MP_SHARED_PREFIX = "parts-size-";

std::string PerPartMPUStrategy::staging_dir_name(const std::string& meta) const
{
  return RGW_MP_STAGING_PREFIX + url_encode(meta, true);
}

bool PerPartMPUStrategy::names_staging_dir(std::string_view name) const
{
  return name.starts_with(RGW_MP_STAGING_PREFIX);
}

std::string_view PerPartMPUStrategy::staging_prefix() const
{
  return RGW_MP_STAGING_PREFIX;
}

std::string PerPartMPUStrategy::part_name(uint32_t part_num) const
{
  return RGW_MP_PART_PREFIX + fmt::format("{:0>5}", part_num);
}

bool PerPartMPUStrategy::is_part_name(std::string_view name) const
{
  return name.starts_with(RGW_MP_PART_PREFIX);
}

std::optional<uint32_t> PerPartMPUStrategy::part_number(std::string_view name) const
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

std::optional<MPUStrategy::PartTarget>
PerPartMPUStrategy::part_target(uint32_t part_num,
			    std::optional<uint64_t> stride) const
{
  /* one file per part, so the part's record and its data are the same
   * file, the bytes start at its beginning, and nothing bounds them.
   * The stride is not consulted:  this layout places a part without
   * one, which is why it never answers nullopt. */
  return PartTarget{part_name(part_num), 0, std::nullopt, false};
}

std::string PerPartMPUStrategy::head_name() const
{
  return part_name(0);
}

std::string PerPartMPUStrategy::meta_name() const
{
  return RGW_MP_META_NAME;
}

std::string PerPartMPUStrategy::assembled_name() const
{
  return RGW_MP_ASSEMBLED_NAME;
}

const ReservedNames& PerPartMPUStrategy::reserved_names() const
{
  static const ReservedNames names{
    .exact = { RGW_MP_META_NAME, RGW_MP_ASSEMBLED_NAME },
    .prefixes = {},
    .staging_prefixes = { RGW_MP_STAGING_PREFIX },
  };
  return names;
}

int PerPartMPUStrategy::assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
			     int dir_fd,
			     const std::vector<PartPlacement>& parts,
			     const std::string& output_name,
			     std::optional<uint64_t> stride) const
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

  /* Each part is its own whole file, so the recorded placement says
   * nothing this layout needs:  only the part numbers are taken from
   * it, and the length still comes from the file, as it always has. */
  off_t out_offset = 0;
  for (const auto& part : parts) {
    const std::string pname = part_name(part.num);
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

std::optional<MPUStrategy::PartTarget>
StridedMPUStrategy::part_target(uint32_t part_num,
				std::optional<uint64_t> stride) const
{
  /* No stride yet -- part 1 has not completed, or the upload was
   * disqualified.  Fall back to the base layout for this part:  its own
   * file, unbounded.  Assembly finds it by its record, so a part placed
   * this way costs a copy and nothing else. */
  if (!stride || *stride == 0) {
    return PerPartMPUStrategy::part_target(part_num, stride);
  }

  /* Part K occupies [(K-1) * stride, K * stride).  The extent is what
   * makes an overrun refusable:  a part longer than the stride would
   * write into part K+1's region, so the writer stops at the boundary
   * and diverts instead of discovering the damage later.  Writing LESS
   * is ordinary -- the final part is normally short, and it is the one
   * case that needs no handling at all. */
  return PartTarget{RGW_MP_SHARED_PREFIX + std::to_string(*stride),
		    (static_cast<uint64_t>(part_num) - 1) * *stride,
		    *stride,
		    true};
}

const ReservedNames& StridedMPUStrategy::reserved_names() const
{
  /* the base layout's names plus the shared data file.  Spelled out
   * rather than copied from the base at runtime:  the base returns a
   * reference to its own static, and appending to a copy of it on
   * first use would be the same list with a race in front of it. */
  static const ReservedNames names{
    .exact = { RGW_MP_META_NAME, RGW_MP_ASSEMBLED_NAME },
    .prefixes = { RGW_MP_SHARED_PREFIX },
    .staging_prefixes = { RGW_MP_STAGING_PREFIX },
  };
  return names;
}

std::optional<std::string>
StridedMPUStrategy::shared_name(uint64_t stride) const
{
  return RGW_MP_SHARED_PREFIX + std::to_string(stride);
}

int StridedMPUStrategy::assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
				 int dir_fd,
				 const std::vector<PartPlacement>& parts,
				 const std::string& output_name,
				 std::optional<uint64_t> stride) const
{
  if (!fs) {
    return -EINVAL;
  }
  if (parts.empty()) {
    return -EINVAL;
  }

  /* the shared file's name is the stride, so without one there is no
   * shared file and nothing here could have been placed in it */
  const std::string sname = stride ? *shared_name(*stride) : std::string();
  if (sname.empty()) {
    return PerPartMPUStrategy::assemble(dpp, fs, dir_fd, parts, output_name,
					stride);
  }

  /* Can the shared file be the object as it stands?
   *
   * Only if every part was placed in it and the placements abut:  part
   * K must begin exactly where part K-1 ended.  Asked of the RECORDS
   * rather than inferred from sizes, which is what lets this be a plain
   * question instead of NooBaa's count of distinct part sizes.  Three
   * things break it, and all three are ordinary rather than
   * exceptional:  a part which arrived before the stride existed, a
   * part which exceeded its extent and diverted, and a non-final part
   * shorter than the stride, which S3 permits and which leaves a hole.
   *
   * Gaps in part numbering do not need a test of their own.  A missing
   * part number means the part before it wrote less than the stride
   * ahead of the next one's offset, so the abutment test already
   * fails. */
  uint64_t total = 0;
  bool contiguous = true;
  for (const auto& part : parts) {
    if (!part.shared || part.offset != total) {
      contiguous = false;
      break;
    }
    total += part.stored;
  }

  if (contiguous) {
    /* The shared file already holds the object, in order, from zero.
     * Link it under the output name -- no copy at any size -- and trim,
     * because it may be longer than the object:  a short final part may
     * have been padded to a block, and a diverted part may have left a
     * tail behind. */
    if (linkat(dir_fd, sname.c_str(),
	       dir_fd, output_name.c_str(), 0) < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not link " << sname
			<< " to " << output_name << ": " << cpp_strerror(ret)
			<< dendl;
      return -ret;
    }

    int out_fd = openat(dir_fd, output_name.c_str(), O_WRONLY);
    if (out_fd < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open " << output_name
			<< " to trim: " << cpp_strerror(ret) << dendl;
      return -ret;
    }
    auto close_out = make_scope_guard([out_fd] { ::close(out_fd); });

    if (ftruncate(out_fd, total) < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not trim " << output_name << " to "
			<< total << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }

    ldpp_dout(dpp, 15) << "StridedMPUStrategy::assemble: linked "
		       << parts.size() << " parts, " << total
		       << " bytes, no copy" << dendl;
    return 0;
  }

  /* Some part is not where the object wants it.  Copy each one from
   * wherever its record says it is;  a part in the shared file is read
   * from its offset there, a part in its own file from the start of
   * that file. */
  int out_fd = openat(dir_fd, output_name.c_str(),
		      O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  if (out_fd < 0) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not create assembly file "
		      << output_name << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }
  auto close_out = make_scope_guard([out_fd] { ::close(out_fd); });

  int shared_fd = -1;
  auto close_shared = make_scope_guard([&shared_fd] {
    if (shared_fd >= 0) {
      ::close(shared_fd);
    }
  });

  off_t out_offset = 0;
  for (const auto& part : parts) {
    int src_fd;
    int own_fd = -1;
    auto close_own = make_scope_guard([&own_fd] {
      if (own_fd >= 0) {
	::close(own_fd);
      }
    });

    if (part.shared) {
      if (shared_fd < 0) {
	shared_fd = openat(dir_fd, sname.c_str(), O_RDONLY);
	if (shared_fd < 0) {
	  int ret = errno;
	  ldpp_dout(dpp, 0) << "ERROR: could not open " << sname
			    << ": " << cpp_strerror(ret) << dendl;
	  return -ret;
	}
      }
      src_fd = shared_fd;
    } else {
      const std::string pname = part_name(part.num);
      own_fd = openat(dir_fd, pname.c_str(), O_RDONLY);
      if (own_fd < 0) {
	int ret = errno;
	ldpp_dout(dpp, 0) << "ERROR: could not open part " << pname << ": "
			  << cpp_strerror(ret) << dendl;
	return -ret;
      }
      src_fd = own_fd;
    }

    int ret = fs->copy_range(dpp, src_fd, part.shared ? part.offset : 0,
			     out_fd, out_offset, part.stored);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not copy part " << part.num << ": "
			<< cpp_strerror(-ret) << dendl;
      return ret;
    }
    out_offset += part.stored;
  }

  ldpp_dout(dpp, 10) << "StridedMPUStrategy::assemble: copied "
		     << parts.size() << " parts, " << out_offset
		     << " bytes -- placement was not contiguous" << dendl;
  return 0;
}

}}} // namespace rgw::sal::nsfs
