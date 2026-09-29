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
#include <dirent.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/stat.h>
#include <unistd.h>

#include <charconv>
#include <algorithm>
#include <sys/xattr.h>

#include <fmt/format.h>

#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#include "fs_strategy.h"
#include "rgw_common.h"
#include "include/uuid.h"
#include "common/ceph_json.h"

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

/* NooBaa's names, from noobaa-core 68ca22d33.  The temp directory is
 * config.NSFS_TEMP_DIR_NAME + '_' + bucket_id, and the bucket id comes
 * from their config store, so only the prefix is knowable here. */
static const std::string NB_TMPDIR_PREFIX = ".noobaa-nsfs_";
static const std::string NB_MPU_ROOT = "multipart-uploads";
static const std::string NB_PART_PREFIX = "part-";
static const std::string NB_CREATE_NAME = "create_object_upload";
static const std::string NB_FINAL_NAME = "final";
static const char* NB_XATTR_PART_SIZE = "user.noobaa.part_size";
static const char* NB_XATTR_PART_OFFSET = "user.noobaa.part_offset";
static const char* NB_XATTR_CONTENT_MD5 = "user.content_md5";


std::string PerPartMPUStrategy::staging_dir_name(const std::string& meta) const
{
  return RGW_MP_STAGING_PREFIX + url_encode(meta, true);
}

bool PerPartMPUStrategy::is_staging_dir(std::string_view name) const
{
  return name.starts_with(RGW_MP_STAGING_PREFIX);
}

/* The name carries the meta, so this costs a decode and a split and
 * opens nothing. */
bool PerPartMPUStrategy::staged_upload(const DoutPrefixProvider* dpp,
				       int root_fd, std::string_view dname,
				       StagedUpload& out) const
{
  if (!is_staging_dir(dname)) {
    return false;
  }
  std::string_view rest = dname;
  rest.remove_prefix(RGW_MP_STAGING_PREFIX.size());

  const std::string meta = url_decode(std::string(rest));
  const auto dot = meta.rfind('.');
  if (dot == std::string::npos || dot == 0 || dot + 1 == meta.size()) {
    return false;
  }
  out.key = meta.substr(0, dot);
  out.upload_id = meta.substr(dot + 1);
  return true;
}

std::string PerPartMPUStrategy::part_name(uint32_t part_num) const
{
  return RGW_MP_PART_PREFIX + fmt::format("{:0>5}", part_num);
}

std::optional<uint32_t> PerPartMPUStrategy::part_number(std::string_view name) const
{
  if (! name.starts_with(RGW_MP_PART_PREFIX)) {
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

/* Never.  A part lives where its record says, in the file the upload's
 * stride names, and a part shorter than the stride leaves the rest of
 * its slot unread.  This layout gives every part its own file in any
 * case. */
std::optional<MPUStrategy::PartTarget>
PerPartMPUStrategy::relocation_target(uint32_t part_num, uint64_t stored,
				      std::optional<uint64_t> stride) const
{
  return std::nullopt;
}

/* Our record is one ceph-encoded blob, written and read through the
 * bucket's XattrStrategy, so it arrives already decoded in `attrs` and
 * the fd is not needed. */
bool PerPartMPUStrategy::part_record(const DoutPrefixProvider* dpp,
				     int dir_fd, std::string_view pname,
				     const Attrs& attrs,
				     PartRecord& out) const
{
  auto i = attrs.find(RGW_NSFS_ATTR_MPUPLOAD);
  if (i == attrs.end()) {
    return false;
  }
  NSFSUploadPartInfo upi;
  try {
    auto bufit = i->second.cbegin();
    decode(upi, bufit);
  } catch (buffer::error&) {
    return false;
  }

  out.size = upi.size;
  out.stored = upi.stored;
  out.offset = upi.offset;
  out.shared = upi.shared;
  out.etag = std::move(upi.etag);
  out.mtime = upi.mtime;
  out.cksum = std::move(upi.cksum);
  return true;
}

/* Nothing.  The record is the ceph-encoded attribute the caller writes
 * through the bucket's XattrStrategy, and the meta file's content is
 * unused -- part_record() and staged_upload() read that attribute and
 * the staging directory's name, neither of which is written here. */
int PerPartMPUStrategy::write_staged_upload(const DoutPrefixProvider* dpp,
					    int dir_fd,
					    const StagedUpload& su) const
{
  return 0;
}

int PerPartMPUStrategy::write_part_record(const DoutPrefixProvider* dpp,
					  int dir_fd, std::string_view pname,
					  const PartRecord& rec) const
{
  return 0;
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

/* Never, for the same reason as the per-part layout:  assembly reads
 * every shared part out of the one file the stride names, at the
 * offset in its record.  A part which is not the stride is either
 * shorter than its slot or was diverted while it was being written. */
std::optional<MPUStrategy::PartTarget>
StridedMPUStrategy::relocation_target(uint32_t part_num, uint64_t stored,
				      std::optional<uint64_t> stride) const
{
  return std::nullopt;
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


std::string NooBaaMPUStrategy::staging_dir_name(const std::string& meta) const
{
  /* Their directory is the upload id alone -- a randomUUID -- so the
   * key half of the meta is dropped.  The key is inside, in
   * create_object_upload, which is what costs them an open per upload
   * when they enumerate. */
  const auto dot = meta.rfind('.');
  if (dot == std::string::npos) {
    return meta;
  }
  return meta.substr(dot + 1);
}

bool NooBaaMPUStrategy::is_staging_dir(std::string_view name) const
{
  /* Their staging never sits among objects -- it is under the bucket's
   * temp directory, which is reserved whole -- so an entry of the
   * bucket directory is never one of theirs.  This is the question
   * asked while enumerating objects, and for this format the answer is
   * always no;  recognising an upload is staged_upload()'s job, and it
   * reads the directory rather than guessing from a uuid. */
  return false;
}

/* Their directory is named for the upload id and the key is inside, in
 * create_object_upload (`namespace_fs.js` create_object_upload, and
 * their own list_uploads reads it back the same way).  So this opens
 * and parses one file per upload, which is what their layout costs. */
bool NooBaaMPUStrategy::staged_upload(const DoutPrefixProvider* dpp,
				      int root_fd, std::string_view dname,
				      StagedUpload& out) const
{
  if (dname.empty() || (dname == ".") || (dname == "..")) {
    return false;
  }

  const std::string path = std::string(dname) + "/" + NB_CREATE_NAME;
  int fd = ::openat(root_fd, path.c_str(), O_RDONLY);
  if (fd < 0) {
    /* not an upload, or one being torn down;  skipped, not failed */
    return false;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  /* their create params are a JSON dump of the request, so bounded by
   * what a CreateMultipartUpload carries;  a file larger than this is
   * not one of theirs */
  static constexpr size_t MAX_CREATE_PARAMS = 64 * 1024;
  std::string buf(MAX_CREATE_PARAMS, '\0');
  ssize_t len = ::pread(fd, buf.data(), buf.size(), 0);
  if (len <= 0) {
    return false;
  }
  buf.resize(len);

  JSONParser parser;
  if (!parser.parse(buf.data(), buf.size())) {
    ldpp_dout(dpp, 4) << "could not parse " << path << " as JSON;  not "
		      << "treating it as an upload" << dendl;
    return false;
  }
  JSONObj* kobj = parser.find_obj("key");
  if (!kobj) {
    ldpp_dout(dpp, 4) << path << " carries no key" << dendl;
    return false;
  }

  out.key = kobj->get_data();
  out.upload_id = std::string(dname);
  return !out.key.empty();
}

/* The bucket's NooBaa temp directory, or nothing when there is none or
 * more than one.  `ambiguous` distinguishes those two, which matters
 * only to the write side:  a tree with none may be created into, a
 * tree with several may not. */
static std::optional<std::string> nb_scan_tmpdir(const DoutPrefixProvider* dpp,
						 int bucket_fd,
						 bool* ambiguous);
static std::optional<std::string> nb_tmpdir(const DoutPrefixProvider* dpp,
					    int bucket_fd);
static bool nb_tmpdir_ambiguous(const DoutPrefixProvider* dpp, int bucket_fd);

/* A bucket id of our own, when the tree has none.  They use
 * crypto.randomUUID(), so this is one too -- the shape is what a
 * NooBaa gateway would expect to find, even though this particular id
 * is not in their config store. */
static std::string gen_uuid()
{
  uuid_d u;
  u.generate_random();
  return u.to_string();
}

std::optional<std::string>
NooBaaMPUStrategy::staging_root(const DoutPrefixProvider* dpp,
				int bucket_fd) const
{
  auto found = nb_tmpdir(dpp, bucket_fd);
  if (!found) {
    return std::nullopt;
  }

  /* The temp directory is not enough.  NooBaa creates it for ordinary
   * object writes too (`namespace_fs.js:1303`, `:1569`) and only
   * creates multipart-uploads/ beneath it at the first
   * create_object_upload, so a bucket which has been written to and
   * never uploaded to has the one and not the other.  Answering with
   * a path that does not exist would send an enumeration at it. */
  const std::string root = *found + "/" + NB_MPU_ROOT;
  if (::faccessat(bucket_fd, root.c_str(), F_OK, 0) != 0) {
    return std::nullopt;
  }
  return root;
}

/* Use the temp directory that is there;  create one only where the
 * tree has none;  refuse more than one.  See the header. */
std::optional<std::string>
NooBaaMPUStrategy::staging_root_for_write(const DoutPrefixProvider* dpp,
					  int bucket_fd) const
{
  auto found = nb_tmpdir(dpp, bucket_fd);
  if (!found) {
    /* nb_tmpdir() reports nothing both for "none" and for "more than
     * one";  only the first may be created into, and it has said so
     * by not logging */
    if (nb_tmpdir_ambiguous(dpp, bucket_fd)) {
      return std::nullopt;
    }
    std::string name = std::string(NB_TMPDIR_PREFIX) + gen_uuid();
    if ((::mkdirat(bucket_fd, name.c_str(), 0755) < 0) &&
	(errno != EEXIST)) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not create " << name << ": "
			<< cpp_strerror(ret) << dendl;
      return std::nullopt;
    }
    found = name;
  }

  const std::string root = *found + "/" + NB_MPU_ROOT;
  if ((::mkdirat(bucket_fd, root.c_str(), 0755) < 0) &&
      (errno != EEXIST)) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not create " << root << ": "
		      << cpp_strerror(ret) << dendl;
    return std::nullopt;
  }
  return root;
}

static std::optional<std::string> nb_scan_tmpdir(const DoutPrefixProvider* dpp,
						 int bucket_fd, bool* ambiguous)
{
  if (ambiguous) {
    *ambiguous = false;
  }
  int fd = ::openat(bucket_fd, ".", O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    return std::nullopt;
  }
  DIR* d = ::fdopendir(fd);
  if (!d) {
    ::close(fd);
    return std::nullopt;
  }
  auto close_d = make_scope_guard([d] { ::closedir(d); });

  std::string found;
  struct dirent* de;
  while ((de = ::readdir(d)) != nullptr) {
    std::string_view n = de->d_name;
    if (!n.starts_with(NB_TMPDIR_PREFIX)) {
      continue;
    }
    if (!found.empty()) {
      /* Two temp directories means two bucket ids have written here.
       * Which one holds the uploads is not decidable from the tree, so
       * refuse rather than pick -- and do not create a third. */
      ldpp_dout(dpp, 0) << "ERROR: bucket holds more than one "
			<< NB_TMPDIR_PREFIX << "* directory (" << found
			<< ", " << n << ");  refusing to guess which holds "
			<< "multipart uploads" << dendl;
      if (ambiguous) {
	*ambiguous = true;
      }
      return std::nullopt;
    }
    found = n;
  }

  if (found.empty()) {
    return std::nullopt;
  }
  return found;
}

static std::optional<std::string> nb_tmpdir(const DoutPrefixProvider* dpp,
					    int bucket_fd)
{
  return nb_scan_tmpdir(dpp, bucket_fd, nullptr);
}

static bool nb_tmpdir_ambiguous(const DoutPrefixProvider* dpp, int bucket_fd)
{
  bool ambiguous = false;
  (void) nb_scan_tmpdir(dpp, bucket_fd, &ambiguous);
  return ambiguous;
}

std::string NooBaaMPUStrategy::part_name(uint32_t part_num) const
{
  /* not zero padded:  theirs is Number(name.slice('part-'.length)) */
  return NB_PART_PREFIX + std::to_string(part_num);
}

std::optional<uint32_t>
NooBaaMPUStrategy::part_number(std::string_view name) const
{
  if (!name.starts_with(NB_PART_PREFIX)) {
    return std::nullopt;
  }
  const std::string_view digits{name.substr(NB_PART_PREFIX.length())};
  if (digits.empty()) {
    return std::nullopt;
  }
  uint32_t num{0};
  auto [p, ec] = std::from_chars(digits.data(), digits.data() + digits.size(),
				 num);
  if ((ec != std::errc{}) || (p != digits.data() + digits.size())) {
    return std::nullopt;
  }
  return num;
}

std::optional<MPUStrategy::PartTarget>
NooBaaMPUStrategy::part_target(uint32_t part_num,
			       std::optional<uint64_t> stride) const
{
  /* Their offset is the part's own size times (n - 1)
   * (`namespace_fs.js` upload_multipart).  A uniform upload makes that
   * the same arithmetic as a stride, which is the case this can serve.
   *
   * Without one, the part goes to its own file and is copied into a
   * size file afterwards -- which is what they do when the size is not
   * known before the body is read.  Their own file is the record,
   * part-<n>, because they write the data there and copy it out. */
  if (!stride || (*stride == 0)) {
    return PartTarget{part_name(part_num), 0, std::nullopt, false};
  }
  return PartTarget{RGW_MP_SHARED_PREFIX + std::to_string(*stride),
		    (static_cast<uint64_t>(part_num) - 1) * *stride,
		    *stride,
		    true};
}

std::optional<std::string>
NooBaaMPUStrategy::shared_name(uint64_t stride) const
{
  return RGW_MP_SHARED_PREFIX + std::to_string(stride);
}

std::string NooBaaMPUStrategy::meta_name() const
{
  return NB_CREATE_NAME;
}

std::string NooBaaMPUStrategy::assembled_name() const
{
  return NB_FINAL_NAME;
}

namespace {

/* one unsigned decimal xattr, or nothing */
std::optional<uint64_t> u64_xattr(int fd, const char* name)
{
  char buf[32];
  ssize_t len = ::fgetxattr(fd, name, buf, sizeof(buf) - 1);
  if (len <= 0) {
    return std::nullopt;
  }
  buf[len] = '\0';
  char* end = nullptr;
  errno = 0;
  unsigned long long v = ::strtoull(buf, &end, 10);
  if (errno || (end == buf) || (*end != '\0')) {
    return std::nullopt;
  }
  return static_cast<uint64_t>(v);
}

std::string base36(uint64_t v)
{
  static const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  if (v == 0) {
    return "0";
  }
  std::string out;
  while (v) {
    out.push_back(digits[v % 36]);
    v /= 36;
  }
  std::reverse(out.begin(), out.end());
  return out;
}

} /* anonymous namespace */

/* Whenever the part is not the stride.
 *
 * Their file is named for the size it holds and their reader derives
 * the name from the part's own record, so `parts-size-17` is where a
 * 17-byte part must be, at 17 * (num - 1) within it.  The stride's
 * file keeps the bytes it was given;  truncating it back is not safe,
 * because a later part may already have written past this one, and
 * assembly does not read beyond the sum of the records. */
std::optional<MPUStrategy::PartTarget>
NooBaaMPUStrategy::relocation_target(uint32_t part_num, uint64_t stored,
				     std::optional<uint64_t> stride) const
{
  if (stride && (*stride == stored)) {
    return std::nullopt;
  }
  return part_target(part_num, stored);
}

/* Their record is three plain attributes of their own, which our
 * XattrStrategy drops as foreign, so this reads them off the file.
 *
 * The etag is the one place their writer and their reader disagree.
 * _finish_upload() writes both user.content_md5 and
 * user.noobaa.part_etag (`namespace_fs.js:1421`, `:1424`), and
 * _get_etag() -- which their list_multiparts and their completion
 * check both use -- reads content_md5 and falls back to a string
 * derived from the stat.  So content_md5 is the answer, part_etag is
 * a cross-check, and the derived form is what a part carries when no
 * digest was computed.  It is not an MD5 and a strict client will say
 * so;  that is their behaviour, and base is their format.
 *
 * `stored` has no counterpart:  nothing sits between their op layer
 * and their writer to change a byte count, so the accounted size is
 * the stored size. */
bool NooBaaMPUStrategy::part_record(const DoutPrefixProvider* dpp,
				    int dir_fd, std::string_view pname,
				    const Attrs& attrs,
				    PartRecord& out) const
{
  /* Our record first, where there is one.  A part we wrote carries
   * both:  theirs, which their gateway reads, and ours, which is the
   * only one of the two with a checksum in it.  Theirs alone is what a
   * part they wrote has. */
  auto i = attrs.find(RGW_NSFS_ATTR_MPUPLOAD);
  if (i != attrs.end()) {
    NSFSUploadPartInfo upi;
    try {
      auto bufit = i->second.cbegin();
      decode(upi, bufit);
      out.size = upi.size;
      out.stored = upi.stored;
      out.offset = upi.offset;
      out.shared = upi.shared;
      out.etag = std::move(upi.etag);
      out.mtime = upi.mtime;
      out.cksum = std::move(upi.cksum);
      return true;
    } catch (buffer::error&) {
      /* fall through to theirs */
    }
  }

  int part_fd = ::openat(dir_fd, std::string(pname).c_str(), O_RDONLY);
  if (part_fd < 0) {
    return false;
  }
  auto close_part = make_scope_guard([part_fd] { ::close(part_fd); });

  auto size = u64_xattr(part_fd, NB_XATTR_PART_SIZE);
  if (!size) {
    return false;
  }
  out.size = *size;
  out.stored = *size;
  out.offset = u64_xattr(part_fd, NB_XATTR_PART_OFFSET).value_or(0);
  /* every part of a size shares that size's file */
  out.shared = true;

  char ebuf[256];
  ssize_t elen = ::fgetxattr(part_fd, NB_XATTR_CONTENT_MD5, ebuf,
			     sizeof(ebuf));
  if (elen > 0) {
    out.etag.assign(ebuf, elen);
  }

  struct statx stx;
  if (statx(part_fd, "", AT_EMPTY_PATH, STATX_MTIME | STATX_INO, &stx) == 0) {
    out.mtime = ceph::real_clock::from_time_t(stx.stx_mtime.tv_sec) +
		std::chrono::nanoseconds(stx.stx_mtime.tv_nsec);
    if (out.etag.empty()) {
      /* _get_version_id_by_stat:  mtime in nanoseconds and the inode,
       * both base 36 */
      const uint64_t ns =
	  static_cast<uint64_t>(stx.stx_mtime.tv_sec) * 1000000000ull +
	  stx.stx_mtime.tv_nsec;
      out.etag = "mtime-" + base36(ns) + "-ino-" + base36(stx.stx_ino);
    }
  }

  return true;
}

/* Their create_object_upload, which is where the key lives.
 *
 * Their gateway writes a JSON dump of the CreateMultipartUpload
 * parameters and reads back only the keys it wants, so an object with
 * `key` in it is one they can serve.  Written with the fields
 * their own readers consume:  the key, which their list_multiparts
 * compares against the request's, and what their completion path puts
 * on the finished object.  The rest of what we know about the upload
 * goes in our attribute, which they ignore.
 *
 * Their reader takes the first `key` it finds and nothing else is
 * required, so this is deliberately the smallest document that is one
 * of theirs rather than a re-encoding of ours in JSON.
 */
int NooBaaMPUStrategy::write_staged_upload(const DoutPrefixProvider* dpp,
					   int dir_fd,
					   const StagedUpload& su) const
{
  /* Their own document is JSON.stringify of the request parameters,
   * and stringify drops what is undefined, so a field the request did
   * not carry is absent rather than null.  Emitted the same way:  a
   * present-but-empty content_type would become the object's content
   * type on their completion path. */
  JSONFormatter f;
  f.open_object_section("create_object_upload");
  encode_json("key", su.key, &f);
  encode_json("bucket", su.bucket, &f);
  /* their name for the upload id, and what their ListMultipartUploads
   * reports as UploadId (`_get_mpu_info`, `namespace_fs.js:3028`).
   * The directory is named for it as well, and their readers use
   * whichever is at hand. */
  encode_json("obj_id", su.upload_id, &f);
  if (!su.content_type.empty()) {
    encode_json("content_type", su.content_type, &f);
  }
  if (!su.content_encoding.empty()) {
    encode_json("content_encoding", su.content_encoding, &f);
  }
  if (!su.storage_class.empty()) {
    encode_json("storage_class", su.storage_class, &f);
  }
  if (!su.xattr.empty()) {
    f.open_object_section("xattr");
    for (const auto& [k, v] : su.xattr) {
      encode_json(k.c_str(), v, &f);
    }
    f.close_section();
  }
  if (!su.retention_mode.empty() || !su.legal_hold.empty()) {
    f.open_object_section("lock_settings");
    if (!su.retention_mode.empty()) {
      f.open_object_section("retention");
      encode_json("mode", su.retention_mode, &f);
      encode_json("retain_until_date", su.retain_until, &f);
      f.close_section();
    }
    if (!su.legal_hold.empty()) {
      f.open_object_section("legal_hold");
      encode_json("status", su.legal_hold, &f);
      f.close_section();
    }
    f.close_section();
  }
  f.close_section();

  std::ostringstream os;
  f.flush(os);
  const std::string doc = os.str();

  int fd = ::openat(dir_fd, NB_CREATE_NAME.c_str(),
		    O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    int ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: creating " << NB_CREATE_NAME << ": "
		      << cpp_strerror(-ret) << dendl;
    return ret;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  ssize_t w = ::pwrite(fd, doc.data(), doc.size(), 0);
  if (w < 0) {
    int ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: writing " << NB_CREATE_NAME << ": "
		      << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (static_cast<size_t>(w) != doc.size()) {
    ldpp_dout(dpp, 0) << "ERROR: short write of " << NB_CREATE_NAME
		      << " (" << w << " of " << doc.size() << ")" << dendl;
    return -EIO;
  }
  return 0;
}

/* Their part attributes:  the bytes on disk and where they begin.
 *
 * `stored` rather than `size`:  theirs describes the file, and the two
 * differ whenever a filter between the op layer and the writer changed
 * the byte count.  Their assembly slices the size-keyed file by these
 * two numbers, so a value that is not the length on disk produces a
 * wrong object rather than a wrong report.
 *
 * No etag.  Theirs is `user.content_md5`, an MD5 of the part's bytes,
 * and what we hold at this point is an RGW etag, which for a part is
 * the same MD5 -- but only when no filter ran.  Their reader falls
 * back to the stat-derived form when it is absent, which is right, so
 * absent is better than wrong.
 */
int NooBaaMPUStrategy::write_part_record(const DoutPrefixProvider* dpp,
					 int dir_fd, std::string_view pname,
					 const PartRecord& rec) const
{
  int fd = ::openat(dir_fd, std::string(pname).c_str(), O_RDONLY);
  if (fd < 0) {
    int ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: opening part " << pname << ": "
		      << cpp_strerror(-ret) << dendl;
    return ret;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  const std::string size = std::to_string(rec.stored);
  const std::string offset = std::to_string(rec.offset);
  if ((::fsetxattr(fd, NB_XATTR_PART_SIZE, size.data(), size.size(), 0) < 0) ||
      (::fsetxattr(fd, NB_XATTR_PART_OFFSET, offset.data(), offset.size(),
		   0) < 0)) {
    int ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: writing part attributes on " << pname
		      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

/* The staging directory's uid.  They record no owner with an upload --
 * create_object_upload is a dump of the request parameters and carries
 * none -- and the directory was created under the account's identity,
 * which is the same answer their object ownership gives. */
bool NooBaaMPUStrategy::upload_owner(const DoutPrefixProvider* dpp,
				     int root_fd, std::string_view dname,
				     ACLOwner& out) const
{
  struct statx stx;
  if (statx(root_fd, std::string(dname).c_str(), AT_SYMLINK_NOFOLLOW,
	    STATX_UID, &stx) < 0) {
    return false;
  }
  if (!(stx.stx_mask & STATX_UID)) {
    return false;
  }
  out.id = rgw_user(std::to_string(stx.stx_uid));
  return true;
}

const ReservedNames& NooBaaMPUStrategy::reserved_names() const
{
  static const ReservedNames names{
    .exact = { NB_CREATE_NAME, NB_FINAL_NAME },
    .prefixes = { RGW_MP_SHARED_PREFIX, NB_PART_PREFIX },
    /* the temp directory holds uploads in flight, which S3 reports and
     * which therefore make a bucket non-empty */
    .staging_prefixes = { NB_TMPDIR_PREFIX },
  };
  return names;
}

int NooBaaMPUStrategy::assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
				int dir_fd,
				const std::vector<PartPlacement>& parts,
				const std::string& output_name,
				std::optional<uint64_t> stride) const
{
  if (!fs || parts.empty()) {
    return -EINVAL;
  }

  /* Their three cases, `complete_object_upload`.  The placements carry
   * what their code reads from the part records, so the distinct-size
   * count is taken from the run rather than accumulated with open file
   * descriptors.
   *
   * Sparse numbering disables both fast paths, as it does for them:
   * with a part missing, the file for a size no longer holds a
   * contiguous prefix of the object. */
  const bool continuous =
      (parts.back().num == static_cast<uint32_t>(parts.size()));

  size_t distinct = 0;
  uint64_t body_size = 0;
  bool tail_is_last = true;
  for (size_t i = 0; i < parts.size(); ++i) {
    if ((i == 0) || (parts[i].stored != parts[i - 1].stored)) {
      ++distinct;
      if (i == 0) {
	body_size = parts[i].stored;
      } else if (i + 1 != parts.size()) {
	tail_is_last = false;
      }
    }
  }

  auto link_size_file = [&](uint64_t size) -> int {
    const std::string sname = RGW_MP_SHARED_PREFIX + std::to_string(size);
    if (::linkat(dir_fd, sname.c_str(), dir_fd, output_name.c_str(), 0) < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not link " << sname << " to "
			<< output_name << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }
    return 0;
  };

  /* every part the same size, in order:  the size file is the object */
  if (continuous && (distinct == 1)) {
    return link_size_file(body_size);
  }

  int out_fd = -1;
  uint64_t total = 0;
  size_t first_to_copy = 0;

  /* a uniform body and a short final part:  link the body's file and
   * append the tail, which is the copy their scheme cannot avoid */
  if (continuous && (distinct == 2) && tail_is_last) {
    int ret = link_size_file(body_size);
    if (ret < 0) {
      return ret;
    }
    first_to_copy = parts.size() - 1;
    total = body_size * first_to_copy;
    out_fd = ::openat(dir_fd, output_name.c_str(), O_WRONLY);
  } else {
    out_fd = ::openat(dir_fd, output_name.c_str(),
		      O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  }
  if (out_fd < 0) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open " << output_name << ": "
		      << cpp_strerror(ret) << dendl;
    return -ret;
  }
  auto close_out = make_scope_guard([out_fd] { ::close(out_fd); });

  for (size_t i = first_to_copy; i < parts.size(); ++i) {
    const auto& part = parts[i];
    const std::string sname =
	RGW_MP_SHARED_PREFIX + std::to_string(part.stored);
    int part_fd = ::openat(dir_fd, sname.c_str(), O_RDONLY);
    if (part_fd < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open " << sname << ": "
			<< cpp_strerror(ret) << dendl;
      return -ret;
    }
    auto close_part = make_scope_guard([part_fd] { ::close(part_fd); });

    int ret = fs->copy_range(dpp, part_fd, part.offset, out_fd, total,
			     part.stored);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not copy part " << part.num
			<< " from " << sname << ": " << cpp_strerror(-ret)
			<< dendl;
      return ret;
    }
    total += part.stored;
  }

  return 0;
}


}}} // namespace rgw::sal::nsfs
