// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/encoding.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include <iostream>
#include <boost/program_options.hpp>

#define dout_subsys ceph_subsys_rbd

namespace rbd {
namespace action {
namespace import_diff {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_import_diff(librbd::Image &image, const char *path,
                          bool no_progress)
{
  int fd, r;
  struct stat stat_buf;
  utils::ProgressContext pc("Importing image diff", no_progress);
  uint64_t size = 0;
  uint64_t off = 0;
  string from, to;
  char buf[utils::RBD_DIFF_BANNER.size() + 1];

  bool from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    fd = 0;
  } else {
    fd = open(path, O_RDONLY);
    if (fd < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << path << std::endl;
      return r;
    }
    r = ::fstat(fd, &stat_buf);
    if (r < 0)
      goto done;
    size = (uint64_t)stat_buf.st_size;
  }

  r = safe_read_exact(fd, buf, utils::RBD_DIFF_BANNER.size());
  if (r < 0)
    goto done;
  buf[utils::RBD_DIFF_BANNER.size()] = '\0';
  if (strcmp(buf, utils::RBD_DIFF_BANNER.c_str())) {
    std::cerr << "invalid banner '" << buf << "', expected '"
              << utils::RBD_DIFF_BANNER << "'" << std::endl;
    r = -EINVAL;
    goto done;
  }

  while (true) {
    __u8 tag;
    r = safe_read_exact(fd, &tag, 1);
    if (r < 0) {
      goto done;
    }

    if (tag == 'e') {
      dout(2) << " end diff" << dendl;
      break;
    } else if (tag == 'f') {
      r = utils::read_string(fd, 4096, &from);   // 4k limit to make sure we don't get a garbage string
      if (r < 0)
        goto done;
      dout(2) << " from snap " << from << dendl;

      bool exists; 
      r = image.snap_exists2(from.c_str(), &exists);
      if (r < 0)
        goto done;

      if (!exists) {
        std::cerr << "start snapshot '" << from
                  << "' does not exist in the image, aborting" << std::endl;
        r = -EINVAL;
        goto done;
      }
    }
    else if (tag == 't') {
      r = utils::read_string(fd, 4096, &to);   // 4k limit to make sure we don't get a garbage string
      if (r < 0)
        goto done;
      dout(2) << "   to snap " << to << dendl;

      // verify this snap isn't already present
      bool exists;
      r = image.snap_exists2(to.c_str(), &exists);
      if (r < 0)
        goto done;
      
      if (exists) {
        std::cerr << "end snapshot '" << to
                  << "' already exists, aborting" << std::endl;
        r = -EEXIST;
        goto done;
      }
    } else if (tag == 's') {
      uint64_t end_size;
      char buf[8];
      r = safe_read_exact(fd, buf, 8);
      if (r < 0)
        goto done;
      bufferlist bl;
      bl.append(buf, 8);
      bufferlist::iterator p = bl.begin();
      ::decode(end_size, p);
      uint64_t cur_size;
      image.size(&cur_size);
      if (cur_size != end_size) {
        dout(2) << "resize " << cur_size << " -> " << end_size << dendl;
        image.resize(end_size);
      } else {
        dout(2) << "size " << end_size << " (no change)" << dendl;
      }
      if (from_stdin)
        size = end_size;
    } else if (tag == 'w' || tag == 'z') {
      uint64_t len;
      char buf[16];
      r = safe_read_exact(fd, buf, 16);
      if (r < 0)
        goto done;
      bufferlist bl;
      bl.append(buf, 16);
      bufferlist::iterator p = bl.begin();
      ::decode(off, p);
      ::decode(len, p);

      if (tag == 'w') {
        bufferptr bp = buffer::create(len);
        r = safe_read_exact(fd, bp.c_str(), len);
        if (r < 0)
          goto done;
        bufferlist data;
        data.append(bp);
        dout(2) << " write " << off << "~" << len << dendl;
        image.write2(off, len, data, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      } else {
        dout(2) << " zero " << off << "~" << len << dendl;
        image.discard(off, len);
      }
    } else {
      std::cerr << "unrecognized tag byte " << (int)tag
                << " in stream; aborting" << std::endl;
      r = -EINVAL;
      goto done;
    }
    if (!from_stdin) {
      // progress through input
      uint64_t off = lseek64(fd, 0, SEEK_CUR);
      pc.update_progress(off, size);
    } else if (size) {
      // progress through image offsets.  this may jitter if blocks
      // aren't in order, but it is better than nothing.
      pc.update_progress(off, size);
    }
  }
  // take final snap
  if (to.length()) {
    dout(2) << " create end snap " << to << dendl;
    r = image.snap_create(to.c_str());
  }

 done:
  if (r < 0)
    pc.fail();
  else
    pc.finish();
  if (!from_stdin)
    close(fd);
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 0), &path);
  if (r < 0) {
    return r;
  }

  size_t arg_index = 1;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_import_diff(image, path.c_str(), vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    cerr << "rbd: import-diff failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"import-diff"}, {}, "Import an incremental diff.", "", &get_arguments,
  &execute);

} // namespace list
} // namespace action
} // namespace rbd
