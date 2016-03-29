// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <unistd.h>

#include "include/compat.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/safe_io.h"
#include "common/debug.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

#define dout_subsys ceph_subsys_rbd

namespace rbd {
namespace action {
namespace merge_diff {

namespace at = argument_types;
namespace po = boost::program_options;

static int parse_diff_header(int fd, __u8 *tag, string *from, string *to, uint64_t *size)
{
  int r;

  {//header
    char buf[utils::RBD_DIFF_BANNER.size() + 1];
    r = safe_read_exact(fd, buf, utils::RBD_DIFF_BANNER.size());
    if (r < 0)
      return r;

    buf[utils::RBD_DIFF_BANNER.size()] = '\0';
    if (strcmp(buf, utils::RBD_DIFF_BANNER.c_str())) {
      std::cerr << "invalid banner '" << buf << "', expected '"
                << utils::RBD_DIFF_BANNER << "'" << std::endl;
      return -EINVAL;
    }
  }

  while (true) {
    r = safe_read_exact(fd, tag, 1);
    if (r < 0)
      return r;

    if (*tag == 'f') {
      r = utils::read_string(fd, 4096, from);   // 4k limit to make sure we don't get a garbage string
      if (r < 0)
        return r;
      dout(2) << " from snap " << *from << dendl;
    } else if (*tag == 't') {
      r = utils::read_string(fd, 4096, to);   // 4k limit to make sure we don't get a garbage string
      if (r < 0)
        return r;
      dout(2) << " to snap " << *to << dendl;
    } else if (*tag == 's') {
      char buf[8];
      r = safe_read_exact(fd, buf, 8);
      if (r < 0)
        return r;

      bufferlist bl;
      bl.append(buf, 8);
      bufferlist::iterator p = bl.begin();
      ::decode(*size, p);
    } else {
      break;
    }
  }

  return 0;
}

static int parse_diff_body(int fd, __u8 *tag, uint64_t *offset, uint64_t *length)
{
  int r;

  if (!(*tag)) {
    r = safe_read_exact(fd, tag, 1);
    if (r < 0)
      return r;
  }

  if (*tag == 'e') {
    offset = 0;
    length = 0;
    return 0;
  }

  if (*tag != 'w' && *tag != 'z')
    return -ENOTSUP;

  char buf[16];
  r = safe_read_exact(fd, buf, 16);
  if (r < 0)
    return r;

  bufferlist bl;
  bl.append(buf, 16);
  bufferlist::iterator p = bl.begin();
  ::decode(*offset, p);
  ::decode(*length, p);

  if (!(*length))
    return -ENOTSUP;

  return 0;
}

/*
 * fd: the diff file to read from
 * pd: the diff file to be written into
 */
static int accept_diff_body(int fd, int pd, __u8 tag, uint64_t offset, uint64_t length)
{
  if (tag == 'e')
    return 0;

  bufferlist bl;
  ::encode(tag, bl);
  ::encode(offset, bl);
  ::encode(length, bl);
  int r;
  r = bl.write_fd(pd);
  if (r < 0)
    return r;

  if (tag == 'w') {
    bufferptr bp = buffer::create(length);
    r = safe_read_exact(fd, bp.c_str(), length);
    if (r < 0)
      return r;
    bufferlist data;
    data.append(bp);
    r = data.write_fd(pd);
    if (r < 0)
      return r;
  }

  return 0;
}

/*
 * Merge two diff files into one single file
 * Note: It does not do the merging work if
 * either of the source diff files is stripped,
 * since which complicates the process and is
 * rarely used
 */
static int do_merge_diff(const char *first, const char *second,
                         const char *path, bool no_progress)
{
  utils::ProgressContext pc("Merging image diff", no_progress);
  int fd = -1, sd = -1, pd = -1, r;

  string f_from, f_to;
  string s_from, s_to;
  uint64_t f_size, s_size, pc_size;

  __u8 f_tag = 0, s_tag = 0;
  uint64_t f_off = 0, f_len = 0;
  uint64_t s_off = 0, s_len = 0;
  bool f_end = false, s_end = false;

  bool first_stdin = !strcmp(first, "-");
  if (first_stdin) {
    fd = 0;
  } else {
    fd = open(first, O_RDONLY);
    if (fd < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << first << std::endl;
      goto done;
    }
  }

  sd = open(second, O_RDONLY);
  if (sd < 0) {
    r = -errno;
    std::cerr << "rbd: error opening " << second << std::endl;
    goto done;
  }

  if (strcmp(path, "-") == 0) {
    pd = 1;
  } else {
    pd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (pd < 0) {
      r = -errno;
      std::cerr << "rbd: error create " << path << std::endl;
      goto done;
    }
  }

  //We just handle the case like 'banner, [ftag], [ttag], stag, [wztag]*,etag',
  // and the (offset,length) in wztag must be ascending order.

  r = parse_diff_header(fd, &f_tag, &f_from, &f_to, &f_size);
  if (r < 0) {
    std::cerr << "rbd: failed to parse first diff header" << std::endl;
    goto done;
  }

  r = parse_diff_header(sd, &s_tag, &s_from, &s_to, &s_size);
  if (r < 0) {
    std::cerr << "rbd: failed to parse second diff header" << std::endl;
    goto done;
  }

  if (f_to != s_from) {
    r = -EINVAL;
    std::cerr << "The first TO snapshot must be equal with the second FROM "
              << "snapshot, aborting" << std::endl;
    goto done;
  }

  {
    // header
    bufferlist bl;
    bl.append(utils::RBD_DIFF_BANNER);

    __u8 tag;
    if (f_from.size()) {
      tag = 'f';
      ::encode(tag, bl);
      ::encode(f_from, bl);
    }

    if (s_to.size()) {
      tag = 't';
      ::encode(tag, bl);
      ::encode(s_to, bl);
    }

    tag = 's';
    ::encode(tag, bl);
    ::encode(s_size, bl);

    r = bl.write_fd(pd);
    if (r < 0) {
      std::cerr << "rbd: failed to write merged diff header" << std::endl;
      goto done;
    }
  }
  if (f_size > s_size)
    pc_size = f_size << 1;
  else
    pc_size = s_size << 1;

  //data block
  while (!f_end || !s_end) {
    // progress through input
    pc.update_progress(f_off + s_off, pc_size);

    if (!f_end && !f_len) {
      uint64_t last_off = f_off;

      r = parse_diff_body(fd, &f_tag, &f_off, &f_len);
      dout(2) << "first diff data chunk: tag=" << f_tag << ", "
              << "off=" << f_off << ", "
              << "len=" << f_len << dendl;
      if (r < 0) {
        std::cerr << "rbd: failed to read first diff data chunk header"
                  << std::endl;
        goto done;
      }

      if (f_tag == 'e') {
        f_end = true;
        f_tag = 'z';
        f_off = f_size;
        if (f_size < s_size)
          f_len = s_size - f_size;
        else
          f_len = 0;
      }

      if (last_off > f_off) {
        r = -ENOTSUP;
        std::cerr << "rbd: out-of-order offset from first diff ("
             << last_off << " > " << f_off << ")" << std::endl;
        goto done;
      }
    }

    if (!s_end && !s_len) {
      uint64_t last_off = s_off;

      r = parse_diff_body(sd, &s_tag, &s_off, &s_len);
      dout(2) << "second diff data chunk: tag=" << s_tag << ", "
              << "off=" << s_off << ", "
              << "len=" << s_len << dendl;
      if (r < 0) {
        std::cerr << "rbd: failed to read second diff data chunk header"
                  << std::endl;
        goto done;
      }

      if (s_tag == 'e') {
        s_end = true;
        s_off = s_size;
        if (s_size < f_size)
          s_len = f_size - s_size;
        else
          s_len = 0;
      }

      if (last_off > s_off) {
        r = -ENOTSUP;
        std::cerr << "rbd: out-of-order offset from second diff ("
                  << last_off << " > " << s_off << ")" << std::endl;
        goto done;
      }
    }

    if (f_off < s_off && f_len) {
      uint64_t delta = s_off - f_off;
      if (delta > f_len)
        delta = f_len;
      r = accept_diff_body(fd, pd, f_tag, f_off, delta);
      f_off += delta;
      f_len -= delta;

      if (!f_len) {
        f_tag = 0;
        continue;
      }
    }
    assert(f_off >= s_off);

    if (f_off < s_off + s_len && f_len) {
      uint64_t delta = s_off + s_len - f_off;
      if (delta > f_len)
        delta = f_len;
      if (f_tag == 'w') {
        if (first_stdin) {
          bufferptr bp = buffer::create(delta);
          r = safe_read_exact(fd, bp.c_str(), delta);
        } else {
          off64_t l = lseek64(fd, delta, SEEK_CUR);
          r = l < 0 ? -errno : 0;
        }
        if (r < 0) {
          std::cerr << "rbd: failed to skip first diff data" << std::endl;
          goto done;
        }
      }
      f_off += delta;
      f_len -= delta;

      if (!f_len) {
        f_tag = 0;
        continue;
      }
    }
    assert(f_off >= s_off + s_len);
    if (s_len) {
      r = accept_diff_body(sd, pd, s_tag, s_off, s_len);
      s_off += s_len;
      s_len = 0;
      s_tag = 0;
    } else
      assert(f_end && s_end);
    continue;
  }

  {//tail
    __u8 tag = 'e';
    bufferlist bl;
    ::encode(tag, bl);
    r = bl.write_fd(pd);
  }

done:
  if (pd > 2)
    close(pd);
  if (sd)
    close(sd);
  if (fd > 2)
    close(fd);

  if(r < 0) {
    pc.fail();
    if (pd > 2)
      unlink(path);
  } else
    pc.finish();

  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  positional->add_options()
    ("diff1-path", "path to first diff (or '-' for stdin)")
    ("diff2-path", "path to second diff");
  at::add_path_options(positional, options,
                       "path to merged diff (or '-' for stdout)");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  std::string first_diff = utils::get_positional_argument(vm, 0);
  if (first_diff.empty()) {
    std::cerr << "rbd: first diff was not specified" << std::endl;
    return -EINVAL;
  }

  std::string second_diff = utils::get_positional_argument(vm, 1);
  if (second_diff.empty()) {
    std::cerr << "rbd: second diff was not specified" << std::endl;
    return -EINVAL;
  }

  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 2),
                          &path);
  if (r < 0) {
    return r;
  }

  r = do_merge_diff(first_diff.c_str(), second_diff.c_str(), path.c_str(),
                    vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    cerr << "rbd: merge-diff error" << std::endl;
    return -r;
  }

  return 0;
}

Shell::Action action(
  {"merge-diff"}, {}, "Merge two diff exports together.", "",
  &get_arguments, &execute);

} // namespace merge_diff
} // namespace action
} // namespace rbd
