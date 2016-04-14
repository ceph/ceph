// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/scope_exit.hpp>

namespace rbd {
namespace action {
namespace export_full {

namespace at = argument_types;
namespace po = boost::program_options;

class C_Export : public Context
{
public:
  C_Export(SimpleThrottle &simple_throttle, librbd::Image &image,
                   uint64_t offset, uint64_t length, int fd)
    : m_aio_completion(
        new librbd::RBD::AioCompletion(this, &utils::aio_context_callback)),
      m_throttle(simple_throttle), m_image(image), m_offset(offset),
      m_length(length), m_fd(fd)
  {
  }

  void send()
  {
    m_throttle.start_op();

    int op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    int r = m_image.aio_read2(m_offset, m_length, m_bufferlist,
                              m_aio_completion, op_flags);
    if (r < 0) {
      cerr << "rbd: error requesting read from source image" << std::endl;
      m_aio_completion->release();
      m_throttle.end_op(r);
    }
  }

  virtual void finish(int r)
  {
    BOOST_SCOPE_EXIT((&m_throttle) (&r))
    {
      m_throttle.end_op(r);
    } BOOST_SCOPE_EXIT_END

    if (r < 0) {
      cerr << "rbd: error reading from source image at offset "
           << m_offset << ": " << cpp_strerror(r) << std::endl;
      return;
    }

    assert(m_bufferlist.length() == static_cast<size_t>(r));
    if (m_fd != STDOUT_FILENO) {
      if (m_bufferlist.is_zero()) {
        return;
      }

      uint64_t chkret = lseek64(m_fd, m_offset, SEEK_SET);
      if (chkret != m_offset) {
        cerr << "rbd: error seeking destination image to offset "
             << m_offset << std::endl;
        r = -errno;
        return;
      }
    }

    r = m_bufferlist.write_fd(m_fd);
    if (r < 0) {
      cerr << "rbd: error writing to destination image at offset "
           << m_offset << std::endl;
    }
  }

private:
  librbd::RBD::AioCompletion *m_aio_completion;
  SimpleThrottle &m_throttle;
  librbd::Image &m_image;
  bufferlist m_bufferlist;
  uint64_t m_offset;
  uint64_t m_length;
  int m_fd;
};

static int do_export(librbd::Image& image, const char *path, bool no_progress)
{
  librbd::image_info_t info;
  int64_t r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  int fd;
  int max_concurrent_ops;
  bool to_stdout = (strcmp(path, "-") == 0);
  if (to_stdout) {
    fd = STDOUT_FILENO;
    max_concurrent_ops = 1;
  } else {
    max_concurrent_ops = max(g_conf->rbd_concurrent_management_ops, 1);
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
      return -errno;
    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }

  utils::ProgressContext pc("Exporting image", no_progress);

  SimpleThrottle throttle(max_concurrent_ops, false);
  uint64_t period = image.get_stripe_count() * (1ull << info.order);
  for (uint64_t offset = 0; offset < info.size; offset += period) {
    if (throttle.pending_error()) {
      break;
    }

    uint64_t length = min(period, info.size - offset);
    C_Export *ctx = new C_Export(throttle, image, offset, length, fd);
    ctx->send();

    pc.update_progress(offset, info.size);
  }

  r = throttle.wait_for_ret();
  if (!to_stdout) {
    if (r >= 0) {
      r = ftruncate(fd, info.size);
    }
    close(fd);
  }

  if (r < 0) {
    pc.fail();
  } else {
    pc.finish();
  }
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_export(image, path.c_str(), vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: export error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"export"}, {}, "Export image to file.", "", &get_arguments, &execute);

} // namespace export_full
} // namespace action
} // namespace rbd
