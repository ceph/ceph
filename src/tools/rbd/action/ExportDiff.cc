// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <boost/program_options.hpp>
#include <boost/scope_exit.hpp>

namespace rbd {
namespace action {
namespace export_diff {

namespace at = argument_types;
namespace po = boost::program_options;

struct ExportDiffContext {
  librbd::Image *image;
  int fd;
  uint64_t totalsize;
  utils::ProgressContext pc;
  OrderedThrottle throttle;

  ExportDiffContext(librbd::Image *i, int f, uint64_t t, int max_ops,
                    bool no_progress) :
    image(i), fd(f), totalsize(t), pc("Exporting image", no_progress),
    throttle(max_ops, true) {
  }
};

class C_ExportDiff : public Context {
public:
  C_ExportDiff(ExportDiffContext *edc, uint64_t offset, uint64_t length,
               bool exists)
    : m_export_diff_context(edc), m_offset(offset), m_length(length),
      m_exists(exists) {
  }

  int send() {
    if (m_export_diff_context->throttle.pending_error()) {
      return m_export_diff_context->throttle.wait_for_ret();
    }

    C_OrderedThrottle *ctx = m_export_diff_context->throttle.start_op(this);
    if (m_exists) {
      librbd::RBD::AioCompletion *aio_completion =
        new librbd::RBD::AioCompletion(ctx, &utils::aio_context_callback);

      int op_flags = LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
      int r = m_export_diff_context->image->aio_read2(
        m_offset, m_length, m_read_data, aio_completion, op_flags);
      if (r < 0) {
        aio_completion->release();
        ctx->complete(r);
      }
    } else {
      ctx->complete(0);
    }
    return 0;
  }

  static int export_diff_cb(uint64_t offset, size_t length, int exists,
                            void *arg) {
    ExportDiffContext *edc = reinterpret_cast<ExportDiffContext *>(arg);

    C_ExportDiff *context = new C_ExportDiff(edc, offset, length, exists);
    return context->send();
  }

protected:
  virtual void finish(int r) {
    if (r >= 0) {
      if (m_exists) {
        m_exists = !m_read_data.is_zero();
      }
      r = write_extent(m_export_diff_context, m_offset, m_length, m_exists);
      if (r == 0 && m_exists) {
        r = m_read_data.write_fd(m_export_diff_context->fd);
      }
    }
    m_export_diff_context->throttle.end_op(r);
  }

private:
  ExportDiffContext *m_export_diff_context;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_exists;
  bufferlist m_read_data;

  static int write_extent(ExportDiffContext *edc, uint64_t offset,
                          uint64_t length, bool exists) {
    // extent
    bufferlist bl;
    __u8 tag = exists ? 'w' : 'z';
    ::encode(tag, bl);
    ::encode(offset, bl);
    ::encode(length, bl);
    int r = bl.write_fd(edc->fd);

    edc->pc.update_progress(offset, edc->totalsize);
    return r;
  }
};

static int do_export_diff(librbd::Image& image, const char *fromsnapname,
                          const char *endsnapname, bool whole_object,
                          const char *path, bool no_progress)
{
  int r;
  librbd::image_info_t info;
  int fd;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  if (strcmp(path, "-") == 0)
    fd = 1;
  else
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0)
    return -errno;

  BOOST_SCOPE_EXIT((&r) (&fd) (&path)) {
    close(fd);
    if (r < 0 && fd != 1) {
      remove(path);
    }
  } BOOST_SCOPE_EXIT_END

  {
    // header
    bufferlist bl;
    bl.append(utils::RBD_DIFF_BANNER);

    __u8 tag;
    if (fromsnapname) {
      tag = 'f';
      ::encode(tag, bl);
      std::string from(fromsnapname);
      ::encode(from, bl);
    }

    if (endsnapname) {
      tag = 't';
      ::encode(tag, bl);
      std::string to(endsnapname);
      ::encode(to, bl);
    }

    tag = 's';
    ::encode(tag, bl);
    uint64_t endsize = info.size;
    ::encode(endsize, bl);

    r = bl.write_fd(fd);
    if (r < 0) {
      return r;
    }
  }
  ExportDiffContext edc(&image, fd, info.size,
                        g_conf->rbd_concurrent_management_ops, no_progress);
  r = image.diff_iterate2(fromsnapname, 0, info.size, true, whole_object,
                          &C_ExportDiff::export_diff_cb, (void *)&edc);
  if (r < 0) {
    goto out;
  }

  r = edc.throttle.wait_for_ret();
  if (r < 0) {
    goto out;
  }

  {
    __u8 tag = 'e';
    bufferlist bl;
    ::encode(tag, bl);
    r = bl.write_fd(fd);
  }

 out:
  if (r < 0)
    edc.pc.fail();
  else
    edc.pc.finish();
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    (at::WHOLE_OBJECT.c_str(), po::bool_switch(), "compare whole object");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_export_diff(image,
                     from_snap_name.empty() ? nullptr : from_snap_name.c_str(),
                     snap_name.empty() ? nullptr : snap_name.c_str(),
                     vm[at::WHOLE_OBJECT].as<bool>(), path.c_str(),
                     vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: export-diff error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({at::WHOLE_OBJECT});
Shell::Action action(
  {"export-diff"}, {}, "Export incremental diff to file.", "",
  &get_arguments, &execute);

} // namespace export_diff
} // namespace action
} // namespace rbd
