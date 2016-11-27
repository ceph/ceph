// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace watch {

namespace at = argument_types;
namespace po = boost::program_options;

class RbdWatchCtx : public librados::WatchCtx2 {
public:
  RbdWatchCtx(librados::IoCtx& io_ctx, const char *image_name,
              std::string header_oid)
    : m_io_ctx(io_ctx), m_image_name(image_name), m_header_oid(header_oid)
  {
  }

  virtual ~RbdWatchCtx() {}

  virtual void handle_notify(uint64_t notify_id,
                             uint64_t cookie,
                             uint64_t notifier_id,
                             bufferlist& bl) {
    std::cout << m_image_name << " received notification: notify_id="
              << notify_id << ", cookie=" << cookie << ", notifier_id="
              << notifier_id << ", bl.length=" << bl.length() << std::endl;
    bufferlist reply;
    m_io_ctx.notify_ack(m_header_oid, notify_id, cookie, reply);
  }

  virtual void handle_error(uint64_t cookie, int err) {
    std::cerr << m_image_name << " received error: cookie=" << cookie << ", "
              << "err=" << cpp_strerror(err) << std::endl;
  }
private:
  librados::IoCtx m_io_ctx;
  const char *m_image_name;
  std::string m_header_oid;
};

static int do_watch(librados::IoCtx& pp, librbd::Image &image,
                    const char *imgname)
{
  uint8_t old_format;
  int r = image.old_format(&old_format);
  if (r < 0) {
    std::cerr << "failed to query format" << std::endl;
    return r;
  }

  std::string header_oid;
  if (old_format != 0) {
    header_oid = std::string(imgname) + RBD_SUFFIX;
  } else {
    std::string id;
    r = image.get_id(&id);
    if (r < 0) {
      return r;
    }

    header_oid = RBD_HEADER_PREFIX + id;
  }

  uint64_t cookie;
  RbdWatchCtx ctx(pp, imgname, header_oid);
  r = pp.watch2(header_oid, &cookie, &ctx);
  if (r < 0) {
    std::cerr << "rbd: watch failed" << std::endl;
    return r;
  }

  std::cout << "press enter to exit..." << std::endl;
  getchar();

  r = pp.unwatch2(cookie);
  if (r < 0) {
    std::cerr << "rbd: unwatch failed" << std::endl;
    return r;
  }
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", true, &rados,
                                 &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_watch(io_ctx, image, image_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: watch failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"watch"}, {}, "Watch events on image.", "", &get_arguments, &execute);

} // namespace watch
} // namespace action
} // namespace rbd
