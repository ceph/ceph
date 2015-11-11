// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace diff {

namespace at = argument_types;
namespace po = boost::program_options;

struct output_method {
  output_method() : f(NULL), t(NULL), empty(true) {}
  Formatter *f;
  TextTable *t;
  bool empty;
};

static int diff_cb(uint64_t ofs, size_t len, int exists, void *arg)
{
  output_method *om = static_cast<output_method *>(arg);
  om->empty = false;
  if (om->f) {
    om->f->open_object_section("extent");
    om->f->dump_unsigned("offset", ofs);
    om->f->dump_unsigned("length", len);
    om->f->dump_string("exists", exists ? "true" : "false");
    om->f->close_section();
  } else {
    assert(om->t);
    *(om->t) << ofs << len << (exists ? "data" : "zero") << TextTable::endrow;
  }
  return 0;
}

static int do_diff(librbd::Image& image, const char *fromsnapname,
                   bool whole_object, Formatter *f)
{
  int r;
  librbd::image_info_t info;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  output_method om;
  if (f) {
    om.f = f;
    f->open_array_section("extents");
  } else {
    om.t = new TextTable();
    om.t->define_column("Offset", TextTable::LEFT, TextTable::LEFT);
    om.t->define_column("Length", TextTable::LEFT, TextTable::LEFT);
    om.t->define_column("Type", TextTable::LEFT, TextTable::LEFT);
  }

  r = image.diff_iterate2(fromsnapname, 0, info.size, true, whole_object,
                          diff_cb, &om);
  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    if (!om.empty)
      std::cout << *om.t;
    delete om.t;
  }
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    (at::WHOLE_OBJECT.c_str(), po::bool_switch(), "compare whole object");
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  bool diff_whole_object = vm[at::WHOLE_OBJECT].as<bool>();

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
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

  r = do_diff(image, from_snap_name.empty() ? nullptr : from_snap_name.c_str(),
              diff_whole_object, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: diff error: " << cpp_strerror(r) << std::endl;
    return -r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({at::WHOLE_OBJECT});
Shell::Action action(
  {"diff"}, {},
  "Print extents that differ since a previous snap, or image creation.", "",
  &get_arguments, &execute);

} // namespace diff
} // namespace action
} // namespace rbd
