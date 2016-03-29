// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/Formatter.h"

namespace rbd {
namespace action {
namespace consgrp {

namespace at = argument_types;
namespace po = boost::program_options;

int execute_list(const po::variables_map &vm) {

  int r;

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter * f = formatter.get();

  std::cout << "value of f is:" << f << std::endl;
  if (f != 0) {
    f->open_object_section("snapshot");
    f->dump_unsigned("id", 5);
    f->dump_string("name", "botva");
    f->dump_unsigned("size", 1234);
    f->close_section();
    f->flush(std::cout);

    f->write_raw_data("Hello botva\n");
    f->flush(std::cout);
  }

  return 0;
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_format_options(options);
}

/*
void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
}
*/

Shell::Action action_list(
  {"cg", "list"}, {"cg", "ls"}, "Dump list of consistency groups.", "",
  &get_list_arguments, &execute_list);
/*
Shell::Action action_create(
  {"cg", "list"}, {"cg", "ls"}, "Dump list of consistency groups.", "",
  &get_list_arguments, &execute_list);
  */
} // namespace snap
} // namespace action
} // namespace rbd
