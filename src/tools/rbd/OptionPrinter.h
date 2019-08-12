// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_OPTION_PRINTER_H
#define CEPH_RBD_OPTION_PRINTER_H

#include "include/int_types.h"
#include <string>
#include <vector>
#include <boost/program_options.hpp>

namespace rbd {

class OptionPrinter {
public:
  typedef boost::program_options::options_description OptionsDescription;

  static const std::string POSITIONAL_ARGUMENTS;
  static const std::string OPTIONAL_ARGUMENTS;

  static const size_t LINE_WIDTH = 80;
  static const size_t MIN_NAME_WIDTH = 20;
  static const size_t MAX_DESCRIPTION_OFFSET = LINE_WIDTH / 2;

  OptionPrinter(const OptionsDescription &positional,
                const OptionsDescription &optional);

  void print_short(std::ostream &os, size_t initial_offset);
  void print_detailed(std::ostream &os);

private:
  const OptionsDescription &m_positional;
  const OptionsDescription &m_optional;

  size_t compute_name_width(size_t indent);
};

} // namespace rbd

#endif // CEPH_RBD_OPTION_PRINTER_H
