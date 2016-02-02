// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/OptionPrinter.h"
#include "tools/rbd/IndentStream.h"

namespace rbd {

namespace po = boost::program_options;

const std::string OptionPrinter::POSITIONAL_ARGUMENTS("Positional arguments");
const std::string OptionPrinter::OPTIONAL_ARGUMENTS("Optional arguments");

const size_t OptionPrinter::MAX_DESCRIPTION_OFFSET;

OptionPrinter::OptionPrinter(const OptionsDescription &positional,
                             const OptionsDescription &optional)
  : m_positional(positional), m_optional(optional) {
}

void OptionPrinter::print_short(std::ostream &os, size_t initial_offset) {
  size_t name_width = std::min(initial_offset, MAX_DESCRIPTION_OFFSET) + 1;

  IndentStream indent_stream(name_width, initial_offset, LINE_WIDTH, os);
  indent_stream.set_delimiter("[");
  for (size_t i = 0; i < m_optional.options().size(); ++i) {
    bool required = m_optional.options()[i]->semantic()->is_required();
    if (!required) {
      indent_stream << "[";
    }
    indent_stream << "--" << m_optional.options()[i]->long_name();
    if (m_optional.options()[i]->semantic()->max_tokens() != 0) {
      indent_stream << " <" << m_optional.options()[i]->long_name() << ">";
    }
    if (!required) {
      indent_stream << "]";
    }
    indent_stream << " ";
  }
  indent_stream << std::endl;

  if (m_positional.options().size() > 0) {
    indent_stream.set_delimiter(" ");
    for (size_t i = 0; i < m_positional.options().size(); ++i) {
      indent_stream << "<" << m_positional.options()[i]->long_name() << "> ";
      if (m_positional.options()[i]->semantic()->max_tokens() > 1) {
        indent_stream << "[<" << m_positional.options()[i]->long_name()
                      << "> ...]";
        break;
      }
    }
    indent_stream << std::endl;
  }
}

void OptionPrinter::print_detailed(std::ostream &os) {
  std::string indent_prefix(2, ' ');
  size_t name_width = compute_name_width(indent_prefix.size());

  if (m_positional.options().size() > 0) {
    std::cout << POSITIONAL_ARGUMENTS << std::endl;
    for (size_t i = 0; i < m_positional.options().size(); ++i) {
      std::stringstream ss;
      ss << indent_prefix << "<" << m_positional.options()[i]->long_name()
         << ">";

      std::cout << ss.str();
      IndentStream indent_stream(name_width, ss.str().size(), LINE_WIDTH, os);
      indent_stream << m_positional.options()[i]->description() << std::endl;
    }
    std::cout << std::endl;
  }

  if (m_optional.options().size() > 0) {
    std::cout << OPTIONAL_ARGUMENTS << std::endl;
    for (size_t i = 0; i < m_optional.options().size(); ++i) {
      std::stringstream ss;
      ss << indent_prefix
         << m_optional.options()[i]->format_name() << " "
         << m_optional.options()[i]->format_parameter();

      std::cout << ss.str();
      IndentStream indent_stream(name_width, ss.str().size(), LINE_WIDTH, os);
      indent_stream << m_optional.options()[i]->description() << std::endl;
    }
    std::cout << std::endl;
  }
}

size_t OptionPrinter::compute_name_width(size_t indent) {
  size_t width = MIN_NAME_WIDTH;
  std::vector<OptionsDescription> descs = {m_positional, m_optional};
  for (size_t desc_idx = 0; desc_idx < descs.size(); ++desc_idx) {
    const OptionsDescription &desc = descs[desc_idx];
    for (size_t opt_idx = 0; opt_idx < desc.options().size(); ++opt_idx) {
      size_t name_width = desc.options()[opt_idx]->format_name().size() +
                          desc.options()[opt_idx]->format_parameter().size()
                          + 1;
      width = std::max(width, name_width);
    }
  }
  width += indent;
  width = std::min(width, MAX_DESCRIPTION_OFFSET) + 1;
  return width;
}

} // namespace rbd
