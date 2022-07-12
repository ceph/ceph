// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
// #define BOOST_SPIRIT_DEBUG

#include <algorithm>
#include <cctype>
#include <experimental/iterator>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/phoenix.hpp>
#include <boost/spirit/include/support_line_pos_iterator.hpp>

#include "include/buffer.h"
#include "common/errno.h"
#include "common/utf8.h"
#include "common/ConfUtils.h"

namespace fs = std::filesystem;

using std::ostringstream;
using std::string;

#define MAX_CONFIG_FILE_SZ 0x40000000

conf_line_t::conf_line_t(const std::string& key, const std::string& val)
  : key{ConfFile::normalize_key_name(key)},
    val{boost::algorithm::trim_copy_if(
          val,
	  [](unsigned char c) {
	    return std::isspace(c);
	  })}
{}

bool conf_line_t::operator<(const conf_line_t &rhs) const
{
  // We only compare keys.
  // If you have more than one line with the same key in a given section, the
  // last one wins.
  return key < rhs.key;
}

std::ostream &operator<<(std::ostream& oss, const conf_line_t &l)
{
  oss << "conf_line_t(key = '" << l.key << "', val='" << l.val << "')";
  return oss;
}

conf_section_t::conf_section_t(const std::string& heading,
			       const std::vector<conf_line_t>& lines)
  : heading{heading}
{
  for (auto& line : lines) {
    auto [where, inserted] = insert(line);
    if (!inserted) {
      erase(where);
      insert(line);
    }
  }
}

///////////////////////// ConfFile //////////////////////////

ConfFile::ConfFile(const std::vector<conf_section_t>& sections)
{
  for (auto& section : sections) {
    auto [old_sec, sec_inserted] = emplace(section.heading, section);
    if (!sec_inserted) {
      // merge lines in section into old_sec
      for (auto& line : section) {
	auto [old_line, line_inserted] = old_sec->second.emplace(line);
	// and replace the existing ones if any
	if (!line_inserted) {
	  old_sec->second.erase(old_line);
	  old_sec->second.insert(line);
	}
      }
    }
  }
}

/* We load the whole file into memory and then parse it.  Although this is not
 * the optimal approach, it does mean that most of this code can be shared with
 * the bufferlist loading function. Since bufferlists are always in-memory, the
 * load_from_buffer interface works well for them.
 * In general, configuration files should be a few kilobytes at maximum, so
 * loading the whole configuration into memory shouldn't be a problem.
 */
int ConfFile::parse_file(const std::string &fname,
			 std::ostream *warnings)
{
  clear();
  try {
    if (auto file_size = fs::file_size(fname); file_size > MAX_CONFIG_FILE_SZ) {
      *warnings << __func__ << ": config file '" << fname
		<< "' is " << file_size << " bytes, "
		<< "but the maximum is " << MAX_CONFIG_FILE_SZ;
      return -EINVAL;
    }
  } catch (const fs::filesystem_error& e) {
    std::error_code ec;
    auto is_other = fs::is_other(fname, ec);
    if (!ec && is_other) {
      // /dev/null?
      return 0;
    } else {
      *warnings << __func__ << ": " << e.what();
      return -e.code().value();
    }
  }
  std::ifstream ifs{fname};
  std::string buffer{std::istreambuf_iterator<char>(ifs),
			               std::istreambuf_iterator<char>()};
  if (parse_buffer(buffer, warnings)) {
    return 0;
  } else {
    return -EINVAL;
  }
}

namespace {

namespace qi = boost::spirit::qi;
namespace phoenix = boost::phoenix;

template<typename Iterator, typename Skipper>
struct IniGrammer : qi::grammar<Iterator, ConfFile(), Skipper>
{
  struct error_handler_t {
    std::ostream& os;
    template<typename Iter>
    auto operator()(Iter first, Iter last, Iter where,
		    const boost::spirit::info& what) const {
      auto line_start = boost::spirit::get_line_start(first, where);
      os << "parse error: expected '" << what
	 << "' in line " << boost::spirit::get_line(where)
	 << " at position " << boost::spirit::get_column(line_start, where) << "\n";
      return qi::fail;
    }
  };
  IniGrammer(Iterator begin, std::ostream& err)
    : IniGrammer::base_type{conf_file},
      report_error{error_handler_t{err}}
  {
    using qi::_1;
    using qi::_2;
    using qi::_val;
    using qi::char_;
    using qi::eoi;
    using qi::eol;
    using qi::blank;
    using qi::lexeme;
    using qi::lit;
    using qi::raw;

    blanks = *blank;
    comment_start = lit('#') | lit(';');
    continue_marker = lit('\\') >> eol;

    text_char %=
      (lit('\\') >> (char_ - eol)) |
      (char_ - (comment_start | eol));

    key %= raw[+(text_char - char_("=[ ")) % +blank];
    quoted_value %=
      lexeme[lit('"') >> *(text_char - '"') > '"'] |
      lexeme[lit('\'') >> *(text_char - '\'') > '\''];
    unquoted_value %= *text_char;
    comment = *blank >> comment_start > *(char_ - eol);
    empty_line = -(blanks|comment) >> eol;
    value %= quoted_value | unquoted_value;
    key_val =
      (blanks >> key >> blanks >> '=' > blanks > value > +empty_line)
      [_val = phoenix::construct<conf_line_t>(_1, _2)];

    heading %= lit('[') > +(text_char - ']') > ']' > +empty_line;
    section =
      (heading >> *(key_val - heading) >> *eol)
      [_val = phoenix::construct<conf_section_t>(_1, _2)];
    conf_file =
      (key_val [_val = phoenix::construct<ConfFile>(_1)]
       |
       (*eol >> (*section)[_val = phoenix::construct<ConfFile>(_1)])
      ) > eoi;

    empty_line.name("empty_line");
    key.name("key");
    quoted_value.name("quoted value");
    unquoted_value.name("unquoted value");
    key_val.name("key=val");
    heading.name("section name");
    section.name("section");

    qi::on_error<qi::fail>(
      conf_file,
      report_error(qi::_1, qi::_2, qi::_3, qi::_4));

    BOOST_SPIRIT_DEBUG_NODE(heading);
    BOOST_SPIRIT_DEBUG_NODE(section);
    BOOST_SPIRIT_DEBUG_NODE(key);
    BOOST_SPIRIT_DEBUG_NODE(quoted_value);
    BOOST_SPIRIT_DEBUG_NODE(unquoted_value);
    BOOST_SPIRIT_DEBUG_NODE(key_val);
    BOOST_SPIRIT_DEBUG_NODE(conf_file);
  }

  qi::rule<Iterator> blanks;
  qi::rule<Iterator> empty_line;
  qi::rule<Iterator> comment_start;
  qi::rule<Iterator> continue_marker;
  qi::rule<Iterator, char()> text_char;
  qi::rule<Iterator, std::string(), Skipper> key;
  qi::rule<Iterator, std::string(), Skipper> quoted_value;
  qi::rule<Iterator, std::string(), Skipper> unquoted_value;
  qi::rule<Iterator> comment;
  qi::rule<Iterator, std::string(), Skipper> value;
  qi::rule<Iterator, conf_line_t(), Skipper> key_val;
  qi::rule<Iterator, std::string(), Skipper> heading;
  qi::rule<Iterator, conf_section_t(), Skipper> section;
  qi::rule<Iterator, ConfFile(), Skipper> conf_file;
  boost::phoenix::function<error_handler_t> report_error;
};
}

bool ConfFile::parse_buffer(std::string_view buf, std::ostream* err)
{
  assert(err);
#ifdef _WIN32
  // We'll need to ensure that there's a new line at the end of the buffer,
  // otherwise the config parsing will fail.
  std::string _buf = std::string(buf) + "\n";
#else
  std::string_view _buf = buf;
#endif
  if (int err_pos = check_utf8(_buf.data(), _buf.size()); err_pos > 0) {
    *err << "parse error: invalid UTF-8 found at line "
	 << std::count(_buf.begin(), std::next(_buf.begin(), err_pos), '\n') + 1;
    return false;
  }
  using iter_t = boost::spirit::line_pos_iterator<decltype(_buf.begin())>;
  iter_t first{_buf.begin()};
  using skipper_t = qi::rule<iter_t>;
  IniGrammer<iter_t, skipper_t> grammar{first, *err};
  skipper_t skipper = grammar.continue_marker | grammar.comment;
  return qi::phrase_parse(first, iter_t{_buf.end()},
			  grammar, skipper, *this);
}

int ConfFile::parse_bufferlist(ceph::bufferlist *bl,
			       std::ostream *warnings)
{
  clear();
  ostringstream oss;
  if (!warnings) {
    warnings = &oss;
  }
  return parse_buffer({bl->c_str(), bl->length()}, warnings) ? 0 : -EINVAL;
}

int ConfFile::read(std::string_view section_name,
		   std::string_view key,
		   std::string &val) const
{
  string k(normalize_key_name(key));

  if (auto s = base_type::find(section_name); s != end()) {
    conf_line_t exemplar{k, {}};
    if (auto line = s->second.find(exemplar); line != s->second.end()) {
      val = line->val;
      return 0;
    }
  }
  return -ENOENT;
}

/* Normalize a key name.
 *
 * Normalized key names have no leading or trailing whitespace, and all
 * whitespace is stored as underscores.  The main reason for selecting this
 * normal form is so that in common/config.cc, we can use a macro to stringify
 * the field names of md_config_t and get a key in normal form.
 */
std::string ConfFile::normalize_key_name(std::string_view key)
{
  std::string k{key};
  boost::algorithm::trim_fill_if(k, "_", isspace);
  return k;
}

void ConfFile::check_old_style_section_names(const std::vector<std::string>& prefixes,
					     std::ostream& os)
{
  // Warn about section names that look like old-style section names
  std::vector<std::string> old_style_section_names;
  for (auto& [name, section] : *this) {
    for (auto& prefix : prefixes) {
      if (name.find(prefix) == 0 && name.size() > 3 && name[3] != '.') {
	old_style_section_names.push_back(name);
      }
    }
  }
  if (!old_style_section_names.empty()) {
    os << "ERROR! old-style section name(s) found: ";
    std::copy(std::begin(old_style_section_names),
              std::end(old_style_section_names),
              std::experimental::make_ostream_joiner(os, ", "));
    os << ". Please use the new style section names that include a period.";
  }
}

std::ostream &operator<<(std::ostream &oss, const ConfFile &cf)
{
  for (auto& [name, section] : cf) {
    oss << "[" << name << "]\n";
    for (auto& [key, val] : section) {
      if (!key.empty()) {
	oss << "\t" << key << " = \"" << val << "\"\n";
      }
    }
  }
  return oss;
}
