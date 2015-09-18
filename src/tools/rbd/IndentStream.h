// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_INDENT_STREAM_H
#define CEPH_RBD_INDENT_STREAM_H

#include "include/int_types.h"
#include <iostream>
#include <streambuf>
#include <iomanip>

namespace rbd {

class IndentBuffer : public std::streambuf {
public:
  IndentBuffer(size_t indent, size_t initial_offset, size_t line_length,
               std::streambuf *streambuf)
    : m_indent(indent), m_initial_offset(initial_offset),
      m_line_length(line_length), m_streambuf(streambuf),
      m_delim(" "), m_indent_prefix(m_indent, ' ') {
  }

  void set_delimiter(const std::string &delim) {
    m_delim = delim;
  }

protected:
  virtual int overflow (int c);

private:
  size_t m_indent;
  size_t m_initial_offset;
  size_t m_line_length;
  std::streambuf *m_streambuf;

  std::string m_delim;
  std::string m_indent_prefix;
  std::string m_buffer;

  void flush_line();
};

class IndentStream : public std::ostream {
public:
  IndentStream(size_t indent, size_t initial_offset, size_t line_length,
               std::ostream &os)
    : std::ostream(&m_indent_buffer),
      m_indent_buffer(indent, initial_offset, line_length, os.rdbuf()) {
  }

  void set_delimiter(const std::string &delim) {
    m_indent_buffer.set_delimiter(delim);
  }
private:
  IndentBuffer m_indent_buffer;
};

} // namespace rbd

#endif // CEPH_RBD_INDENT_STREAM_ITERATOR_H
