// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/IndentStream.h"

namespace rbd {

int IndentBuffer::overflow (int c) {
  if (traits_type::eq_int_type(traits_type::eof(), c)) {
    return traits_type::not_eof(c);
  }

  int r;
  switch (c) {
  case '\n':
    m_buffer += c;
    flush_line();
    r = m_streambuf->sputn(m_buffer.c_str(), m_buffer.size());
    m_buffer.clear();
    return r;
  case '\t':
    // convert tab to single space and fall-through
    c = ' ';
  default:
    if (m_indent + m_buffer.size() >= m_line_length) {
      size_t word_offset = m_buffer.find_last_of(m_delim);
      bool space_delim = (m_delim == " ");
      if (word_offset == std::string::npos && !space_delim) {
        word_offset = m_buffer.find_last_of(" ");
      }

      if (word_offset != std::string::npos) {
        flush_line();
        m_streambuf->sputn(m_buffer.c_str(), word_offset);
        m_buffer = std::string(m_buffer,
                               word_offset + (space_delim ? 1 : 0));
      } else {
        flush_line();
        m_streambuf->sputn(m_buffer.c_str(), m_buffer.size());
        m_buffer.clear();
      }
      m_streambuf->sputc('\n');
    }
    m_buffer += c;
    return c;
  }
}

void IndentBuffer::flush_line() {
  if (m_initial_offset >= m_indent) {
    m_initial_offset = 0;
    m_streambuf->sputc('\n');
  }

  m_streambuf->sputn(m_indent_prefix.c_str(), m_indent - m_initial_offset);
  m_initial_offset = 0;
}

} // namespace rbd
