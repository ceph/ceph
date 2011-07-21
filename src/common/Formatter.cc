
#define LARGE_SIZE 8192

#include <stdarg.h>
#include <stdio.h>
#include <inttypes.h>

#include <iostream>

#include "Formatter.h"

// -----------------------
namespace ceph {

void Formatter::reset()
{
  m_stack.clear();
  m_ss.clear();
  m_pending_string.clear();
}

void Formatter::flush(std::ostream& os)
{
  finish_pending_string();
  os << m_ss.str();
  m_ss.clear();
}

// -----------------------


void JSONFormatter::print_comma(formatter_stack_entry_d& entry)
{
  if (entry.size) {
    if (m_pretty) {
      m_ss << ",\n";
      for (unsigned i=1; i < m_stack.size(); i++)
	m_ss << "    ";
    } else {
      m_ss << ",";
    }
  } else if (entry.is_array && m_pretty) {
    m_ss << "\n";
    for (unsigned i=1; i < m_stack.size(); i++)
      m_ss << "    ";
  }
  if (entry.is_array)
    m_ss << "    ";
}

void JSONFormatter::print_name(const char *name)
{
  if (m_stack.empty())
    return;
  struct formatter_stack_entry_d& entry = m_stack.back();
  print_comma(entry);
  if (!entry.is_array) {
    if (m_pretty) {
      if (entry.size)
	m_ss << "  ";
      else
	m_ss << " ";
    }
    m_ss << "\"" << name << "\": ";
  }
  ++entry.size;
}
  
void JSONFormatter::open_section(const char *name, bool is_array)
{
  print_name(name);
  if (is_array)
    m_ss << '[';
  else
    m_ss << '{';

  formatter_stack_entry_d n;
  n.is_array = is_array;
  m_stack.push_back(n);
}

void JSONFormatter::open_array_section(const char *name)
{
  finish_pending_string();
  open_section(name, true);
}

void JSONFormatter::open_object_section(const char *name)
{
  finish_pending_string();
  open_section(name, false);
}

void JSONFormatter::close_section()
{
  finish_pending_string();

  struct formatter_stack_entry_d& entry = m_stack.back();
  m_ss << (entry.is_array ? ']' : '}');
  m_stack.pop_back();
}

void JSONFormatter::finish_pending_string()
{
  struct formatter_stack_entry_d& entry = m_stack.back();
  if (entry.pending_string) {
    // FIXME: escape this properly
    m_ss << "\"" << m_pending_string.str() << "\"";
    m_pending_string.str(std::string());
    entry.pending_string = false;
  }
}

void JSONFormatter::dump_unsigned(const char *name, uint64_t u)
{
  finish_pending_string();
  print_name(name);
  m_ss << u;
}

void JSONFormatter::dump_int(const char *name, int64_t s)
{
  finish_pending_string();
  print_name(name);
  m_ss << s;
}

void JSONFormatter::dump_string(const char *name, std::string s)
{
  finish_pending_string();
  print_name(name);
  m_ss << "\"" << s << "\"";
}

std::ostream& JSONFormatter::dump_stream(const char *name)
{
  finish_pending_string();
  print_name(name);
  m_stack.back().pending_string = true;
  return m_pending_string;
}

void JSONFormatter::dump_format(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);

  finish_pending_string();
  print_name(name);
  m_ss << "\"" << buf << "\"";
}

}
