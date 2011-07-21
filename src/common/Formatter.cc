
#define LARGE_SIZE 8192

#include <stdarg.h>
#include <stdio.h>
#include <inttypes.h>

#include <iostream>

#include "assert.h"
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
  assert(m_stack.empty());
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
  if (m_pretty && entry.is_array)
    m_ss << "    ";
}

void JSONFormatter::print_quoted_string(const char *s)
{
  m_ss << '\"';
  while (*s) {
    if (*s == '\\')
      m_ss << "\\";
    else if (*s == '\"')
      m_ss << "\\\"";
    else if (*s < 32) {
      // do a few common control characters
      switch (*s) {
      case '\n':
	m_ss << "\\n";
	break;
      case '\r':
	m_ss << "\\r";
	break;
      case '\t':
	m_ss << "\\t";
	break;
      default:
	{
	  // otherwise...
	  char s[10];
	  sprintf(s, "\\u%04x", (int)*s);
	}
      }
    } else
      m_ss << *s;
    s++;
  }
  m_ss << '\"';
}

void JSONFormatter::print_name(const char *name)
{
  finish_pending_string();
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
    print_quoted_string(name);
    if (m_pretty)
      m_ss << ": ";
    else
      m_ss << ':';
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
  open_section(name, true);
}

void JSONFormatter::open_object_section(const char *name)
{
  open_section(name, false);
}

void JSONFormatter::close_section()
{
  assert(!m_stack.empty());
  finish_pending_string();

  struct formatter_stack_entry_d& entry = m_stack.back();
  m_ss << (entry.is_array ? ']' : '}');
  m_stack.pop_back();
}

void JSONFormatter::finish_pending_string()
{
  if (m_is_pending_string) {
    print_quoted_string(m_pending_string.str().c_str());
    m_pending_string.str(std::string());
    m_is_pending_string = false;
  }
}

void JSONFormatter::dump_unsigned(const char *name, uint64_t u)
{
  print_name(name);
  m_ss << u;
}

void JSONFormatter::dump_int(const char *name, int64_t s)
{
  print_name(name);
  m_ss << s;
}

void JSONFormatter::dump_float(const char *name, double d)
{
  char foo[30];
  snprintf(foo, sizeof(foo), "%lf", d);
  dump_string(name, foo);
}

void JSONFormatter::dump_string(const char *name, std::string s)
{
  print_name(name);
  print_quoted_string(s.c_str());
}

std::ostream& JSONFormatter::dump_stream(const char *name)
{
  print_name(name);
  m_is_pending_string = true;
  return m_pending_string;
}

void JSONFormatter::dump_format(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);

  print_name(name);
  print_quoted_string(buf);
}

}
