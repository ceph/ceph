// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ostream>
#include <cxxabi.h>
#include <string.h>

#include "BackTrace.h"
#include "common/version.h"
#include "common/Formatter.h"

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

namespace ceph {

std::tuple<std::string_view, std::string, uint64_t>
  BackTrace::split_backtrace_line(const char* addr_line) {
  uint64_t offset = 0;
  const char* p = addr_line;
  const char* r = p;
  while (*p!=0 && *p!='<' && *p!='(')
    p++;
  std::string_view elf_object(r, p - r);
  if (*p!=0)
    p++;
  r = p;
  while (*p!=0 && *p!='>' && *p!=')' && *p!='+')
    p++;
  char func[p - r + 1];
  memcpy(func, r, p - r);
  func[p - r] = 0;
  if (*p=='+') {
    p++;
    offset = strtoll(p, nullptr, 0);
  }

  std::string demangled;
  size_t sz = 1024; // just a guess, template names will go much wider
  char *function = (char *)malloc(sz);

  int status;
  char *ret = nullptr;

  ret = abi::__cxa_demangle(func, function, &sz, &status);
  switch (status) {
    case 0: //The demangling operation succeeded.
      demangled = ret;
      break;
    case -2: //mangled_name is not a valid name under the C++ ABI mangling rules.
      demangled = func;
      demangled += "()";
      break;
    case -1: //A memory allocation failiure occurred.
    case -3: //One of the arguments is invalid.
    default:
      break;
  }
  free(function);
  return std::make_tuple(elf_object, demangled, offset);
}

void BackTrace::print(std::ostream& out) const
{
  out << " " << pretty_version_to_str() << std::endl;
  for (size_t i = skip; i < size; i++) {
    auto [object_elf, function, offset] = split_backtrace_line(strings[i]);
    out << " " << std::dec << (i-skip+1) << ": "
        << object_elf << " (" << function << "+" << std::hex << "0x" << offset
        << ") [" << this->array[i] << "]" << std::endl;
  }
}

void BackTrace::dump(Formatter *f) const
{
  f->open_array_section("backtrace");
  for (size_t i = skip; i < size; i++) {
    //      out << " " << (i-skip+1) << ": " << strings[i] << std::endl;

    size_t sz = 1024; // just a guess, template names will go much wider
    char *function = (char *)malloc(sz);
    if (!function)
      return;
    char *begin = 0, *end = 0;

    // find the parentheses and address offset surrounding the mangled name
#ifdef __FreeBSD__
    static constexpr char OPEN = '<';
#else
    static constexpr char OPEN = '(';
#endif
    for (char *j = strings[i]; *j; ++j) {
      if (*j == OPEN)
	begin = j+1;
      else if (*j == '+')
	end = j;
    }
    if (begin && end) {
      int len = end - begin;
      char *foo = (char *)malloc(len+1);
      if (!foo) {
	free(function);
	return;
      }
      memcpy(foo, begin, len);
      foo[len] = 0;

      int status;
      char *ret = nullptr;
      // only demangle a C++ mangled name
      if (foo[0] == '_' && foo[1] == 'Z')
	ret = abi::__cxa_demangle(foo, function, &sz, &status);
      if (ret) {
	// return value may be a realloc() of the input
	function = ret;
      }
      else {
	// demangling failed, just pretend it's a C function with no args
	strncpy(function, foo, sz);
	strncat(function, "()", sz);
	function[sz-1] = 0;
      }
      f->dump_stream("frame") << OPEN << function << end;
      //fprintf(out, "    %s:%s\n", stack.strings[i], function);
      free(foo);
    } else {
      // didn't find the mangled name, just print the whole line
      //out << " " << (i-skip+1) << ": " << strings[i] << std::endl;
      f->dump_string("frame", strings[i]);
    }
    free(function);
  }
  f->close_section();
}

}
