#include <iostream>
#include <memory.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

void dout_emergency(const char * const str)
{
  std::cerr << str;
  std::cerr.flush();
}

void dout_emergency(const std::string &str)
{
  std::cerr << str;
  std::cerr.flush();
}

// adapted from
// http://stackoverflow.com/questions/2342162/stdstring-formatting-like-sprintf
// because the original req'd c++11
std::string dout_format(const std::string fmt_str, ...) {
  int final_n, n = fmt_str.size() * 2;
  std::string str;
  char *formatted;
  va_list ap;
  while(1) {
    formatted = (char*)malloc(n+1);
    va_start(ap, fmt_str);
    final_n = vsnprintf(formatted, n, fmt_str.c_str(), ap);
    va_end(ap);
    if (final_n >= 0 && final_n < n)
      break;
    n += abs(final_n - n + 1);
  }
  str = std::string(formatted);
  free(formatted);
  return str;
}
