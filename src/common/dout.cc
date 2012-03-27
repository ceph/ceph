
#include <iostream>

void dout_emergency(const char * const str)
{
  std::cerr << str << std::endl;
}

void dout_emergency(const std::string &str)
{
  std::cerr << str << std::endl;
}
