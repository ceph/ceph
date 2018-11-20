#pragma once

#include <iostream>
#undef dout
#undef ldout
#undef dendl
#undef derr
#undef dout_impl
#define dout_impl(v)                                          \
  do {                                                                  \
      std::ostringstream _out;                                          \
      std::ostream* _dout = &_out;
#define dendl                              \
     "";                                        \
      const std::string _s = _out.str();        \
      std::ostream & objOstream = std::cout;    \
      objOstream <<_s.c_str()<<"\n";                 \
  } while (0)
#define dout(v)  dout_impl(v) dout_prefix
#define ldout(cct,v) dout(v)
#define derr dout(-1)

