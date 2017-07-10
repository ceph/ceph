// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "options.cc"


int main(int argc, char **argv)
{
  for (unsigned i=0; ceph_options[i].name.size(); ++i) {
    Option& o = ceph_options[i];
    printf("\t%s %s;\n", o.type_to_str(o.type), o.name.c_str());
  }
  return 0;
}
