// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>

#include "options.c"

const char *type_to_str(ceph_option_type_t t)
{
  switch (t) {
  case OPT_INT: return "int";
  case OPT_LONGLONG: return "long long";
  case OPT_STR: return "std::string";
  case OPT_DOUBLE: return "double";
  case OPT_FLOAT: return "float";
  case OPT_BOOL: return "bool";
  case OPT_ADDR: return "entity_addr_t";
  case OPT_U32: return "uint32_t";
  case OPT_U64: return "uint64_t";
  case OPT_UUID: return "uuid_d";
  default: return "unknown_type";
  }
};


int main(int argc, char **argv)
{
  for (unsigned i=0; ceph_options[i].name; ++i) {
    struct ceph_option *o = &ceph_options[i];
    printf("\t%s %s;\n", type_to_str(o->type), o->name);
  }
  return 0;
}
