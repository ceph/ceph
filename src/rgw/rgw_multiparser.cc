// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_multi.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
                                  
int main(int argc, char **argv) {
  RGWMultiXMLParser parser;

  if (!parser.init())
    exit(1);

  char buf[1024];

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, sizeof(buf), stdin);
    if (ferror(stdin)) {
      fprintf(stderr, "Read error\n");
      exit(-1);
    }
    done = feof(stdin);

    bool result = parser.parse(buf, len, done);
    if (!result) {
      cerr << "failed to parse!" << std::endl;
    }

    if (done)
      break;
  }

  exit(0);
}

